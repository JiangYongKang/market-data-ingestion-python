"""Monotonic checkpointing and guarded replay positioning.

The global checkpoint offset equals the number of accepted events durably
stored. It moves only forward. ``require_forward`` is the replay guard:

* ``requested > current``  -> allowed (replay may resume ahead after the
  matching journal data has been provided);
* ``requested <= current`` -> rejected with ``CHECKPOINT_REGRESSED`` and no
  state is altered. Replaying "from the middle" or re-emitting is never done
  implicitly; a rewind requires a fresh engine/state directory.

Per-source ``(source, seq)`` positions are tracked too: a redelivery whose
sequence number regressed relative to what was already accepted for that
source is rejected with ``SEQ_REGRESSED`` (ordinary same-id duplicates are
still handled as idempotent no-ops in the dedupe index first).
"""
from __future__ import annotations

import time
from typing import Any

from .errors import CheckpointError, RejectReason, SequenceError
from .models import Checkpoint


class CheckpointManager:
    def __init__(self) -> None:
        self._offset = 0
        self._source_seq: dict[str, int] = {}

    @property
    def offset(self) -> int:
        return self._offset

    def current(self) -> Checkpoint:
        return Checkpoint(offset=self._offset, created_at_ms=int(time.time() * 1000))

    def advance(self, n: int) -> Checkpoint:
        if n <= 0:
            raise ValueError("advance() requires a positive increment")
        self._offset += n
        return self.current()

    def note_source_seq(self, source: str, seq: int) -> None:
        prev = self._source_seq.get(source)
        if prev is not None and seq < prev:
            raise SequenceError(
                f"source {source!r} seq regressed: {seq} < {prev}",
            )
        if prev is None or seq > prev:
            self._source_seq[source] = seq

    def require_forward(self, requested_offset: int) -> None:
        if requested_offset < 0:
            raise CheckpointError(
                f"checkpoint offset must be >= 0, got {requested_offset}",
                reason=RejectReason.SCHEMA_INVALID_VALUE,
            )
        if requested_offset <= self._offset:
            raise CheckpointError(
                f"refusing checkpoint rewind: requested={requested_offset} "
                f"current={self._offset}; checkpoints are monotonic and "
                f"replay never resumes at or behind the committed position",
                reason=RejectReason.CHECKPOINT_REGRESSED,
            )

    def snapshot(self) -> dict[str, Any]:
        return {"offset": self._offset, "source_seq": dict(self._source_seq)}

    def restore(self, state: dict[str, Any], *, force: bool = False) -> None:
        """Restore positions.

        ``force=False`` (external snapshot load) refuses an older offset.
        ``force=True`` is reserved for the engine's atomic rollback path,
        which must be able to return to the pre-batch position.
        """
        restored = int(state.get("offset", 0))
        if not force and restored < self._offset:
            raise CheckpointError(
                f"refusing to restore older checkpoint: {restored} < "
                f"{self._offset}",
                reason=RejectReason.CHECKPOINT_REGRESSED,
            )
        self._offset = restored
        self._source_seq = dict(state.get("source_seq", {}))

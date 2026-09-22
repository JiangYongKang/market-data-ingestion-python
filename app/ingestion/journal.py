"""Durable, replayable event journal plus atomic state snapshots.

On-disk layout under ``state_dir``:

* ``events.log`` -- append-only journal. A batch is one length-framed
  pickle record ``[u64 big-endian length][payload]`` where payload is
  ``{"events": [Event, ...]}``. The file is fsync'ed before the batch is
  acknowledged; a torn tail record from a crash is truncated on open.
* ``state.snapshot`` -- the complete engine state
  (dedupe index, windows, watermarks, quarantine, checkpoints) atomically
  replaced via ``write tmp -> fsync -> rename -> fsync dir``. Snapshot
  records the journal offset it covers; recovery replays only the events
  beyond it (the journal alone is also sufficient: replay from 0 is
  idempotent).

No external broker is involved: a local directory is the entire durability
mechanism.
"""
from __future__ import annotations

import logging
import os
import pickle
import struct
from dataclasses import dataclass
from typing import Any, BinaryIO

from .models import Event

logger = logging.getLogger("ingestion.journal")

JOURNAL_NAME = "events.log"
SNAPSHOT_NAME = "state.snapshot"
_LEN = struct.Struct(">Q")


@dataclass
class JournalRecord:
    offset: int
    event: Event


def _fsync_dir(path: str) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class DurableStore:
    def __init__(self, state_dir: str, *, fsync: bool = True) -> None:
        self.state_dir = state_dir
        self.fsync = fsync
        self._fh: BinaryIO | None = None
        self._next_offset = 0

    @property
    def journal_path(self) -> str:
        return os.path.join(self.state_dir, JOURNAL_NAME)

    @property
    def snapshot_path(self) -> str:
        return os.path.join(self.state_dir, SNAPSHOT_NAME)

    # ------------------------------------------------------------------ open
    def open(self) -> int:
        os.makedirs(self.state_dir, exist_ok=True)
        self._fh = open(self.journal_path, "a+b")
        self._fh.seek(0, os.SEEK_END)
        end = self._fh.tell()
        # Validate/truncate a possibly-torn tail record (crash mid-append).
        valid_end, count = self._valid_prefix_end()
        if valid_end != end:
            logger.warning(
                "journal truncating torn tail: %s -> %s", end, valid_end
            )
            self._fh.seek(valid_end)
            self._fh.truncate()
            self._fh.flush()
            if self.fsync:
                os.fsync(self._fh.fileno())
        self._next_offset = count
        return self._next_offset

    def close(self) -> None:
        if self._fh is not None:
            self._fh.flush()
            if self.fsync:
                try:
                    os.fsync(self._fh.fileno())
                except OSError:
                    pass
            self._fh.close()
            self._fh = None

    # ----------------------------------------------------------------- write
    def append_events(self, events: list[Event]) -> None:
        if self._fh is None:
            raise RuntimeError("DurableStore is not open")
        if not events:
            return
        blob = pickle.dumps({"events": events}, protocol=pickle.HIGHEST_PROTOCOL)
        self._fh.seek(0, os.SEEK_END)
        self._fh.write(_LEN.pack(len(blob)))
        self._fh.write(blob)
        self._fh.flush()
        if self.fsync:
            os.fsync(self._fh.fileno())
        self._next_offset += len(events)

    def write_snapshot(self, state: dict[str, Any], offset: int) -> None:
        payload = pickle.dumps(
            {"offset": offset, "state": state}, protocol=pickle.HIGHEST_PROTOCOL
        )
        tmp = self.snapshot_path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(payload)
            f.flush()
            if self.fsync:
                os.fsync(f.fileno())
        os.replace(tmp, self.snapshot_path)
        if self.fsync:
            _fsync_dir(self.state_dir)

    # ------------------------------------------------------------------ read
    def _iter_records(self) -> list[list[Event]]:
        path = self.journal_path
        if not os.path.exists(path):
            return []
        batches: list[list[Event]] = []
        with open(path, "rb") as f:
            data = f.read()
        pos = 0
        while pos < len(data):
            if len(data) - pos < _LEN.size:
                break  # torn length header
            (length,) = _LEN.unpack_from(data, pos)
            start = pos + _LEN.size
            if start + length > len(data):
                break  # torn payload
            try:
                rec = pickle.loads(data[start : start + length])
                batches.append(list(rec["events"]))
            except Exception:  # corrupt record: treat as torn tail
                break
            pos = start + length
        return batches

    def _valid_prefix_end(self) -> tuple[int, int]:
        """Return (valid byte length, event count) prefix of the journal."""
        path = self.journal_path
        if not os.path.exists(path):
            return 0, 0
        with open(path, "rb") as f:
            data = f.read()
        pos, count = 0, 0
        while pos < len(data):
            if len(data) - pos < _LEN.size:
                break
            (length,) = _LEN.unpack_from(data, pos)
            start = pos + _LEN.size
            if start + length > len(data):
                break
            try:
                rec = pickle.loads(data[start : start + length])
                count += len(rec["events"])
            except Exception:
                break
            pos = start + length
        return pos, count

    def read_all(self) -> list[Event]:
        events: list[Event] = []
        for batch in self._iter_records():
            events.extend(batch)
        return events

    def read_snapshot(self) -> tuple[dict[str, Any], int] | None:
        if not os.path.exists(self.snapshot_path):
            return None
        with open(self.snapshot_path, "rb") as f:
            rec = pickle.load(f)
        return rec["state"], int(rec["offset"])

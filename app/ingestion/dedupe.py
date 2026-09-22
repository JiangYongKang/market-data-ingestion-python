"""Stable-idempotency index: identical redelivery vs conflicting redelivery.

The index maps ``event_id`` -> content fingerprint (a tuple of the event's
business fields). A repeated id is classified as:

* ``DUPLICATE_IDENTICAL`` -- fingerprint matches: idempotent no-op;
* ``DUPLICATE_CONFLICT``  -- fingerprint differs: hard reject, never counted.

Entries are retained for the lifetime of the store so the idempotency
guarantee never silently expires. Memory is bounded by ``max_entries``;
hitting the cap raises :class:`DedupeCapacityError` (surfaced by the engine
as a backpressure reject) rather than evicting and risking a double count.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any

from .errors import BackpressureError
from .models import Event

logger = logging.getLogger("ingestion.dedupe")

# Rough resident-size estimate of one dict entry + key + fingerprint tuple.
_ENTRY_ESTIMATED_BYTES = 256


class DedupeStatus(str, Enum):
    NEW = "NEW"
    DUPLICATE_IDENTICAL = "DUPLICATE_IDENTICAL"
    DUPLICATE_CONFLICT = "DUPLICATE_CONFLICT"


class DedupeCapacityError(BackpressureError):
    """Raised when remembering events would exceed the configured cap."""

    def __init__(self, used: int, cap: int, requested: int) -> None:
        super().__init__(
            f"dedupe index full: used={used} cap={cap} requested={requested}",
        )
        self.used = used
        self.cap = cap
        self.requested = requested


@dataclass(frozen=True, slots=True)
class DedupeDecision:
    status: DedupeStatus
    detail: str = ""
    existing_fingerprint: tuple[Any, ...] | None = None


class DedupeIndex:
    def __init__(self, max_entries: int) -> None:
        if max_entries <= 0:
            raise ValueError("max_entries must be positive")
        self.max_entries = max_entries
        self._items: dict[str, tuple[Any, ...]] = {}

    def check(self, event: Event) -> DedupeDecision:
        existing = self._items.get(event.event_id)
        if existing is None:
            return DedupeDecision(DedupeStatus.NEW)
        fp = event.content_fingerprint()
        if existing == fp:
            logger.info(
                "dedupe event_id=%s decision=DUPLICATE_IDENTICAL "
                "basis=same-fingerprint",
                event.event_id,
            )
            return DedupeDecision(
                DedupeStatus.DUPLICATE_IDENTICAL,
                detail="event already ingested with identical content",
                existing_fingerprint=existing,
            )
        logger.info(
            "dedupe event_id=%s decision=DUPLICATE_CONFLICT "
            "basis=id-reseen fingerprint-stored=%s fingerprint-new=%s",
            event.event_id, existing, fp,
        )
        return DedupeDecision(
            DedupeStatus.DUPLICATE_CONFLICT,
            detail=(
                f"event_id {event.event_id!r} redelivered with different "
                f"content; stored={existing!r} new={fp!r}"
            ),
            existing_fingerprint=existing,
        )

    def remember(self, events: list[Event]) -> None:
        """Commit-phase insertion.

        Capacity is checked against the *whole* batch first so a failure
        never leaves a partial index. Callers must only pass events classified
        NEW; duplicate ids inside the batch are rejected as conflicts.
        """
        additions: dict[str, tuple[Any, ...]] = {}
        for event in events:
            if event.event_id in self._items or event.event_id in additions:
                # Should never happen (engine filters), but stay atomic.
                raise DedupeCapacityError(
                    len(self._items), self.max_entries, len(events)
                )
            additions[event.event_id] = event.content_fingerprint()

        if len(self._items) + len(additions) > self.max_entries:
            raise DedupeCapacityError(
                len(self._items), self.max_entries, len(additions)
            )
        self._items.update(additions)

    def __len__(self) -> int:
        return len(self._items)

    def memory_bytes(self) -> int:
        return len(self._items) * _ENTRY_ESTIMATED_BYTES

    def snapshot(self) -> dict[str, Any]:
        # JSON keys are strings; fingerprint elements are JSON-simple types.
        return {"items": [list(it) for it in self._items.items()]}

    def restore(self, state: dict[str, Any]) -> None:
        self._items = {k: tuple(v) for k, v in state.get("items", [])}

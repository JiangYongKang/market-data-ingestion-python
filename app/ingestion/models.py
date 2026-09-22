"""Core domain models: events, window results, quarantine, checkpoints."""
from __future__ import annotations

import time
from dataclasses import dataclass, field, replace
from typing import Any

from .errors import RejectReason


@dataclass(frozen=True, slots=True)
class Event:
    """One normalized market-data tick.

    Identity is ``(source, seq, event_id)`` semantics:

    * ``event_id`` is the stable business identifier used for idempotency;
    * ``(source, seq)`` is the per-source monotonic arrival ordering token;
    * ``event_time_ms`` drives windowing/watermark; ``ingest_time_ms`` is the
      wall-clock arrival time used for observability only.
    """

    event_id: str
    source: str
    seq: int
    symbol: str
    event_time_ms: int
    price: float
    quantity: float
    schema_version: str = "v1"
    # v2+ fields. Deterministic defaults for missing/older versions:
    notional_ccy: str | None = None      # v2: null => "USD" at aggregation
    venue: str | None = None             # v2: null => "UNKNOWN"
    ingest_time_ms: int = field(default_factory=lambda: int(time.time() * 1000))

    def content_fingerprint(self) -> tuple[Any, ...]:
        """Tuple used to detect *conflicting* redeliveries of the same id."""
        return (
            self.source,
            self.seq,
            self.symbol,
            self.event_time_ms,
            round(self.price, 9),
            round(self.quantity, 9),
            self.schema_version,
            self.notional_ccy,
            self.venue,
        )


@dataclass(frozen=True, slots=True)
class WindowResult:
    """Published (or provisional) feature values for one tumbling window."""

    symbol: str
    window_start_ms: int
    window_end_ms: int
    event_count: int
    total_quantity: float
    vwap: float                     # sum(price*qty)/sum(qty)
    mean_price: float
    price_stddev: float             # sample stddev (0.0 for n<2)
    volatility_bps: float           # price_stddev / mean_price * 1e4
    min_price: float
    max_price: float
    first_event_time_ms: int
    last_event_time_ms: int
    event_ids: tuple[str, ...]     # explainability / reproducibility
    published: bool                 # frozen once watermark passes end
    recomputed: bool = False        # True if a late-but-allowed event changed it


@dataclass(frozen=True, slots=True)
class IngestRecord:
    """Per-event disposition returned in a write response."""

    event_id: str
    accepted: bool
    reason: RejectReason | None = None       # None on clean accept
    detail: str = ""
    window_start_ms: int | None = None
    watermark_ms: int | None = None
    quarantine_id: int | None = None


@dataclass(frozen=True, slots=True)
class BatchResult:
    accepted: int
    duplicates: int            # identical redeliveries (idempotent no-op)
    conflicts: int             # same id, different content
    quarantined: int
    rejected: int              # schema/seq/other hard rejects
    records: tuple[IngestRecord, ...]
    checkpoint: int | None
    committed: bool            # False => whole batch rolled back
    duration_us: int


@dataclass(frozen=True, slots=True)
class QuarantineEntry:
    """An event that could not be applied to a live window."""

    quarantine_id: int
    event_id: str
    source: str
    seq: int
    symbol: str
    event_time_ms: int
    watermark_ms: int
    window_end_ms: int | None
    reason: RejectReason
    detail: str
    payload: dict[str, Any]
    ingest_time_ms: int


@dataclass(frozen=True, slots=True)
class Checkpoint:
    """Monotonic durability position."""

    offset: int                 # number of accepted journaled events
    created_at_ms: int

    def is_after(self, other: Checkpoint) -> bool:
        return self.offset > other.offset


def make_event(**kw: Any) -> Event:
    """Convenience constructor with defaults for tests."""
    kw.setdefault("event_id", f"evt-{kw.get('seq', 0)}")
    kw.setdefault("source", "default")
    kw.setdefault("symbol", "BTCUSD")
    kw.setdefault("event_time_ms", int(kw.get("seq", 0)))
    kw.setdefault("price", 100.0)
    kw.setdefault("quantity", 1.0)
    return Event(**kw)


def clone_event(event: Event, **changes: Any) -> Event:
    return replace(event, **changes)

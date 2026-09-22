"""Configuration for the ingestion engine.

All knobs that affect correctness or resource usage live in
:class:`EngineConfig` so behaviour is fully reproducible from one value.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum

from .errors import RejectReason  # noqa: F401  (re-exported for callers)


class BackpressurePolicy(str, Enum):
    """What to do when an in-flight or queued write exceeds the cap."""

    DELAY = "DELAY"   # block until capacity is available (bounded wait)
    REJECT = "REJECT"  # fail fast with BACKPRESSURE_REJECTED


class UnknownFieldPolicy(str, Enum):
    """How payload fields not declared by the event schema are treated."""

    IGNORE = "IGNORE"  # stripped, counted in metrics (forward compatibility)
    REJECT = "REJECT"  # rejected with SCHEMA_UNKNOWN_FIELD


@dataclass(frozen=True)
class EngineConfig:
    # ---- windows / watermark -------------------------------------------------
    window_size_ms: int = 1_000
    # Watermark = max observed event time - allowed_lateness_ms.
    # Events behind the watermark are quarantined; events inside the
    # allowed-lateness gap still trigger a correct window recomputation.
    allowed_lateness_ms: int = 500
    # Watermark only advances; idle symbols do not advance it automatically.

    # ---- schema --------------------------------------------------------------
    supported_versions: tuple[str, ...] = ("v1", "v2")
    unknown_field_policy: UnknownFieldPolicy = UnknownFieldPolicy.IGNORE

    # ---- backpressure / resources -------------------------------------------
    max_inflight_batches: int = 64
    backpressure_policy: BackpressurePolicy = BackpressurePolicy.DELAY
    backpressure_max_wait_ms: int = 5_000
    # Hard cap on unique event ids retained for idempotency.
    max_dedupe_entries: int = 1_000_000
    # How many quarantined events are retained per symbol for queries.
    max_quarantine_per_symbol: int = 1_000

    # ---- durability ----------------------------------------------------------
    state_dir: str | None = None  # None => ephemeral, in-memory only
    fsync: bool = True
    # Boot-time replay position: on start(), rebuild deterministically from
    # the journal prefix [0, start_offset) instead of the latest snapshot.
    # Only meaningful for a *fresh* engine process; a live engine never
    # rewinds via replay_from().
    start_offset: int | None = None

    # ---- benchmark gates (observed via /metrics; enforced by benchmark test)
    bench_max_write_us: int = 20_000      # per-batch p99 budget
    bench_max_dedupe_kb: int = 200_000    # ~200 MB resident dedupe budget
    bench_scale_events: int = 20_000

    # ---- logging -------------------------------------------------------------
    log_decisions: bool = True

    @staticmethod
    def default_state_dir() -> str:
        d = os.path.join(os.getcwd(), ".state")
        return d


@dataclass
class RuntimeOverrides:
    """Mutable per-instance overrides (reserved for tests / admin API)."""

    extra: dict[str, str] = field(default_factory=dict)

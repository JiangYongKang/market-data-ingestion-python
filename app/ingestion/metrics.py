"""In-process observability: counters, latency percentiles, memory estimates.

Exposed via ``GET /metrics`` and used by the benchmark test to assert the
configured time/memory budgets. No external monitoring system is needed.
Timing lists are bounded (ring of the most recent ``_MAX_SAMPLES`` values)
so observability itself never grows without bound.
"""
from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field

_MAX_SAMPLES = 10_000


@dataclass(slots=True)
class Metrics:
    accepted: int = 0
    duplicates_identical: int = 0
    duplicates_conflict: int = 0
    quarantined: int = 0
    rejected: int = 0
    schema_unknown_fields: int = 0
    schema_deprecated_fields: int = 0
    published_windows: int = 0
    recomputed_windows: int = 0
    backpressure_delayed: int = 0
    backpressure_rejected: int = 0
    rollbacks: int = 0
    batches: int = 0
    quarantine_evicted: int = 0
    _batch_us: deque[int] = field(default_factory=lambda: deque(maxlen=_MAX_SAMPLES))
    _write_us: deque[int] = field(default_factory=lambda: deque(maxlen=_MAX_SAMPLES))
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record_batch(self, duration_us: int) -> None:
        with self._lock:
            self.batches += 1
            self._batch_us.append(max(0, int(duration_us)))

    def record_write(self, duration_us: int) -> None:
        with self._lock:
            self._write_us.append(max(0, int(duration_us)))

    _COUNTERS = (
        "accepted", "duplicates_identical", "duplicates_conflict",
        "quarantined", "rejected", "schema_unknown_fields",
        "schema_deprecated_fields", "published_windows", "recomputed_windows",
        "backpressure_delayed", "backpressure_rejected", "rollbacks",
        "batches", "quarantine_evicted",
    )

    def counters_snapshot(self) -> dict[str, int]:
        with self._lock:
            return {name: getattr(self, name) for name in self._COUNTERS}

    def counters_restore(self, snap: dict[str, int]) -> None:
        with self._lock:
            for name, value in snap.items():
                setattr(self, name, value)

    @staticmethod
    def percentile(values: list[int] | deque[int], p: float) -> float:
        if not values:
            return 0.0
        xs = sorted(values)
        if len(xs) == 1:
            return float(xs[0])
        k = (len(xs) - 1) * p
        lo = int(k)
        hi = min(lo + 1, len(xs) - 1)
        frac = k - lo
        return xs[lo] * (1 - frac) + xs[hi] * frac

    def snapshot(self, dedupe_entries: int, dedupe_bytes: int,
                 inflight: int) -> dict[str, int | float]:
        with self._lock:
            batch = list(self._batch_us)
            write = list(self._write_us)
            return {
                "accepted": self.accepted,
                "duplicates_identical": self.duplicates_identical,
                "duplicates_conflict": self.duplicates_conflict,
                "quarantined": self.quarantined,
                "rejected": self.rejected,
                "schema_unknown_fields": self.schema_unknown_fields,
                "schema_deprecated_fields": self.schema_deprecated_fields,
                "published_windows": self.published_windows,
                "recomputed_windows": self.recomputed_windows,
                "backpressure_delayed": self.backpressure_delayed,
                "backpressure_rejected": self.backpressure_rejected,
                "rollbacks": self.rollbacks,
                "batches": self.batches,
                "quarantine_evicted": self.quarantine_evicted,
                "inflight_batches": inflight,
                "dedupe_entries": dedupe_entries,
                "dedupe_memory_bytes": dedupe_bytes,
                "batch_us_p50": round(self.percentile(batch, 0.50), 1),
                "batch_us_p99": round(self.percentile(batch, 0.99), 1),
                "batch_us_max": max(batch, default=0),
                "write_us_p50": round(self.percentile(write, 0.50), 1),
                "write_us_p99": round(self.percentile(write, 0.99), 1),
                "write_us_max": max(write, default=0),
            }

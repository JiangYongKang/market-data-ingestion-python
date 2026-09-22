"""观测信息：成功/重复/冲突/隔离/拒绝计数、积压与耗时分布。

耗时以"每条事件纳秒"记录（批耗时 / 批大小），保留 p50/p99，
用于在本地基准下验证 ``ingest_time_budget_ms``；
``estimated_state_bytes`` 给出状态内存的保守估计，供资源上限核对。
所有数值仅用于观测，不影响任何业务判定。
"""
from __future__ import annotations

from dataclasses import dataclass, field


def _percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    k = (len(sorted_vals) - 1) * pct
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = k - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


@dataclass(slots=True)
class Metrics:
    accepted: int = 0
    duplicates: int = 0
    conflicts: int = 0
    quarantined: int = 0
    rejected: int = 0
    deprecated_seen: int = 0
    published_windows: int = 0
    rollback_rejected: int = 0
    backpressure_rejected: int = 0
    stale_sequence: int = 0
    batches: int = 0
    events_seen: int = 0
    pending_events: int = 0
    max_pending_events: int = 0
    estimated_state_bytes: int = 0
    total_ingest_ns: int = 0
    max_batch_ns: int = 0
    _batch_ns_per_event: list[float] = field(default_factory=list)

    def record_batch(self, n: int, elapsed_ns: int) -> None:
        self.batches += 1
        self.events_seen += n
        self.total_ingest_ns += elapsed_ns
        if elapsed_ns > self.max_batch_ns:
            self.max_batch_ns = elapsed_ns
        if n > 0:
            self._batch_ns_per_event.append(elapsed_ns / n)

    def reset_timing(self) -> None:
        self._batch_ns_per_event.clear()
        self.total_ingest_ns = 0
        self.max_batch_ns = 0

    def _timing(self) -> tuple[float, float, float]:
        vals = sorted(self._batch_ns_per_event)
        return (_percentile(vals, 0.50), _percentile(vals, 0.99),
                self.total_ingest_ns / self.events_seen if self.events_seen else 0.0)

    def snapshot(self) -> dict:
        p50, p99, avg = self._timing()
        return {
            "accepted": self.accepted,
            "duplicates": self.duplicates,
            "conflicts": self.conflicts,
            "quarantined": self.quarantined,
            "rejected": self.rejected,
            "deprecated_seen": self.deprecated_seen,
            "published_windows": self.published_windows,
            "rollback_rejected": self.rollback_rejected,
            "backpressure_rejected": self.backpressure_rejected,
            "stale_sequence": self.stale_sequence,
            "batches": self.batches,
            "events_seen": self.events_seen,
            "pending_events": self.pending_events,
            "max_pending_events": self.max_pending_events,
            "estimated_state_bytes": self.estimated_state_bytes,
            "ingest": {
                "total_ms": self.total_ingest_ns / 1e6,
                "max_batch_ms": self.max_batch_ns / 1e6,
                "avg_ns_per_event": avg,
                "p50_ns_per_event": p50,
                "p99_ns_per_event": p99,
            },
        }

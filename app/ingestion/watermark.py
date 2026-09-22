"""Per-symbol event-time watermarks.

Rule (deterministic, independent of arrival wall-clock):

    watermark(symbol) = max(event_time_ms seen for symbol)
                        - allowed_lateness_ms

A watermark is **monotonic**: observing an older event time never retreats
it. An event is considered "late beyond allowance" iff

    event.event_time_ms < watermark(symbol)   (at the moment it is processed)

Events in the half-open lateness gap
``[watermark, watermark + allowed_lateness_ms)`` are NOT late: they are
out-of-order but still inside the configured allowance and recompute the
affected window before it is published.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("ingestion.watermark")

NEG_INF = -(10**18)


class WatermarkTracker:
    def __init__(self, allowed_lateness_ms: int) -> None:
        if allowed_lateness_ms < 0:
            raise ValueError("allowed_lateness_ms must be >= 0")
        self.allowed_lateness_ms = allowed_lateness_ms
        self._max_seen: dict[str, int] = {}
        self._watermark: dict[str, int] = {}

    def observe(self, symbol: str, event_time_ms: int) -> int:
        prev_max = self._max_seen.get(symbol, NEG_INF)
        if event_time_ms > prev_max:
            self._max_seen[symbol] = event_time_ms
            wm = event_time_ms - self.allowed_lateness_ms
            prev_wm = self._watermark.get(symbol, NEG_INF)
            if wm > prev_wm:
                self._watermark[symbol] = wm
                logger.info(
                    "watermark symbol=%s advanced %s -> %s basis=max_event_time=%s lateness=%s",
                    symbol, prev_wm, wm, event_time_ms,
                    self.allowed_lateness_ms,
                )
        return self.get(symbol)

    def get(self, symbol: str) -> int:
        return self._watermark.get(symbol, NEG_INF)

    def is_behind(self, symbol: str, event_time_ms: int) -> bool:
        return event_time_ms < self.get(symbol)

    def snapshot(self) -> dict[str, Any]:
        return {
            "max_seen": dict(self._max_seen),
            "watermark": dict(self._watermark),
        }

    def restore(self, state: dict[str, Any]) -> None:
        self._max_seen = dict(state.get("max_seen", {}))
        self._watermark = dict(state.get("watermark", {}))

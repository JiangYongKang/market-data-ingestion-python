"""Bounded, queryable quarantine area for events that cannot be applied.

Nothing is silently dropped: late events whose window is already published
(or that otherwise cannot be applied) are stored here with the event id,
event time, the then-current watermark and an explicit
:class:`RejectReason`. Per-symbol capacity is bounded; oldest entries are
evicted first (FIFO) and evictions are counted by the engine metrics.
"""
from __future__ import annotations

import logging
from collections import deque
from itertools import count
from typing import TYPE_CHECKING, Any

from .errors import RejectReason
from .models import Event, QuarantineEntry

if TYPE_CHECKING:  # avoid import cycle at runtime
    from .metrics import Metrics

logger = logging.getLogger("ingestion.quarantine")


class Quarantine:
    def __init__(
        self, max_per_symbol: int, metrics: Metrics | None = None
    ) -> None:
        if max_per_symbol <= 0:
            raise ValueError("max_per_symbol must be positive")
        self.max_per_symbol = max_per_symbol
        self._metrics = metrics
        self._by_symbol: dict[str, deque[QuarantineEntry]] = {}
        self._ids = count(1)
        self.evicted = 0

    def add(self, event: Event, *, watermark_ms: int,
            window_end_ms: int | None, reason: RejectReason,
            detail: str, payload: dict[str, Any]) -> QuarantineEntry:
        entry = QuarantineEntry(
            quarantine_id=next(self._ids),
            event_id=event.event_id,
            source=event.source,
            seq=event.seq,
            symbol=event.symbol,
            event_time_ms=event.event_time_ms,
            watermark_ms=watermark_ms,
            window_end_ms=window_end_ms,
            reason=reason,
            detail=detail,
            payload=payload,
            ingest_time_ms=event.ingest_time_ms,
        )
        q = self._by_symbol.setdefault(event.symbol, deque())
        q.append(entry)
        while len(q) > self.max_per_symbol:
            q.popleft()
            self.evicted += 1
            if self._metrics is not None:
                self._metrics.quarantine_evicted += 1
        logger.info(
            "quarantine id=%s event_id=%s event_time_ms=%s watermark=%s "
            "window_end=%s reason=%s basis=%s",
            entry.quarantine_id, event.event_id, event.event_time_ms,
            watermark_ms, window_end_ms, reason.value, detail,
        )
        return entry

    def query(self, symbol: str | None = None,
              reason: RejectReason | None = None) -> list[QuarantineEntry]:
        out: list[QuarantineEntry] = []
        symbols = [symbol] if symbol is not None else sorted(self._by_symbol)
        for s in symbols:
            for e in self._by_symbol.get(s, ()):  # oldest first
                if reason is None or e.reason is reason:
                    out.append(e)
        return out

    def snapshot(self) -> dict[str, Any]:
        return {
            "max_per_symbol": self.max_per_symbol,
            "evicted": self.evicted,
            "next_id": next(self._ids),
            "symbols": {
                s: [
                    {
                        "quarantine_id": e.quarantine_id,
                        "event_id": e.event_id,
                        "source": e.source,
                        "seq": e.seq,
                        "symbol": e.symbol,
                        "event_time_ms": e.event_time_ms,
                        "watermark_ms": e.watermark_ms,
                        "window_end_ms": e.window_end_ms,
                        "reason": e.reason.value,
                        "detail": e.detail,
                        "payload": e.payload,
                        "ingest_time_ms": e.ingest_time_ms,
                    }
                    for e in q
                ]
                for s, q in sorted(self._by_symbol.items())
            },
        }

    def restore(self, state: dict[str, Any]) -> None:
        self.max_per_symbol = state.get("max_per_symbol", self.max_per_symbol)
        self.evicted = state.get("evicted", 0)
        self._by_symbol = {}
        max_id = 0
        for s, entries in state.get("symbols", {}).items():
            dq: deque[QuarantineEntry] = deque()
            for d in entries:
                max_id = max(max_id, d["quarantine_id"])
                dq.append(
                    QuarantineEntry(
                        quarantine_id=d["quarantine_id"],
                        event_id=d["event_id"],
                        source=d["source"],
                        seq=d["seq"],
                        symbol=d["symbol"],
                        event_time_ms=d["event_time_ms"],
                        watermark_ms=d["watermark_ms"],
                        window_end_ms=d["window_end_ms"],
                        reason=RejectReason(d["reason"]),
                        detail=d["detail"],
                        payload=d["payload"],
                        ingest_time_ms=d["ingest_time_ms"],
                    )
                )
            self._by_symbol[s] = dq
        self._ids = count(max_id + 1)

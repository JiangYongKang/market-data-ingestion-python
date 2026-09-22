"""Tumbling event-time windows with explainable feature aggregation.

Window assignment is deterministic::

    window_start = event_time_ms // window_size_ms * window_size_ms
    window_end   = window_start + window_size_ms

Per window the store maintains:

* ``event_count``, ``total_quantity``;
* ``sum_pq``  -- sum(price * quantity), so VWAP = sum_pq / total_quantity;
* ``sum_p`` / ``sum_p2`` -- mean and sample variance in one pass;
* ``min_price`` / ``max_price``;
* ordered ``event_ids`` for explainability / reproducibility.

Volatility is the sample standard deviation of trade prices
(``n < 2 => 0``); ``volatility_bps = stddev / mean * 1e4``.

A window becomes *published* (frozen, immutable, replay-stable) once the
symbol watermark reaches its end. Adding an event into an unpublished window
that already contained events marks it ``recomputed`` -- the engine uses
that to surface out-of-order corrections. Adding to a published window is
refused; the engine quarantines such events instead.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from .models import Event, WindowResult

logger = logging.getLogger("ingestion.windows")


@dataclass(slots=True)
class _Acc:
    symbol: str
    start: int
    end: int
    count: int = 0
    total_qty: float = 0.0
    sum_pq: float = 0.0
    sum_p: float = 0.0
    sum_p2: float = 0.0
    min_p: float = float("inf")
    max_p: float = float("-inf")
    first_t: int = 0
    last_t: int = 0
    event_ids: list[str] = field(default_factory=list)
    published: bool = False
    recomputed: bool = False

    def add(self, event: Event, *, out_of_order: bool = False) -> None:
        had_events = self.count > 0
        self.count += 1
        self.total_qty += event.quantity
        self.sum_pq += event.price * event.quantity
        self.sum_p += event.price
        self.sum_p2 += event.price * event.price
        self.min_p = min(self.min_p, event.price)
        self.max_p = max(self.max_p, event.price)
        if not had_events:
            self.first_t = event.event_time_ms
        self.last_t = max(self.last_t, event.event_time_ms)
        self.event_ids.append(event.event_id)
        # A window is "recomputed" only when an out-of-order event (event time
        # earlier than the latest already aggregated) rewrites its features.
        if had_events and out_of_order:
            self.recomputed = True

    def result(self) -> WindowResult:
        if self.count == 0:
            raise ValueError("cannot materialize an empty window")
        vwap = self.sum_pq / self.total_qty
        mean = self.sum_p / self.count
        if self.count >= 2:
            # sample variance, guarded against tiny negative round-off
            var = (self.sum_p2 - self.sum_p * self.sum_p / self.count) / (
                self.count - 1
            )
            stddev = var ** 0.5 if var > 0 else 0.0
        else:
            stddev = 0.0
        vol_bps = (stddev / mean * 1e4) if mean else 0.0
        return WindowResult(
            symbol=self.symbol,
            window_start_ms=self.start,
            window_end_ms=self.end,
            event_count=self.count,
            total_quantity=round(self.total_qty, 9),
            vwap=round(vwap, 9),
            mean_price=round(mean, 9),
            price_stddev=round(stddev, 9),
            volatility_bps=round(vol_bps, 6),
            min_price=self.min_p,
            max_price=self.max_p,
            first_event_time_ms=self.first_t,
            last_event_time_ms=self.last_t,
            event_ids=tuple(self.event_ids),
            published=self.published,
            recomputed=self.recomputed,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "start": self.start,
            "end": self.end,
            "count": self.count,
            "total_qty": self.total_qty,
            "sum_pq": self.sum_pq,
            "sum_p": self.sum_p,
            "sum_p2": self.sum_p2,
            "min_p": self.min_p,
            "max_p": self.max_p,
            "first_t": self.first_t,
            "last_t": self.last_t,
            "event_ids": list(self.event_ids),
            "published": self.published,
            "recomputed": self.recomputed,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> _Acc:
        return cls(
            symbol=d["symbol"], start=d["start"], end=d["end"],
            count=d["count"], total_qty=d["total_qty"], sum_pq=d["sum_pq"],
            sum_p=d["sum_p"], sum_p2=d["sum_p2"], min_p=d["min_p"],
            max_p=d["max_p"], first_t=d["first_t"], last_t=d["last_t"],
            event_ids=list(d["event_ids"]), published=d["published"],
            recomputed=d["recomputed"],
        )


class WindowPublishedError(RuntimeError):
    """Engine-facing signal: event targets an already published window."""


class WindowStore:
    def __init__(self, window_size_ms: int) -> None:
        if window_size_ms <= 0:
            raise ValueError("window_size_ms must be positive")
        self.window_size_ms = window_size_ms
        # symbol -> {start: acc}; insertion order groups by arrival ordering
        self._windows: dict[str, dict[int, _Acc]] = {}

    def _bounds(self, event_time_ms: int) -> tuple[int, int]:
        start = (event_time_ms // self.window_size_ms) * self.window_size_ms
        return start, start + self.window_size_ms

    def add(self, event: Event) -> WindowResult:
        start, end = self._bounds(event.event_time_ms)
        sym = self._windows.setdefault(event.symbol, {})
        acc = sym.get(start)
        if acc is not None and acc.published:
            raise WindowPublishedError(
                f"window {event.symbol}@{start} already published"
            )
        if acc is None:
            acc = _Acc(symbol=event.symbol, start=start, end=end)
            sym[start] = acc
        out_of_order = acc.count > 0 and event.event_time_ms < acc.last_t
        acc.add(event, out_of_order=out_of_order)
        logger.info(
            "window-add event_id=%s event_time_ms=%s symbol=%s "
            "window=[%s,%s) count=%s vwap_provisional=%.6f",
            event.event_id, event.event_time_ms, event.symbol,
            start, end, acc.count,
            acc.sum_pq / acc.total_qty if acc.total_qty else 0.0,
        )
        return acc.result()

    def publish_up_to(self, symbol: str, watermark_ms: int) -> list[WindowResult]:
        frozen: list[WindowResult] = []
        sym = self._windows.get(symbol)
        if not sym:
            return frozen
        # Deterministic ascending window order.
        for start in sorted(sym):
            acc = sym[start]
            if not acc.published and acc.end <= watermark_ms:
                acc.published = True
                frozen.append(acc.result())
                logger.info(
                    "window-publish symbol=%s window=[%s,%s) "
                    "basis=end<=%s<=watermark count=%s vwap=%.6f",
                    symbol, acc.start, acc.end, watermark_ms,
                    acc.count, acc.sum_pq / acc.total_qty,
                )
        return frozen

    def is_window_published(self, symbol: str, window_start_ms: int) -> bool:
        sym = self._windows.get(symbol)
        return bool(sym and window_start_ms in sym and sym[window_start_ms].published)

    def list_published(self, symbol: str | None = None) -> list[WindowResult]:
        out: list[WindowResult] = []
        symbols = [symbol] if symbol is not None else sorted(self._windows)
        for s in symbols:
            sym = self._windows.get(s, {})
            for start in sorted(sym):
                acc = sym[start]
                if acc.published:
                    out.append(acc.result())
        return out

    def list_all(self, symbol: str | None = None) -> list[WindowResult]:
        out: list[WindowResult] = []
        symbols = [symbol] if symbol is not None else sorted(self._windows)
        for s in symbols:
            sym = self._windows.get(s, {})
            for start in sorted(sym):
                out.append(sym[start].result())
        return out

    def snapshot(self) -> dict[str, Any]:
        return {
            "window_size_ms": self.window_size_ms,
            "symbols": {
                s: {str(k): a.to_dict() for k, a in sorted(sym.items())}
                for s, sym in sorted(self._windows.items())
            },
        }

    def restore(self, state: dict[str, Any]) -> None:
        self.window_size_ms = state.get("window_size_ms", self.window_size_ms)
        self._windows = {}
        for s, starts in state.get("symbols", {}).items():
            self._windows[s] = {
                int(k): _Acc.from_dict(a) for k, a in starts.items()
            }

"""固定滚动窗口聚合。

窗口划分
--------
按标的各自独立，窗口为左闭右开区间
``[w_start, w_start + window_size_ms)``，
``w_start = floor(event_time_ms / window_size_ms) * window_size_ms``。

可解释特征
----------
* ``vwap`` —— 成交量加权均价 ``Σ(p*q)/Σq``；
* ``volatility`` —— 成交量加权的成交价总体标准差
  ``sqrt(Σ q*(p-vwap)^2 / Σq)``。

确定性与乱序修正
----------------
未发布窗口内的事件（含窗口内迟到）直接并入，发布时统一按
``(event_time_ms, event_id)`` 排序后用加权 Welford 算法计算，
因此**结果只取决于事件集合，与到达顺序无关**，重放可逐位复现。
窗口一经发布即不可变；再向已发布窗口写入会被拒绝（由上层转入隔离区）。
"""
from __future__ import annotations

import math

from .models import Event, WindowFeature

#: 未见任何参与渠道时的发布水位：任何窗口都不可发布。
NEG_INF = -(1 << 62)


def _welford_weighted(points: list[tuple[float, float]]) -> tuple[float, float, float]:
    """points 为 (quantity, price) 的确定性序列；返回 (Σq, vwap, volatility)。"""
    total_q = 0.0
    mean = 0.0
    m2 = 0.0
    for w, x in points:
        total_q += w
        delta = x - mean
        mean += delta * (w / total_q)
        m2 += w * delta * (x - mean)
    if total_q <= 0:
        return 0.0, None, None
    variance = m2 / total_q
    if variance < 0.0:  # 浮点误差保护
        variance = 0.0
    return total_q, mean, math.sqrt(variance)


class WindowAggregator:
    def __init__(self, window_size_ms: int) -> None:
        if window_size_ms <= 0:
            raise ValueError("window_size_ms 必须为正数")
        self._size = window_size_ms
        # (symbol, w_start) -> {event_id: Event}
        self._open: dict[tuple[str, int], dict[str, Event]] = {}
        # symbol -> {w_start: WindowFeature}
        self._published: dict[str, dict[int, WindowFeature]] = {}

    def window_start_for(self, event_time_ms: int) -> int:
        return (event_time_ms // self._size) * self._size

    def is_published(self, symbol: str, w_start: int) -> bool:
        return w_start in self._published.get(symbol, ())

    def add(self, event: Event) -> bool:
        """并入未发布窗口。返回 True；窗口已发布则返回 False（不改任何状态）。"""
        w0 = self.window_start_for(event.event_time_ms)
        if self.is_published(event.symbol, w0):
            return False
        bucket = self._open.setdefault((event.symbol, w0), {})
        bucket[event.event_id] = event  # 去重已在上层保证；键防御重复计数
        return True

    def remove_open_event(self, symbol: str, w_start: int,
                          event_id: str) -> Event | None:
        """从未发布窗口移除一个事件（多来源高优先级主报替换时用）。

        仅允许在窗口发布前操作；窗口已发布或事件不存在时返回 None，
        绝不改动不可变的已发布结果。
        """
        if self.is_published(symbol, w_start):
            return None
        bucket = self._open.get((symbol, w_start))
        if not bucket:
            return None
        ev = bucket.pop(event_id, None)
        if not bucket:
            self._open.pop((symbol, w_start), None)
        return ev

    def publish_due(self, watermark_ms: int) -> list[WindowFeature]:
        """发布所有右边界已越过水位的窗口，确定性顺序 (symbol, w_start)。

        判定：不存在还能落入该窗口的未到达事件，即
        ``window_end_ms - 1 <= watermark_ms``（可接受事件须
        ``event_time > watermark``，故其时间必 >= window_end）。
        """
        due = sorted(
            (key for key, bucket in self._open.items()
             if key[1] + self._size - 1 <= watermark_ms and bucket),
        )
        out: list[WindowFeature] = []
        for symbol, w0 in due:
            bucket = self._open.pop((symbol, w0))
            events = sorted(bucket.values(), key=lambda e: (e.event_time_ms, e.event_id))
            points = [(e.quantity, e.price) for e in events]
            total_q, vwap, vol = _welford_weighted(points)
            feat = WindowFeature(
                symbol=symbol,
                window_start_ms=w0,
                window_end_ms=w0 + self._size,
                count=len(events),
                total_quantity=total_q,
                vwap=vwap,
                volatility=vol,
                event_ids=tuple(e.event_id for e in events),
                published=True,
            )
            self._published.setdefault(symbol, {})[w0] = feat
            out.append(feat)
        return out

    def publish_due_by_symbol(self, watermark_by_symbol: dict[str, int]) -> list[WindowFeature]:
        """多来源模式：每个标的按各自发布水位决定窗口可否发布。

        ``watermark_by_symbol`` 为 symbol -> 该标的参与渠道水位最小值；
        未列入的标的（无参与渠道）不发布。发布顺序仍按 (symbol, w_start)
        确定，保证落盘与可复现。
        """
        due = sorted(
            (key for key, bucket in self._open.items()
             if key[1] + self._size - 1 <= watermark_by_symbol.get(key[0], NEG_INF)
             and bucket),
        )
        out: list[WindowFeature] = []
        for symbol, w0 in due:
            bucket = self._open.pop((symbol, w0))
            events = sorted(bucket.values(), key=lambda e: (e.event_time_ms, e.event_id))
            points = [(e.quantity, e.price) for e in events]
            total_q, vwap, vol = _welford_weighted(points)
            feat = WindowFeature(
                symbol=symbol, window_start_ms=w0,
                window_end_ms=w0 + self._size, count=len(events),
                total_quantity=total_q, vwap=vwap, volatility=vol,
                event_ids=tuple(e.event_id for e in events), published=True,
            )
            self._published.setdefault(symbol, {})[w0] = feat
            out.append(feat)
        return out

    def provisional(self, symbol: str, w_start: int) -> WindowFeature | None:
        """读取尚未发布窗口的当前重算结果（同样按确定性顺序计算）。"""
        bucket = self._open.get((symbol, w_start))
        if not bucket:
            return None
        events = sorted(bucket.values(), key=lambda e: (e.event_time_ms, e.event_id))
        total_q, vwap, vol = _welford_weighted([(e.quantity, e.price) for e in events])
        return WindowFeature(
            symbol=symbol, window_start_ms=w_start,
            window_end_ms=w_start + self._size,
            count=len(events), total_quantity=total_q,
            vwap=vwap, volatility=vol,
            event_ids=tuple(e.event_id for e in events), published=False,
        )

    def open_window_keys(self) -> list[tuple[str, int]]:
        return sorted(self._open.keys())

    def all_symbols(self) -> list[str]:
        syms = {sym for sym, _ in self._open} | set(self._published)
        return sorted(syms)

    def pending_event_count(self) -> int:
        return sum(len(b) for b in self._open.values())

    def get_published(self, symbol: str, window_start_ms: int | None = None) -> list[WindowFeature]:
        tables = self._published.get(symbol, {})
        if window_start_ms is not None:
            feat = tables.get(window_start_ms)
            return [feat] if feat else []
        return [tables[k] for k in sorted(tables)]

    def restore_published(self, feats: list[WindowFeature]) -> None:
        """启动恢复：回灌历史已发布窗口（不可变），用于重启后的查询与防写。"""
        for f in feats:
            self._published.setdefault(f.symbol, {})[f.window_start_ms] = f

    def all_published_pairs(self):
        for symbol in sorted(self._published):
            for w0 in sorted(self._published[symbol]):
                yield symbol, w0, self._published[symbol][w0]

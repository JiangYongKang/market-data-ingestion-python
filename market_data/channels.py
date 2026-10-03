"""多渠道事件时间线：按 (标的, 渠道) 独立推进、停滞检测与可观测状态。

多来源模式（``config.multi_source=True``）下，**每个标的下的每个渠道**
事件时间各自推进，互不影响：

* 同一渠道同时推多个标的时，一个标的跑得快不会把该渠道在另一个标的上的
  正常事件误判成迟到；
* 同一标的下，落后渠道也不会被跑得快的渠道误伤。

窗口何时可以发布由所有"参与该标的且未停滞"渠道共同决定（取各自在该标的
上的允许迟到水位的最小值）。

判定（全部确定、可解释）::

    timeline_wm(sym, src) = max(已接纳 event_time_ms of (sym, src)) - allowed_lateness_ms
    symbol_publish_wm(sym) = min(timeline_wm(sym, src))
        src ∈ 参与 sym 的渠道，剔除停滞渠道；若全部停滞则退化为全集

* 迟到判定只与**本标的本渠道**的水位比较；
* 某渠道长时间无数据（处理时间超过 ``idle_timeout_ms``）标记为停滞，
  暂时不参与取最小值，避免结果窗口被一个卡死渠道永久拖住；
  渠道恢复来数后自动重新参与（停滞只影响"何时发布"，永不丢数据）；
* 停滞判定使用处理时间时钟（可注入，便于测试），且重启后重置；
  已发布结果仍只取决于事件集合（发布特征落盘为唯一真相）。
"""
from __future__ import annotations

import copy
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass

from .errors import IngestionError
from .models import RejectReason

#: 尚未见任何事件时的水位：没有任何事件会被判迟到 / 窗口不可发布。
NEG_INF = -(1 << 62)


@dataclass(slots=True)
class ChannelState:
    """单个渠道的处理时间观测（停滞检测与源级聚合展示用）。"""

    source: str
    last_seen_ms: int | None = None        # 最近一次有数据的处理时间（毫秒）
    stalled: bool = False                  # 当前是否被判定为停滞


@dataclass(slots=True)
class TimelineState:
    """(symbol, source) 的事件时间推进状态（迟到判定与关窗依据）。"""

    max_event_time_ms: int                 # 该 (标的, 渠道) 已接纳事件的最大事件时间
    accepted: int = 0                      # 该 (标的, 渠道) 接纳（含被合并/隔离）事件数


class ChannelTimelines:
    def __init__(self, allowed_lateness_ms: int, idle_timeout_ms: int,
                 max_sources: int, lag_alert_ms: int = 0,
                 *, clock: Callable[[], int] | None = None) -> None:
        if allowed_lateness_ms < 0:
            raise ValueError("allowed_lateness_ms 不得为负")
        if idle_timeout_ms <= 0:
            raise ValueError("source_idle_timeout_ms 必须为正数")
        if max_sources <= 0:
            raise ValueError("max_sources 必须为正数")
        self._allowed = allowed_lateness_ms
        self._idle_timeout = idle_timeout_ms
        self._max_sources = max_sources
        self._lag_alert = lag_alert_ms
        self._clock = clock or (lambda: time.time_ns() // 1_000_000)
        # 源级处理时间观测（停滞检测）
        self._sources: dict[str, ChannelState] = {}
        # (symbol, source) -> 事件时间推进状态
        self._timelines: dict[tuple[str, str], TimelineState] = {}
        # symbol -> 曾为该标的报过主报数的渠道集合（只在事件接纳时登记）
        self._symbol_sources: dict[str, set[str]] = {}

    # ---------- 登记 / 推进 ----------
    def _ensure_source(self, source: str) -> ChannelState:
        st = self._sources.get(source)
        if st is None:
            if len(self._sources) >= self._max_sources:
                raise IngestionError(
                    RejectReason.SOURCE_LIMIT_REJECTED,
                    f"渠道数量已达上限 {self._max_sources}，拒绝新渠道 "
                    f"{source!r}（防止渠道标识无限增长占用内存）",
                    event_id=None)
            st = ChannelState(source=source)
            self._sources[source] = st
        return st

    def observe(self, symbol: str, source: str, event_time_ms: int,
                now_ms: int | None = None) -> TimelineState:
        """用一条已终态处理的事件推进该 (标的, 渠道) 时间线。"""
        st = self._ensure_source(source)
        key = (symbol, source)
        tl = self._timelines.get(key)
        if tl is None:
            tl = TimelineState(max_event_time_ms=event_time_ms)
            self._timelines[key] = tl
        elif event_time_ms > tl.max_event_time_ms:
            tl.max_event_time_ms = event_time_ms
        tl.accepted += 1
        st.last_seen_ms = self._clock() if now_ms is None else now_ms
        st.stalled = False  # 来数即恢复
        return tl

    def note_symbol_source(self, symbol: str, source: str) -> None:
        """登记某渠道参与某标的的关窗判定（与 observe 同临界区调用）。"""
        self._symbol_sources.setdefault(symbol, set()).add(source)

    def restore(self, symbol: str, source: str, max_event_time_ms: int,
                accepted: int = 0) -> None:
        """重放恢复：只重建事件时间推进；处理时间停滞状态重启后重置。"""
        st = self._ensure_source(source)
        key = (symbol, source)
        tl = self._timelines.get(key)
        if tl is None:
            tl = TimelineState(max_event_time_ms=max_event_time_ms)
            self._timelines[key] = tl
        elif max_event_time_ms > tl.max_event_time_ms:
            tl.max_event_time_ms = max_event_time_ms
        tl.accepted += accepted
        st.last_seen_ms = None
        st.stalled = False

    # ---------- 判定 ----------
    def is_late(self, symbol: str, source: str, event_time_ms: int) -> bool:
        """该事件相对**本标的本渠道**水位是否超迟到（含等于）。"""
        tl = self._timelines.get((symbol, source))
        if tl is None:
            return False
        return event_time_ms <= tl.max_event_time_ms - self._allowed

    def channel_watermark(self, symbol: str, source: str) -> int:
        tl = self._timelines.get((symbol, source))
        if tl is None:
            return NEG_INF
        return tl.max_event_time_ms - self._allowed

    def max_event_time(self, symbol: str, source: str) -> int | None:
        tl = self._timelines.get((symbol, source))
        return None if tl is None else tl.max_event_time_ms

    def timelines(self) -> Iterator[tuple[tuple[str, str], int]]:
        """当前全部 (symbol, source) -> max_event_time_ms（供批次模拟初值）。"""
        for key, tl in self._timelines.items():
            yield key, tl.max_event_time_ms

    def _is_stalled(self, st: ChannelState, now_ms: int) -> bool:
        """逐渠道按自身空闲时长判定。从未到数（重放恢复态 last_seen=None）
        的渠道不判停滞：重启不应把渠道立刻当卡死。"""
        if st.last_seen_ms is None:
            return False
        return (now_ms - st.last_seen_ms) > self._idle_timeout

    def publish_watermark_for(self, symbol: str,
                              now_ms: int | None = None) -> int:
        """该标的可安全发布到的水位：参与渠道（去停滞）各自在该标的上
        水位的最小值。"""
        sources = self._symbol_sources.get(symbol)
        if not sources:
            return NEG_INF
        now = self._clock() if now_ms is None else now_ms
        active = [s for s in sources
                  if not self._is_stalled(self._sources[s], now)]
        effective = active if active else sorted(sources)
        wms = [self.channel_watermark(symbol, s) for s in effective]
        return min(wms) if wms else NEG_INF

    # ---------- 观测 ----------
    def sources(self) -> list[str]:
        return sorted(self._sources)

    def state_of(self, source: str) -> ChannelState | None:
        return self._sources.get(source)

    def symbol_participants(self, symbol: str) -> list[str]:
        return sorted(self._symbol_sources.get(symbol, ()))

    def _source_max_event_time(self, source: str) -> int | None:
        vals = [tl.max_event_time_ms for (sym, src), tl in self._timelines.items()
                if src == source]
        return max(vals) if vals else None

    def snapshot_observability(self, now_ms: int | None = None) -> dict:
        now = self._clock() if now_ms is None else now_ms
        max_evt = max((tl.max_event_time_ms for tl in self._timelines.values()),
                      default=None)
        chans = []
        for src in sorted(self._sources):
            st = self._sources[src]
            stalled = self._is_stalled(st, now)
            src_max = self._source_max_event_time(src)
            lag = None if (max_evt is None or src_max is None) \
                else max_evt - src_max
            per_symbol = {}
            for (sym, s), tl in sorted(self._timelines.items()):
                if s != src:
                    continue
                per_symbol[sym] = {
                    "max_event_time_ms": tl.max_event_time_ms,
                    "watermark_ms": tl.max_event_time_ms - self._allowed,
                    "accepted": tl.accepted,
                }
            chans.append({
                "source": src,
                "max_event_time_ms": src_max,
                "watermark_ms": None if src_max is None
                    else src_max - self._allowed,
                "last_seen_ms": st.last_seen_ms,
                "idle_for_ms": None if st.last_seen_ms is None
                    else max(0, now - st.last_seen_ms),
                "accepted": sum(tl.accepted for (sym, s), tl
                                in self._timelines.items() if s == src),
                "lag_ms": lag,
                "stalled": stalled,
                "lag_alert": bool(self._lag_alert and lag is not None
                                  and lag >= self._lag_alert),
                "symbols": per_symbol,
            })
        return {
            "channel_count": len(self._sources),
            "channel_limit": self._max_sources,
            "idle_timeout_ms": self._idle_timeout,
            "max_event_time_ms": max_evt,
            "channels": chans,
        }

    def __len__(self) -> int:
        return len(self._sources)

    # ---------- 批次快照 / 回滚（不深拷贝时钟） ----------
    def _snapshot(self):
        return (
            {s: copy.deepcopy(st) for s, st in self._sources.items()},
            {k: copy.deepcopy(tl) for k, tl in self._timelines.items()},
            {s: set(srcs) for s, srcs in self._symbol_sources.items()},
        )

    def _restore(self, snap) -> None:
        sources, timelines, symsrcs = snap
        self._sources = {s: copy.deepcopy(st) for s, st in sources.items()}
        self._timelines = {k: copy.deepcopy(tl) for k, tl in timelines.items()}
        self._symbol_sources = {s: set(srcs) for s, srcs in symsrcs.items()}

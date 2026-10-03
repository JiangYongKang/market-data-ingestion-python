"""多渠道事件时间线：按 (标的, 渠道) 独立水位推进、停滞检测与可观测状态。

多来源模式（``config.multi_source=True``）下，**每个标的的每个渠道**的事件
时间各自推进，互不影响：同一渠道同时推多个标的时，一个标的跑得快不会把
另一标的的正常事件误判迟到；同一标的下落后渠道也不会被跑得快渠道误伤。
窗口何时可以发布由所有"参与该标的且未停滞"渠道共同决定（取各自允许迟到
水位的最小值）。

判定（全部确定、可解释）::

    channel_wm(sym, src) = max(已接纳 event_time_ms of (sym, src)) - allowed_lateness_ms
    symbol_publish_wm(sym) = min(channel_wm(sym, src))
        src ∈ 参与 sym 的渠道，剔除停滞渠道；若全部停滞则退化为全集

* 迟到判定只与**本标的本渠道**水位比较 -> 快慢标的、快慢渠道互不误伤；
* 某渠道在某标的上长时间无数据（处理时间超过 ``idle_timeout_ms``）标记为
  停滞，暂时不参与该标的取最小值，避免结果窗口被一个卡死渠道永久拖住；
  渠道恢复来数后自动重新参与（停滞只影响"何时发布"，永不丢数据）；
* 停滞判定使用处理时间时钟（可注入，便于测试），且重启后重置；
  已发布结果仍只取决于事件集合（发布特征落盘为唯一真相）。
"""
from __future__ import annotations

import copy
import time
from collections.abc import Callable
from dataclasses import dataclass

from .errors import IngestionError
from .models import RejectReason

#: 尚未见任何事件时的水位：没有任何事件会被判迟到 / 窗口不可发布。
NEG_INF = -(1 << 62)


@dataclass(slots=True)
class ChannelState:
    """单个 (标的, 渠道) 的推进状态（观测与判定共用）。"""

    source: str
    symbol: str
    max_event_time_ms: int | None = None   # 该 (标的,渠道) 已接纳事件的最大事件时间
    last_seen_ms: int | None = None        # 最近一次有数据的处理时间（毫秒）
    accepted: int = 0                      # 该 (标的,渠道) 接纳（含被合并/隔离）事件数
    stalled: bool = False                  # 当前是否被判定为停滞


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
        # (symbol, source) -> 推进状态：推进判定按标的和渠道各自独立
        self._channels: dict[tuple[str, str], ChannelState] = {}
        # symbol -> 曾为该标的报过数的渠道集合（只在事件接纳时登记）
        self._symbol_sources: dict[str, set[str]] = {}

    # ---------- 登记 / 推进 ----------
    def _ensure(self, symbol: str, source: str) -> ChannelState:
        key = (symbol, source)
        st = self._channels.get(key)
        if st is None:
            if source not in self._distinct_sources() \
                    and len(self._distinct_sources()) >= self._max_sources:
                raise IngestionError(
                    RejectReason.SOURCE_LIMIT_REJECTED,
                    f"渠道数量已达上限 {self._max_sources}，拒绝新渠道 "
                    f"{source!r}（防止渠道标识无限增长占用内存）",
                    event_id=None)
            st = ChannelState(source=source, symbol=symbol)
            self._channels[key] = st
        return st

    def _distinct_sources(self) -> set[str]:
        return {src for _sym, src in self._channels}

    def observe(self, symbol: str, source: str, event_time_ms: int,
                now_ms: int | None = None) -> ChannelState:
        """用一条已终态接纳的事件推进该 (标的,渠道) 时间线。"""
        st = self._ensure(symbol, source)
        if st.max_event_time_ms is None or event_time_ms > st.max_event_time_ms:
            st.max_event_time_ms = event_time_ms
        st.last_seen_ms = self._clock() if now_ms is None else now_ms
        st.accepted += 1
        st.stalled = False  # 来数即恢复
        return st

    def note_symbol_source(self, symbol: str, source: str) -> None:
        """登记某渠道参与某标的（与 observe 同临界区调用）。"""
        self._symbol_sources.setdefault(symbol, set()).add(source)

    def restore(self, symbol: str, source: str, max_event_time_ms: int,
                accepted: int = 0) -> None:
        """重放恢复：只重建事件时间推进；处理时间停滞状态重启后重置。"""
        st = self._ensure(symbol, source)
        if st.max_event_time_ms is None or max_event_time_ms > st.max_event_time_ms:
            st.max_event_time_ms = max_event_time_ms
        st.accepted += accepted
        st.last_seen_ms = None
        st.stalled = False

    # ---------- 判定 ----------
    def is_late(self, symbol: str, source: str, event_time_ms: int) -> bool:
        """该事件相对**本标的本渠道**水位是否超迟到（含等于）。"""
        st = self._channels.get((symbol, source))
        if st is None or st.max_event_time_ms is None:
            return False
        return event_time_ms <= st.max_event_time_ms - self._allowed

    def channel_watermark(self, symbol: str, source: str) -> int:
        st = self._channels.get((symbol, source))
        if st is None or st.max_event_time_ms is None:
            return NEG_INF
        return st.max_event_time_ms - self._allowed

    def _is_stalled(self, st: ChannelState, now_ms: int) -> bool:
        """逐 (标的,渠道) 按自身空闲时长判定。从未到数（重放恢复态
        last_seen=None）的不判停滞：重启不应把渠道立刻当卡死。"""
        if st.last_seen_ms is None:
            return False
        return (now_ms - st.last_seen_ms) > self._idle_timeout

    def publish_watermark_for(self, symbol: str,
                              now_ms: int | None = None) -> int:
        """该标的可安全发布到的水位：参与渠道（去停滞）各自水位的最小值。"""
        sources = self._symbol_sources.get(symbol)
        if not sources:
            return NEG_INF
        now = self._clock() if now_ms is None else now_ms
        states = {s: self._channels[(symbol, s)] for s in sources
                  if (symbol, s) in self._channels}
        active = [s for s, st in states.items() if not self._is_stalled(st, now)]
        effective = active if active else sorted(states)
        wms = [self.channel_watermark(symbol, s) for s in effective]
        return min(wms) if wms else NEG_INF

    # ---------- 观测 ----------
    def sources(self) -> list[str]:
        return sorted(self._distinct_sources())

    def state_of(self, symbol: str, source: str) -> ChannelState | None:
        return self._channels.get((symbol, source))

    def symbol_participants(self, symbol: str) -> list[str]:
        return sorted(self._symbol_sources.get(symbol, ()))

    def snapshot_observability(self, now_ms: int | None = None) -> dict:
        """观测快照：按渠道聚合（推进到哪/落后多少/是否停滞），
        并附每 (标的,渠道) 明细，重启后同样可见。"""
        now = self._clock() if now_ms is None else now_ms
        max_evt = max((st.max_event_time_ms for st in self._channels.values()
                       if st.max_event_time_ms is not None), default=None)
        by_source: dict[str, list[ChannelState]] = {}
        for (_sym, src), st in self._channels.items():
            by_source.setdefault(src, []).append(st)
        chans = []
        for src in sorted(by_source):
            states = by_source[src]
            src_max = max((st.max_event_time_ms for st in states
                           if st.max_event_time_ms is not None), default=None)
            seen = [st.last_seen_ms for st in states
                    if st.last_seen_ms is not None]
            last_seen = max(seen) if seen else None
            stalled = bool(states) and all(
                self._is_stalled(st, now) for st in states)
            lag = None if (max_evt is None or src_max is None) \
                else max_evt - src_max
            per_symbol = [{
                "symbol": st.symbol,
                "max_event_time_ms": st.max_event_time_ms,
                "watermark_ms": self.channel_watermark(st.symbol, src),
                "last_seen_ms": st.last_seen_ms,
                "idle_for_ms": None if st.last_seen_ms is None
                    else max(0, now - st.last_seen_ms),
                "accepted": st.accepted,
                "stalled": self._is_stalled(st, now),
            } for st in sorted(states, key=lambda s: s.symbol)]
            chans.append({
                "source": src,
                "max_event_time_ms": src_max,
                "watermark_ms": None if src_max is None
                    else src_max - self._allowed,
                "last_seen_ms": last_seen,
                "idle_for_ms": None if last_seen is None
                    else max(0, now - last_seen),
                "accepted": sum(st.accepted for st in states),
                "lag_ms": lag,
                "stalled": stalled,
                "lag_alert": bool(self._lag_alert and lag is not None
                                  and lag >= self._lag_alert),
                "symbols": per_symbol,
            })
        return {
            "channel_count": len(by_source),
            "channel_limit": self._max_sources,
            "idle_timeout_ms": self._idle_timeout,
            "max_event_time_ms": max_evt,
            "channels": chans,
        }

    def __len__(self) -> int:
        return len(self._distinct_sources())

    # ---------- 批次快照 / 回滚（不深拷贝时钟） ----------
    def _snapshot(self):
        return (
            {k: copy.deepcopy(st) for k, st in self._channels.items()},
            {s: set(srcs) for s, srcs in self._symbol_sources.items()},
        )

    def _restore(self, snap) -> None:
        chans, symsrcs = snap
        self._channels = {k: copy.deepcopy(st) for k, st in chans.items()}
        self._symbol_sources = {s: set(srcs) for s, srcs in symsrcs.items()}

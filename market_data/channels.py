"""多渠道事件时间线：每渠道独立水位推进、停滞检测与可观测状态。

多来源模式（``config.multi_source=True``）下，同一标的的各渠道事件时间
各自推进，互不影响：落后渠道不会因其他渠道跑得快而被误判迟到。窗口何时
可以发布由所有"参与该标的且未停滞"渠道共同决定（取各自允许迟到水位的
最小值）。

判定（全部确定、可解释）::

    channel_wm(src) = max(已接纳 event_time_ms of src) - allowed_lateness_ms
    symbol_publish_wm(sym) = min(channel_wm(src))
        src ∈ 参与 sym 的渠道，剔除停滞渠道；若全部停滞则退化为全集

* 迟到判定只与**本渠道**水位比较 -> 慢渠道不被快渠道误伤；
* 某渠道长时间无数据（处理时间超过 ``idle_timeout_ms``）标记为停滞，
  暂时不参与取最小值，避免结果窗口被一个卡死渠道永久拖住；
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
    """单个渠道的推进状态（观测与判定共用）。"""

    source: str
    max_event_time_ms: int | None = None   # 该渠道已接纳事件的最大事件时间
    last_seen_ms: int | None = None        # 最近一次有数据的处理时间（毫秒）
    accepted: int = 0                      # 该渠道接纳（含被合并）事件数
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
        self._channels: dict[str, ChannelState] = {}
        # symbol -> 曾为该标的报过数的渠道集合（只在事件接纳时登记）
        self._symbol_sources: dict[str, set[str]] = {}

    # ---------- 登记 / 推进 ----------
    def _ensure(self, source: str) -> ChannelState:
        st = self._channels.get(source)
        if st is None:
            if len(self._channels) >= self._max_sources:
                raise IngestionError(
                    RejectReason.SOURCE_LIMIT_REJECTED,
                    f"渠道数量已达上限 {self._max_sources}，拒绝新渠道 "
                    f"{source!r}（防止渠道标识无限增长占用内存）",
                    event_id=None)
            st = ChannelState(source=source)
            self._channels[source] = st
        return st

    def observe(self, source: str, event_time_ms: int,
                now_ms: int | None = None) -> ChannelState:
        """用一条已终态接纳的事件推进该渠道时间线。"""
        st = self._ensure(source)
        if st.max_event_time_ms is None or event_time_ms > st.max_event_time_ms:
            st.max_event_time_ms = event_time_ms
        st.last_seen_ms = self._clock() if now_ms is None else now_ms
        st.accepted += 1
        st.stalled = False  # 来数即恢复
        return st

    def note_symbol_source(self, symbol: str, source: str) -> None:
        """登记某渠道参与某标的（与 observe 同临界区调用）。"""
        self._symbol_sources.setdefault(symbol, set()).add(source)

    def restore(self, source: str, max_event_time_ms: int,
                accepted: int = 0) -> None:
        """重放恢复：只重建事件时间推进；处理时间停滞状态重启后重置。"""
        st = self._ensure(source)
        if st.max_event_time_ms is None or max_event_time_ms > st.max_event_time_ms:
            st.max_event_time_ms = max_event_time_ms
        st.accepted += accepted
        st.last_seen_ms = None
        st.stalled = False

    # ---------- 判定 ----------
    def is_late(self, source: str, event_time_ms: int) -> bool:
        """该事件相对**本渠道**水位是否超迟到（含等于）。"""
        st = self._channels.get(source)
        if st is None or st.max_event_time_ms is None:
            return False
        return event_time_ms <= st.max_event_time_ms - self._allowed

    def channel_watermark(self, source: str) -> int:
        st = self._channels.get(source)
        if st is None or st.max_event_time_ms is None:
            return NEG_INF
        return st.max_event_time_ms - self._allowed

    def _is_stalled(self, st: ChannelState, now_ms: int) -> bool:
        """逐渠道按自身空闲时长判定。从未到数（重放恢复态 last_seen=None）
        的渠道不判停滞：重启不应把渠道立刻当卡死。"""
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
        active = [s for s in sources
                  if not self._is_stalled(self._channels[s], now)]
        effective = active if active else sorted(sources)
        wms = [self.channel_watermark(s) for s in effective]
        return min(wms) if wms else NEG_INF

    # ---------- 观测 ----------
    def sources(self) -> list[str]:
        return sorted(self._channels)

    def state_of(self, source: str) -> ChannelState | None:
        return self._channels.get(source)

    def symbol_participants(self, symbol: str) -> list[str]:
        return sorted(self._symbol_sources.get(symbol, ()))

    def snapshot_observability(self, now_ms: int | None = None) -> dict:
        now = self._clock() if now_ms is None else now_ms
        max_evt = max((st.max_event_time_ms for st in self._channels.values()
                       if st.max_event_time_ms is not None), default=None)
        chans = []
        for src in sorted(self._channels):
            st = self._channels[src]
            stalled = self._is_stalled(st, now)
            lag = None if (max_evt is None or st.max_event_time_ms is None) \
                else max_evt - st.max_event_time_ms
            chans.append({
                "source": src,
                "max_event_time_ms": st.max_event_time_ms,
                "watermark_ms": self.channel_watermark(src),
                "last_seen_ms": st.last_seen_ms,
                "idle_for_ms": None if st.last_seen_ms is None
                    else max(0, now - st.last_seen_ms),
                "accepted": st.accepted,
                "lag_ms": lag,
                "stalled": stalled,
                "lag_alert": bool(self._lag_alert and lag is not None
                                  and lag >= self._lag_alert),
            })
        return {
            "channel_count": len(self._channels),
            "channel_limit": self._max_sources,
            "idle_timeout_ms": self._idle_timeout,
            "max_event_time_ms": max_evt,
            "channels": chans,
        }

    def __len__(self) -> int:
        return len(self._channels)

    # ---------- 批次快照 / 回滚（不深拷贝时钟） ----------
    def _snapshot(self):
        return (
            {s: copy.deepcopy(st) for s, st in self._channels.items()},
            {s: set(srcs) for s, srcs in self._symbol_sources.items()},
        )

    def _restore(self, snap) -> None:
        chans, symsrcs = snap
        self._channels = {s: copy.deepcopy(st) for s, st in chans.items()}
        self._symbol_sources = {s: set(srcs) for s, srcs in symsrcs.items()}

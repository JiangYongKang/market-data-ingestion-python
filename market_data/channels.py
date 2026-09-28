"""多渠道注册与各渠道独立时间线（per-(symbol,source) 水位）。

背景
====
同一个标的的行情会同时从多个渠道推送，各渠道序号体系互相独立、推进
速度也互相独立。单来源实现里只有一条全局水位
``max(已见事件时间) - allowed_lateness_ms``，多渠道共用时跑得快的渠道
会把水位推高，导致落后渠道的正常事件被误判成迟到而进隔离区。

规则（确定、可解释）
====================
* **渠道独立水位**：每个 ``(symbol, source)`` 各维护
  ``channel_watermark = 本渠道已见最大事件时间 - allowed_lateness_ms``。
  迟到判定只看事件所属渠道自己的水位，渠道间互不影响；
* **窗口关闭按最慢渠道**：多来源标的的标的水位取该标的**全部已注册
  渠道水位的最小值**。窗口只在最慢渠道都越过窗口右边界后才发布，
  保证"窗口结果按各渠道实际到齐的事件算对"；
* **注册方式**：配置 ``multi_source_symbols`` 中声明的渠道在启动时即
  注册（其水位在首条事件到达前为 -∞，表现为窗口等待该渠道）；未声明
  的渠道在首条事件到达时动态注册，但单标的渠道数不得超过
  ``max_channels_per_symbol``，超出按 ``channel_limit_exceeded`` 硬拒绝；
* **卡住可观测**：``lag_ms``/``stuck`` 仅用于观测（基于处理时间），
  绝不参与事件时间判定，从而不影响结果可复现性。
"""
from __future__ import annotations

from dataclasses import dataclass

from .errors import IngestionError
from .models import RejectReason

#: 未见任何事件时的水位：不会有事件被判迟到，同时取 min 时会压住窗口。
NEG_INF_WATERMARK = -(1 << 62)


@dataclass(slots=True)
class ChannelState:
    """单个 (symbol, source) 渠道的推进状态。"""

    symbol: str
    source: str
    declared: bool
    max_event_time_ms: int | None = None
    last_seen_ms: int | None = None

    @property
    def observed(self) -> bool:
        return self.max_event_time_ms is not None

    def watermark_ms(self, allowed_lateness_ms: int) -> int:
        if self.max_event_time_ms is None:
            return NEG_INF_WATERMARK
        return self.max_event_time_ms - allowed_lateness_ms


class ChannelRegistry:
    def __init__(self,
                 multi_source_symbols: dict[str, list[str]] | None = None,
                 max_channels_per_symbol: int = 16,
                 channel_idle_timeout_ms: int = 600_000) -> None:
        if max_channels_per_symbol <= 0:
            raise ValueError("max_channels_per_symbol 必须为正数")
        self._multi: dict[str, list[str]] = {
            sym: list(srcs) for sym, srcs in (multi_source_symbols or {}).items()
        }
        self._max_channels = max_channels_per_symbol
        self._idle_timeout = channel_idle_timeout_ms
        # (symbol, source) -> ChannelState
        self._channels: dict[tuple[str, str], ChannelState] = {}
        for sym, srcs in self._multi.items():
            for src in srcs:
                self._channels[(sym, src)] = ChannelState(sym, src, declared=True)

    # ---------- 注册与查询 ----------
    def is_multi_source(self, symbol: str) -> bool:
        return symbol in self._multi

    def register(self, symbol: str, source: str, now_ms: int) -> ChannelState:
        """确保渠道已注册，返回其状态。

        声明渠道直接返回（无论是否已有事件）；未声明渠道动态注册，
        超过 ``max_channels_per_symbol`` 时硬失败（不留任何注册痕迹）。
        """
        st = self._channels.get((symbol, source))
        if st is not None:
            return st
        if symbol not in self._multi:
            # 未声明为多来源的标的必须保持单来源路径（含跨标的全局水位语义），
            # 动态渠道一律在配置中声明的标的内接入。
            raise IngestionError(
                RejectReason.CHANNEL_LIMIT_EXCEEDED,
                f"标的 {symbol!r} 未在 multi_source_symbols 中声明为多来源，"
                f"拒绝按多渠道接入来源 {source!r}")
        existing = sum(1 for (sym, _s) in self._channels if sym == symbol)
        if existing >= self._max_channels:
            raise IngestionError(
                RejectReason.CHANNEL_LIMIT_EXCEEDED,
                f"标的 {symbol!r} 已注册 {existing} 个渠道，达到上限 "
                f"{self._max_channels}，拒绝新渠道 {source!r}；"
                f"请在 multi_source_symbols 中声明或调高 max_channels_per_symbol",
            )
        st = ChannelState(symbol, source, declared=False, last_seen_ms=now_ms)
        self._channels[(symbol, source)] = st
        return st

    def observe(self, symbol: str, source: str, event_time_ms: int,
                now_ms: int) -> None:
        """用一条事件推进该渠道自己的时间线。"""
        st = self.register(symbol, source, now_ms)
        if st.max_event_time_ms is None or event_time_ms > st.max_event_time_ms:
            st.max_event_time_ms = event_time_ms
        st.last_seen_ms = now_ms

    def channel_watermark_ms(self, symbol: str, source: str,
                             allowed_lateness_ms: int) -> int:
        st = self._channels.get((symbol, source))
        if st is None:
            return NEG_INF_WATERMARK
        return st.watermark_ms(allowed_lateness_ms)

    def symbol_watermark_ms(self, symbol: str,
                            allowed_lateness_ms: int) -> int | None:
        """多来源标的的合并水位：取全部已注册渠道水位的最小值。

        返回 None 表示该标的没有任何已注册渠道（调用方应走单来源路径）。
        """
        sts = [st for (sym, _s), st in self._channels.items() if sym == symbol]
        if not sts:
            return None
        return min(st.watermark_ms(allowed_lateness_ms) for st in sts)

    def lag_ms(self, symbol: str, source: str, now_ms: int) -> int | None:
        """距上次事件的处理时间间隔（毫秒）；从未到达返回 None。仅观测用。"""
        st = self._channels.get((symbol, source))
        if st is None or st.last_seen_ms is None:
            return None
        return max(0, now_ms - st.last_seen_ms)

    def is_stuck(self, symbol: str, source: str, now_ms: int) -> bool:
        """声明后从未到达，或超过空闲超时无事件 -> 卡住（仅观测）。"""
        st = self._channels.get((symbol, source))
        if st is None:
            return False
        if not st.observed:
            return True
        lag = self.lag_ms(symbol, source, now_ms)
        return lag is not None and lag > self._idle_timeout

    def channels(self, symbol: str | None = None) -> list[ChannelState]:
        if symbol is None:
            return [self._channels[k] for k in sorted(self._channels)]
        return [self._channels[k] for k in sorted(self._channels)
                if k[0] == symbol]

    def snapshot(self, now_ms: int, allowed_lateness_ms: int) -> dict:
        """观测快照：各渠道推进位置、水位、落后量、是否卡住；按标的聚合。"""
        out: dict[str, dict] = {}
        for (sym, src) in sorted(self._channels):
            st = self._channels[(sym, src)]
            lag = self.lag_ms(sym, src, now_ms)
            entry = {
                "source": src,
                "declared": st.declared,
                "max_event_time_ms": st.max_event_time_ms,
                "watermark_ms": st.watermark_ms(allowed_lateness_ms),
                "lag_ms": lag,
                "observed": st.observed,
                "stuck": self.is_stuck(sym, src, now_ms),
            }
            out.setdefault(sym, {"channels": [], "symbol_watermark_ms": None})
            out[sym]["channels"].append(entry)
        for sym, body in out.items():
            wms = [c["watermark_ms"] for c in body["channels"]]
            body["symbol_watermark_ms"] = min(wms) if wms else None
            body["channel_count"] = len(body["channels"])
            body["stuck_count"] = sum(1 for c in body["channels"] if c["stuck"])
        return out

    # ---------- 恢复 ----------
    def restore(self, symbol: str, source: str, max_event_time_ms: int | None,
                last_seen_ms: int | None, declared: bool | None = None) -> None:
        """重放恢复：重建渠道时间线。声明性以配置为准，不允许历史数据改写。"""
        st = self._channels.get((symbol, source))
        if st is None:
            st = ChannelState(symbol, source, declared=False,
                              last_seen_ms=last_seen_ms)
            self._channels[(symbol, source)] = st
            self._multi.setdefault(symbol, [])
        if max_event_time_ms is not None and (
                st.max_event_time_ms is None
                or max_event_time_ms > st.max_event_time_ms):
            st.max_event_time_ms = max_event_time_ms
        if last_seen_ms is not None and (
                st.last_seen_ms is None or last_seen_ms > st.last_seen_ms):
            st.last_seen_ms = last_seen_ms

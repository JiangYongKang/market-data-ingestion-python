"""水位线（watermark）推进与迟到判定。

定义（全部基于事件时间，确定且可解释）::

    watermark_ms = max(已见 event_time_ms) - allowed_lateness_ms

* ``event_time_ms <= watermark_ms`` 的事件判定为"超水位迟到"，
  进入隔离区，不参与聚合、不改变已发布结果；
* 允许窗口内的迟到（``event_time_ms > watermark_ms``）正常进入窗口，
  可修正尚未发布的窗口结果；
* 水位线只进不退（与乱序到达顺序无关，重放结果一致）；
* 处理时间 ``now_ms`` 仅用于隔离区记录与日志，不参与水位判定，
  从而保证结果与运行速度无关、可复现。
"""
from __future__ import annotations


class WatermarkManager:
    def __init__(self, allowed_lateness_ms: int) -> None:
        if allowed_lateness_ms < 0:
            raise ValueError("allowed_lateness_ms 不得为负")
        self._allowed = allowed_lateness_ms
        self._max_event_time: int | None = None

    def observe(self, event_time_ms: int, now_ms: int | None = None) -> int:
        """用一个新事件时间推进水位，返回推进后的水位。"""
        if self._max_event_time is None or event_time_ms > self._max_event_time:
            self._max_event_time = event_time_ms
        return self.watermark_ms

    def is_late(self, event_time_ms: int) -> bool:
        """该事件时间是否已落在水位线之下（含等于）。"""
        if self._max_event_time is None:
            return False
        return event_time_ms <= self.watermark_ms

    @property
    def watermark_ms(self) -> int:
        if self._max_event_time is None:
            return -(1 << 62)  # 未见任何事件时：没有任何事件会被判迟到
        return self._max_event_time - self._allowed

    @property
    def max_event_time_ms(self) -> int | None:
        return self._max_event_time

    def restore(self, max_event_time_ms: int | None) -> None:
        if max_event_time_ms is None:
            return
        if self._max_event_time is None or max_event_time_ms > self._max_event_time:
            self._max_event_time = max_event_time_ms

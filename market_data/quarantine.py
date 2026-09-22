"""隔离区：超水位迟到事件不静默丢弃、不改变已发布结果。

记录可按标的查询，携带原因、当时水位、判定说明与进入时间。
设有容量上限：达到上限后新的迟到事件会被拒绝（背压的一种，
绝不无界占用内存），并通过返回值告知调用方归类。
"""
from __future__ import annotations

from collections import deque

from .models import Event, QuarantinedEvent, RejectReason


class Quarantine:
    def __init__(self, max_size: int) -> None:
        if max_size <= 0:
            raise ValueError("max_quarantine_size 必须为正数")
        self._max = max_size
        self._items: deque[QuarantinedEvent] = deque()

    def add(self, event: Event, reason: RejectReason, watermark_ms: int,
            detail: str, accepted_at_ms: int) -> bool:
        """隔离一条事件。容量满时返回 False 且不留任何记录。"""
        if len(self._items) >= self._max:
            return False
        self._items.append(QuarantinedEvent(
            event=event, reason=reason, watermark_ms=watermark_ms,
            detail=detail, accepted_at_ms=accepted_at_ms,
        ))
        return True

    def restore(self, items: list[QuarantinedEvent]) -> None:
        """启动恢复：回灌历史隔离记录（按文件顺序，确定性）。"""
        for item in items:
            if len(self._items) < self._max:
                self._items.append(item)

    def list(self, symbol: str | None = None) -> list[QuarantinedEvent]:
        if symbol is None:
            return list(self._items)
        return [q for q in self._items if q.event.symbol == symbol]

    @property
    def size(self) -> int:
        return len(self._items)

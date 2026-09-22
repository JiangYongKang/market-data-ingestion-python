"""明确分类的异常体系，失败原因彼此可区分。"""
from __future__ import annotations

from .models import RejectReason


class IngestionError(Exception):
    """所有可归类失败的基类。"""

    reason: RejectReason = RejectReason.SCHEMA_TYPE_MISMATCH

    def __init__(self, reason: RejectReason, message: str, *, event_id: str | None = None):
        self.reason = reason
        self.event_id = event_id
        super().__init__(message)


class SchemaError(IngestionError):
    """结构演进类失败：未知字段/类型不匹配/版本不支持/缺字段。"""


class DuplicateError(IngestionError):
    """重复投递。identical=True 为普通重复；False 为内容冲突。"""

    def __init__(self, message: str, *, event_id: str, identical: bool):
        super().__init__(
            RejectReason.DUPLICATE_IDENTICAL if identical
            else RejectReason.DUPLICATE_CONFLICT,
            message,
            event_id=event_id,
        )
        self.identical = identical


class LateEventError(IngestionError):
    """超过水位线的迟到事件（进入隔离区，不抛给调用方崩溃）。"""

    def __init__(self, message: str, *, event_id: str, watermark_ms: int):
        super().__init__(RejectReason.LATE_BEYOND_WATERMARK, message, event_id=event_id)
        self.watermark_ms = watermark_ms


class CheckpointRollbackError(IngestionError):
    """请求回退到更早位点。"""

    def __init__(self, message: str, *, source: str, requested: int, current: int):
        super().__init__(RejectReason.CHECKPOINT_ROLLBACK, message)
        self.source = source
        self.requested = requested
        self.current = current


class BackpressureError(IngestionError):
    """背压策略为 reject 且积压已达上限。"""


class StaleSequenceError(IngestionError):
    """来源序号早于已确认序号（且事件内容不是已知重复）。"""

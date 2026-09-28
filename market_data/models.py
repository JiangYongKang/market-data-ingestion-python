"""领域类型定义：事件协议、判定原因、窗口聚合结果。

事件协议版本
------------
``schema_version`` 为字符串（当前 "1"）。规则：

* 新版本只能新增字段，不得改变既有字段的语义或类型；
* 新增字段必须在 ``DEFAULTS`` 中声明稳定缺省值（缺省即"未知"，
  绝不与真实业务值混淆，例如未知价格用 ``None`` 而非 0）；
* 已废弃字段移入 ``DEPRECATED``，提交时允许携带但会被忽略并记录；
* 未知字段默认拒绝（``reject_unknown``），可在配置中显式开启忽略；
* 类型不匹配一律拒绝。缺省值与缺失字段不参与加权计算，保证
  缺省语义确定、稳定、可解释。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


SCHEMA_CURRENT = "1"

#: 新增字段 -> 稳定缺省值。None 表示"未知"，聚合时不参与计算。
DEFAULTS: dict[str, Any] = {
    "trade_id": None,
    "venue": None,
}

#: 已废弃字段：允许携带，解析时忽略并计入观测。
DEPRECATED: frozenset[str] = frozenset({"exchange_code"})

#: 第 1 版已知业务字段（除协议包装字段外）。
KNOWN_FIELDS: frozenset[str] = frozenset(
    {"symbol", "price", "quantity", "event_time_ms", "trade_id", "venue"}
)


class RejectReason(str, Enum):
    """失败归类。不同原因彼此可区分，用于日志、隔离区与测试断言。"""

    # 结构演进相关
    SCHEMA_UNKNOWN_FIELD = "schema_unknown_field"
    SCHEMA_TYPE_MISMATCH = "schema_type_mismatch"
    SCHEMA_VERSION_UNSUPPORTED = "schema_version_unsupported"
    SCHEMA_MISSING_FIELD = "schema_missing_field"
    # 幂等相关
    DUPLICATE_IDENTICAL = "duplicate_identical"  # 完全一致的重复投递
    DUPLICATE_CONFLICT = "duplicate_conflict"    # 同 event_id 内容冲突
    # 多来源合并相关
    CROSS_SOURCE_MERGED = "cross_source_merged"  # 跨渠道同一笔成交，被合并（非主报）
    MERGE_CONFLICT = "merge_conflict"            # 跨渠道同笔成交价格/数量对不上
    SOURCE_LIMIT_REJECTED = "source_limit_rejected"  # 渠道数量超过上限
    # 乱序/水位相关
    LATE_BEYOND_WATERMARK = "late_beyond_watermark"  # 超水位迟到 -> 隔离区
    # 位点相关
    CHECKPOINT_ROLLBACK = "checkpoint_rollback"
    # 背压/资源
    BACKPRESSURE_REJECTED = "backpressure_rejected"
    # 序号
    STALE_SEQUENCE = "stale_sequence"


@dataclass(frozen=True, slots=True)
class Event:
    """规范化后的行情成交通知（内部表示，不可变）。

    ``event_id`` 为稳定事件标识；``source``/``seq`` 为来源序号，
    同一 (source, seq) 的事件也被视为同一事件的再次投递。
    """

    event_id: str
    source: str
    seq: int
    symbol: str
    price: float
    quantity: float
    event_time_ms: int
    schema_version: str = SCHEMA_CURRENT
    trade_id: str | None = None
    venue: str | None = None


@dataclass(slots=True)
class QuarantinedEvent:
    """隔离区记录：超水位迟到事件不静默丢弃、不改变已发布结果。"""

    event: Event
    reason: RejectReason
    watermark_ms: int
    detail: str
    accepted_at_ms: int


@dataclass(frozen=True, slots=True)
class WindowFeature:
    """窗口特征（窗口结束后发布即不可变）。

    ``vwap`` 为成交量加权均价；``volatility`` 为以成交量加权的
    成交价标准差（总体方差，确定算法，重放可复现）。
    采用 Welford 在线算法 + 收尾合并，避免浮点求和顺序差异。
    """

    symbol: str
    window_start_ms: int
    window_end_ms: int
    count: int
    total_quantity: float
    vwap: float | None
    volatility: float | None
    event_ids: tuple[str, ...] = field(default_factory=tuple)
    published: bool = False


@dataclass(frozen=True, slots=True)
class MergedTradeRecord:
    """跨渠道同一笔成交的合并记录：以主报为准，被合并路留痕。"""

    trade_key: tuple[str, str]          # (symbol, trade_id)
    winner_event_id: str
    winner_source: str
    loser_event_id: str
    loser_source: str
    event_time_ms: int                  # 被合并（次报）事件时间
    loser_event: Event | None = None    # 被合并事件完整内容（持久化/恢复用）
    loser_seq: int = -1                 # 被合并事件来源序号（位点/去重恢复用）


@dataclass(frozen=True, slots=True)
class IngestResult:
    """单条/批次写入的判定结果（全部原子成功，或全部不生效）。"""

    accepted: int
    duplicate: int
    quarantined: int
    rejected: int
    merged: int = 0                     # 跨渠道被合并掉的重复上报数
    details: tuple[tuple[str, RejectReason, str], ...] = ()

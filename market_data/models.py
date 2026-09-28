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
    MERGED_IDENTICAL = "merged_identical"        # 跨渠道同一笔成交，内容一致被合并
    CROSS_SOURCE_CONFLICT = "cross_source_conflict"  # 同笔成交跨渠道价格/数量对不上
    CHANNEL_LIMIT_EXCEEDED = "channel_limit_exceeded"  # 标的渠道数超过配置上限
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
class MergeRecord:
    """跨渠道合并/冲突的审计记录（不可变）。

    * ``kind="merged"``：同一笔成交（同 symbol + trade_id）的不同渠道副本，
      关键内容（价格、数量）一致，``winner`` 副本被计入聚合，其余副本被合并；
    * ``kind="conflict"``：关键内容不一致，获胜副本计入聚合，失败副本进入
      隔离区，原因 :data:`RejectReason.CROSS_SOURCE_CONFLICT`。

    ``winner`` 为获胜渠道，``loser`` 为该次判定处理的副本渠道；
    一条成交涉及多渠道时，每个失败副本各留一条记录。
    """

    symbol: str
    trade_id: str
    kind: str  # "merged" | "conflict"
    winner: str
    loser: str
    winner_event_id: str
    loser_event_id: str
    decided_at_ms: int
    #: 失败副本的完整事件负载。一致合并副本不落事件日志，借此在重启后
    #: 重建合并组身份与去重表；冲突副本同时也在 quarantine.jsonl 中。
    loser_event_payload: object = None


@dataclass(frozen=True, slots=True)
class IngestResult:
    """单条/批次写入的判定结果（全部原子成功，或全部不生效）。"""

    accepted: int
    duplicate: int
    quarantined: int
    rejected: int
    merged: int = 0  # 跨渠道一致合并掉的副本数
    details: tuple[tuple[str, RejectReason, str], ...] = ()

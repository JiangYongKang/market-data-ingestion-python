"""行情结构兼容演进：解析、缺省填充、废弃/未知/类型规则。

判定顺序（确定、可解释）：
1. schema_version：仅接受 "1"；缺失按 "1" 处理；其他版本拒绝；
2. 未知字段：``reject_unknown_fields=True`` 时拒绝（首个未知字段为准，
   按字段名字典序判定，保证稳定）；废弃字段一律剥离并计数，不拒绝；
3. 必填字段缺失 -> 拒绝；
4. 类型不匹配 -> 拒绝（bool 不视为 int；NaN/Inf 不视为合法数字）；
5. 通过后用 ``DEFAULTS`` 填充新增可选字段（缺省即 None=未知，
   绝不使用 0 之类的伪装业务值），得到不可变 :class:`Event`。
"""
from __future__ import annotations

import math
from typing import Any

from .config import Config
from .errors import SchemaError
from .models import DEFAULTS, DEPRECATED, Event, KNOWN_FIELDS, SCHEMA_CURRENT, RejectReason

_WRAPPER_FIELDS = frozenset({"event_id", "source", "seq", "schema_version"})
_REQUIRED = ("event_id", "source", "seq", "symbol", "price", "quantity", "event_time_ms")
_INT_FIELDS = frozenset({"seq", "event_time_ms"})
_FLOAT_FIELDS = frozenset({"price", "quantity"})
_STR_FIELDS = frozenset({"event_id", "source", "symbol"})
_OPT_STR_FIELDS = frozenset({"trade_id", "venue"})


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _is_num(v: Any) -> bool:
    return (isinstance(v, (int, float)) and not isinstance(v, bool)
            and math.isfinite(float(v)))


def _is_opt_str(v: Any) -> bool:
    return v is None or isinstance(v, str)


def parse_event(raw: dict, config: Config) -> Event:
    """严格解析；任何不兼容直接抛 :class:`SchemaError`（带可区分原因）。"""
    if not isinstance(raw, dict):
        raise SchemaError(RejectReason.SCHEMA_TYPE_MISMATCH,
                          f"事件必须是对象，实际为 {type(raw).__name__}")

    eid = raw.get("event_id") if isinstance(raw.get("event_id"), str) else None

    version = raw.get("schema_version", SCHEMA_CURRENT)
    if not isinstance(version, str) or version != SCHEMA_CURRENT:
        raise SchemaError(RejectReason.SCHEMA_VERSION_UNSUPPORTED,
                          f"不支持的 schema_version={version!r}，当前仅支持 {SCHEMA_CURRENT!r}",
                          event_id=eid)

    # 未知字段（确定性：按名称排序后取首个）；废弃字段不报错。
    allowed = KNOWN_FIELDS | _WRAPPER_FIELDS | DEPRECATED
    unknown = sorted(k for k in raw if k not in allowed)
    if unknown and config.reject_unknown_fields:
        raise SchemaError(RejectReason.SCHEMA_UNKNOWN_FIELD,
                          f"未知字段 {unknown[0]!r}（共 {len(unknown)} 个）；"
                          f"新增字段须先在协议中声明缺省语义",
                          event_id=eid)

    for name in _REQUIRED:
        if name not in raw or raw[name] is None:
            raise SchemaError(RejectReason.SCHEMA_MISSING_FIELD,
                              f"缺少必填字段 {name!r}", event_id=eid)

    for name in _STR_FIELDS & raw.keys():
        if not isinstance(raw[name], str):
            raise SchemaError(RejectReason.SCHEMA_TYPE_MISMATCH,
                           f"字段 {name!r} 应为 str，实际为 {type(raw[name]).__name__}",
                           event_id=eid)
    for name in _OPT_STR_FIELDS & raw.keys():
        v = raw[name]
        if v is not None and not isinstance(v, str):
            raise SchemaError(RejectReason.SCHEMA_TYPE_MISMATCH,
                              f"字段 {name!r} 应为 str|null，实际为 {type(v).__name__}",
                              event_id=eid)
    for name in _INT_FIELDS:
        if not _is_int(raw[name]):
            raise SchemaError(RejectReason.SCHEMA_TYPE_MISMATCH,
                              f"字段 {name!r} 应为 int，实际为 {type(raw[name]).__name__}",
                              event_id=eid)
    for name in _FLOAT_FIELDS:
        if not _is_num(raw[name]):
            raise SchemaError(RejectReason.SCHEMA_TYPE_MISMATCH,
                              f"字段 {name!r} 应为有限数字，实际为 {raw[name]!r}",
                              event_id=eid)

    if raw["seq"] < 0:
        raise SchemaError(RejectReason.SCHEMA_TYPE_MISMATCH,
                          "字段 'seq' 不得为负", event_id=eid)
    if raw["event_time_ms"] < 0:
        raise SchemaError(RejectReason.SCHEMA_TYPE_MISMATCH,
                          "字段 'event_time_ms' 不得为负", event_id=eid)
    if raw["price"] < 0:
        raise SchemaError(RejectReason.SCHEMA_TYPE_MISMATCH,
                          "字段 'price' 不得为负", event_id=eid)
    if raw["quantity"] <= 0:
        raise SchemaError(RejectReason.SCHEMA_TYPE_MISMATCH,
                          "字段 'quantity' 必须为正数", event_id=eid)

    # 新增可选字段：稳定缺省（None=未知，不参与聚合）
    optional = {k: raw.get(k, DEFAULTS[k]) for k in DEFAULTS}
    for k, v in optional.items():
        if not _is_opt_str(v):
            raise SchemaError(RejectReason.SCHEMA_TYPE_MISMATCH,
                              f"字段 {k!r} 应为 str|null，实际为 {type(v).__name__}",
                              event_id=eid)

    return Event(
        event_id=raw["event_id"],
        source=raw["source"],
        seq=raw["seq"],
        symbol=raw["symbol"],
        price=float(raw["price"]),
        quantity=float(raw["quantity"]),
        event_time_ms=raw["event_time_ms"],
        schema_version=version,
        trade_id=optional["trade_id"],
        venue=optional["venue"],
    )


def count_deprecated(raw: dict) -> int:
    return sum(1 for k in raw if k in DEPRECATED)


def parse_event_safe(raw: dict, config: Config):
    """不抛异常的解析：(Event|None, (event_id, reason, detail)|None, deprecated_count)。

    未知字段在严格模式下拒绝；非严格模式下未知字段被忽略（确定性）。
    废弃字段始终剥离并计数。
    """
    if not isinstance(raw, dict):
        return None, (None, RejectReason.SCHEMA_TYPE_MISMATCH,
                      f"事件必须是对象，实际为 {type(raw).__name__}"), 0
    deprecated = count_deprecated(raw)
    if not config.reject_unknown_fields:
        allowed = KNOWN_FIELDS | _WRAPPER_FIELDS | DEPRECATED
        raw = {k: v for k, v in raw.items() if k in allowed}
        deprecated = sum(1 for k in raw if k in DEPRECATED)
    try:
        return parse_event(raw, config), None, deprecated
    except SchemaError as exc:
        eid = exc.event_id
        return None, (eid, exc.reason, str(exc)), deprecated

"""结构演进专题：新增字段缺省语义确定；未知/废弃/类型不匹配规则明确。"""
from __future__ import annotations

import pytest

from market_data.config import Config
from market_data.errors import IngestionError
from market_data.models import RejectReason
from market_data.schema import parse_event
from market_data.service import MarketDataService

from .conftest import make_event


BASE = dict(event_id="e1", source="s", seq=1, symbol="A",
            price=10, quantity=2, event_time_ms=1000)


def test_new_optional_fields_have_stable_none_default():
    ev = parse_event(dict(BASE), Config())
    assert ev.trade_id is None and ev.venue is None
    assert ev.schema_version == "1"
    # 缺省值绝不是伪装的业务值：VWAP 只用 price/quantity，不受影响
    ev2 = parse_event({**BASE, "trade_id": "T-1", "venue": "X"}, Config())
    assert ev2.trade_id == "T-1" and ev2.venue == "X"


def test_unknown_field_rejected_by_default_with_clear_reason():
    svc = MarketDataService(Config(data_dir=":memory:"))
    with pytest.raises(IngestionError) as exc:
        svc.ingest_sync({**BASE, "event_id": "u1", "mystery_field": 123})
    assert exc.value.reason is RejectReason.SCHEMA_UNKNOWN_FIELD
    assert "mystery_field" in str(exc.value)


def test_unknown_field_can_be_explicitly_ignored(tmp_path):
    cfg = Config(data_dir=str(tmp_path), reject_unknown_fields=False)
    svc = MarketDataService(cfg)
    r = svc.ingest_sync({**BASE, "event_id": "u2", "future": {"a": 1}})
    assert r.accepted == 1  # 显式选择忽略，不静默：配置可见


def test_deprecated_field_accepted_stripped_and_counted(service):
    r = service.ingest_sync(make_event("dp1", 1_000, 10, 1, exchange_code="OLD"))
    assert r.accepted == 1
    assert service.metrics_snapshot()["deprecated_seen"] == 1


def test_type_mismatches_are_classified():
    cases = [
        {"price": "10"},
        {"price": True},
        {"price": float("nan")},
        {"price": float("inf")},
        {"quantity": 0},
        {"quantity": -1},
        {"seq": 1.5},
        {"seq": True},
        {"event_time_ms": -1},
        {"symbol": 42},
        {"venue": 9},
    ]
    for bad in cases:
        svc = MarketDataService(Config(data_dir=":memory:"))
        with pytest.raises(IngestionError) as exc:
            svc.ingest_sync({**BASE, **bad})
        assert exc.value.reason in (
            RejectReason.SCHEMA_TYPE_MISMATCH,
            RejectReason.SCHEMA_MISSING_FIELD,
        ), bad


def test_missing_required_fields_rejected():
    for missing in ["event_id", "source", "seq", "symbol", "price",
                    "quantity", "event_time_ms"]:
        payload = {k: v for k, v in BASE.items() if k != missing}
        svc = MarketDataService(Config(data_dir=":memory:"))
        with pytest.raises(IngestionError) as exc:
            svc.ingest_sync(payload)
        assert exc.value.reason is RejectReason.SCHEMA_MISSING_FIELD, missing


def test_unsupported_schema_version_rejected():
    svc = MarketDataService(Config(data_dir=":memory:"))
    with pytest.raises(IngestionError) as exc:
        svc.ingest_sync({**BASE, "schema_version": "2"})
    assert exc.value.reason is RejectReason.SCHEMA_VERSION_UNSUPPORTED


def test_non_object_payload_rejected(service):
    with pytest.raises(IngestionError) as exc:
        service.ingest_sync([42])
    assert exc.value.reason is RejectReason.SCHEMA_TYPE_MISMATCH


def test_schema_failure_aborts_whole_batch_without_side_effects(service):
    good = make_event("ok1", 2_000, 10, 1, seq=2)
    bad = {"event_id": "bad1", "source": "s", "seq": 3, "symbol": "A",
           "price": "not-a-number", "quantity": 1, "event_time_ms": 3_000}
    with pytest.raises(IngestionError) as exc:
        service.ingest_sync([good, bad])
    assert exc.value.reason is RejectReason.SCHEMA_TYPE_MISMATCH
    assert service.query_provisional("A", 0) is None  # good 也未生效

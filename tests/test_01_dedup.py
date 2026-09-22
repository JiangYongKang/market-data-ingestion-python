"""重复投递专题：普通重复与内容冲突必须被正确归类且原因可区分。"""
from __future__ import annotations

import logging

import pytest

from market_data.errors import IngestionError
from market_data.models import RejectReason

from .conftest import make_event


def test_identical_redelivery_is_idempotent(service, caplog):
    r1 = service.ingest_sync(make_event("e1", 1_000, 10, 3))
    assert r1.accepted == 1

    with caplog.at_level(logging.INFO, logger="market_data"):
        r2 = service.ingest_sync(make_event("e1", 1_000, 10, 3))

    assert r2.accepted == 0
    assert r2.duplicate == 1
    assert r2.details[0][1] is RejectReason.DUPLICATE_IDENTICAL
    # 聚合未被重复计入
    prov = service.query_provisional("A", 0)
    assert prov.count == 1 and prov.total_quantity == 3
    # 日志含事件标识/事件时间/水位/判定依据
    msg = caplog.text
    assert "event_id=e1" in msg and "event_time_ms=1000" in msg
    assert "duplicate_identical" in msg and "watermark_ms=" in msg


def test_conflicting_redelivery_rejected_and_distinguished(service):
    service.ingest_sync(make_event("e1", 1_000, 10, 3))
    with pytest.raises(IngestionError) as exc:
        service.ingest_sync(make_event("e1", 1_000, 99, 3))
    assert exc.value.reason is RejectReason.DUPLICATE_CONFLICT
    # 冲突不得污染聚合：窗口内仍是原始事件
    prov = service.query_provisional("A", 0)
    assert prov.vwap == 10.0 and prov.count == 1


def test_conflict_on_each_business_field(service):
    service.ingest_sync(make_event("e1", 1_000, 10, 3, venue="X"))
    for changed in [
        {"p": 11}, {"q": 4}, {"t": 2_000}, {"symbol": "B"},
        {"venue": "Y"}, {"source": "s2"},
    ]:
        kw = {"p": changed.get("p", 10), "q": changed.get("q", 3),
              "t": changed.get("t", 1_000), "symbol": changed.get("symbol", "A"),
              "venue": changed.get("venue", "X"), "source": changed.get("source", "s")}
        with pytest.raises(IngestionError) as exc:
            service.ingest_sync(make_event("e1", seq=1, **kw))
        assert exc.value.reason is RejectReason.DUPLICATE_CONFLICT, changed


def test_source_seq_duplicate_with_different_event_id_is_conflict(service):
    service.ingest_sync(make_event("e1", 1_000, 10, 3, seq=7))
    with pytest.raises(IngestionError) as exc:
        service.ingest_sync(make_event("e2", 1_000, 10, 3, seq=7))
    assert exc.value.reason is RejectReason.DUPLICATE_CONFLICT


def test_batch_internal_identical_duplicate_counts_once(service):
    ev = make_event("e1", 1_000, 10, 3)
    r = service.ingest_sync([dict(ev), dict(ev)])
    assert r.accepted == 1 and r.duplicate == 1
    assert service.query_provisional("A", 0).count == 1


def test_batch_internal_conflict_aborts_whole_batch(service):
    good = make_event("new9", 2_000, 20, 1, seq=9)
    bad = make_event("new9", 2_000, 21, 1, seq=9)  # 同 id 不同价
    with pytest.raises(IngestionError) as exc:
        service.ingest_sync([good, bad])
    assert exc.value.reason is RejectReason.DUPLICATE_CONFLICT
    # 整批不生效
    assert service.query_provisional("A", 0) is None
    # 之后 good 仍可正常写入（无身份残留）
    assert service.ingest_sync(make_event("new9", 2_000, 20, 1, seq=9)).accepted == 1


def test_repeated_replay_does_not_change_published_result(service):
    from .conftest import advance_and_publish
    service.ingest_sync([
        make_event("e1", 1_000, 10, 3),
        make_event("e2", 5_000, 20, 1),
    ])
    advance_and_publish(service, 20_000)
    w = service.query("A", 0)[0]
    # 整体重放 3 遍
    for _ in range(3):
        r = service.ingest_sync([
            make_event("e1", 1_000, 10, 3),
            make_event("e2", 5_000, 20, 1),
        ])
        assert r.accepted == 0 and r.duplicate == 2
        again = service.query("A", 0)[0]
        assert again.vwap == w.vwap and again.event_ids == w.event_ids
        assert again.count == 2

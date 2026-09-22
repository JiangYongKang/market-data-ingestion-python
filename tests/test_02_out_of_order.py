"""乱序/迟到专题：窗口内修正正确；超水位进隔离区、不改已发布结果。"""
from __future__ import annotations

import pytest

from market_data.errors import IngestionError
from market_data.models import RejectReason

from .conftest import advance_and_publish, make_event


VWAP_E123 = (10 * 3 + 20 * 1 + 30 * 2) / 6  # 110/6


def test_within_window_out_of_order_recomputes(service):
    # 故意逆序投递
    r = service.ingest_sync([
        make_event("e3", 9_000, 30, 2),
        make_event("e1", 1_000, 10, 3),
        make_event("e2", 5_000, 20, 1),
    ])
    assert r.accepted == 3
    prov = service.query_provisional("A", 0)
    assert prov.event_ids == ("e1", "e2", "e3")  # 按(时间,id)排序
    assert prov.vwap == pytest.approx(VWAP_E123)
    assert prov.volatility > 0


def test_order_independence_two_ingestions(config):
    """两条到达顺序路径产出逐位一致的窗口结果。"""
    from market_data.service import MarketDataService
    events = [
        make_event("e3", 9_000, 30, 2),
        make_event("e1", 1_000, 10, 3),
        make_event("e2", 5_000, 20, 1),
    ]
    s1 = MarketDataService(Config := config)
    s1.ingest_sync(list(events))
    advance_and_publish(s1, 20_000)
    w1 = s1.query("A", 0)[0]

    s2 = MarketDataService(config)
    s2.ingest_sync(list(reversed(events)))
    advance_and_publish(s2, 20_000)
    w2 = s2.query("A", 0)[0]

    assert (w1.vwap, w1.volatility, w1.event_ids, w1.total_quantity) == \
           (w2.vwap, w2.volatility, w2.event_ids, w2.total_quantity)


def test_late_within_allowed_lateness_corrects_open_window(service):
    service.ingest_sync([make_event("e1", 1_000, 10, 1)])
    # 推进到 10s：水位 5s；t=6s 的迟到仍在允许窗口内
    service.ingest_sync([make_event("fwd", 10_000, 10, 1, seq=50)])
    assert service.watermark_ms == 5_000
    r = service.ingest_sync([make_event("late_ok", 6_000, 40, 1, seq=51)])
    assert r.accepted == 1 and r.quarantined == 0
    prov = service.query_provisional("A", 0)
    assert prov.event_ids == ("e1", "late_ok")


def test_beyond_watermark_goes_to_quarantine(service, caplog):
    service.ingest_sync([
        make_event("e1", 1_000, 10, 1),
        make_event("e2", 2_000, 11, 1),
    ])
    advance_and_publish(service, 20_000)  # wm=15_000，窗口0已发布
    before = service.query("A", 0)[0]

    r = service.ingest_sync([make_event("lateX", 3_000, 99, 100, seq=90)])
    assert r.accepted == 0 and r.quarantined == 1
    assert r.details[0][1] is RejectReason.LATE_BEYOND_WATERMARK

    q = service.quarantine_list("A")
    assert len(q) == 1
    rec = q[0]
    assert rec.event.event_id == "lateX"
    assert rec.reason is RejectReason.LATE_BEYOND_WATERMARK
    assert rec.watermark_ms == 15_000
    assert "3000" in rec.detail and "15000" in rec.detail
    # 已发布结果不变
    after = service.query("A", 0)[0]
    assert after.vwap == before.vwap and after.count == before.count
    # 日志可解释
    assert "lateX" in caplog.text and "late_beyond_watermark" in caplog.text


def test_event_for_published_window_quarantined_even_above_global_wm(service):
    # 窗口0发布后，即使全局水位不高，也不能改写已发布窗口
    service.ingest_sync([make_event("e1", 1_000, 10, 1)])
    service.ingest_sync([make_event("fwd", 10_000, 10, 1, seq=50)])
    # 水位=5s，窗口0右边界-1=9999 > 5s，尚未发布；推进到15s发布
    service.ingest_sync([make_event("fwd2", 15_000, 10, 1, seq=51)])
    assert service.query("A", 0)[0].published is True
    # t=12_000 的事件属于窗口10s（未发布），正常接纳
    r = service.ingest_sync([make_event("in10", 12_000, 20, 1, seq=60)])
    assert r.accepted == 1
    # t=2_000 属于窗口0（已发布），隔离
    r = service.ingest_sync([make_event("old0", 2_000, 20, 1, seq=61)])
    assert r.quarantined == 1


def test_cross_window_disorder_in_single_batch(service):
    # 同一批次中两个窗口的事件乱序，均应正确落桶，互不误判
    r = service.ingest_sync([
        make_event("b1", 25_000, 30, 1, seq=22),
        make_event("a1", 21_000, 10, 2, seq=21),
        make_event("b2", 28_000, 40, 1, seq=23),
    ])
    assert r.accepted == 3 and r.quarantined == 0
    w20 = service.query_provisional("A", 20_000)
    assert w20.event_ids == ("a1", "b1", "b2")
    assert w20.vwap == pytest.approx((10 * 2 + 30 + 40) / 4)


def test_quarantine_queryable_and_durable(config):
    from market_data.service import MarketDataService
    svc = MarketDataService(config)
    svc.ingest_sync([make_event("e1", 1_000, 10, 1)])
    advance_and_publish(svc, 20_000)
    svc.ingest_sync([make_event("L1", 1_000, 1, 1, seq=88)])
    assert len(svc.quarantine_list("A")) == 1
    # 重启后隔离记录仍可查询
    svc2 = MarketDataService(config)
    q = svc2.quarantine_list("A")
    assert len(q) == 1 and q[0].event.event_id == "L1"
    # 重投被隔离的事件：身份已知，按幂等重复处理
    r = svc2.ingest_sync([make_event("L1", 1_000, 1, 1, seq=88)])
    assert r.duplicate == 1

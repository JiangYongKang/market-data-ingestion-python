"""窗口特征专题：VWAP/波动定义可解释、确定性、边界与单事件退化。"""
from __future__ import annotations

import math

import pytest

from .conftest import advance_and_publish, make_event


def test_vwap_is_quantity_weighted(service):
    service.ingest_sync([
        make_event("a", 1_000, 10.0, 3.0, seq=1),
        make_event("b", 2_000, 20.0, 1.0, seq=2),
    ])
    advance_and_publish(service, 20_000)
    w = service.query("A", 0)[0]
    # (10*3 + 20*1)/4
    assert w.vwap == pytest.approx(12.5)
    assert w.total_quantity == 4.0 and w.count == 2


def test_volatility_zero_for_single_price(service):
    service.ingest_sync([make_event("solo", 1_000, 42.0, 7.0, seq=1)])
    advance_and_publish(service, 20_000)
    w = service.query("A", 0)[0]
    assert w.volatility == 0.0 and w.vwap == 42.0


def test_volatility_is_quantity_weighted_spread(service):
    # 两笔等权价格 10 和 20 -> VWAP 15，加权总体标准差 5
    service.ingest_sync([
        make_event("lo", 1_000, 10.0, 1.0, seq=1),
        make_event("hi", 2_000, 20.0, 1.0, seq=2),
    ])
    advance_and_publish(service, 20_000)
    w = service.query("A", 0)[0]
    assert w.vwap == pytest.approx(15.0)
    assert w.volatility == pytest.approx(5.0)


def test_windows_are_fixed_tumbling_and_symbol_isolated(service):
    service.ingest_sync([
        make_event("a1", 1_000, 10, 1, symbol="A", seq=1, source="srcA"),
        make_event("b1", 1_000, 30, 1, symbol="B", seq=1, source="srcB"),
        make_event("a2", 10_500, 12, 1, symbol="A", seq=2, source="srcA"),
    ])
    advance_and_publish(service, 30_000)
    a0 = service.query("A", 0)[0]
    a10 = service.query("A", 10_000)[0]
    b0 = service.query("B", 0)[0]
    assert a0.event_ids == ("a1",) and a10.event_ids == ("a2",)
    assert b0.event_ids == ("b1",) and b0.vwap == 30.0


def test_window_boundary_left_closed_right_open(service):
    service.ingest_sync([
        make_event("edge0", 0, 10, 1, seq=1),
        make_event("edge10", 10_000, 20, 1, seq=2),
        make_event("edge30", 30_000, 20, 1, seq=3),
    ])
    assert service.query("A", 0)[0].event_ids == ("edge0",)
    assert service.query("A", 10_000)[0].event_ids == ("edge10",)


def test_published_features_are_finite_and_deterministic(service):
    prices = [10, 12, 9, 15, 11, 8, 14, 13]
    service.ingest_sync([
        make_event(f"t{i:02d}", 500 + i * 100, float(p), 1.0 + (i % 4), seq=i)
        for i, p in enumerate(prices)])
    advance_and_publish(service, 20_000)
    w = service.query("A", 0)[0]
    assert math.isfinite(w.vwap) and math.isfinite(w.volatility)
    assert w.volatility >= 0
    # 明细可审计：事件标识有序
    assert list(w.event_ids) == sorted(w.event_ids)

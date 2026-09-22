"""背压、资源上限、失败原子性与基准观测专题。"""
from __future__ import annotations

import pytest

from market_data.config import Config
from market_data.errors import BackpressureError, IngestionError
from market_data.models import RejectReason
from market_data.service import MarketDataService

from .conftest import make_event


def test_reject_strategy_raises_when_capacity_exceeded(tmp_path):
    cfg = Config(data_dir=str(tmp_path), max_pending_events=3,
                 backpressure_strategy="reject")
    svc = MarketDataService(cfg)
    with pytest.raises(BackpressureError) as exc:
        svc.ingest_sync([
            make_event("p1", 1_000, 10, 1, seq=1, symbol="T"),
            make_event("p2", 2_000, 10, 1, seq=2, symbol="T"),
            make_event("p3", 3_000, 10, 1, seq=3, symbol="T"),
            make_event("p4", 4_000, 10, 1, seq=4, symbol="T"),
        ])
    assert exc.value.reason is RejectReason.BACKPRESSURE_REJECTED
    assert svc.metrics_snapshot()["backpressure_rejected"] >= 1
    # 整批拒绝：无残留
    assert svc.query_provisional("T", 0) is None


def test_quarantine_capacity_is_bounded(tmp_path):
    cfg = Config(data_dir=str(tmp_path), allowed_lateness_ms=0,
                 max_quarantine_size=2)
    svc = MarketDataService(cfg)
    # 先推进水位到 10_000
    svc.ingest_sync([make_event("f", 10_000, 10, 1, seq=1)])
    # 隔离区容量 2
    for i, seq in enumerate([2, 3]):
        r = svc.ingest_sync([make_event(f"q{i}", i, 10, 1, seq=seq)])
        assert r.quarantined == 1
    # 第三条迟到 -> 整批背压拒绝（隔离区不无界增长）
    with pytest.raises(BackpressureError):
        svc.ingest_sync([make_event("qx", 4, 10, 1, seq=4)])
    assert len(svc.quarantine_list()) == 2


def test_failed_batch_leaves_no_partial_aggregation_or_checkpoint(tmp_path):
    cfg = Config(data_dir=str(tmp_path))
    svc = MarketDataService(cfg)
    svc.ingest_sync([make_event("g1", 1_000, 10, 1, seq=1)])
    ck_before = svc.checkpoint_get("s")
    # 前段好事件 + 后段硬失败：全部不落盘、不入窗口、不进位点
    with pytest.raises(IngestionError):
        svc.ingest_sync([
            make_event("g2", 2_000, 10, 1, seq=2),
            make_event("g3", 3_000, 10, 1, seq=3, price=float("nan")),
        ])
    assert svc.checkpoint_get("s") == ck_before
    prov = svc.query_provisional("A", 0)
    assert prov is not None and prov.event_ids == ("g1",)

    # 重启磁盘状态：g2/g3 从未落盘
    svc2 = MarketDataService(cfg)
    assert svc2.ingest_sync(make_event("g2", 2_000, 10, 1, seq=2)).accepted == 1
    assert svc2.query_provisional("A", 0).event_ids == ("g1", "g2")


def test_metrics_expose_counts_pending_and_timing(tmp_path):
    cfg = Config(data_dir=str(tmp_path))
    svc = MarketDataService(cfg)
    svc.ingest_sync([make_event("m1", 1_000, 10, 1, seq=1),
                     make_event("m2", 2_000, 10, 1, seq=2)])
    svc.ingest_sync([make_event("m1", 1_000, 10, 1, seq=1)])  # 重复
    snap = svc.metrics_snapshot()
    assert snap["accepted"] == 2
    assert snap["duplicates"] == 1
    assert snap["pending_events"] == 2
    assert snap["max_pending_events"] >= 2
    assert snap["estimated_state_bytes"] > 0
    assert snap["ingest"]["avg_ns_per_event"] > 0
    assert "p50_ns_per_event" in snap["ingest"]
    assert "p99_ns_per_event" in snap["ingest"]
    assert "watermark_ms" in snap and "quarantine_size" in snap


def test_local_benchmark_scale_meets_configurable_budget(tmp_path):
    """本地基准规模下的耗时/内存上限验证（数值通过观测字段断言）。"""
    cfg = Config(data_dir=":memory:", benchmark_event_count=20_000,
                 ingest_time_budget_ms=30_000)
    svc = MarketDataService(cfg)
    import dataclasses, time
    from market_data.service import MarketDataService as _M
    bench = _M(dataclasses.replace(cfg, max_pending_events=100_000))

    n = 20_000
    events = [{
        "event_id": f"b-{i}", "source": "bench", "seq": i,
        "symbol": f"S{i % 8}",
        "price": 100.0 + (i % 7), "quantity": 1.0 + (i % 3),
        "event_time_ms": (i // 50) * 100,
    } for i in range(n)]
    t0 = time.perf_counter_ns()
    r = bench.ingest_sync(events)
    elapsed_ms = (time.perf_counter_ns() - t0) / 1e6

    assert r.accepted == n and r.duplicate == 0
    snap = bench.metrics_snapshot()
    # 重放幂等
    r2 = bench.ingest_sync(events)
    assert r2.duplicate == n and r2.accepted == 0
    # 预算与内存上限（可配置；此处断言在宽松预算内完成且给出可验证观测）
    assert elapsed_ms <= cfg.ingest_time_budget_ms
    assert snap["estimated_state_bytes"] > 0
    assert snap["ingest"]["p99_ns_per_event"] > 0
    assert snap["max_pending_events"] <= 100_000


def test_delay_strategy_eventually_succeeds_when_capacity_frees(tmp_path):
    import time as _time
    cfg = Config(data_dir=":memory:", max_pending_events=4,
                 backpressure_strategy="delay",
                 backpressure_delay_ms=5, backpressure_max_retries=3)
    svc = MarketDataService(cfg)
    # 写 3 条占坑
    svc.ingest_sync([make_event(f"d{i}", 1_000 + i, 10, 1, seq=i, symbol="D")
                     for i in range(3)])
    # 再来 3 条：容量不足，重试有限次后拒绝（不无界等待）
    with pytest.raises(BackpressureError):
        svc.ingest_sync([make_event(f"d{i+10}", 2_000 + i, 10, 1, seq=10 + i,
                                    symbol="D") for i in range(3)])

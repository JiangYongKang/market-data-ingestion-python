"""检查点/重放专题：位点单调；回退明确拒绝；重启结果可再现。"""
from __future__ import annotations

import pytest

from market_data.errors import CheckpointRollbackError, IngestionError
from market_data.models import RejectReason
from market_data.service import MarketDataService

from .conftest import advance_and_publish, make_event


def test_checkpoint_advances_monotonically(service):
    assert service.checkpoint_get("s") == -1
    assert service.checkpoint("s", 10) is True
    assert service.checkpoint("s", 10) is False  # 同值幂等
    assert service.checkpoint("s", 11) is True
    assert service.checkpoint_get("s") == 11


def test_checkpoint_rollback_explicitly_rejected(service):
    service.checkpoint("s", 100)
    with pytest.raises(CheckpointRollbackError) as exc:
        service.checkpoint("s", 99)
    assert exc.value.reason is RejectReason.CHECKPOINT_ROLLBACK
    assert exc.value.requested == 99 and exc.value.current == 100
    # 拒绝后位点不变
    assert service.checkpoint_get("s") == 100


def test_checkpoints_are_isolated_per_source(service):
    service.checkpoint("a", 5)
    service.checkpoint("b", 50)
    assert service.checkpoint_get("a") == 5
    assert service.checkpoint_get("b") == 50
    with pytest.raises(CheckpointRollbackError):
        service.checkpoint("b", 49)
    assert service.checkpoints() == {"a": 5, "b": 50}


def test_ingest_advances_checkpoint_only_on_accepted_or_quarantined(service):
    service.ingest_sync([
        make_event("e1", 1_000, 10, 1, seq=10),
        make_event("e2", 2_000, 11, 1, seq=11),
    ])
    assert service.checkpoint_get("s") == 11
    # 纯重复不回退位点，也不需要再推进
    r = service.ingest_sync(make_event("e1", 1_000, 10, 1, seq=10))
    assert r.duplicate == 1
    assert service.checkpoint_get("s") == 11


def test_rejected_batch_does_not_advance_checkpoint(service):
    service.ingest_sync([make_event("e1", 1_000, 10, 1, seq=1)])
    # 批次中含结构错误 -> 整批拒绝，位点不动
    with pytest.raises(IngestionError):
        service.ingest_sync([
            make_event("e9", 3_000, 10, 1, seq=9),
            {"event_id": "bad", "source": "s", "seq": 10, "symbol": "A",
             "price": "x", "quantity": 1, "event_time_ms": 4_000},
        ])
    assert service.checkpoint_get("s") == 1


def test_restart_reproduces_published_results_and_checkpoints(config):
    svc = MarketDataService(config)
    svc.ingest_sync([
        make_event("e1", 1_000, 10, 3),
        make_event("e2", 5_000, 20, 1),
        make_event("e3", 9_000, 30, 2),
    ])
    advance_and_publish(svc, 20_000)
    w_before = svc.query("A", 0)[0]
    ck_before = svc.checkpoints()

    svc2 = MarketDataService(config)
    w_after = svc2.query("A", 0)[0]
    assert w_after.event_ids == w_before.event_ids
    assert w_after.vwap == w_before.vwap
    assert w_after.volatility == w_before.volatility
    assert w_after.count == w_before.count
    assert svc2.checkpoints() == ck_before


def test_replay_after_restart_is_idempotent_and_complete(config):
    svc = MarketDataService(config)
    payload = [
        make_event("e1", 1_000, 10, 3, seq=1),
        make_event("e2", 5_000, 20, 1, seq=2),
    ]
    svc.ingest_sync(payload)
    advance_and_publish(svc, 20_000)
    published_before = svc.query("A", 0)[0]

    # 模拟重放方从位点续传 + 整段重复重放（进程重启后）
    svc2 = MarketDataService(config)
    r = svc2.ingest_sync(payload)  # 事件已在日志，去重表恢复
    assert r.accepted == 0 and r.duplicate == 2
    again = svc2.query("A", 0)[0]
    assert again.event_ids == published_before.event_ids
    assert again.vwap == published_before.vwap


def test_rollback_request_then_forward_replay_does_not_duplicate(service):
    service.ingest_sync([
        make_event("e1", 1_000, 10, 1, seq=1),
        make_event("e2", 2_000, 11, 1, seq=2),
    ])
    # 试图把位点拉回去重放 -> 必须拒绝，不产生重复
    with pytest.raises(CheckpointRollbackError):
        service.checkpoint("s", 0)
    r = service.ingest_sync([make_event("e1", 1_000, 10, 1, seq=1)])
    assert r.duplicate == 1
    assert service.query_provisional("A", 0).count == 2

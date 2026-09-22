"""资源留存专题：已发布事件指纹压缩，去重不失效、内存不无界增长。"""
from __future__ import annotations

from .conftest import advance_and_publish, make_event


def test_published_event_fingerprints_compact_but_identity_retained(service):
    service.ingest_sync([make_event("e1", 1_000, 10, 1, seq=1),
                         make_event("e2", 2_000, 11, 1, seq=2)])
    assert service._dedup.full_fingerprint_count == 2
    advance_and_publish(service, 20_000)
    # 发布后压缩：身份键仍在，业务指纹已压缩（仅剩哨兵自己的未发布指纹）
    assert service._dedup.full_fingerprint_count == 1
    assert not service._dedup._by_id["e1"] != service._dedup._PUBLISHED
    assert not service._dedup._by_id["e2"] != service._dedup._PUBLISHED
    assert service._dedup.known("e1") and service._dedup.known("e2")
    # 重投仍幂等识别为重复（绝不二次计入）
    r = service.ingest_sync([make_event("e1", 1_000, 10, 1, seq=1),
                             make_event("e2", 2_000, 11, 1, seq=2)])
    assert r.duplicate == 2 and r.accepted == 0
    assert service.query("A", 0)[0].count == 2


def test_compaction_survives_restart_and_state_is_bounded_per_open_window(config):
    from market_data.service import MarketDataService
    svc = MarketDataService(config)
    # 两个窗口：第一个发布后压缩；第二个保持完整指纹
    svc.ingest_sync([make_event("o1", 1_000, 10, 1, seq=1),
                     make_event("o2", 2_000, 11, 1, seq=2),
                     make_event("n1", 11_000, 12, 1, seq=3)])
    advance_and_publish(svc, 20_000)
    # 完整指纹只剩：未发布窗口的 n1 + 哨兵自己
    assert svc._dedup.full_fingerprint_count == 2
    assert svc._dedup._by_id["n1"] != svc._dedup._PUBLISHED
    assert svc._dedup._by_id["o1"] == svc._dedup._PUBLISHED

    svc2 = MarketDataService(config)
    assert svc2._dedup.known("o1")
    assert svc2._dedup.known("o2")
    # 重启后压缩状态恢复：o1/o2 重投重复，且结果窗口不变
    r = svc2.ingest_sync([make_event("o1", 1_000, 10, 1, seq=1)])
    assert r.duplicate == 1
    assert svc2.query("A", 0)[0].event_ids == ("o1", "o2")


def test_unpublished_conflict_still_detected_after_other_window_published(service):
    # 窗口0事件发布压缩后，窗口10s内的未发布事件仍具备完整冲突检测
    service.ingest_sync([make_event("p1", 1_000, 10, 1, seq=1),
                         make_event("u1", 12_000, 20, 1, seq=2)])
    advance_and_publish(service, 20_000)
    import pytest
    from market_data.errors import IngestionError
    from market_data.models import RejectReason
    with pytest.raises(IngestionError) as exc:
        service.ingest_sync([make_event("u1", 12_000, 99, 1, seq=2)])
    assert exc.value.reason is RejectReason.DUPLICATE_CONFLICT

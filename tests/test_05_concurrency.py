"""并发专题：同一标的并发写入/重放/查询一致，无部分可见、重复计数、位点跳变。"""
from __future__ import annotations

import asyncio

import pytest

from market_data.errors import BackpressureError, IngestionError
from market_data.models import RejectReason

from .conftest import make_event


async def _gather(coro):
    return await coro


def test_concurrent_writes_same_symbol_no_duplicate_count(service):
    async def scenario():
        # 多协程向同一标的写入不同事件 + 重复投递同一事件
        batches = []
        for i in range(20):
            batches.append(service.ingest(
                [make_event(f"c{i}", 1_000 + i, 10.0 + i, 1.0, seq=i)]))
        for _ in range(10):
            batches.append(service.ingest(
                [make_event("c0", 1_000, 10.0, 1.0, seq=0)]))
        results = await asyncio.gather(*batches, return_exceptions=True)
        return results

    results = asyncio.run(scenario())
    assert not any(isinstance(r, Exception) for r in results), \
        [r for r in results if isinstance(r, Exception)]
    total_accepted = sum(r.accepted for r in results)
    total_duplicate = sum(r.duplicate for r in results)
    assert total_accepted == 20
    assert total_duplicate == 10
    prov = service.query_provisional("A", 0)
    assert prov.count == 20
    assert prov.total_quantity == 20.0  # 每条 quantity=1


def test_concurrent_queries_never_see_partial_window(service):
    async def scenario():
        done = {"v": 0}

        async def writer():
            # 分多个批次写入同一窗口
            for i in range(50):
                await service.ingest(
                    [make_event(f"w{i:03d}", 1_000 + i, 10.0, 1.0, seq=100 + i)])

        async def reader():
            for _ in range(200):
                # 已发布窗口查询必须返回完整特征或还没有该窗口
                published = service.query("A", 0)
                if published:
                    f = published[0]
                    assert f.published and f.count >= 1
                    assert f.vwap is not None
                done["v"] += 1

        await asyncio.gather(writer(), reader())
        return done

    out = asyncio.run(scenario())
    assert out["v"] == 200


def test_concurrent_conflict_batches_all_or_nothing(service):
    async def scenario():
        tasks = []
        for i in range(10):
            # 同一 event_id 的不同内容并发提交：最多一个成功，
            # 其余必须明确报冲突；聚合中该事件数量只能是 0 或 1
            tasks.append(service.ingest(
                [make_event("hot", 5_000, 10.0 + i, 1.0, seq=500)]))
        return await asyncio.gather(*tasks, return_exceptions=True)

    results = asyncio.run(scenario())
    accepted = sum(r.accepted for r in results if not isinstance(r, Exception))
    conflicts = sum(1 for r in results
                    if isinstance(r, IngestionError)
                    and r.reason is RejectReason.DUPLICATE_CONFLICT)
    assert accepted + conflicts == 10
    # 最终只有一种价格落地
    prov = service.query_provisional("A", 0)
    assert prov is None or prov.count <= 1


def test_concurrent_replay_during_writes_stays_consistent(config):
    async def scenario():
        from market_data.service import MarketDataService
        svc = MarketDataService(config)
        base = [make_event(f"r{i}", 1_000 + i, 10.0, 1.0, seq=i)
                for i in range(30)]

        async def write_once():
            await svc.ingest(base)

        async def replay_loop():
            for _ in range(5):
                await svc.ingest(base)  # 与写入争锁：要么接纳要么重复，无丢失
                await asyncio.sleep(0)

        await asyncio.gather(write_once(),
                             *[asyncio.create_task(replay_loop())
                               for _ in range(3)])
        prov = svc.query_provisional("A", 0)
        # 核心不变式：无论锁顺序如何，聚合恰好是 30 条不同事件
        assert prov.count == 30 and prov.total_quantity == 30.0
        snap = svc.metrics_snapshot()
        assert snap["accepted"] == 30
        # 其余 15 批投递全部是幂等重复（写入与重放共 16 批）
        assert snap["duplicates"] == 15 * 30
        return snap

    snap = asyncio.run(scenario())
    assert snap["quarantined"] == 0 and snap["rejected"] == 0

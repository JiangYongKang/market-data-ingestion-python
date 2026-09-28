"""多来源合并专题。

覆盖：
* 同标的多渠道一致副本合并（成交量/价格特征不重复）、确定性取舍；
* 关键内容（价格/数量）对不上 -> cross_source_conflict 隔离，与普通重复区分；
* 渠道间时间线互不影响：落后渠道不被跑得快渠道误判迟到；窗口按最慢渠道关闭；
* 高优先级渠道晚到替换获胜者（含跨批次），旧副本翻转进冲突隔离；
* 重启/整段重放后结果逐位一致、合并副本不重复计数；
* 渠道数上限硬拒绝；观测信息含各渠道推进位置/落后/卡住。
"""
from __future__ import annotations

import pytest

from market_data.config import Config
from market_data.errors import IngestionError
from market_data.models import RejectReason
from market_data.service import MarketDataService

from .conftest import make_event


def _ms_config(tmp_data_dir, sources=("p0", "p1", "p2"), **kw):
    return Config(data_dir=tmp_data_dir,
                  multi_source_symbols={"A": list(sources)}, **kw)


def _ev(eid, src, seq, t, p=10.0, q=1.0, tid=None, symbol="A"):
    return make_event(eid, t, p, q, symbol=symbol, source=src, seq=seq,
                      trade_id=tid)


def _advance_all(svc, sources, t):
    """每个渠道推进一条独立成交（不同 trade_id）到事件时间 t。"""
    payload = [_ev(f"fwd-{src}-{t}", src, 10_000 + t, t, 1.0, 1.0,
                   tid=f"fwd-{src}-{t}") for src in sources]
    return svc.ingest_sync(payload)


# ---------- 一致合并 ----------
def test_identical_cross_source_copies_merge_once(service):
    """单来源服务默认不启用多来源；此用例直接构造多来源服务。"""
    cfg = Config(data_dir=":memory:",
                 multi_source_symbols={"A": ["p0", "p1"]})
    svc = MarketDataService(cfg)
    r = svc.ingest_sync([
        _ev("e0", "p0", 1, 1_000, 10.0, 2.0, "T1"),
        _ev("e1", "p1", 1, 2_000, 10.0, 2.0, "T1"),
    ])
    assert r.accepted == 1 and r.merged == 1 and r.quarantined == 0
    assert r.details[0][1] is RejectReason.MERGED_IDENTICAL
    prov = svc.query_provisional("A", 0)
    assert prov.event_ids == ("e0",)
    assert prov.count == 1 and prov.total_quantity == 2.0
    assert prov.vwap == pytest.approx(10.0)


def test_merge_winner_by_declared_priority_regardless_of_arrival(config):
    """到达顺序不影响获胜者：后声明的渠道先到，p0 后到仍夺魁。"""
    svc = MarketDataService(Config(data_dir=config.data_dir,
                                   multi_source_symbols={"A": ["p0", "p1"]}))
    r1 = svc.ingest_sync([_ev("late0", "p1", 1, 1_000, 10.0, 1.0, "T1")])
    assert r1.accepted == 1
    r2 = svc.ingest_sync([_ev("hi0", "p0", 1, 3_000, 10.0, 1.0, "T1")])
    assert r2.accepted == 1 and r2.merged == 0
    prov = svc.query_provisional("A", 0)
    assert prov.event_ids == ("hi0",)
    # 旧获胜者内容一致：移出窗口但不进隔离，审计为 merged
    kinds = {m.loser_event_id: m.kind for m in svc.merge_records()}
    assert kinds["late0"] == "merged"
    assert svc.quarantine_list("A") == []


def test_three_channel_consistent_merge_counts_one_trade(tmp_data_dir):
    cfg = _ms_config(tmp_data_dir)
    svc = MarketDataService(cfg)
    r = svc.ingest_sync([
        _ev("a", "p0", 1, 1_000, 11.0, 3.0, "T1"),
        _ev("b", "p1", 1, 1_500, 11.0, 3.0, "T1"),
        _ev("c", "p2", 1, 2_500, 11.0, 3.0, "T1"),
    ])
    assert r.accepted == 1 and r.merged == 2
    prov = svc.query_provisional("A", 0)
    assert prov.total_quantity == 3.0 and prov.count == 1
    assert prov.vwap == pytest.approx(11.0) and prov.volatility == 0.0


def test_events_without_trade_id_are_not_merged(tmp_data_dir):
    """没有 trade_id 无法证明是同一笔成交：各自独立计入。"""
    cfg = _ms_config(tmp_data_dir, ("p0", "p1"))
    svc = MarketDataService(cfg)
    r = svc.ingest_sync([
        _ev("x", "p0", 1, 1_000, 10.0, 1.0, None),
        _ev("y", "p1", 1, 2_000, 10.0, 1.0, None),
    ])
    assert r.accepted == 2 and r.merged == 0
    prov = svc.query_provisional("A", 0)
    assert prov.event_ids == ("x", "y")


# ---------- 冲突隔离 ----------
def test_price_mismatch_quarantined_as_cross_source_conflict(tmp_data_dir):
    cfg = _ms_config(tmp_data_dir, ("p0", "p1"))
    svc = MarketDataService(cfg)
    svc.ingest_sync([_ev("w", "p0", 1, 1_000, 10.0, 2.0, "T1")])
    r = svc.ingest_sync([_ev("bad", "p1", 1, 2_000, 10.5, 2.0, "T1")])
    assert r.accepted == 0 and r.quarantined == 1
    assert r.details[0][1] is RejectReason.CROSS_SOURCE_CONFLICT
    q = svc.quarantine_list("A")
    assert len(q) == 1
    assert q[0].reason is RejectReason.CROSS_SOURCE_CONFLICT
    assert q[0].event.event_id == "bad"
    # 说明里能看到双方价格
    assert "10.5" in q[0].detail and "10.0" in q[0].detail
    # 获胜副本照常计数
    prov = svc.query_provisional("A", 0)
    assert prov.event_ids == ("w",) and prov.total_quantity == 2.0


def test_quantity_mismatch_also_conflict(tmp_data_dir):
    cfg = _ms_config(tmp_data_dir, ("p0", "p1"))
    svc = MarketDataService(cfg)
    svc.ingest_sync([_ev("w", "p0", 1, 1_000, 10.0, 2.0, "T1")])
    r = svc.ingest_sync([_ev("bad", "p1", 1, 2_000, 10.0, 5.0, "T1")])
    assert r.details[0][1] is RejectReason.CROSS_SOURCE_CONFLICT
    assert r.quarantined == 1


def test_conflict_distinct_from_duplicate_and_late_reasons(service, tmp_data_dir):
    cfg = _ms_config(tmp_data_dir, ("p0", "p1"))
    svc = MarketDataService(cfg)
    svc.ingest_sync([_ev("w", "p0", 1, 1_000, 10.0, 1.0, "T1")])
    # 完全一致副本 -> merged_identical
    r_ok = svc.ingest_sync([_ev("dup", "p1", 1, 2_000, 10.0, 1.0, "T1")])
    assert r_ok.details[0][1] is RejectReason.MERGED_IDENTICAL
    # 同 event_id 原样重投 -> duplicate_identical
    r_redel = svc.ingest_sync([_ev("w", "p0", 1, 1_000, 10.0, 1.0, "T1")])
    assert r_redel.duplicate == 1
    assert r_redel.details[0][1] is RejectReason.DUPLICATE_IDENTICAL
    # 冲突 -> cross_source_conflict（第三个渠道）
    r_conf = svc.ingest_sync([_ev("cf", "p2", 1, 3_000, 11.0, 1.0, "T1")])
    assert r_conf.details[0][1] is RejectReason.CROSS_SOURCE_CONFLICT
    reasons = {d[1] for d in
               r_ok.details + r_redel.details + r_conf.details}
    assert reasons == {
        RejectReason.MERGED_IDENTICAL,
        RejectReason.DUPLICATE_IDENTICAL,
        RejectReason.CROSS_SOURCE_CONFLICT,
    }


def test_higher_priority_arrives_late_replaces_and_demotes(tmp_data_dir):
    """先一致合并低优先级两路，高优先级带不同内容晚到：旧两路翻转为冲突。"""
    cfg = _ms_config(tmp_data_dir)
    svc = MarketDataService(cfg)
    svc.ingest_sync([_ev("lo1", "p1", 1, 1_000, 10.0, 2.0, "T1")])
    svc.ingest_sync([_ev("lo2", "p2", 1, 2_000, 10.0, 2.0, "T1")])
    r = svc.ingest_sync([_ev("hi", "p0", 1, 3_000, 12.0, 2.0, "T1")])
    assert r.accepted == 1 and r.quarantined == 2
    prov = svc.query_provisional("A", 0)
    assert prov.event_ids == ("hi",)
    assert prov.total_quantity == 2.0 and prov.vwap == pytest.approx(12.0)
    qids = {q.event.event_id for q in svc.quarantine_list("A")}
    assert qids == {"lo1", "lo2"}
    assert all(q.reason is RejectReason.CROSS_SOURCE_CONFLICT
               for q in svc.quarantine_list("A"))


# ---------- 渠道时间线互不影响 ----------
def test_slow_channel_not_marked_late_by_fast_channel(tmp_data_dir):
    cfg = _ms_config(tmp_data_dir, ("fast", "slow"))
    svc = MarketDataService(cfg)
    for i, tt in enumerate(range(1_000, 55_000, 5_000)):
        svc.ingest_sync([_ev(f"F{i}", "fast", i, tt, tid=f"FT{i}")])
    # fast 已到 51s；slow 才到 2s，按自己的水位绝不迟到
    r = svc.ingest_sync([_ev("S0", "slow", 1, 2_000, tid="ST0")])
    assert r.accepted == 1 and r.quarantined == 0
    prov = svc.query_provisional("A", 0)
    assert "S0" in prov.event_ids


def test_window_closes_only_after_slowest_channel_advances(tmp_data_dir):
    cfg = _ms_config(tmp_data_dir, ("p0", "p1"))
    svc = MarketDataService(cfg)
    svc.ingest_sync([_ev("a0", "p0", 1, 1_000, 10.0, 1.0, "T1"),
                     _ev("a1", "p1", 1, 2_000, 10.0, 1.0, "T1")])
    # 只有 p0 推进到 20s：p1 水位仍压住窗口 0，不能发布
    svc.ingest_sync([_ev("f0", "p0", 90, 20_000, 1.0, 1.0, "F0")])
    assert svc.query("A", 0) == []
    # p1 也越过窗口 -> 发布，结果只含合并后的一个获胜副本
    svc.ingest_sync([_ev("f1", "p1", 90, 20_000, 1.0, 1.0, "F1")])
    w = svc.query("A", 0)
    assert len(w) == 1 and w[0].count == 1 and w[0].total_quantity == 1.0


def test_independent_channels_within_window_disorder(tmp_data_dir):
    """各渠道在窗口内乱序到达，结果与顺序无关且按各自水位判定。"""
    cfg = _ms_config(tmp_data_dir, ("p0", "p1"))
    svc = MarketDataService(cfg)
    r = svc.ingest_sync([
        _ev("t2", "p1", 3, 8_000, 20.0, 1.0, "T2"),
        _ev("t1b", "p1", 2, 4_000, 20.0, 1.0, "T1b"),
        _ev("t2b", "p0", 3, 7_000, 20.0, 1.0, "T2"),
        _ev("t1a", "p0", 2, 3_000, 20.0, 1.0, "T1b"),
    ])
    # T1b 合并一次，T2 合并一次 -> accepted 2
    assert r.accepted == 2 and r.merged == 2 and r.quarantined == 0
    prov = svc.query_provisional("A", 0)
    assert prov.event_ids == ("t1a", "t2b")


# ---------- 重启 / 重放一致性 ----------
def test_restart_reproduces_merged_window_results(config):
    cfg = Config(data_dir=config.data_dir,
                 multi_source_symbols={"A": ["p0", "p1", "p2"]})
    svc = MarketDataService(cfg)
    svc.ingest_sync([
        _ev("a0", "p0", 1, 1_000, 10.0, 2.0, "T1"),
        _ev("a1", "p1", 1, 2_000, 10.0, 2.0, "T1"),
    ])
    svc.ingest_sync([_ev("a2", "p2", 1, 3_000, 10.0, 2.0, "T1")])
    _advance_all(svc, ["p0", "p1", "p2"], 20_000)
    before = svc.query("A", 0)[0]
    svc.close()

    svc2 = MarketDataService(cfg)
    after = svc2.query("A", 0)[0]
    assert (after.event_ids, after.count, after.total_quantity,
            after.vwap, after.volatility) == (
        before.event_ids, before.count, before.total_quantity,
        before.vwap, before.volatility)
    # 渠道推进位置也恢复
    snap = svc2.channel_snapshot()["A"]
    assert {c["source"]: c["max_event_time_ms"]
            for c in snap["channels"]} == {"p0": 20_000, "p1": 20_000,
                                           "p2": 20_000}


def test_restart_reproduces_conflict_quarantine(config):
    cfg = Config(data_dir=config.data_dir,
                 multi_source_symbols={"A": ["p0", "p1"]})
    svc = MarketDataService(cfg)
    svc.ingest_sync([_ev("w", "p0", 1, 1_000, 10.0, 1.0, "T1")])
    svc.ingest_sync([_ev("bad", "p1", 1, 2_000, 11.0, 1.0, "T1")])
    svc.close()

    svc2 = MarketDataService(cfg)
    q = svc2.quarantine_list("A")
    assert len(q) == 1
    assert q[0].event.event_id == "bad"
    assert q[0].reason is RejectReason.CROSS_SOURCE_CONFLICT
    # 合并审计也恢复
    recs = {m.loser_event_id: m.kind for m in svc2.merge_records()}
    assert recs["bad"] == "conflict"
    # 窗口仍只算获胜副本
    prov = svc2.query_provisional("A", 0)
    assert prov.event_ids == ("w",)


def test_full_replay_after_restart_is_idempotent(config):
    cfg = Config(data_dir=config.data_dir,
                 multi_source_symbols={"A": ["p0", "p1"]})
    svc = MarketDataService(cfg)
    payload = [
        _ev("a0", "p0", 1, 1_000, 10.0, 2.0, "T1"),
        _ev("a1", "p1", 1, 2_000, 10.0, 2.0, "T1"),
    ]
    svc.ingest_sync(payload)
    _advance_all(svc, ["p0", "p1"], 20_000)
    svc.close()

    svc2 = MarketDataService(cfg)
    r = svc2.ingest_sync(payload)  # 整段重放
    assert r.accepted == 0 and r.duplicate == 2 and r.merged == 0
    after = svc2.query("A", 0)[0]
    assert after.count == 1 and after.event_ids == ("a0",)
    # 审计记录不重复
    assert len(svc2.merge_records()) == 1


def test_replay_order_does_not_change_winner(config):
    """合并是副本集合的纯函数：先到任一渠道，最终获胜者/特征一致。"""
    results = []
    for order in (("p0", "p1"), ("p1", "p0")):
        cfg = Config(data_dir=config.data_dir + f"-{order[0]}",
                     multi_source_symbols={"A": ["p0", "p1"]})
        svc = MarketDataService(cfg)
        payload = [_ev(f"e-{s}", s, 1, 1_000 + i, 10.0, 1.0, "T1")
                   for i, s in enumerate(order)]
        svc.ingest_sync(payload)
        _advance_all(svc, ["p0", "p1"], 20_000)
        w = svc.query("A", 0)[0]
        results.append((w.event_ids, w.vwap, w.total_quantity))
        svc.close()
    assert results[0] == results[1] == (("e-p0",), 10.0, 1.0)


# ---------- 容量上限 ----------
def test_channel_limit_hard_rejects_batch(tmp_data_dir):
    cfg = Config(data_dir=tmp_data_dir,
                 multi_source_symbols={"A": ["p0", "p1"]},
                 max_channels_per_symbol=2)
    svc = MarketDataService(cfg)
    svc.ingest_sync([_ev("ok", "p0", 1, 1_000, tid="T1")])
    with pytest.raises(IngestionError) as exc:
        svc.ingest_sync([_ev("rogue", "pX", 1, 2_000, tid="T2")])
    assert exc.value.reason is RejectReason.CHANNEL_LIMIT_EXCEEDED
    # 整批拒绝：渠道未注册、位点未推进、窗口无该事件
    snap = svc.channel_snapshot()["A"]
    assert {c["source"] for c in snap["channels"]} == {"p0", "p1"}
    assert svc.checkpoint_get("pX") == -1
    assert svc.query_provisional("A", 0).event_ids == ("ok",)
    assert svc.metrics_snapshot()["channel_rejected"] == 1


def test_undeclared_symbol_stays_single_source(tmp_data_dir):
    """未声明多来源的标的收到第二 source：不启用合并，各自独立成交。"""
    cfg = Config(data_dir=tmp_data_dir,
                 multi_source_symbols={"A": ["p0", "p1"]})
    svc = MarketDataService(cfg)
    r = svc.ingest_sync([
        make_event("z1", 1_000, 10.0, 1.0, symbol="Z", source="q0", seq=1),
        make_event("z2", 2_000, 10.0, 1.0, symbol="Z", source="q1", seq=1),
    ])
    assert r.accepted == 2 and r.merged == 0
    prov = svc.query_provisional("Z", 0)
    assert prov.event_ids == ("z1", "z2")


# ---------- 观测 ----------
def test_channel_observability_progress_lag_and_stuck(tmp_data_dir):
    cfg = Config(data_dir=tmp_data_dir,
                 multi_source_symbols={"A": ["p0", "p1", "p2"]},
                 channel_idle_timeout_ms=1)
    svc = MarketDataService(cfg)
    svc.ingest_sync([_ev("a0", "p0", 1, 5_000, tid="T1")])
    # p1 有事件；p2 从未到达
    svc.ingest_sync([_ev("a1", "p1", 1, 2_000, tid="T2")])
    import time
    time.sleep(0.01)
    snap = svc.channel_snapshot()["A"]
    by_src = {c["source"]: c for c in snap["channels"]}
    assert by_src["p0"]["max_event_time_ms"] == 5_000
    assert by_src["p1"]["max_event_time_ms"] == 2_000
    # 落后渠道水位更低；标的水位取最慢（p2 未到达 -> -inf）
    assert by_src["p1"]["watermark_ms"] < by_src["p0"]["watermark_ms"]
    assert snap["symbol_watermark_ms"] < -1 << 40
    assert by_src["p2"]["observed"] is False and by_src["p2"]["stuck"] is True
    assert snap["stuck_count"] >= 1
    # metrics 快照内也能看到
    m = svc.metrics_snapshot()
    assert "channels" in m and m["cross_source_conflicts"] == 0


def test_copy_after_window_published_is_late_quarantined(tmp_data_dir):
    """窗口发布后多来源判定冻结：更晚到达的其它渠道副本按迟到隔离。"""
    cfg = _ms_config(tmp_data_dir, ("p0", "p1"))
    svc = MarketDataService(cfg)
    svc.ingest_sync([_ev("w", "p0", 1, 1_000, 10.0, 2.0, "T1")])
    _advance_all(svc, ["p0", "p1"], 20_000)
    assert svc.query("A", 0)[0].event_ids == ("w",)
    r = svc.ingest_sync([_ev("late-copy", "p1", 99_999, 2_000, 10.0, 2.0, "T1")])
    assert r.accepted == 0 and r.quarantined == 1
    q = svc.quarantine_list("A")[-1]
    assert q.reason is RejectReason.LATE_BEYOND_WATERMARK
    assert "冻结" in q.detail
    # 已发布结果不变
    assert svc.query("A", 0)[0].event_ids == ("w",)


def test_merged_loser_redelivery_after_restart_is_duplicate(config):
    """被合并副本重启后整段重投：身份已恢复，按幂等重复、不二次计数。"""
    cfg = Config(data_dir=config.data_dir,
                 multi_source_symbols={"A": ["p0", "p1"]})
    svc = MarketDataService(cfg)
    svc.ingest_sync([
        _ev("w", "p0", 1, 1_000, 10.0, 2.0, "T1"),
        _ev("m", "p1", 1, 2_000, 10.0, 2.0, "T1"),
    ])
    _advance_all(svc, ["p0", "p1"], 20_000)
    svc.close()
    svc2 = MarketDataService(cfg)
    r = svc2.ingest_sync([_ev("m", "p1", 1, 2_000, 10.0, 2.0, "T1")])
    assert r.duplicate == 1 and r.accepted == 0 and r.merged == 0
    assert svc2.query("A", 0)[0].count == 1


def test_dynamic_channel_within_cap_then_over_cap(tmp_data_dir):
    """声明空渠道列表：动态接入直到上限，超过硬拒绝。"""
    cfg = Config(data_dir=tmp_data_dir,
                 multi_source_symbols={"A": []},
                 max_channels_per_symbol=2)
    svc = MarketDataService(cfg)
    r1 = svc.ingest_sync([_ev("d1", "dA", 1, 1_000, tid="D1")])
    r2 = svc.ingest_sync([_ev("d2", "dB", 1, 2_000, tid="D2")])
    assert r1.accepted == 1 and r2.accepted == 1
    with pytest.raises(IngestionError) as exc:
        svc.ingest_sync([_ev("d3", "dC", 1, 3_000, tid="D3")])
    assert exc.value.reason is RejectReason.CHANNEL_LIMIT_EXCEEDED


def test_batch_with_conflict_and_capacity_is_atomic(tmp_data_dir):
    """隔离区容量不足时冲突批次整体不生效：窗口、隔离区均无半批残留。"""
    cfg = Config(data_dir=tmp_data_dir,
                 multi_source_symbols={"A": ["p0", "p1"]},
                 max_quarantine_size=1)
    svc = MarketDataService(cfg)
    svc.ingest_sync([_ev("w1", "p0", 1, 1_000, 10.0, 1.0, "T1"),
                     _ev("w2", "p0", 2, 2_000, 10.0, 1.0, "T2")])
    from market_data.errors import BackpressureError
    with pytest.raises(BackpressureError):
        svc.ingest_sync([
            _ev("c1", "p1", 1, 1_100, 11.0, 1.0, "T1"),
            _ev("c2", "p1", 2, 2_100, 11.0, 1.0, "T2"),
        ])
    assert svc.quarantine_list("A") == []
    assert svc.query_provisional("A", 0).event_ids == ("w1", "w2")


def test_concurrent_multi_source_writes_merge_deterministically(config):
    """多渠道协程并发投递同一批成交：窗口结果确定、每笔只计一次。"""
    import asyncio
    cfg = Config(data_dir=config.data_dir,
                 multi_source_symbols={"A": ["p0", "p1", "p2"]})
    svc = MarketDataService(cfg)

    async def scenario():
        batches = []
        for i in range(30):
            for src in ("p0", "p1", "p2"):
                # p2 始终报不同价格 -> 每笔恰好一个冲突副本
                price = 10.0 + i + (0.5 if src == "p2" else 0.0)
                batches.append(svc.ingest(
                    [_ev(f"e-{src}-{i}", src, i, 1_000 + (i % 70) * 100,
                         price, 1.0, f"T{i}")]))
        return await asyncio.gather(*batches, return_exceptions=True)

    results = asyncio.run(scenario())
    assert not any(isinstance(r, Exception) for r in results), \
        [r for r in results if isinstance(r, Exception)]
    accepted = sum(r.accepted for r in results)
    merged = sum(r.merged for r in results)
    quarantined = sum(r.quarantined for r in results)
    # 不变式：每笔成交恰好 1 个副本计入窗口，1 个一致副本被合并，
    # 1 个内容冲突副本被隔离（并发到达顺序不改变最终归类）。
    assert accepted == 30 and quarantined == 30 and merged >= 30
    # 每笔获胜者一定是优先级最高渠道 p0；汇总所有未发布窗口
    total_qty = 0.0
    winner_ids = []
    for w0 in range(0, 8_000, 10_000):
        prov = svc.query_provisional("A", w0)
        if prov:
            total_qty += prov.total_quantity
            winner_ids.extend(prov.event_ids)
    assert len(winner_ids) == 30
    assert all(eid.startswith("e-p0-") for eid in winner_ids)
    assert total_qty == 30.0  # 每笔 quantity=1，无重复计数
    svc.close()

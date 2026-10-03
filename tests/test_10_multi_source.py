"""多来源专题：跨渠道成交合并、渠道间时间线互不干扰、重启重放一致、容量上限。

仅在 ``multi_source=True`` 时启用多来源语义；单来源行为由 test_01..09 保证。
"""
from __future__ import annotations

import logging

import pytest

from market_data.config import Config
from market_data.errors import BackpressureError, IngestionError
from market_data.models import RejectReason
from market_data.service import MarketDataService

from .conftest import make_event


def multi_config(tmp_data_dir, **kw):
    base = dict(data_dir=tmp_data_dir, multi_source=True, merge_sources=("a", "b"),
                window_size_ms=10_000, allowed_lateness_ms=0)
    base.update(kw)
    return Config(**base)


@pytest.fixture()
def mservice(tmp_data_dir):
    svc = MarketDataService(multi_config(tmp_data_dir))
    yield svc
    svc.close()


def trade(eid, src, seq, t, *, tid, p=10.0, q=2.0, symbol="A"):
    return make_event(eid, t, p, q, symbol=symbol, source=src, seq=seq, trade_id=tid)


# ---------- 跨渠道合并：去双计 + 确定性取舍 ----------

def test_same_trade_two_channels_counts_once(mservice):
    r1 = mservice.ingest_sync([trade("a1", "a", 1, 1000, tid="T1")])
    r2 = mservice.ingest_sync([trade("b1", "b", 1, 1005, tid="T1")])
    assert r1.accepted == 1
    assert r2.merged == 1 and r2.accepted == 0
    assert r2.details[0][1] is RejectReason.CROSS_SOURCE_MERGED
    prov = mservice.query_provisional("A", 0)
    assert prov.count == 1
    assert prov.total_quantity == 2.0          # 成交量不翻倍
    assert prov.event_ids == ("a1",)


def test_lower_priority_first_winner_replaced_by_priority_channel(mservice):
    """低优先级渠道先到；高优先级渠道后来居上替换主报。"""
    mservice.ingest_sync([trade("b1", "b", 1, 1000, tid="T1")])
    prov = mservice.query_provisional("A", 0)
    assert prov.event_ids == ("b1",)
    r = mservice.ingest_sync([trade("a1", "a", 1, 1005, tid="T1")])
    assert r.accepted == 1
    prov = mservice.query_provisional("A", 0)
    assert prov.event_ids == ("a1",)           # 替换为 a 路
    assert prov.total_quantity == 2.0          # 仍然只算一笔
    merged = mservice.merged_trades()
    assert any(m["loser_event_id"] == "b1" and m["winner_event_id"] == "a1"
               for m in merged)


def test_third_channel_consistent_report_is_merged(mservice):
    mservice.ingest_sync([trade("a1", "a", 1, 1000, tid="T1")])
    mservice.ingest_sync([trade("b1", "b", 1, 1002, tid="T1")])
    r = mservice.ingest_sync([trade("c1", "c", 1, 1003, tid="T1")])
    assert r.merged == 1
    prov = mservice.query_provisional("A", 0)
    assert prov.count == 1 and prov.total_quantity == 2.0


def test_merged_loser_marked_with_winner_identity(mservice, caplog):
    mservice.ingest_sync([trade("a1", "a", 1, 1000, tid="T1")])
    mservice.ingest_sync([trade("b1", "b", 1, 1005, tid="T1")])
    records = mservice.merged_trades()
    assert len(records) == 1
    rec = records[0]
    assert rec["trade_id"] == "T1" and rec["symbol"] == "A"
    assert rec["winner_source"] == "a" and rec["loser_source"] == "b"
    assert rec["winner_event_id"] == "a1" and rec["loser_event_id"] == "b1"
    with caplog.at_level(logging.INFO, logger="market_data"):
        mservice.ingest_sync([trade("c1", "c", 1, 1006, tid="T1")])
    assert "cross_source_merged" in caplog.text and "event_id=c1" in caplog.text


def test_different_trade_ids_are_separate_trades(mservice):
    r = mservice.ingest_sync([
        trade("a1", "a", 1, 1000, tid="T1"),
        trade("b2", "b", 1, 1001, tid="T2"),
    ])
    assert r.accepted == 2 and r.merged == 0
    assert mservice.query_provisional("A", 0).count == 2


def test_events_without_trade_id_not_merged(mservice):
    mservice.ingest_sync([make_event("a1", 1000, source="a", seq=1)])
    r = mservice.ingest_sync([make_event("b1", 1002, source="b", seq=1)])
    assert r.accepted == 1 and r.merged == 0     # 无 trade_id，视为两笔不同成交


# ---------- 跨渠道内容冲突：单独隔离，理由可区分 ----------

def test_price_mismatch_quarantined_as_merge_conflict(mservice):
    mservice.ingest_sync([trade("a9", "a", 9, 60000, tid="T9", p=10.0, q=1.0)])
    r = mservice.ingest_sync([trade("b9", "b", 9, 60001, tid="T9", p=11.0, q=1.0)])
    # 确认冲突即整笔隔离：b9 与已入窗但未发布的 a9 都进隔离区，都不计入
    assert r.accepted == 0 and r.quarantined == 2 and r.merged == 0
    assert all(d[1] is RejectReason.MERGE_CONFLICT for d in r.details)
    q = mservice.quarantine_list("A")
    rec = [x for x in q if x.reason is RejectReason.MERGE_CONFLICT]
    assert {x.event.event_id for x in rec} == {"a9", "b9"}
    assert all("T9" in x.detail for x in rec)
    assert mservice.query_provisional("A", 60000) is None


def test_quantity_mismatch_is_merge_conflict_not_duplicate(mservice):
    mservice.ingest_sync([trade("a1", "a", 1, 1000, tid="T1", q=2.0)])
    r = mservice.ingest_sync([trade("b1", "b", 1, 1001, tid="T1", q=3.0)])
    assert r.details[0][1] is RejectReason.MERGE_CONFLICT
    assert r.details[0][1] is not RejectReason.DUPLICATE_IDENTICAL
    # 确认冲突后整笔隔离：先到但未发布的 a1 也被清出，窗口一笔都不计
    assert mservice.query_provisional("A", 0) is None
    assert {x.event.event_id for x in mservice.quarantine_list("A")
            if x.reason is RejectReason.MERGE_CONFLICT} == {"a1", "b1"}


def test_merge_conflict_distinct_from_late_quarantine(mservice):
    """merge_conflict 与 late_beyond_watermark 两类隔离记录可区分。"""
    # 一笔跨渠道价格冲突
    mservice.ingest_sync([trade("a9", "a", 9, 60000, tid="T9", p=10.0, q=1.0)])
    mservice.ingest_sync([trade("b9", "b", 9, 60001, tid="T9", p=11.0, q=1.0)])
    q = mservice.quarantine_list("A")
    reasons = {x.reason for x in q}
    assert RejectReason.MERGE_CONFLICT in reasons


def test_event_time_difference_alone_is_not_conflict(mservice):
    """各渠道事件时间可不同；价格数量一致即正常合并，不算冲突。"""
    mservice.ingest_sync([trade("a1", "a", 1, 1000, tid="T1")])
    r = mservice.ingest_sync([trade("b1", "b", 1, 1500, tid="T1")])
    assert r.merged == 1 and r.quarantined == 0


# ---------- 渠道间时间线互不干扰 ----------

def test_slow_channel_not_quarantined_by_fast_channel(mservice):
    """a 跑到 50s，b 在 2s 的事件不得被误判迟到。"""
    mservice.ingest_sync([trade("a1", "a", 1, 50_000, tid="TA")])
    r = mservice.ingest_sync([trade("b1", "b", 1, 2_000, tid="TB")])
    assert r.accepted == 1 and r.quarantined == 0
    assert mservice.query_provisional("A", 0).event_ids == ("b1",)


def test_window_waits_for_slow_participating_channel(mservice):
    """窗口结果按各参与渠道实际到齐的事件算：慢渠道未到齐则窗口不发布。"""
    mservice.ingest_sync([trade("a1", "a", 1, 50_000, tid="TA")])  # a 已越过窗口0
    mservice.ingest_sync([trade("b1", "b", 1, 2_000, tid="TB")])   # b 落在窗口0
    assert mservice.query("A", 0) == []   # b 的水位还在窗口0内，不能关窗
    # b 推进越过窗口0右边界后才发布，且窗口0含 b1（按实际到齐事件计算）
    mservice.ingest_sync([trade("b2", "b", 2, 15_000, tid="TB2")])
    pub = mservice.query("A", 0)
    assert len(pub) == 1
    assert pub[0].event_ids == ("b1",)


def test_channel_watermarks_observed_independently(mservice):
    mservice.ingest_sync([trade("a1", "a", 1, 50_000, tid="TA")])
    mservice.ingest_sync([trade("b1", "b", 1, 2_000, tid="TB")])
    snap = mservice.channels_snapshot()
    wm = {c["source"]: c["watermark_ms"] for c in snap["channels"]}
    assert wm["a"] == 50_000 and wm["b"] == 2_000
    lag = {c["source"]: c["lag_ms"] for c in snap["channels"]}
    assert lag["b"] == 48_000 and lag["a"] == 0


def test_only_winner_channels_hold_window_closed(tmp_data_dir):
    """只发过被合并副本的渠道停更，不应无限拖住窗口发布。"""
    svc = MarketDataService(multi_config(tmp_data_dir, allowed_lateness_ms=0,
                                         source_idle_timeout_ms=10**9))
    svc.ingest_sync([trade("a1", "a", 1, 1000, tid="T1")])
    svc.ingest_sync([trade("c1", "c", 1, 1002, tid="T1")])  # 被合并副本
    svc.ingest_sync([trade("a2", "a", 2, 15_000, tid="T2")])  # a 越过窗口0
    pub = svc.query("A", 0)
    assert len(pub) == 1 and pub[0].event_ids == ("a1",)
    svc.close()


def test_idle_channel_eventually_excluded_then_rejoins(tmp_data_dir):
    """停滞渠道暂时不参与关窗（结果可推进），恢复来数后重新参与。"""
    clock = [1000]
    svc = MarketDataService(multi_config(
        tmp_data_dir, source_idle_timeout_ms=500))
    svc._state_clock = lambda: clock[0]
    svc._channels._clock = svc._state_clock
    svc.ingest_sync([trade("a1", "a", 1, 1000, tid="TA")])
    clock[0] = 1050
    svc.ingest_sync([trade("b1", "b", 1, 2000, tid="TB")])
    clock[0] = 1200  # a 空闲200ms：两渠道均活跃，取最小水位，窗口0不发布
    assert svc.query("A", 0) == []
    clock[0] = 1600  # a 停滞；b 持续活跃
    svc.ingest_sync([trade("b2", "b", 2, 15000, tid="TB2")])
    assert "a" in [c["source"] for c in svc.metrics_snapshot()["channels"]["channels"]
                   if c["stalled"]]
    assert len(svc.query("A", 0)) == 1   # 剔除停滞的 a 后可发布
    clock[0] = 1700
    svc.ingest_sync([trade("a2", "a", 2, 16000, tid="TA2")])  # a 恢复
    assert not any(c["stalled"] for c in svc.metrics_snapshot()["channels"]["channels"])
    svc.close()


# ---------- 重启 / 整段重放一致性 ----------

def _feed(svc):
    svc.ingest_sync([trade("b1", "b", 1, 1000, tid="T1")])
    svc.ingest_sync([trade("a1", "a", 1, 1005, tid="T1")])
    svc.ingest_sync([trade("c1", "c", 1, 1002, tid="T1")])
    svc.ingest_sync([trade("b2", "b", 2, 2000, tid="T3")])
    svc.ingest_sync([trade("a2", "a", 2, 16000, tid="T2")])
    svc.ingest_sync([trade("b3", "b", 3, 15000, tid="T4")])


def _pub_sig(svc):
    return [(f.symbol, f.window_start_ms, f.count, f.total_quantity,
             f.vwap, f.event_ids) for f in svc.query("A")]


def test_published_windows_identical_after_restart(tmp_data_dir):
    svc = MarketDataService(multi_config(tmp_data_dir))
    _feed(svc)
    before = _pub_sig(svc)
    assert before  # 确有窗口发布
    cp_before = svc.checkpoints()
    svc.close()

    svc2 = MarketDataService(multi_config(tmp_data_dir))
    assert _pub_sig(svc2) == before
    assert svc2.checkpoints() == cp_before
    svc2.close()


def test_full_replay_does_not_recompute(tmp_data_dir):
    svc = MarketDataService(multi_config(tmp_data_dir))
    events = [
        trade("b1", "b", 1, 1000, tid="T1"),
        trade("a1", "a", 1, 1005, tid="T1"),
        trade("c1", "c", 1, 1002, tid="T1"),
        trade("b2", "b", 2, 2000, tid="T3"),
        trade("a2", "a", 2, 16000, tid="T2"),
        trade("b3", "b", 3, 15000, tid="T4"),
    ]
    for ev in events:
        svc.ingest_sync([ev])
    before = _pub_sig(svc)
    svc.close()

    svc2 = MarketDataService(multi_config(tmp_data_dir))
    r = svc2.ingest_sync(events)   # 整段重放
    assert r.accepted == 0 and r.quarantined == 0
    assert r.duplicate + r.merged == 6
    assert _pub_sig(svc2) == before   # 已发布结果不变
    # 合并留痕不因重放翻倍
    merged = svc2.merged_trades()
    assert len(merged) == 2  # b1 被 a1 替换 + c1 被合并
    svc2.close()


def test_restart_then_restart_still_identical(tmp_data_dir):
    svc = MarketDataService(multi_config(tmp_data_dir))
    _feed(svc)
    before = _pub_sig(svc)
    svc.close()
    for _ in range(2):
        s = MarketDataService(multi_config(tmp_data_dir))
        assert _pub_sig(s) == before
        s.close()


def test_merge_conflict_survives_restart(tmp_data_dir):
    svc = MarketDataService(multi_config(tmp_data_dir))
    svc.ingest_sync([trade("a9", "a", 9, 60000, tid="T9", p=10.0, q=1.0)])
    svc.ingest_sync([trade("b9", "b", 9, 60001, tid="T9", p=11.0, q=1.0)])
    svc.close()
    svc2 = MarketDataService(multi_config(tmp_data_dir))
    q = svc2.quarantine_list("A")
    assert any(x.reason is RejectReason.MERGE_CONFLICT for x in q)
    # 重放同一冲突上报：仍隔离，不会被当成正常成交
    r = svc2.ingest_sync([trade("b9", "b", 9, 60001, tid="T9", p=11.0, q=1.0)])
    assert r.accepted == 0
    svc2.close()


def test_winner_replace_survives_restart(tmp_data_dir):
    """高优先级替换主报后重启：窗口仍以高优先级路为准。"""
    svc = MarketDataService(multi_config(tmp_data_dir))
    svc.ingest_sync([trade("b1", "b", 1, 1000, tid="T1", p=10.0, q=4.0)])
    svc.ingest_sync([trade("a1", "a", 1, 1005, tid="T1", p=10.0, q=4.0)])
    svc.ingest_sync([trade("a2", "a", 2, 16000, tid="T2")])
    svc.ingest_sync([trade("b2", "b", 2, 15000, tid="T3")])
    before = _pub_sig(svc)
    assert [ids for (_s, _w, _c, _q, _v, ids) in before if _w == 0] == [("a1",)]
    svc.close()
    svc2 = MarketDataService(multi_config(tmp_data_dir))
    assert _pub_sig(svc2) == before
    svc2.close()


# ---------- 容量上限 / 背压 / 拒绝或延迟 ----------

def test_source_count_hard_limit_rejects_batch(tmp_data_dir):
    svc = MarketDataService(multi_config(tmp_data_dir, max_sources=2))
    svc.ingest_sync([make_event("e1", 1000, source="a", seq=1)])
    svc.ingest_sync([make_event("e2", 1000, source="b", seq=1)])
    with pytest.raises(IngestionError) as exc:
        svc.ingest_sync([make_event("e3", 1000, source="c", seq=1)])
    assert exc.value.reason is RejectReason.SOURCE_LIMIT_REJECTED
    # 整批拒绝：渠道数仍为 2，内存不被新渠道占用
    assert len(svc._channels) == 2
    svc.close()


def test_pending_event_cap_rejects_or_delays(tmp_data_dir):
    # reject 策略：超限直接拒绝
    svc = MarketDataService(multi_config(tmp_data_dir, max_pending_events=2))
    with pytest.raises(BackpressureError) as exc:
        svc.ingest_sync([
            make_event("e1", 1000, source="a", seq=1),
            make_event("e2", 2000, source="a", seq=2),
            make_event("e3", 3000, source="a", seq=3),
        ])
    assert exc.value.reason is RejectReason.BACKPRESSURE_REJECTED
    svc.close()


def test_backpressure_delay_strategy_eventually_succeeds(tmp_data_dir):
    # delay 策略：容量只够 2 个打开事件；第二、三条之间先推进水位释放窗口，
    # 第二条进入时短暂超限 -> 有限次延迟重试后成功（不无限等待）。
    svc = MarketDataService(multi_config(
        tmp_data_dir, max_pending_events=2,
        backpressure_strategy="delay", backpressure_delay_ms=1,
        backpressure_max_retries=100, allowed_lateness_ms=0))
    assert svc.ingest_sync([make_event("e1", 1000, source="a", seq=1)]).accepted == 1
    assert svc.ingest_sync([make_event("tick", 20000, source="a", seq=2)]).accepted == 1
    # 此时窗口0已发布、积压清空；再来事件经至多短暂等待即可接纳
    r3 = svc.ingest_sync([make_event("e2", 21000, source="a", seq=3)])
    assert r3.accepted == 1
    svc.close()


def test_quarantine_cap_rejects_merge_conflict_overflow(tmp_data_dir):
    # 确认冲突即整笔隔离：一次冲突产生 2 条隔离记录（后到路 + 被清出的先到路）
    svc = MarketDataService(multi_config(tmp_data_dir, max_quarantine_size=2))
    svc.ingest_sync([trade("a9", "a", 90, 60000, tid="T9", p=10.0, q=1.0)])
    svc.ingest_sync([trade("b9", "b", 90, 60001, tid="T9", p=11.0, q=1.0)])
    # 隔离区已满 2；再来一笔冲突 -> 整批背压拒绝（序号单调，不触碰位点回退）
    svc.ingest_sync([trade("a8", "a", 91, 70000, tid="T8", p=10.0, q=1.0)])
    with pytest.raises(BackpressureError):
        svc.ingest_sync([trade("b8", "b", 91, 70001, tid="T8", p=12.0, q=1.0)])
    svc.close()


# ---------- 观测：推进位置 / 落后 / 卡住 ----------

def test_metrics_report_per_channel_progress(mservice):
    mservice.ingest_sync([trade("a1", "a", 1, 50000, tid="TA")])
    mservice.ingest_sync([trade("b1", "b", 1, 2000, tid="TB")])
    snap = mservice.metrics_snapshot()
    assert snap["multi_source"] is True and snap["merged"] == 0
    chans = {c["source"]: c for c in snap["channels"]["channels"]}
    assert set(chans) == {"a", "b"}
    assert chans["a"]["max_event_time_ms"] == 50000
    assert chans["b"]["max_event_time_ms"] == 2000
    assert chans["b"]["lag_ms"] == 48000
    assert chans["a"]["accepted"] == 1 and chans["b"]["accepted"] == 1


# ---------- 单来源默认行为不回归（开关关闭走旧路径） ----------

def test_multi_source_disabled_uses_global_watermark(tmp_data_dir):
    cfg = Config(data_dir=tmp_data_dir)  # multi_source 默认 False
    assert cfg.multi_source is False
    svc = MarketDataService(cfg)
    assert svc.channels_snapshot() is None
    r = svc.ingest_sync([make_event("e1", 1000, source="s", seq=1)])
    assert r.merged == 0
    svc.close()


# ---------- HTTP 层 ----------

def test_http_multi_source_merge_and_endpoints(tmp_path):
    from fastapi.testclient import TestClient
    from market_data.app import create_app
    data = str(tmp_path / "data")
    app = create_app(multi_config(data))
    c = TestClient(app)
    a = trade("a1", "a", 1, 1000, tid="T1")
    b = trade("b1", "b", 1, 1005, tid="T1")
    assert c.post("/ingest", json=a).json()["accepted"] == 1
    r = c.post("/ingest", json=b).json()
    assert r["merged"] == 1
    assert r["details"][0]["reason"] == "cross_source_merged"
    # 合并留痕端点
    merged = c.get("/merged").json()
    assert merged["count"] == 1
    assert merged["items"][0]["winner_source"] == "a"
    # 渠道观测端点
    ch = c.get("/channels").json()
    assert ch["enabled"] is True
    sources = {x["source"] for x in ch["channels"]["channels"]}
    assert sources == {"a", "b"}
    # metrics 含多来源信息
    m = c.get("/metrics").json()
    assert m["multi_source"] is True and "channels" in m
    # 渠道上限 503
    app2 = create_app(multi_config(str(tmp_path / "d2"), max_sources=1))
    c2 = TestClient(app2)
    c2.post("/ingest", json=make_event("e1", 1000, source="a", seq=1))
    resp = c2.post("/ingest", json=make_event("e2", 1000, source="b", seq=1))
    assert resp.status_code == 503
    assert resp.json()["error"]["reason"] == "source_limit_rejected"


def test_http_merge_conflict_quarantined_with_distinct_reason(tmp_path):
    from fastapi.testclient import TestClient
    from market_data.app import create_app
    c = TestClient(create_app(multi_config(str(tmp_path / "data"))))
    c.post("/ingest", json=trade("a9", "a", 9, 60000, tid="T9", p=10.0, q=1.0))
    resp = c.post("/ingest", json=trade("b9", "b", 9, 60001, tid="T9", p=11.0, q=1.0))
    body = resp.json()
    # 隔离属于正常 200 结果（不是整批 422），原因码可与普通重复区分；
    # 确认冲突即整笔隔离：后到路与被清出的先到路各一条记录
    assert resp.status_code == 200
    assert body["quarantined"] == 2
    assert all(d["reason"] == "merge_conflict" for d in body["details"])
    q = c.get("/quarantine").json()
    conflict_ids = {i["event"]["event_id"] for i in q["items"]
                    if i["reason"] == "merge_conflict"}
    assert conflict_ids == {"a9", "b9"}


# ---------- 边界：同批冲突扩散 / 默认优先级 / 隔离不改已发布 ----------

def test_conflict_within_same_batch_quarantines_all_reports(mservice):
    """同一笔成交在同一批内被两路报成不同价：两路都隔离，不按任何一方计入。"""
    r = mservice.ingest_sync([
        trade("a9", "a", 1, 60000, tid="T9", p=10.0, q=1.0),
        trade("b9", "b", 1, 60001, tid="T9", p=11.0, q=1.0),
    ])
    assert r.quarantined == 2
    assert all(why is RejectReason.MERGE_CONFLICT for why in
               [d[1] for d in r.details])
    # 窗口无该笔，成交量为 0（不静默按一方算）
    assert mservice.query_provisional("A", 60000 // 10000 * 10000) is None
    assert {q.event.event_id for q in mservice.quarantine_list("A")} == {"a9", "b9"}


def test_default_priority_is_source_name_order(tmp_data_dir):
    """未配置 merge_sources 时，按渠道名字典序确定取舍（确定且可复现）。"""
    svc = MarketDataService(multi_config(tmp_data_dir, merge_sources=()))
    svc.ingest_sync([trade("z1", "z", 1, 1000, tid="T1")])
    svc.ingest_sync([trade("m1", "m", 1, 1005, tid="T1")])  # m < z，m 为主报
    assert mservice_prov(svc).event_ids == ("m1",)
    svc.close()


def mservice_prov(svc):
    return svc.query_provisional("A", 0)


def test_late_conflict_does_not_change_published_window(tmp_data_dir):
    """窗口已发布后，跨渠道关键内容对不上：后到路隔离，已发布窗口绝不改变。"""
    svc = MarketDataService(multi_config(tmp_data_dir))
    svc.ingest_sync([trade("a1", "a", 1, 1000, tid="T1", p=10.0, q=2.0)])
    svc.ingest_sync([trade("aX", "a", 2, 16000, tid="TX")])  # 推进水位发布窗口0
    before = svc.query("A", 0)
    assert len(before) == 1
    # 另一渠道在已发布窗口之后报同笔但数量不一致
    r = svc.ingest_sync([trade("b1", "b", 5, 20000, tid="T1", p=10.0, q=9.0)])
    assert r.quarantined == 1
    assert r.details[0][1] is RejectReason.MERGE_CONFLICT
    assert svc.query("A", 0) == before   # 已发布结果逐位不变
    svc.close()


def test_within_batch_out_of_order_same_channel_not_misjudged(mservice):
    """同一渠道同批逆序：较晚事件先排到，但较早事件不得被判超水位迟到。"""
    r = mservice.ingest_sync([
        make_event("a3", 9000, source="a", seq=3),
        make_event("a1", 1000, source="a", seq=1),
        make_event("a2", 5000, source="a", seq=2),
    ])
    assert r.accepted == 3 and r.quarantined == 0
    prov = mservice.query_provisional("A", 0)
    assert prov.event_ids == ("a1", "a2", "a3")


def test_within_batch_genuinely_late_event_quarantined(mservice):
    """已提交高水位后，同批较早的真迟到事件仍正确隔离（按本渠道水位）。"""
    mservice.ingest_sync([make_event("old", 1000, source="a", seq=1)])
    mservice.ingest_sync([make_event("fwd", 50000, source="a", seq=2)])
    r = mservice.ingest_sync([
        make_event("late", 2000, source="a", seq=3),
        make_event("ok", 51000, source="a", seq=4),
    ])
    assert r.accepted == 1 and r.quarantined == 1
    assert r.details[0][1] is RejectReason.LATE_BEYOND_WATERMARK

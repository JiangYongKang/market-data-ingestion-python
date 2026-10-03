"""多来源修复回归（bugfix-b）：

1. 同一渠道推多个标的时，推进判定按 (标的, 渠道) 各自独立；
2. 跨渠道冲突的隔离结果与"分几次投递、一次投几路"无关；
3. 重启/整段重放后，合并与渠道推进状态与重启前一致。
"""
from __future__ import annotations

from market_data.models import RejectReason
from market_data.service import MarketDataService

from .conftest import make_event
from .test_10_multi_source import multi_config, trade

# ---------- 1. 同一渠道多标的：推进互不影响 ----------

def test_same_channel_two_symbols_do_not_interfere(tmp_data_dir):
    svc = MarketDataService(multi_config(tmp_data_dir))
    # 渠道 a 把标的 X 推到 50s
    svc.ingest_sync([make_event("ax1", 50_000, source="a", seq=1, symbol="X")])
    # 同一渠道 a 的标的 Y 在 2s 的正常事件不得被判迟到
    r = svc.ingest_sync([make_event("ay1", 2_000, source="a", seq=2, symbol="Y")])
    assert r.accepted == 1 and r.quarantined == 0
    assert svc.query_provisional("Y", 0).event_ids == ("ay1",)
    svc.close()


def test_per_symbol_channel_watermarks_observed(tmp_data_dir):
    svc = MarketDataService(multi_config(tmp_data_dir))
    svc.ingest_sync([make_event("ax1", 50_000, source="a", seq=1, symbol="X")])
    svc.ingest_sync([make_event("ay1", 2_000, source="a", seq=2, symbol="Y")])
    snap = svc.channels_snapshot()
    ch = {c["source"]: c for c in snap["channels"]}["a"]
    per_symbol = {s["symbol"]: s for s in ch["symbols"]}
    assert per_symbol["X"]["max_event_time_ms"] == 50_000
    assert per_symbol["Y"]["max_event_time_ms"] == 2_000
    svc.close()


def test_window_close_waits_per_symbol_not_per_channel(tmp_data_dir):
    """标的 Y 的窗口只等参与 Y 的渠道推进，不被同渠道标的 X 的进度提前关掉。"""
    svc = MarketDataService(multi_config(tmp_data_dir))
    svc.ingest_sync([make_event("ax0", 1_000, source="a", seq=1, symbol="X")])
    svc.ingest_sync([make_event("ax1", 50_000, source="a", seq=2, symbol="X")])
    svc.ingest_sync([make_event("ay1", 2_000, source="a", seq=3, symbol="Y")])
    # X 在窗口0有事件且 (X,a) 水位已越过 -> 发布；Y 的 (Y,a) 水位仍在窗口0内 -> 不发布
    assert len(svc.query("X", 0)) == 1
    assert svc.query("Y", 0) == []
    svc.ingest_sync([make_event("ay2", 15_000, source="a", seq=4, symbol="Y")])
    pub = svc.query("Y", 0)
    assert len(pub) == 1 and pub[0].event_ids == ("ay1",)
    svc.close()


# ---------- 2. 冲突结果与投递形状无关 ----------

def _conflict_state(svc):
    q = svc.quarantine_list("A")
    return (
        sorted((x.event.event_id, x.reason.value) for x in q),
        svc.query_provisional("A", 60_000 // 10_000 * 10_000),
        svc.query("A"),
    )


def test_conflict_split_delivery_evicts_prior_winner(tmp_data_dir):
    """先到路已入窗但窗口未发布：确认冲突时必须把它清出结果并整笔隔离。"""
    svc = MarketDataService(multi_config(tmp_data_dir))
    r1 = svc.ingest_sync([trade("a9", "a", 9, 60_000, tid="T9", p=10.0, q=1.0)])
    assert r1.accepted == 1
    r2 = svc.ingest_sync([trade("b9", "b", 9, 60_001, tid="T9", p=11.0, q=1.0)])
    assert r2.quarantined == 2          # b9 + 被清出的 a9
    assert all(d[1] is RejectReason.MERGE_CONFLICT for d in r2.details)
    q_ids, prov, pub = _conflict_state(svc)
    assert q_ids == [("a9", "merge_conflict"), ("b9", "merge_conflict")]
    assert prov is None                 # 窗口里一笔都不剩
    assert pub == []
    svc.close()


def test_conflict_result_independent_of_delivery_shape(tmp_data_dir):
    """同批投递与分两次投递，最终隔离/窗口/发布状态完全一致。"""
    svc1 = MarketDataService(multi_config(tmp_data_dir + "-one"))
    svc1.ingest_sync([
        trade("a9", "a", 9, 60_000, tid="T9", p=10.0, q=1.0),
        trade("b9", "b", 9, 60_001, tid="T9", p=11.0, q=1.0),
    ])
    svc2 = MarketDataService(multi_config(tmp_data_dir + "-two"))
    svc2.ingest_sync([trade("a9", "a", 9, 60_000, tid="T9", p=10.0, q=1.0)])
    svc2.ingest_sync([trade("b9", "b", 9, 60_001, tid="T9", p=11.0, q=1.0)])
    assert _conflict_state(svc1) == _conflict_state(svc2)
    svc1.close()
    svc2.close()


# ---------- 3. 重启 / 重放后推进与合并状态一致 ----------

def test_restart_preserves_channel_progress_from_quarantine(tmp_data_dir):
    """冲突隔离事件推进过的渠道位置，重启后不丢、观测可见、判定一致。"""
    svc = MarketDataService(multi_config(tmp_data_dir))
    svc.ingest_sync([trade("a1", "a", 1, 1_000, tid="T1", p=10.0, q=1.0)])
    # b 路同笔冲突：b1 隔离，但渠道 b 的事件时间推进到 50s
    svc.ingest_sync([trade("b1", "b", 1, 50_000, tid="T1", p=11.0, q=1.0)])
    # b 路 2s 的事件相对本渠道水位 50s 已迟到 -> 隔离
    r = svc.ingest_sync([trade("b2", "b", 2, 2_000, tid="T2")])
    assert r.quarantined == 1
    snap_before = svc.channels_snapshot()
    svc.close()

    svc2 = MarketDataService(multi_config(tmp_data_dir))
    snap_after = svc2.channels_snapshot()
    ch_b = {c["source"]: c for c in snap_after["channels"]}["b"]
    ch_b_before = {c["source"]: c for c in snap_before["channels"]}["b"]
    assert ch_b["max_event_time_ms"] == ch_b_before["max_event_time_ms"] == 50_000
    assert ch_b["lag_ms"] is not None
    # 重启后同一条路同类的迟到事件仍被判迟到隔离，而不是被重新接纳
    r2 = svc2.ingest_sync([trade("b3", "b", 3, 3_000, tid="T3")])
    assert r2.accepted == 0 and r2.quarantined == 1
    assert r2.details[0][1] is RejectReason.LATE_BEYOND_WATERMARK
    # 隔离过的冲突事件重放仍是重复/隔离，不会被重新计入
    r3 = svc2.ingest_sync([trade("b1", "b", 1, 50_000, tid="T1", p=11.0, q=1.0)])
    assert r3.accepted == 0
    assert svc2.query_provisional("A", 0) is None
    svc2.close()


def test_restart_keeps_merge_winner_for_open_window(tmp_data_dir):
    """主报所在窗口未发布时重启：另一路一致上报仍被合并，不双计。"""
    svc = MarketDataService(multi_config(tmp_data_dir))
    svc.ingest_sync([trade("a1", "a", 1, 1_000, tid="T1")])
    svc.close()
    svc2 = MarketDataService(multi_config(tmp_data_dir))
    r = svc2.ingest_sync([trade("b1", "b", 1, 1_005, tid="T1")])
    assert r.merged == 1 and r.accepted == 0
    prov = svc2.query_provisional("A", 0)
    assert prov.count == 1 and prov.event_ids == ("a1",)
    svc2.close()


def test_restart_does_not_resurrect_superseded_winner(tmp_data_dir):
    """被高优先级替换的主报，重启后不得重新出现在未发布窗口里。"""
    svc = MarketDataService(multi_config(tmp_data_dir))
    svc.ingest_sync([trade("b1", "b", 1, 1_000, tid="T1")])
    svc.ingest_sync([trade("a1", "a", 1, 1_005, tid="T1")])  # 替换 b1
    assert svc.query_provisional("A", 0).event_ids == ("a1",)
    svc.close()
    svc2 = MarketDataService(multi_config(tmp_data_dir))
    prov = svc2.query_provisional("A", 0)
    assert prov.count == 1 and prov.event_ids == ("a1",)
    svc2.close()


def test_full_replay_after_restart_keeps_progress_and_results(tmp_data_dir):
    """整段重放：合并/冲突/迟到混合流重启后整体重投，推进与结果不变。"""
    stream = [
        trade("a1", "a", 1, 1_000, tid="T1", p=10.0, q=2.0),
        trade("b1", "b", 1, 1_005, tid="T1", p=10.0, q=2.0),   # 一致 -> 合并
        trade("a2", "a", 2, 2_000, tid="T2", p=10.0, q=1.0),
        trade("b2", "b", 2, 50_000, tid="T2", p=12.0, q=1.0),  # 冲突 -> 整笔隔离
        trade("b3", "b", 3, 3_000, tid="T3"),                  # 相对 b 已迟到 -> 隔离
        trade("a3", "a", 3, 16_000, tid="T4"),                 # 推进 a，发布窗口0/1
    ]
    svc = MarketDataService(multi_config(tmp_data_dir))
    for ev in stream:
        svc.ingest_sync([ev])
    pub_before = [(f.symbol, f.window_start_ms, f.count, f.total_quantity,
                   f.vwap, f.event_ids) for f in svc.query("A")]
    ch_before = {c["source"]: c["max_event_time_ms"]
                 for c in svc.channels_snapshot()["channels"]}
    q_before = sorted((x.event.event_id, x.reason.value)
                      for x in svc.quarantine_list("A"))
    svc.close()

    svc2 = MarketDataService(multi_config(tmp_data_dir))
    r = svc2.ingest_sync(stream)   # 整段重放
    assert r.accepted == 0 and r.quarantined == 0     # 不重新计算任何一笔
    pub_after = [(f.symbol, f.window_start_ms, f.count, f.total_quantity,
                  f.vwap, f.event_ids) for f in svc2.query("A")]
    assert pub_after == pub_before                    # 已发布结果逐位不变
    ch_after = {c["source"]: c["max_event_time_ms"]
                for c in svc2.channels_snapshot()["channels"]}
    assert ch_after == ch_before                      # 渠道推进位置一致
    q_after = sorted((x.event.event_id, x.reason.value)
                     for x in svc2.quarantine_list("A"))
    assert q_after == q_before                        # 隔离记录不翻倍
    svc2.close()

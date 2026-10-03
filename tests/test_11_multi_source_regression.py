"""多来源回归专题：三类线上发现的问题的稳定覆盖。

1. 多标的共用同一渠道：推进判定按 (标的, 渠道) 各自独立，互不干扰；
2. 跨渠道冲突：结果与"分几次投递、一次投几路"无关，确认冲突即整笔隔离
   （含已入窗未发布的主报），理由与普通重复可区分；
3. 重启 / 整段重放：合并与推进状态与重启前一致——隔离带来的渠道推进
   不丢、已隔离成交不被重算、已发布结果逐位不变、推进位置可观测。

单来源路径（multi_source=False）由 test_01..09 保证不回归。
"""
from __future__ import annotations

import pytest

from market_data.models import RejectReason
from market_data.service import MarketDataService

from .test_10_multi_source import multi_config, trade


@pytest.fixture()
def mservice(tmp_data_dir):
    svc = MarketDataService(multi_config(tmp_data_dir))
    yield svc
    svc.close()


def _chan(svc, source):
    snap = svc.channels_snapshot()
    return {c["source"]: c for c in snap["channels"]}[source]


def _event_time_view(svc, source):
    """渠道观测中的事件时间推进字段（处理时间字段重启后重置，不在比较内）。"""
    c = _chan(svc, source)
    return {k: c[k] for k in ("max_event_time_ms", "watermark_ms", "accepted")}


# ============================================================
# 场景一：多标的共用同一渠道，推进判定按 (标的, 渠道) 各自独立
# ============================================================

def test_fast_symbol_does_not_quarantine_slow_symbol_same_channel(mservice):
    """渠道 a 把标的 A 推到 50s 后，a 在标的 B 上 t=2s 的正常事件不得被判迟到。"""
    mservice.ingest_sync([trade("a1", "a", 1, 50_000, tid="TA", symbol="A")])
    r = mservice.ingest_sync([trade("a2", "a", 2, 2_000, tid="TB", symbol="B")])
    assert r.accepted == 1 and r.quarantined == 0
    assert mservice.query_provisional("B", 0).event_ids == ("a2",)


def test_same_channel_two_symbols_windows_close_independently(mservice):
    """同一渠道推两个标的：各标的窗口按各自实际到齐的事件关窗。"""
    mservice.ingest_sync([trade("a1", "a", 1, 1_000, tid="TA1", symbol="A")])
    mservice.ingest_sync([trade("b1", "a", 2, 2_000, tid="TB1", symbol="B")])
    # a 在 A 上推进到 15s：A 的窗口0关闭（含 a1）；B 的窗口0不能关
    mservice.ingest_sync([trade("a2", "a", 3, 15_000, tid="TA2", symbol="A")])
    assert [f.event_ids for f in mservice.query("A", 0)] == [("a1",)]
    assert mservice.query("B", 0) == []
    # a 在 B 上推进越过窗口0右边界后，B 的窗口0按 B 实际到齐事件关闭
    mservice.ingest_sync([trade("b2", "a", 4, 16_000, tid="TB2", symbol="B")])
    assert [f.event_ids for f in mservice.query("B", 0)] == [("b1",)]


def test_late_judgement_is_per_symbol_within_same_channel(mservice):
    """A 跑到 50s 后，A 上 t=2s 判迟到；同一渠道 B 上 t=2s 仍正常接纳。"""
    mservice.ingest_sync([trade("a1", "a", 1, 50_000, tid="TA", symbol="A")])
    late = mservice.ingest_sync([trade("a2", "a", 2, 2_000, tid="TA2", symbol="A")])
    assert late.quarantined == 1
    assert late.details[0][1] is RejectReason.LATE_BEYOND_WATERMARK
    ok = mservice.ingest_sync([trade("b1", "a", 3, 2_000, tid="TB", symbol="B")])
    assert ok.accepted == 1 and ok.quarantined == 0


def test_observability_breaks_down_per_symbol(mservice):
    """观测信息能看到同一渠道在各标的上各自推进到哪。"""
    mservice.ingest_sync([trade("a1", "a", 1, 50_000, tid="TA", symbol="A")])
    mservice.ingest_sync([trade("b1", "a", 2, 2_000, tid="TB", symbol="B")])
    syms = _chan(mservice, "a")["symbols"]
    assert syms["A"]["max_event_time_ms"] == 50_000
    assert syms["A"]["watermark_ms"] == 50_000
    assert syms["B"]["max_event_time_ms"] == 2_000
    assert syms["B"]["watermark_ms"] == 2_000


# ============================================================
# 场景二：冲突结果与投递批次划分无关，确认冲突即整笔隔离
# ============================================================

def _conflict_state(svc):
    """冲突相关可观测终态：隔离区内容 + 窗口内容 + 计数。"""
    q = sorted((x.event.event_id, x.reason) for x in svc.quarantine_list("A"))
    prov = svc.query_provisional("A", 60_000)
    return q, None if prov is None else prov.event_ids


def test_conflict_result_identical_same_batch_or_split(tmp_data_dir):
    """同一笔冲突：两路同批投递 vs 分两批先后投递，终态必须一致。"""
    # (i) 同批投递
    svc1 = MarketDataService(multi_config(tmp_data_dir + "_1"))
    r1 = svc1.ingest_sync([
        trade("a9", "a", 9, 60_000, tid="T9", p=10.0, q=1.0),
        trade("b9", "b", 9, 60_001, tid="T9", p=11.0, q=1.0),
    ])
    # (ii) 分两批先后投递
    svc2 = MarketDataService(multi_config(tmp_data_dir + "_2"))
    svc2.ingest_sync([trade("a9", "a", 9, 60_000, tid="T9", p=10.0, q=1.0)])
    r2 = svc2.ingest_sync([trade("b9", "b", 9, 60_001, tid="T9", p=11.0, q=1.0)])

    # 两路的第二批（含冲突确认）结果一致：两路都隔离、窗口一笔都不计
    assert r1.quarantined == r2.quarantined == 2
    assert _conflict_state(svc1) == _conflict_state(svc2)
    q, prov_ids = _conflict_state(svc2)
    assert q == [("a9", RejectReason.MERGE_CONFLICT),
                 ("b9", RejectReason.MERGE_CONFLICT)]
    assert prov_ids is None               # 先到的一路也被清出窗口
    svc1.close()
    svc2.close()


def test_conflict_evicts_unpublished_winner_and_never_counts(mservice):
    """先到的一路已入窗但窗口未发布：确认冲突时把它从结果里清出去。"""
    mservice.ingest_sync([trade("a9", "a", 9, 60_000, tid="T9", p=10.0, q=1.0)])
    assert mservice.query_provisional("A", 60_000).event_ids == ("a9",)
    r = mservice.ingest_sync([trade("b9", "b", 9, 60_001, tid="T9", p=11.0, q=1.0)])
    assert r.quarantined == 2 and r.merged == 0
    # 窗口里一笔都不计：成交量与价格特征都不含任一冲突路
    assert mservice.query_provisional("A", 60_000) is None
    # 推进水位越过该窗口：空窗口不发布，冲突成交永不计入已发布结果
    mservice.ingest_sync([trade("aX", "a", 10, 80_000, tid="TX")])
    assert mservice.query("A", 60_000) == []
    # 隔离理由与普通重复/普通迟到可区分
    reasons = {x.reason for x in mservice.quarantine_list("A")}
    assert RejectReason.MERGE_CONFLICT in reasons
    assert RejectReason.DUPLICATE_IDENTICAL not in reasons


def test_third_channel_report_after_conflict_also_quarantined(mservice):
    """冲突成立后，第三路同笔上报（无论内容）同样隔离，不按任何一方计入。"""
    mservice.ingest_sync([trade("a9", "a", 9, 60_000, tid="T9", p=10.0, q=1.0)])
    mservice.ingest_sync([trade("b9", "b", 9, 60_001, tid="T9", p=11.0, q=1.0)])
    r = mservice.ingest_sync([trade("c9", "c", 9, 60_002, tid="T9", p=10.0, q=1.0)])
    assert r.quarantined == 1 and r.accepted == 0 and r.merged == 0
    assert r.details[0][1] is RejectReason.MERGE_CONFLICT
    assert mservice.query_provisional("A", 60_000) is None


def test_conflict_does_not_poison_other_trades_in_same_window(mservice):
    """同窗口内其他无冲突成交不受影响，仍正常计入。"""
    mservice.ingest_sync([trade("a1", "a", 1, 61_000, tid="T1", p=5.0, q=4.0)])
    mservice.ingest_sync([trade("a9", "a", 9, 62_000, tid="T9", p=10.0, q=1.0)])
    mservice.ingest_sync([trade("b9", "b", 9, 62_001, tid="T9", p=11.0, q=1.0)])
    prov = mservice.query_provisional("A", 60_000)
    assert prov.event_ids == ("a1",) and prov.total_quantity == 4.0


# ============================================================
# 场景三：重启 / 整段重放后，合并与推进状态与重启前一致
# ============================================================

def test_quarantine_driven_channel_progress_survives_restart(tmp_data_dir):
    """渠道推进完全来自隔离事件时，重启后推进位置不丢、观测可见、
    早先被判迟到的同类事件不会被重新接纳。"""
    svc = MarketDataService(multi_config(tmp_data_dir))
    svc.ingest_sync([trade("a9", "a", 9, 60_000, tid="T9", p=10.0, q=1.0)])
    svc.ingest_sync([trade("b9", "b", 9, 60_001, tid="T9", p=11.0, q=1.0)])  # 冲突
    r = svc.ingest_sync([trade("b10", "b", 10, 59_000, tid="T10")])          # 迟到
    assert r.quarantined == 1
    before = _event_time_view(svc, "b")
    assert before["max_event_time_ms"] == 60_001
    svc.close()

    svc2 = MarketDataService(multi_config(tmp_data_dir))
    # 观测：b 渠道推进到哪、落后多少，重启后依然可见且一致
    assert _event_time_view(svc2, "b") == before
    assert _chan(svc2, "b")["lag_ms"] == 0
    # 重启前会被判迟到的事件，重启后同样判迟到，绝不重新接纳计入
    r2 = svc2.ingest_sync([trade("b11", "b", 11, 59_500, tid="T11")])
    assert r2.accepted == 0 and r2.quarantined == 1
    assert r2.details[0][1] is RejectReason.LATE_BEYOND_WATERMARK
    svc2.close()


def test_conflict_isolation_survives_restart_and_full_replay(tmp_data_dir):
    """冲突整笔隔离跨重启保持：主报不重入窗、隔离记录可查、
    整段重放不重新计算、已发布结果逐位不变。"""
    events = [
        trade("a1", "a", 1, 61_000, tid="T1", p=5.0, q=4.0),   # 正常成交
        trade("a9", "a", 9, 62_000, tid="T9", p=10.0, q=1.0),  # 冲突主报
        trade("b9", "b", 9, 62_001, tid="T9", p=11.0, q=1.0),  # 冲突次报
        trade("aX", "a", 10, 80_000, tid="TX"),                # 推进关窗
    ]
    svc = MarketDataService(multi_config(tmp_data_dir))
    for ev in events:
        svc.ingest_sync([ev])
    pub_before = [(f.window_start_ms, f.event_ids, f.total_quantity)
                  for f in svc.query("A")]
    q_before = sorted((x.event.event_id, x.reason)
                      for x in svc.quarantine_list("A"))
    ch_before = {s: _event_time_view(svc, s) for s in ("a", "b")}
    svc.close()

    svc2 = MarketDataService(multi_config(tmp_data_dir))
    # 已发布结果逐位一致；冲突主报 a9 没有因重启重新入窗
    assert [(f.window_start_ms, f.event_ids, f.total_quantity)
            for f in svc2.query("A")] == pub_before
    assert all("a9" not in f.event_ids for f in svc2.query("A"))
    assert svc2.query_provisional("A", 60_000) is None or \
        "a9" not in svc2.query_provisional("A", 60_000).event_ids
    # 隔离记录与渠道推进位置一致
    assert sorted((x.event.event_id, x.reason)
                  for x in svc2.quarantine_list("A")) == q_before
    assert {s: _event_time_view(svc2, s) for s in ("a", "b")} == ch_before
    # 整段重放：全部幂等，隔离过的成交不被重新算一遍
    r = svc2.ingest_sync(events)
    assert r.accepted == 0 and r.quarantined == 0
    assert [(f.window_start_ms, f.event_ids, f.total_quantity)
            for f in svc2.query("A")] == pub_before
    assert sorted((x.event.event_id, x.reason)
                  for x in svc2.quarantine_list("A")) == q_before
    svc2.close()


def test_published_trade_merge_state_survives_restart(tmp_data_dir):
    """已发布成交的合并状态重启后不丢：同笔迟到上报仍合并/隔离，
    绝不当成新成交重复计入，已发布窗口逐位不变。"""
    svc = MarketDataService(multi_config(tmp_data_dir))
    svc.ingest_sync([trade("a1", "a", 1, 1_000, tid="T1")])
    svc.ingest_sync([trade("aX", "a", 2, 16_000, tid="TX")])   # 发布窗口0
    before = svc.query("A", 0)
    assert len(before) == 1
    svc.close()

    svc2 = MarketDataService(multi_config(tmp_data_dir))
    # 一致迟到上报：仍按 loser_published 合并，不双计
    r = svc2.ingest_sync([trade("b1", "b", 1, 20_000, tid="T1")])
    assert r.merged == 1 and r.accepted == 0
    # 冲突迟到上报：隔离且理由可区分，已发布窗口不变
    r2 = svc2.ingest_sync([trade("b2", "b", 2, 21_000, tid="T1", q=9.0)])
    assert r2.quarantined == 1
    assert r2.details[0][1] is RejectReason.MERGE_CONFLICT
    assert svc2.query("A", 0) == before
    svc2.close()


def test_multi_symbol_channel_progress_consistent_after_restart(tmp_data_dir):
    """多标的共用渠道的场景重启后：各 (标的, 渠道) 推进位置与窗口结果一致。"""
    svc = MarketDataService(multi_config(tmp_data_dir))
    svc.ingest_sync([trade("a1", "a", 1, 50_000, tid="TA", symbol="A")])
    svc.ingest_sync([trade("b1", "a", 2, 2_000, tid="TB", symbol="B")])
    svc.ingest_sync([trade("b2", "b", 1, 3_000, tid="TB2", symbol="B")])
    snap_before = _chan(svc, "a")["symbols"]
    prov_b = svc.query_provisional("B", 0).event_ids
    svc.close()

    svc2 = MarketDataService(multi_config(tmp_data_dir))
    assert _chan(svc2, "a")["symbols"] == snap_before
    assert svc2.query_provisional("B", 0).event_ids == prov_b
    # A 上 t=2s 仍判迟到（重启前同理），B 上正常事件仍接纳
    r = svc2.ingest_sync([trade("a2", "a", 3, 2_000, tid="TA2", symbol="A")])
    assert r.quarantined == 1
    r2 = svc2.ingest_sync([trade("b3", "a", 4, 4_000, tid="TB3", symbol="B")])
    assert r2.accepted == 1
    svc2.close()

"""端到端本地演示：不依赖任何外部服务，直接运行 ``python -m market_data.demo``。

覆盖：正常写入 -> 重复/冲突区分 -> 窗口内乱序修正 -> 发布特征 ->
超水位迟到进隔离区 -> 位点回退拒绝 -> 重启重放一致 -> 基准观测。
"""
from __future__ import annotations

import dataclasses
import logging
import tempfile
import time

from .config import Config
from .errors import CheckpointRollbackError, IngestionError
from .service import MarketDataService

logging.basicConfig(level=logging.INFO, format="%(message)s")


def ev(eid, t, p, q, seq=None, sym="A", src="s", **extra):
    d = dict(event_id=eid, source=src, seq=seq if seq is not None else int(eid[1:]),
             symbol=sym, price=p, quantity=q, event_time_ms=t)
    d.update(extra)
    return d


def main() -> None:
    with tempfile.TemporaryDirectory() as d:
        print("== 1. 批量写入（含乱序） ==")
        svc = MarketDataService(Config(data_dir=d))
        r = svc.ingest_sync([
            ev("e3", 9_000, 30, 2),
            ev("e1", 1_000, 10, 3),
            ev("e2", 5_000, 20, 1),
        ])
        print("  ->", r)

        print("== 2. 普通重复 vs 内容冲突 ==")
        print("  identical:", svc.ingest_sync(ev("e1", 1_000, 10, 3)))
        try:
            svc.ingest_sync(ev("e1", 1_000, 99, 3))
        except IngestionError as exc:
            print("  conflict:", exc.reason.value)

        print("== 3. 推进水位并发布窗口 ==")
        svc.ingest_sync([ev("f1", 20_000, 1, 1, seq=50)])
        for w in svc.query("A"):
            print(f"  window[{w.window_start_ms},{w.window_end_ms}) "
                  f"vwap={w.vwap:.4f} vol={w.volatility:.4f} ids={w.event_ids}")

        print("== 4. 超水位迟到 -> 隔离区 ==")
        r = svc.ingest_sync([ev("late1", 2_000, 50, 5, seq=90)])
        print("  ->", r)
        for q in svc.quarantine_list("A"):
            print(f"  quarantined {q.event.event_id} @wm={q.watermark_ms}: {q.detail}")

        print("== 5. 位点单调，回退拒绝 ==")
        try:
            svc.checkpoint("s", 0)
        except CheckpointRollbackError as exc:
            print(" ", exc.reason.value, exc)

        print("== 6. 重启：结果逐位一致、重放全幂等 ==")
        svc2 = MarketDataService(Config(data_dir=d))
        before = svc.query("A", 0)[0]
        after = svc2.query("A", 0)[0]
        assert (before.vwap, before.volatility, before.event_ids) == \
               (after.vwap, after.volatility, after.event_ids)
        print("  published identical:", after.event_ids, f"vwap={after.vwap:.4f}")
        print("  replay:", svc2.ingest_sync([
            ev("e1", 1_000, 10, 3), ev("e2", 5_000, 20, 1), ev("e3", 9_000, 30, 2)]))

        print("== 7. 本地基准（内存，1 万事件 + 逆序 + 整段重放） ==")
        n = 10_000
        bench = MarketDataService(dataclasses.replace(
            svc2.config, data_dir=":memory:", max_pending_events=n * 2 + 100))
        events = [{
            "event_id": f"b-{i}", "source": "bench", "seq": i,
            "symbol": f"S{i % 8}", "price": 100.0 + (i % 7),
            "quantity": 1.0 + (i % 3), "event_time_ms": (i // 50) * 100,
        } for i in range(n)]
        t0 = time.perf_counter_ns()
        r1 = bench.ingest_sync(events)
        first_ms = (time.perf_counter_ns() - t0) / 1e6
        t1 = time.perf_counter_ns()
        r2 = bench.ingest_sync(events)
        replay_ms = (time.perf_counter_ns() - t1) / 1e6
        snap = bench.metrics_snapshot()
        print(f"  accepted={r1.accepted} in {first_ms:.1f}ms; "
              f"replay duplicate={r2.duplicate} in {replay_ms:.1f}ms")
        print(f"  ns/event avg={snap['ingest']['avg_ns_per_event']:.0f} "
              f"p99={snap['ingest']['p99_ns_per_event']:.0f}; "
              f"state≈{snap['estimated_state_bytes']/1e6:.1f}MB; "
              f"published={snap['published_windows']}")

        print("== 8. 多来源合并（同标的三渠道，优先级 venueA>venueB>venueC） ==")
        ms = MarketDataService(Config(
            data_dir=":memory:",
            multi_source_symbols={"MS": ["venueA", "venueB", "venueC"]}))
        r = ms.ingest_sync([
            ev("m1", 1_000, 10, 2, sym="MS", src="venueA", seq=1, trade_id="X1"),
            ev("m2", 2_000, 10, 2, sym="MS", src="venueB", seq=1, trade_id="X1"),
            ev("m3", 3_000, 11, 2, sym="MS", src="venueC", seq=1, trade_id="X1"),
        ])
        print(f"  accepted={r.accepted} merged={r.merged} "
              f"quarantined={r.quarantined}（同一笔只计一次量，价格冲突隔离）")
        for q in ms.quarantine_list("MS"):
            print(f"  conflict {q.event.event_id}: {q.detail}")
        print("  渠道推进:", {
            c["source"]: c["max_event_time_ms"]
            for c in ms.channel_snapshot()["MS"]["channels"]})
        print("DEMO_OK")


if __name__ == "__main__":
    main()

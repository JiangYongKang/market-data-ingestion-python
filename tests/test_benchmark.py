"""Local reproducible benchmark gates: latency and dedupe memory budgets.

The scale and budgets come from :class:`EngineConfig` so the gates are
configurable and the same numbers are observable on ``GET /metrics``.
These are functional SLO tests, not hardware-dependent microbenchmarks:
generous default budgets assert the implementation stays linear/bounded on
any normal local machine while still catching an accidental O(n^2) or an
unbounded index.
"""
from __future__ import annotations

import logging
import os
import time

import pytest

from app.ingestion.config import EngineConfig
from app.ingestion.engine import IngestionEngine
from tests.conftest import payload


@pytest.mark.benchmark
def test_dedupe_and_aggregation_within_configured_budgets() -> None:
    scale = int(os.environ.get("BENCH_SCALE", "5000"))
    logging.getLogger("ingestion").setLevel(logging.WARNING)
    cfg = EngineConfig(
        window_size_ms=10_000,
        allowed_lateness_ms=1_000,
        bench_scale_events=scale,
        # Per-event write budget (p99 microseconds). Generous by default so
        # the gate is stable on a shared CI box; tighten via env if desired.
        bench_max_write_us=int(os.environ.get("BENCH_MAX_WRITE_US", "8000")),
        # Resident dedupe budget: 256 bytes/entry estimate.
        bench_max_dedupe_kb=int(os.environ.get("BENCH_MAX_DEDUPE_KB", "200000")),
    )
    eng = IngestionEngine(cfg).start()

    # Deterministic, reproducible dataset; spread across a few symbols.
    batch = 500
    # write_us_* measures one batch; scale the per-event budget by batch size.
    batch_budget_us = cfg.bench_max_write_us * batch
    t_start = time.perf_counter()
    produced = 0
    for base in range(0, scale, batch):
        rows = [
            payload(
                f"evt-{base + i}",
                seq=base + i + 1,
                symbol=f"SYM{i % 4}",
                event_time_ms=5_000 + (i % 10),
                price=100.0 + (i % 100),
                quantity=1.0 + (i % 7),
            )
            for i in range(min(batch, scale - base))
        ]
        r = eng.write_batch(rows)
        assert r.committed
        produced += r.accepted
    wall_s = time.perf_counter() - t_start

    m = eng.metrics_snapshot()
    assert produced == scale
    assert m["accepted"] == scale
    assert eng.current_checkpoint().offset == scale

    # --- latency gates (observed, hence also verifiable via /metrics) ------
    assert m["write_us_p99"] <= batch_budget_us, (
        f"p99 batch write {m['write_us_p99']}us > budget {batch_budget_us}us "
        f"({cfg.bench_max_write_us}us/event x {batch})"
    )
    # Aggregate throughput gate: asserts no super-linear blowup; the budget
    # is deliberately generous to stay stable on a shared local machine.
    mean_us_per_event = wall_s * 1_000_000 / scale
    assert mean_us_per_event <= 2_000, (
        f"mean {mean_us_per_event:.1f}us/event suggests non-linear processing"
    )

    # --- memory gate --------------------------------------------------------
    assert m["dedupe_entries"] == scale
    dedupe_kb = m["dedupe_memory_bytes"] / 1024
    assert dedupe_kb <= cfg.bench_max_dedupe_kb, (
        f"dedupe resident ~{dedupe_kb:.0f}KB > budget "
        f"{cfg.bench_max_dedupe_kb}KB"
    )
    # Explicit per-entry estimate must stay bounded/constant.
    per_entry = m["dedupe_memory_bytes"] / scale
    assert per_entry <= 512, f"per-entry estimate grew to {per_entry} bytes"

    # --- determinism: re-querying yields identical aggregates --------------
    first = [(w.symbol, w.window_start_ms, w.event_count, w.vwap)
             for w in eng.query_windows(include_unpublished=True)]
    second = [(w.symbol, w.window_start_ms, w.event_count, w.vwap)
              for w in eng.query_windows(include_unpublished=True)]
    assert first == second
    total_in_windows = sum(c for _, _, c, _ in first)
    assert total_in_windows == scale

    logging.getLogger("ingestion").setLevel(logging.INFO)
    print(
        f"\n[benchmark] scale={scale} wall={wall_s:.3f}s "
        f"mean/event={mean_us_per_event:.1f}us "
        f"p99_batch_write={m['write_us_p99']}us "
        f"(budget {batch_budget_us}us) dedupe={dedupe_kb:.1f}KB "
        f"per_entry={per_entry:.0f}B accepted={m['accepted']}"
    )
    eng.close()


@pytest.mark.benchmark
def test_duplicate_heavy_workload_stays_bounded() -> None:
    # 80% redelivery: dedupe index must NOT grow, latency stays flat.
    logging.getLogger("ingestion").setLevel(logging.WARNING)
    scale = 2000
    eng = IngestionEngine(EngineConfig(
        window_size_ms=10_000, allowed_lateness_ms=1_000,
        max_dedupe_entries=10_000,
    )).start()
    # seed 20% unique
    uniq = scale // 5
    for i in range(uniq):
        eng.write_one(payload(f"u{i}", seq=i + 1, event_time_ms=5_000,
                              price=100 + i % 50))
    assert len(eng.dedupe) == uniq

    for j in range(scale - uniq):
        i = j % uniq
        r = eng.write_one(payload(f"u{i}", seq=i + 1, event_time_ms=5_000,
                                  price=100 + i % 50))
        assert r.duplicates == 1
    m = eng.metrics_snapshot()
    assert len(eng.dedupe) == uniq  # redelivery did not grow the index
    assert m["duplicates_identical"] == scale - uniq
    assert eng.current_checkpoint().offset == uniq
    logging.getLogger("ingestion").setLevel(logging.INFO)
    eng.close()

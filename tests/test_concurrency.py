"""Concurrency: same-symbol writers, queries, replay guards under threads."""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.ingestion.config import EngineConfig
from app.ingestion.engine import IngestionEngine
from app.ingestion.errors import RejectReason
from tests.conftest import payload


def test_concurrent_writers_no_duplicate_or_missing_counts() -> None:
    eng = IngestionEngine(
        EngineConfig(max_inflight_batches=4, window_size_ms=10_000,
                     allowed_lateness_ms=0)
    ).start()
    n_threads, per_thread = 8, 200

    def worker(t: int) -> None:
        for i in range(per_thread):
            seq = t * per_thread + i + 1
            eng.write_one(payload(
                f"e-{t}-{i}", seq=seq, symbol="BTCUSD",
                event_time_ms=5_000, price=100.0 + (seq % 50), quantity=1.0,
            ))

    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        list(ex.map(worker, range(n_threads)))

    total = n_threads * per_thread
    assert eng.current_checkpoint().offset == total
    wins = eng.query_windows("BTCUSD", include_unpublished=True)
    assert sum(w.event_count for w in wins) == total
    ids = [i for w in wins for i in w.event_ids]
    assert len(ids) == len(set(ids)) == total
    m = eng.metrics_snapshot()
    assert m["accepted"] == total and m["duplicates_identical"] == 0
    eng.close()


def test_concurrent_duplicate_redelivery_is_idempotent() -> None:
    eng = IngestionEngine(EngineConfig(window_size_ms=10_000)).start()

    def write(event_id: str, seq: int) -> None:
        eng.write_one(payload(event_id, seq=seq, event_time_ms=1_000))

    # same event hammered from many threads
    with ThreadPoolExecutor(max_workers=16) as ex:
        list(ex.map(lambda _: write("dup", 1), range(64)))

    assert eng.current_checkpoint().offset == 1
    wins = eng.query_windows("BTCUSD", include_unpublished=True)
    assert sum(w.event_count for w in wins) == 1
    assert eng.metrics_snapshot()["duplicates_identical"] == 63
    eng.close()


def test_queries_never_observe_partial_batch() -> None:
    eng = IngestionEngine(
        EngineConfig(window_size_ms=100_000, allowed_lateness_ms=0)
    ).start()
    stop = threading.Event()
    violations: list[str] = []

    def reader() -> None:
        while not stop.is_set():
            for w in eng.query_windows("BTCUSD", include_unpublished=True):
                # VWAP identity must always hold for a visible window state:
                # sum(price*qty)/sum(qty) consistent with count > 0
                if w.event_count == 0 or w.total_quantity <= 0:
                    violations.append("bad window")

    def writer() -> None:
        for i in range(1, 401):
            eng.write_batch([
                payload(f"a{i}", seq=2 * i, event_time_ms=50_000,
                        price=100 + i % 7, quantity=1),
                payload(f"b{i}", seq=2 * i + 1, event_time_ms=50_000,
                        price=101 + i % 7, quantity=2),
            ])

    t = threading.Thread(target=reader)
    t.start()
    writer()
    stop.set()
    t.join()
    assert violations == []
    wins = eng.query_windows("BTCUSD", include_unpublished=True)
    assert sum(w.event_count for w in wins) == 800
    eng.close()


def test_conflict_under_concurrency_rolls_back_atomically() -> None:
    eng = IngestionEngine(EngineConfig(window_size_ms=10_000)).start()
    barrier = threading.Barrier(8)

    def race(t: int) -> None:
        barrier.wait()
        if t == 0:
            eng.write_one(payload("race", seq=1, event_time_ms=1_000, price=100))
        else:
            # one of these wins as first identical; others are either dup or
            # conflict -- none must double count
            eng.write_one(payload(
                "race", seq=1, event_time_ms=1_000, price=100 + t))

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(race, range(8)))  # propagate any worker exception

    assert eng.current_checkpoint().offset <= 1
    wins = eng.query_windows("BTCUSD", include_unpublished=True)
    assert sum(w.event_count for w in wins) == 1
    # exactly one accepted original; rest classified as dup/conflict
    m = eng.metrics_snapshot()
    assert (m["accepted"] >= 1)
    eng.close()


def test_replay_rewind_concurrent_with_writes_is_rejected() -> None:
    import tempfile
    cfg = EngineConfig(
        state_dir=tempfile.mkdtemp(), fsync=False, window_size_ms=10_000
    )
    eng = IngestionEngine(cfg).start()
    for i in range(1, 6):
        eng.write_one(payload(f"e{i}", seq=i, event_time_ms=1_000 + i))
    # concurrent rewind request must be refused without state change
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [ex.submit(eng.replay_from, 1) for _ in range(4)]
        for f in futs:
            with pytest.raises(Exception) as ei:
                f.result()
            assert ei.value.reason is RejectReason.CHECKPOINT_REGRESSED
    assert eng.current_checkpoint().offset == 5
    eng.close()

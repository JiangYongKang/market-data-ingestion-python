"""Backpressure policies and resource caps: bounded memory, no partial state."""
from __future__ import annotations

import threading

import pytest

from app.ingestion.config import BackpressurePolicy, EngineConfig
from app.ingestion.engine import IngestionEngine
from app.ingestion.errors import BackpressureError, RejectReason
from tests.conftest import payload


def _cfg(**kw) -> EngineConfig:
    kw.setdefault("window_size_ms", 10_000)
    return EngineConfig(**kw)


def test_reject_policy_raises_under_inflight_cap() -> None:
    eng = IngestionEngine(_cfg(
        max_inflight_batches=1,
        backpressure_policy=BackpressurePolicy.REJECT,
    )).start()
    # Prime the single permit: service is now saturated.
    assert eng._gate.acquire(blocking=False)
    try:
        with pytest.raises(BackpressureError) as ei:
            eng.write_one(payload("e2", seq=2, event_time_ms=200))
        assert ei.value.reason is RejectReason.BACKPRESSURE_REJECTED
        assert eng.metrics_snapshot()["backpressure_rejected"] >= 1
        # nothing was applied while saturated
        assert eng.current_checkpoint().offset == 0
    finally:
        eng._gate.release()
    # capacity restored -> a normal write succeeds
    assert eng.write_one(payload("e1", seq=1, event_time_ms=100)).committed
    eng.close()


def test_delay_policy_waits_then_succeeds_deterministically() -> None:
    eng = IngestionEngine(_cfg(
        max_inflight_batches=1,
        backpressure_policy=BackpressurePolicy.DELAY,
        backpressure_max_wait_ms=5_000,
    )).start()
    # Saturate the single permit, then free it shortly after a waiter starts.
    assert eng._gate.acquire(blocking=False)
    done = threading.Event()
    result = {}

    def delayed() -> None:
        result["batch"] = eng.write_one(
            payload("d1", seq=1, event_time_ms=100)
        )
        done.set()

    t = threading.Thread(target=delayed)
    t.start()
    # ensure the waiter is queued, then release capacity
    import time as _t
    _t.sleep(0.1)
    eng._gate.release()
    assert done.wait(timeout=5), "delayed writer never completed"
    t.join()
    m = eng.metrics_snapshot()
    assert m["backpressure_delayed"] >= 1
    assert m["backpressure_rejected"] == 0
    assert result["batch"].committed and result["batch"].accepted == 1
    assert eng.current_checkpoint().offset == 1
    eng.close()


def test_many_threads_under_cap_1_complete_without_loss() -> None:
    eng = IngestionEngine(_cfg(
        max_inflight_batches=1,
        backpressure_policy=BackpressurePolicy.DELAY,
        backpressure_max_wait_ms=10_000,
    )).start()

    def worker(start: int) -> None:
        for i in range(50):
            eng.write_one(payload(
                f"w{start}-{i}", seq=start * 50 + i + 1, event_time_ms=100))

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)
        assert not t.is_alive(), "writer thread deadlocked"
    assert eng.current_checkpoint().offset == 200
    eng.close()


def test_delay_policy_gives_up_after_wait_budget() -> None:
    eng = IngestionEngine(_cfg(
        max_inflight_batches=1,
        backpressure_policy=BackpressurePolicy.DELAY,
        backpressure_max_wait_ms=50,
    )).start()
    assert eng._gate.acquire(blocking=False)  # saturate
    done = threading.Event()

    def delayed() -> None:
        try:
            eng.write_one(payload("e9", seq=9, event_time_ms=900))
        except BackpressureError:
            done.set()

    t = threading.Thread(target=delayed)
    t.start()
    assert done.wait(timeout=5), "wait budget was not enforced"
    t.join()
    assert eng.metrics_snapshot()["backpressure_rejected"] >= 1
    eng._gate.release()
    eng.close()


def test_dedupe_capacity_is_a_hard_bound_no_silent_eviction() -> None:
    eng = IngestionEngine(EngineConfig(max_dedupe_entries=5)).start()
    for i in range(5):
        r = eng.write_one(payload(f"e{i}", seq=i + 1, event_time_ms=i))
        assert r.committed
    # capacity exhaustion is a classified, atomic backpressure rejection
    r = eng.write_batch([payload("e6", seq=6, event_time_ms=6)])
    assert not r.committed and r.rejected == 1
    assert r.records[0].reason is RejectReason.BACKPRESSURE_REJECTED
    # the failed batch left no residue
    assert eng.current_checkpoint().offset == 5
    assert len(eng.dedupe) == 5
    assert eng.metrics_snapshot()["backpressure_rejected"] >= 1
    eng.close()


def test_quarantine_is_bounded_per_symbol_with_fifo_eviction_counted() -> None:
    eng = IngestionEngine(
        EngineConfig(max_quarantine_per_symbol=3, window_size_ms=1000,
                     allowed_lateness_ms=0)
    ).start()
    eng.write_one(payload("ahead", seq=1, event_time_ms=10_000))
    for i in range(5):
        eng.write_one(payload(f"late{i}", seq=10 + i, event_time_ms=i))
    rows = eng.query_quarantine()
    assert [r.event_id for r in rows] == ["late2", "late3", "late4"]
    assert eng.quarantine.evicted == 2
    assert eng.metrics_snapshot()["quarantine_evicted"] == 2
    eng.close()


def test_journal_failure_rolls_back_entire_batch(tmp_path, monkeypatch) -> None:
    state = tmp_path / "state"
    eng = IngestionEngine(
        EngineConfig(state_dir=str(state), fsync=False)
    ).start()

    def boom(events) -> None:
        raise OSError("simulated disk failure")

    monkeypatch.setattr(eng.store, "append_events", boom)
    with pytest.raises(OSError):
        eng.write_batch([
            payload("x1", seq=1, event_time_ms=10),
            payload("x2", seq=2, event_time_ms=20),
        ])
    assert eng.current_checkpoint().offset == 0
    assert eng.query_windows(include_unpublished=True) == []
    assert len(eng.dedupe) == 0
    assert eng.metrics_snapshot()["rollbacks"] == 1
    # journaled counter also untouched: nothing was half-acknowledged
    assert eng._journaled == 0
    eng.close()

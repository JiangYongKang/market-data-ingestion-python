"""Checkpoint monotonicity, rewind rejection, replay reproducibility."""
from __future__ import annotations

import pytest

from app.ingestion.config import EngineConfig
from app.ingestion.engine import IngestionEngine
from app.ingestion.errors import CheckpointError, RejectReason
from tests.conftest import payload


def _feed(eng: IngestionEngine, n: int, *, t0: int = 0, step: int = 100) -> None:
    # event times far enough apart to publish windows deterministically
    for i in range(1, n + 1):
        eng.write_one(payload(
            f"e{i}", seq=i, event_time_ms=t0 + i * step,
            price=100.0 + i, quantity=1.0,
        ))


def test_checkpoint_only_advances_on_commit(ephemeral_engine) -> None:
    eng = ephemeral_engine
    assert eng.current_checkpoint().offset == 0
    eng.write_one(payload("e1", seq=1, event_time_ms=100))
    assert eng.current_checkpoint().offset == 1
    # duplicate / rejected writes leave the offset untouched
    eng.write_one(payload("e1", seq=1, event_time_ms=100))
    bad = eng.write_one(payload("e1", seq=1, event_time_ms=100, price=42))
    assert not bad.committed
    assert eng.current_checkpoint().offset == 1


def test_replay_rewind_is_refused_on_live_engine(durable_engine) -> None:
    make = durable_engine
    eng = make()
    _feed(eng, 5)
    assert eng.current_checkpoint().offset == 5
    for target in (0, 3, 5):
        with pytest.raises(CheckpointError) as ei:
            eng.replay_from(target)
        assert ei.value.reason is RejectReason.CHECKPOINT_REGRESSED
        assert "monotonic" in str(ei.value)
    assert eng.current_checkpoint().offset == 5


def test_unknown_offset_actually_rejected(durable_engine) -> None:
    eng = durable_engine()
    _feed(eng, 3)
    with pytest.raises(CheckpointError) as ei:
        eng.replay_from(4)
    assert ei.value.reason is RejectReason.CHECKPOINT_UNKNOWN


def test_restart_reproduces_published_results_without_gaps_or_dupes(
    durable_engine,
) -> None:
    state = None
    make = durable_engine
    eng = make()
    cfg = eng.config
    state = cfg.state_dir
    _feed(eng, 20, step=200)  # t=200..4000, windows 0..3000
    before = [(w.window_start_ms, w.vwap, w.event_count, w.event_ids)
              for w in eng.query_windows()]
    cp_before = eng.current_checkpoint().offset
    eng.close()

    eng2 = IngestionEngine(EngineConfig(state_dir=state, fsync=False)).start()
    assert eng2.current_checkpoint().offset == cp_before
    after = [(w.window_start_ms, w.vwap, w.event_count, w.event_ids)
             for w in eng2.query_windows()]
    assert before == after
    # no duplicate counting after recovery
    all_counts = [c for _, _, c, _ in after]
    assert sum(all_counts) == len({i for _, _, _, ids in after for i in ids})
    eng2.close()


def test_boot_time_prefix_replay_is_deterministic(durable_engine) -> None:
    eng = durable_engine()
    state = eng.config.state_dir
    _feed(eng, 10, step=200)
    full = [(w.window_start_ms, w.vwap) for w in eng.query_windows()]
    eng.close()

    prefix_eng = IngestionEngine(
        EngineConfig(state_dir=state, fsync=False, start_offset=6)
    ).start()
    assert prefix_eng.current_checkpoint().offset == 6
    prefix = [(w.window_start_ms, w.vwap)
              for w in prefix_eng.query_windows()]
    reference = IngestionEngine(
        EngineConfig(state_dir=state, fsync=False, start_offset=6)
    ).start()
    expected = [(w.window_start_ms, w.vwap)
                for w in reference.query_windows()]
    reference.close()
    assert prefix == expected  # deterministic (nothing published yet here)
    # include provisional state comparison too
    prov = [(w.window_start_ms, w.vwap, w.event_ids)
            for w in prefix_eng.query_windows(include_unpublished=True)]
    prov_ref = [(w.window_start_ms, w.vwap, w.event_ids)
                for w in reference.query_windows(include_unpublished=True)]
    assert prov == prov_ref and len(prov) >= 1
    # and then forward replay to end reproduces everything exactly
    prefix_eng.replay_from(7)
    # ... jump one at a time through to 10
    cur = 7
    while cur < 10:
        prefix_eng.replay_from(cur + 1)
        cur += 1
    again = [(w.window_start_ms, w.vwap)
             for w in prefix_eng.query_windows()]
    assert again == full
    prefix_eng.close()


def test_restart_still_rejects_duplicate_ids(durable_engine) -> None:
    eng = durable_engine()
    state = eng.config.state_dir
    eng.write_one(payload("e1", seq=1, event_time_ms=100))
    eng.close()
    eng2 = IngestionEngine(EngineConfig(state_dir=state, fsync=False)).start()
    r = eng2.write_one(payload("e1", seq=1, event_time_ms=100))
    assert r.duplicates == 1 and r.checkpoint == 1
    r2 = eng2.write_one(payload("e1", seq=1, event_time_ms=100, price=9))
    assert not r2.committed and r2.conflicts == 1


def test_recovery_from_journal_when_snapshot_absent(durable_engine) -> None:
    eng = durable_engine()
    state = eng.config.state_dir
    _feed(eng, 12, step=200)
    before = [(w.window_start_ms, w.vwap, w.event_count, w.event_ids)
              for w in eng.query_windows()]
    cp = eng.current_checkpoint().offset
    eng.close()

    import os
    snap = os.path.join(state, "state.snapshot")
    assert os.path.exists(snap)
    os.remove(snap)  # force full journal replay from offset 0

    eng2 = durable_engine.__self__ if False else None
    from app.ingestion.config import EngineConfig
    eng2 = IngestionEngine(EngineConfig(state_dir=state, fsync=False)).start()
    assert eng2.current_checkpoint().offset == cp
    after = [(w.window_start_ms, w.vwap, w.event_count, w.event_ids)
             for w in eng2.query_windows()]
    assert after == before
    eng2.close()


def test_torn_journal_tail_is_truncated_not_corrupted(durable_engine) -> None:
    eng = durable_engine()
    state = eng.config.state_dir
    _feed(eng, 6, step=200)
    cp = eng.current_checkpoint().offset
    eng.close()

    import os
    path = os.path.join(state, "events.log")
    with open(path, "ab") as f:
        f.write(b"\x00\x00\x00\x00\x00\x00\x09\x00partial-garbage")
    from app.ingestion.config import EngineConfig
    eng2 = IngestionEngine(EngineConfig(state_dir=state, fsync=False)).start()
    # exactly the pre-crash commits survive; no half batch, no over-count
    assert eng2.current_checkpoint().offset == cp
    assert len(eng2.store.read_all()) == cp
    eng2.close()

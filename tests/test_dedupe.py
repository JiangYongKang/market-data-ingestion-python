"""Idempotency: identical redelivery vs conflicting redelivery."""
from __future__ import annotations

from app.ingestion.errors import RejectReason
from tests.conftest import event, payload


def test_identical_redelivery_is_idempotent(ephemeral_engine) -> None:
    eng = ephemeral_engine
    r1 = eng.write_one(payload("e1", seq=1, event_time_ms=10, price=100, quantity=2))
    assert r1.committed and r1.accepted == 1
    assert r1.records[0].reason is None
    assert eng.current_checkpoint().offset == 1

    r2 = eng.write_one(payload("e1", seq=1, event_time_ms=10, price=100, quantity=2))
    assert r2.committed
    assert r2.accepted == 0
    assert r2.duplicates == 1
    rec = r2.records[0]
    assert rec.accepted is True
    assert rec.reason is RejectReason.DUPLICATE_IDENTICAL
    # checkpoint must not advance on redelivery
    assert r2.checkpoint == 1
    assert eng.current_checkpoint().offset == 1

    wins = eng.query_windows(include_unpublished=True)
    assert wins[0].event_count == 1
    assert wins[0].event_ids == ("e1",)


def test_conflicting_redelivery_is_rejected_with_distinct_reason(
    ephemeral_engine, caplog
) -> None:
    eng = ephemeral_engine
    eng.write_one(payload("e1", seq=1, event_time_ms=10, price=100, quantity=2))
    r = eng.write_one(payload("e1", seq=1, event_time_ms=10, price=250, quantity=2))
    assert not r.committed
    assert r.conflicts == 1
    rec = r.records[0]
    assert rec.accepted is False
    assert rec.reason is RejectReason.DUPLICATE_CONFLICT
    assert "stored=" in rec.detail and "new=" in rec.detail
    assert r.checkpoint == 1  # nothing committed
    # log must carry event id and the decision basis
    texts = [r2.message for r2 in caplog.records]
    assert any("e1" in t and "DUPLICATE_CONFLICT" in t for t in texts)
    # original aggregate untouched
    wins = eng.query_windows(include_unpublished=True)
    assert wins[0].event_count == 1 and wins[0].vwap == 100.0


def test_each_field_conflict_is_detected(ephemeral_engine) -> None:
    eng = ephemeral_engine
    base = payload("e1", seq=1, event_time_ms=10, price=100, quantity=1)
    eng.write_one(dict(base))
    for changes in (
        {"price": 101.0},
        {"quantity": 2.0},
        {"event_time_ms": 11},
        {"symbol": "ETHUSD"},
        {"seq": 2},
        {"source": "other"},
    ):
        bad = dict(base)
        bad.update(changes)
        r = eng.write_one(bad)
        assert not r.committed and r.conflicts == 1, changes
        assert r.records[0].reason is RejectReason.DUPLICATE_CONFLICT


def test_batch_with_one_conflict_rolls_back_whole_batch(ephemeral_engine) -> None:
    eng = ephemeral_engine
    eng.write_one(payload("e1", seq=1, event_time_ms=0))
    r = eng.write_batch([
        payload("e2", seq=2, event_time_ms=10),
        payload("e1", seq=1, event_time_ms=0, price=999),  # conflict
        payload("e3", seq=3, event_time_ms=20),
    ])
    assert not r.committed
    assert r.conflicts == 1 and r.accepted == 0
    assert eng.current_checkpoint().offset == 1
    wins = eng.query_windows(include_unpublished=True)
    assert [w.event_ids for w in wins] == [("e1",)]


def test_identical_duplicate_mixed_with_new_events_commits_new_only(
    ephemeral_engine,
) -> None:
    eng = ephemeral_engine
    eng.write_one(payload("e1", seq=1, event_time_ms=0))
    r = eng.write_batch([
        payload("e1", seq=1, event_time_ms=0),              # identical dup
        payload("e2", seq=2, event_time_ms=10),             # new
    ])
    assert r.committed
    assert r.duplicates == 1 and r.accepted == 1
    assert eng.current_checkpoint().offset == 2
    wins = eng.query_windows(include_unpublished=True)
    assert wins[0].event_count == 2


def test_event_object_path_is_supported(ephemeral_engine) -> None:
    eng = ephemeral_engine
    eng.write_one(event("e1", seq=1, event_time_ms=0))
    r = eng.write_one(event("e1", seq=1, event_time_ms=0))
    assert r.duplicates == 1


def test_duplicate_id_within_one_batch_is_idempotent_or_conflict(
    ephemeral_engine,
) -> None:
    eng = ephemeral_engine
    # identical id twice inside a single batch -> one counts, one duplicate
    r = eng.write_batch([
        payload("same", seq=1, event_time_ms=0),
        payload("same", seq=1, event_time_ms=0),
    ])
    assert r.committed and r.accepted == 1 and r.duplicates == 1
    assert eng.current_checkpoint().offset == 1
    wins = eng.query_windows(include_unpublished=True)
    assert [w.event_ids for w in wins] == [("same",)]

    # same id twice with differing content in one batch -> abort
    r2 = eng.write_batch([
        payload("other", seq=2, event_time_ms=10, price=10),
        payload("other", seq=3, event_time_ms=10, price=11),  # conflicts
    ])
    assert not r2.committed and r2.conflicts == 1
    assert eng.current_checkpoint().offset == 1

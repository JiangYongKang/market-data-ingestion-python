"""Source sequence regression within a single delivery batch.

Cross-batch arrivals may interleave/out-of-order legitimately and are handled
by event-time/watermark plus idempotency. The seq token guards against a
producer/transport *rewind* observed inside the same ordered delivery.
"""
from __future__ import annotations

from app.ingestion.errors import RejectReason
from tests.conftest import payload


def test_seq_regression_within_batch_is_rejected_atomically(ephemeral_engine) -> None:
    eng = ephemeral_engine
    eng.write_one(payload("a", source="feed", seq=10, event_time_ms=0))
    r = eng.write_batch([
        payload("c", source="feed", seq=12, event_time_ms=20),
        payload("old", source="feed", seq=11 - 2, event_time_ms=30),  # 9 < 12
    ])
    assert not r.committed and r.rejected == 1
    rejected = [x for x in r.records if not x.accepted]
    assert len(rejected) == 1
    assert rejected[0].reason is RejectReason.SEQ_REGRESSED
    assert "regressed within batch" in rejected[0].detail
    assert eng.current_checkpoint().offset == 1
    wins = eng.query_windows(include_unpublished=True)
    assert [w.event_ids for w in wins] == [("a",)]


def test_equal_or_higher_seq_in_batch_is_allowed(ephemeral_engine) -> None:
    eng = ephemeral_engine
    r = eng.write_batch([
        payload("a", source="s1", seq=5, event_time_ms=0),
        payload("b", source="s1", seq=5, event_time_ms=10),  # equal: ok
        payload("c", source="s1", seq=6, event_time_ms=20),
    ])
    assert r.committed and r.accepted == 3


def test_seq_is_scoped_per_source_and_not_global_across_batches(
    ephemeral_engine,
) -> None:
    eng = ephemeral_engine
    eng.write_one(payload("a", source="s1", seq=100, event_time_ms=0))
    # different independent source, low seq: still accepted
    r = eng.write_one(payload("b", source="s2", seq=1, event_time_ms=10))
    assert r.committed and r.accepted == 1
    # interleaved/out-of-order cross-batch arrival for s1 is allowed
    r2 = eng.write_one(payload("c", source="s1", seq=50, event_time_ms=20))
    assert r2.committed and r2.accepted == 1


def test_seq_regression_is_distinct_from_duplicate(ephemeral_engine) -> None:
    eng = ephemeral_engine
    eng.write_one(payload("a", source="f", seq=5, event_time_ms=0))
    # identical redelivery keeps seq and is a plain idempotent duplicate
    r = eng.write_one(payload("a", source="f", seq=5, event_time_ms=0))
    assert r.records[0].reason is RejectReason.DUPLICATE_IDENTICAL
    # in-batch backwards jump with fresh ids is SEQ_REGRESSED
    r2 = eng.write_batch([
        payload("x", source="f", seq=8, event_time_ms=1),
        payload("y", source="f", seq=7, event_time_ms=2),
    ])
    assert not r2.committed
    assert r2.records[-1].reason is RejectReason.SEQ_REGRESSED

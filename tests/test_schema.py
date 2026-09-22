"""Schema evolution: defaults, unknown/deprecated fields, type mismatches."""
from __future__ import annotations

from app.ingestion.config import EngineConfig, UnknownFieldPolicy
from app.ingestion.engine import IngestionEngine
from app.ingestion.errors import RejectReason
from tests.conftest import payload


def _engine(policy: UnknownFieldPolicy = UnknownFieldPolicy.IGNORE) -> IngestionEngine:
    return IngestionEngine(
        EngineConfig(unknown_field_policy=policy)
    ).start()


def test_v1_event_gets_deterministic_v2_defaults() -> None:
    eng = _engine()
    r = eng.write_one(payload("e1"))
    assert r.committed and r.accepted == 1
    # a v2 event with explicit defaults has identical semantics
    r2 = eng.write_one(payload("e2", schema_version="v2",
                               notional_ccy="USD", venue="UNKNOWN"))
    assert r2.committed and r2.accepted == 1


def test_new_v2_fields_are_accepted_and_defaulted() -> None:
    eng = _engine()
    r = eng.write_one(payload("e1", schema_version="v2", venue="CNX"))
    assert r.committed and r.accepted == 1
    # missing notional_ccy -> deterministic USD
    m = eng.metrics_snapshot()
    assert m["schema_unknown_fields"] == 0


def test_unknown_fields_are_stripped_not_silently_mapped() -> None:
    eng = _engine(UnknownFieldPolicy.IGNORE)
    r = eng.write_one(payload("e1", mystery=42, symbol2="X"))
    assert r.committed and r.accepted == 1
    assert eng.metrics_snapshot()["schema_unknown_fields"] == 2


def test_unknown_fields_rejected_when_policy_requires() -> None:
    eng = _engine(UnknownFieldPolicy.REJECT)
    r = eng.write_one(payload("e1", mystery=42))
    assert not r.committed and r.rejected == 1
    assert r.records[0].reason is RejectReason.SCHEMA_UNKNOWN_FIELD
    assert eng.current_checkpoint().offset == 0


def test_deprecated_alias_ccy_is_used_and_flagged() -> None:
    eng = _engine()
    r = eng.write_one(payload("e1", schema_version="v2", ccy="EUR"))
    assert r.committed and r.accepted == 1
    assert eng.metrics_snapshot()["schema_deprecated_fields"] == 1


def test_deprecated_alias_conflict_is_rejected() -> None:
    eng = _engine()
    r = eng.write_one(payload(
        "e1", schema_version="v2", ccy="EUR", notional_ccy="USD"))
    assert not r.committed and r.rejected == 1
    assert r.records[0].reason is RejectReason.SCHEMA_INVALID_VALUE
    assert "conflicts" in r.records[0].detail


def test_type_mismatches_are_distinct_from_missing_fields() -> None:
    eng = _engine()
    cases = [
        ({"seq": "1"}, RejectReason.SCHEMA_TYPE_MISMATCH),
        ({"price": True}, RejectReason.SCHEMA_TYPE_MISMATCH),
        ({"quantity": "2"}, RejectReason.SCHEMA_TYPE_MISMATCH),
        ({"event_time_ms": 1.5}, RejectReason.SCHEMA_TYPE_MISMATCH),
    ]
    for i, (over, reason) in enumerate(cases):
        p = payload(f"e{i}", **over)
        r = eng.write_one(p)
        assert not r.committed and r.records[0].reason is reason, (over, r.records[0].reason)


def test_invalid_values_and_missing_and_bad_version_are_classified() -> None:
    eng = _engine()
    cases = [
        (payload("e1", price=0), RejectReason.SCHEMA_INVALID_VALUE),
        (payload("e2", price=float("inf")), RejectReason.SCHEMA_INVALID_VALUE),
        (payload("e3", quantity=-1), RejectReason.SCHEMA_INVALID_VALUE),
        (payload("e4", event_time_ms=-5), RejectReason.SCHEMA_INVALID_VALUE),
    ]
    for p, reason in cases:
        r = eng.write_one(p)
        assert not r.committed and r.records[0].reason is reason

    p = payload("e5")
    del p["symbol"]
    r = eng.write_one(p)
    assert r.records[0].reason is RejectReason.SCHEMA_MISSING_FIELD

    r = eng.write_one(payload("e6", schema_version="v99"))
    assert r.records[0].reason is RejectReason.SCHEMA_UNSUPPORTED_VERSION


def test_non_dict_payload_is_rejected_cleanly() -> None:
    eng = _engine()
    r = eng.write_batch(["not-an-object"])  # type: ignore[list-item]
    assert not r.committed and r.rejected == 1
    assert r.records[0].reason is RejectReason.SCHEMA_INVALID_VALUE

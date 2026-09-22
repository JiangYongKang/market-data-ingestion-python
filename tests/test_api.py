"""HTTP surface: endpoints, status codes, stable reason strings."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import app.api as api_mod
from app.ingestion.config import EngineConfig
from app.ingestion.engine import IngestionEngine
from app.ingestion.errors import RejectReason


@pytest.fixture
def client(ephemeral_engine: IngestionEngine):
    api_mod.set_engine(ephemeral_engine)
    with TestClient(api_mod.app) as c:
        yield c
    api_mod.set_engine(None)


def _ev(eid: str, seq: int, t: int, **kw) -> dict:
    return {
        "event_id": eid, "source": "feed", "seq": seq, "symbol": "BTC",
        "event_time_ms": t, "price": 100.0, "quantity": 1.0, **kw,
    }


def test_health_and_root(client: TestClient) -> None:
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/").json()["service"] == "market-data-ingestion"


def test_single_and_batch_ingest_and_windows(client: TestClient) -> None:
    r = client.post("/ingest/events", json=_ev("e1", 1, 0, price=100, quantity=2))
    assert r.status_code == 200 and r.json()["accepted"] == 1
    r = client.post("/ingest/batch", json={"events": [
        _ev("e2", 2, 100, price=110, quantity=3),
        _ev("e3", 3, 2000, price=1, quantity=1),  # advances wm -> publish [0,1000)
    ]})
    assert r.status_code == 200 and r.json()["committed"] is True
    wins = client.get("/windows", params={"symbol": "BTC"}).json()
    assert len(wins) == 1
    assert wins[0]["event_ids"] == ["e1", "e2"]
    assert wins[0]["vwap"] == pytest.approx((100 * 2 + 110 * 3) / 5)


def test_duplicate_vs_conflict_are_distinguished(client: TestClient) -> None:
    client.post("/ingest/events", json=_ev("e1", 1, 0))
    # identical redelivery: still 200 with DUPLICATE_IDENTICAL
    r = client.post("/ingest/events", json=_ev("e1", 1, 0))
    rec = r.json()["records"][0]
    assert rec["reason_code"] == RejectReason.DUPLICATE_IDENTICAL.value
    assert r.json()["duplicates"] == 1
    # bad body shapes
    r = client.post("/ingest/batch", json={"nope": []})
    assert r.status_code == 400
    r = client.post("/ingest/batch", json={"events": "x"})
    assert r.status_code == 400


def test_schema_conflict_and_type_mismatch_status(client: TestClient) -> None:
    client.post("/ingest/events", json=_ev("e1", 1, 0, price=100))
    r = client.post("/ingest/events", json=_ev("e1", 1, 0, price=42))
    assert r.status_code == 422
    body = r.json()
    assert body["committed"] is False
    assert body["records"][0]["reason_code"] == \
        RejectReason.DUPLICATE_CONFLICT.value

    bad = _ev("e2", 2, 0)
    bad["seq"] = "x"
    r = client.post("/ingest/events", json=bad)
    assert r.status_code == 422
    assert r.json()["records"][0]["reason_code"] == \
        RejectReason.SCHEMA_TYPE_MISMATCH.value


def test_late_event_is_quarantined_and_queryable(client: TestClient) -> None:
    client.post("/ingest/events", json=_ev("a", 1, 0))
    client.post("/ingest/events", json=_ev("b", 2, 2000))  # wm=1500 -> publish
    client.post("/ingest/events", json=_ev("late", 3, 100))
    rows = client.get("/quarantine", params={"symbol": "BTC"}).json()
    assert len(rows) == 1
    assert rows[0]["event_id"] == "late"
    assert rows[0]["reason"] == RejectReason.LATE_BEYOND_WATERMARK.value
    assert rows[0]["watermark_ms"] == 1500


def test_checkpoint_and_replay_endpoints(tmp_path) -> None:
    import tempfile
    state = tempfile.mkdtemp()
    engine = IngestionEngine(EngineConfig(state_dir=state, fsync=False)).start()
    api_mod.set_engine(engine)
    with TestClient(api_mod.app) as c:
        for i in range(1, 4):
            r = c.post("/ingest/events", json=_ev(f"e{i}", i, i * 1000))
            assert r.status_code == 200
        assert c.get("/checkpoint").json()["offset"] == 3
        # rewind -> 409 with explicit reason
        r = c.post("/replay", json={"offset": 1})
        assert r.status_code == 409
        assert r.json()["reason"] == RejectReason.CHECKPOINT_REGRESSED.value
        # future/unknown -> 409 with distinct reason
        r = c.post("/replay", json={"offset": 99})
        assert r.status_code == 409
        assert r.json()["reason"] == RejectReason.CHECKPOINT_UNKNOWN.value
        # malformed replay body
        assert c.post("/replay", json={"offset": "1"}).status_code == 400
    engine.close()
    api_mod.set_engine(None)


def test_backpressure_reject_is_429() -> None:
    engine = IngestionEngine(EngineConfig(
        max_inflight_batches=1,
        backpressure_policy=__import__(
            "app.ingestion.config", fromlist=["BackpressurePolicy"]
        ).BackpressurePolicy.REJECT,
    )).start()
    assert engine._gate.acquire(blocking=False)
    api_mod.set_engine(engine)
    with TestClient(api_mod.app) as c:
        r = c.post("/ingest/events", json=_ev("x", 1, 0))
        assert r.status_code == 429
        assert r.json()["reason"] == RejectReason.BACKPRESSURE_REJECTED.value
    engine._gate.release()
    engine.close()
    api_mod.set_engine(None)


def test_metrics_expose_timings_and_memory(client: TestClient) -> None:
    for i in range(5):
        client.post("/ingest/events", json=_ev(f"e{i}", i + 1, i * 1000))
    m = client.get("/metrics").json()
    for key in (
        "accepted", "duplicates_identical", "quarantined",
        "dedupe_entries", "dedupe_memory_bytes", "batch_us_p99",
        "write_us_p99", "backpressure_rejected", "rollbacks",
    ):
        assert key in m
    assert m["accepted"] >= 5 and m["dedupe_entries"] == 5
    assert m["dedupe_memory_bytes"] > 0

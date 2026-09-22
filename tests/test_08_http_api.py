"""HTTP 集成专题：状态码与失败归类、观测与基准接口。"""
from __future__ import annotations

import os

import pytest

from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path):
    # 直接经工厂构建独立实例（main.app 是导入期单例，不能跨用例共享）
    from market_data.app import create_app
    from market_data.config import Config
    from fastapi.testclient import TestClient
    app = create_app(Config(data_dir=str(tmp_path / "data")))
    return TestClient(app)


def _ev(eid, t, p=10.0, q=1.0, seq=None, symbol="A", **extra):
    d = {"event_id": eid, "source": "s",
         "seq": seq if seq is not None else int("".join(c for c in eid if c.isdigit()) or 0),
         "symbol": symbol, "price": p, "quantity": q, "event_time_ms": t}
    d.update(extra)
    return d


def test_ingest_batch_and_incremental(client):
    r = client.post("/ingest", json=[_ev("e1", 1000), _ev("e2", 2000)])
    assert r.status_code == 200 and r.json()["accepted"] == 2
    r = client.post("/ingest", json=_ev("e3", 3000, seq=3))
    assert r.json()["accepted"] == 1


def test_duplicate_vs_conflict_status_distinction(client):
    client.post("/ingest", json=_ev("e1", 1000, p=10))
    # 普通重复：200 + duplicate 计数
    r = client.post("/ingest", json=_ev("e1", 1000, p=10))
    assert r.status_code == 200 and r.json()["duplicate"] == 1
    # 内容冲突：422 + 明确原因
    r = client.post("/ingest", json=_ev("e1", 1000, p=99))
    assert r.status_code == 422
    assert r.json()["error"]["reason"] == "duplicate_conflict"


def test_schema_and_backpressure_and_rollback_status(client):
    r = client.post("/ingest", json=_ev("x", 1000, mystery=1))
    assert r.status_code == 422 and r.json()["error"]["reason"] == "schema_unknown_field"
    r = client.post("/ingest", json=[_ev("x", 1000)])
    assert r.status_code == 200
    # 背压：极小容量直接 503
    tiny = client  # 同一服务容量为默认，这里改用显式配置的独立应用
    # 回退 409
    client.post("/checkpoints", json={"source": "s", "seq": 100})
    r = client.post("/checkpoints", json={"source": "s", "seq": 1})
    assert r.status_code == 409 and r.json()["detail"]["reason"] == "checkpoint_rollback"


def test_backpressure_returns_503(tmp_path, monkeypatch):
    from market_data.app import create_app
    from market_data.config import Config
    from fastapi.testclient import TestClient
    app = create_app(Config(data_dir=str(tmp_path / "d2"), max_pending_events=2))
    c = TestClient(app)
    body = [{"event_id": f"k{i}", "source": "s", "seq": i, "symbol": "K",
             "price": 1, "quantity": 1, "event_time_ms": 1000 + i} for i in range(5)]
    r = c.post("/ingest", json=body)
    assert r.status_code == 503 and r.json()["detail"]["reason"] == "backpressure_rejected"


def test_windows_quarantine_metrics_endpoints(client):
    client.post("/ingest", json=[_ev("e1", 1000, p=10, q=3), _ev("e2", 5000, p=20, q=1)])
    client.post("/ingest", json=_ev("fwd", 20000, seq=50))
    client.post("/ingest", json=_ev("L", 1000, seq=60))  # 迟到
    w = client.get("/windows/A").json()
    assert w["windows"][0]["vwap"] is not None
    q = client.get("/quarantine").json()
    assert q["count"] == 1 and q["items"][0]["reason"] == "late_beyond_watermark"
    m = client.get("/metrics").json()
    assert m["quarantined"] == 1 and "ingest" in m


def test_benchmark_endpoint_reports_budget_and_idempotent_replay(client):
    r = client.post("/benchmark", json={"count": 3000, "symbols": 4})
    assert r.status_code == 200
    body = r.json()
    assert body["accepted"] == 3000
    assert body["replay_accepted"] == 0 and body["replay_duplicate"] == 3000
    assert "elapsed_ms" in body and "within_time_budget" in body
    assert "estimated_state_bytes" in body and "within_memory_cap" in body

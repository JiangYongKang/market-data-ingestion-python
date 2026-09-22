"""Shared fixtures: deterministic engines, logging, payload builders."""
from __future__ import annotations

import logging

import pytest

from app.ingestion.config import EngineConfig
from app.ingestion.engine import IngestionEngine
from app.ingestion.models import Event


@pytest.fixture(autouse=True)
def _decision_logs(caplog: pytest.LogCaptureFixture) -> None:
    """Surface event-id / event-time / watermark / basis in test logs."""
    caplog.set_level(logging.INFO, logger="ingestion")


@pytest.fixture
def ephemeral_engine() -> IngestionEngine:
    engine = IngestionEngine(EngineConfig()).start()
    yield engine
    engine.close()


@pytest.fixture
def durable_engine(tmp_path: pytest.TempPathFactory):  # type: ignore[name-defined]
    def _make(**overrides) -> IngestionEngine:
        cfg = EngineConfig(state_dir=str(tmp_path / "state"), fsync=False, **overrides)
        return IngestionEngine(cfg).start()

    return _make


def payload(
    event_id: str = "e1",
    *,
    source: str = "feed",
    seq: int = 1,
    symbol: str = "BTCUSD",
    event_time_ms: int = 0,
    price: float = 100.0,
    quantity: float = 1.0,
    **extra,
) -> dict:
    d = {
        "event_id": event_id,
        "source": source,
        "seq": seq,
        "symbol": symbol,
        "event_time_ms": event_time_ms,
        "price": price,
        "quantity": quantity,
    }
    d.update(extra)
    return d


def event(event_id: str = "e1", **kw) -> Event:
    kw.setdefault("source", "feed")
    kw.setdefault("seq", 1)
    kw.setdefault("symbol", "BTCUSD")
    kw.setdefault("event_time_ms", 0)
    kw.setdefault("price", 100.0)
    kw.setdefault("quantity", 1.0)
    return Event(event_id=event_id, **kw)

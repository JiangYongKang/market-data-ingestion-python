"""FastAPI HTTP surface (no external middleware/broker required).

Endpoints
---------
POST /ingest/events   single event write
POST /ingest/batch    {"events": [...]} batch write (atomic per batch)
GET  /windows         published window features (?symbol=, ?all=true)
GET  /quarantine      queryable late-event isolation (?symbol=, ?reason=)
GET  /checkpoint      current monotonic offset
POST /replay          {"offset": n} forward-only guarded reposition
GET  /metrics         counters, latency percentiles, dedupe memory
GET  /healthz         liveness

All domain failures return HTTP 409/422/400 with a stable
``reason`` string drawn from :class:`RejectReason`.
"""
from __future__ import annotations

import logging
import os
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .ingestion import serialize as sz
from .ingestion.config import EngineConfig
from .ingestion.engine import IngestionEngine
from .ingestion.errors import (
    IngestionError,
    RejectReason,
)

logger = logging.getLogger("ingestion.api")

app = FastAPI(title="market-data-ingestion", version="0.1.0")

_engine: IngestionEngine | None = None


def build_engine(config: EngineConfig | None = None) -> IngestionEngine:
    if config is None:
        state_dir = os.environ.get("INGESTION_STATE_DIR")
        config = EngineConfig(
            state_dir=state_dir,
            fsync=os.environ.get("INGESTION_FSYNC", "1") != "0",
        )
    return IngestionEngine(config).start()


def get_engine() -> IngestionEngine:
    global _engine
    if _engine is None:
        _engine = build_engine()
    return _engine


def set_engine(engine: IngestionEngine | None) -> None:
    """Test/admin hook to replace the process-local engine."""
    global _engine
    if _engine is not None:
        _engine.close()
    _engine = engine


@app.exception_handler(IngestionError)
async def ingestion_error_handler(
    request: Request, exc: IngestionError
) -> JSONResponse:
    status = 422
    if exc.reason in (
        RejectReason.CHECKPOINT_REGRESSED,
        RejectReason.CHECKPOINT_UNKNOWN,
    ):
        status = 409
    elif exc.reason is RejectReason.BACKPRESSURE_REJECTED:
        status = 429
    logger.info("http-error path=%s reason=%s detail=%s",
                request.url.path, exc.reason.value, exc)
    return JSONResponse(
        status_code=status,
        content={"error": type(exc).__name__, "reason": exc.reason.value,
                 "detail": str(exc)},
    )


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/")
def read_root() -> dict[str, str]:
    return {"service": "market-data-ingestion", "status": "ok"}


@app.post("/ingest/events")
def ingest_one(payload: dict[str, Any]) -> Any:
    engine = get_engine()
    result = engine.write_one(payload)
    body = sz.batch_to_dict(result)
    # A batch that was entirely aborted for a domain reason (conflict, seq
    # regression, schema/resource rejection) is a 422 with the full record
    # detail; an idempotent duplicate or a quarantined late event still
    # commits and returns 200.
    if not result.committed:
        return JSONResponse(status_code=422, content=body)
    return body


@app.post("/ingest/batch")
def ingest_batch(body: dict[str, Any]) -> Any:
    if not isinstance(body, dict) or "events" not in body:
        return JSONResponse(  # type: ignore[return-value]
            status_code=400,
            content={
                "error": "BadRequest",
                "reason": RejectReason.SCHEMA_INVALID_VALUE.value,
                "detail": "body must be an object with an 'events' array",
            },
        )
    events = body["events"]
    if not isinstance(events, list):
        return JSONResponse(  # type: ignore[return-value]
            status_code=400,
            content={
                "error": "BadRequest",
                "reason": RejectReason.SCHEMA_TYPE_MISMATCH.value,
                "detail": "'events' must be an array",
            },
        )
    engine = get_engine()
    result = engine.write_batch(events)
    body = sz.batch_to_dict(result)
    if not result.committed:
        return JSONResponse(status_code=422, content=body)
    return body


@app.get("/windows")
def list_windows(
    symbol: str | None = None, all: bool = False
) -> list[dict[str, Any]]:
    engine = get_engine()
    rows = engine.query_windows(symbol, include_unpublished=all)
    return [sz.window_to_dict(w) for w in rows]


@app.get("/quarantine")
def list_quarantine(
    symbol: str | None = None, reason: str | None = None
) -> list[dict[str, Any]]:
    engine = get_engine()
    reason_enum = RejectReason(reason) if reason is not None else None
    rows = engine.query_quarantine(symbol)
    if reason_enum is not None:
        rows = [r for r in rows if r.reason is reason_enum]
    return [sz.quarantine_to_dict(q) for q in rows]


@app.get("/checkpoint")
def get_checkpoint() -> dict[str, Any]:
    return sz.checkpoint_to_dict(get_engine().current_checkpoint())


@app.post("/replay")
def replay(body: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(body, dict) or not isinstance(body.get("offset"), int):
        return JSONResponse(  # type: ignore[return-value]
            status_code=400,
            content={
                "error": "BadRequest",
                "reason": RejectReason.SCHEMA_TYPE_MISMATCH.value,
                "detail": "body must be {'offset': int}",
            },
        )
    cp = get_engine().replay_from(body["offset"])
    return sz.checkpoint_to_dict(cp)


@app.get("/metrics")
def metrics() -> dict[str, Any]:
    return dict(get_engine().metrics_snapshot())

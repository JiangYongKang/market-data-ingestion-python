"""Pure-JSON conversion helpers for the HTTP layer."""
from __future__ import annotations

from typing import Any

from .models import BatchResult, Checkpoint, IngestRecord, QuarantineEntry, WindowResult


def record_to_dict(r: IngestRecord) -> dict[str, Any]:
    return {
        "event_id": r.event_id,
        "accepted": r.accepted,
        "reason_code": r.reason.value if r.reason else None,
        "detail": r.detail,
        "window_start_ms": r.window_start_ms,
        "watermark_ms": r.watermark_ms,
        "quarantine_id": r.quarantine_id,
    }


def batch_to_dict(b: BatchResult) -> dict[str, Any]:
    return {
        "committed": b.committed,
        "accepted": b.accepted,
        "duplicates": b.duplicates,
        "conflicts": b.conflicts,
        "quarantined": b.quarantined,
        "rejected": b.rejected,
        "checkpoint": b.checkpoint,
        "duration_us": b.duration_us,
        "records": [record_to_dict(r) for r in b.records],
    }


def window_to_dict(w: WindowResult) -> dict[str, Any]:
    return {
        "symbol": w.symbol,
        "window_start_ms": w.window_start_ms,
        "window_end_ms": w.window_end_ms,
        "event_count": w.event_count,
        "total_quantity": w.total_quantity,
        "vwap": w.vwap,
        "mean_price": w.mean_price,
        "price_stddev": w.price_stddev,
        "volatility_bps": w.volatility_bps,
        "min_price": w.min_price,
        "max_price": w.max_price,
        "first_event_time_ms": w.first_event_time_ms,
        "last_event_time_ms": w.last_event_time_ms,
        "event_ids": list(w.event_ids),
        "published": w.published,
        "recomputed": w.recomputed,
    }


def quarantine_to_dict(q: QuarantineEntry) -> dict[str, Any]:
    return {
        "quarantine_id": q.quarantine_id,
        "event_id": q.event_id,
        "source": q.source,
        "seq": q.seq,
        "symbol": q.symbol,
        "event_time_ms": q.event_time_ms,
        "watermark_ms": q.watermark_ms,
        "window_end_ms": q.window_end_ms,
        "reason": q.reason.value,
        "detail": q.detail,
        "payload": q.payload,
        "ingest_time_ms": q.ingest_time_ms,
    }


def checkpoint_to_dict(c: Checkpoint) -> dict[str, Any]:
    return {"offset": c.offset, "created_at_ms": c.created_at_ms}

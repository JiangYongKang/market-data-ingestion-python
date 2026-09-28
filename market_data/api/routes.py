"""HTTP 接口。

* ``POST /ingest``          批量写入（单条也接受），两参数化增量语义；
* ``POST /ingest?replay=1`` 重放写入（幂等路径相同，走同一判定管线）；
* ``GET  /windows/{symbol}`` 查询已发布窗口特征（可按窗口起点过滤）；
* ``GET  /quarantine``       查询隔离区（可按标的过滤）；
* ``GET/POST /checkpoints``  查看/推进位点（回退返回 409）；
* ``GET  /metrics``          观测信息（计数、水位、耗时、内存估计）；
* ``POST /benchmark``        本地可复现基准，回报是否满足预算/内存上限。

失败响应统一为 ``{"error": {...,"reason": ...}}``，reason 取自
:class:`market_data.models.RejectReason`，与普通重复（200 结果中的
``duplicate`` 计数）明确可区分。
"""
from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from ..errors import BackpressureError, CheckpointRollbackError, IngestionError
from ..models import RejectReason


def _feature_dict(f) -> dict[str, Any]:
    return {
        "symbol": f.symbol,
        "window_start_ms": f.window_start_ms,
        "window_end_ms": f.window_end_ms,
        "count": f.count,
        "total_quantity": f.total_quantity,
        "vwap": f.vwap,
        "volatility": f.volatility,
        "event_ids": list(f.event_ids),
        "published": f.published,
    }


def _quarantine_dict(q) -> dict[str, Any]:
    e = q.event
    return {
        "event": {
            "event_id": e.event_id, "source": e.source, "seq": e.seq,
            "symbol": e.symbol, "price": e.price, "quantity": e.quantity,
            "event_time_ms": e.event_time_ms, "trade_id": e.trade_id,
            "venue": e.venue,
        },
        "reason": q.reason.value,
        "watermark_ms": q.watermark_ms,
        "detail": q.detail,
        "accepted_at_ms": q.accepted_at_ms,
    }


def _result_dict(r) -> dict[str, Any]:
    return {
        "accepted": r.accepted,
        "duplicate": r.duplicate,
        "quarantined": r.quarantined,
        "rejected": r.rejected,
        "merged": r.merged,
        "details": [
            {"event_id": eid, "reason": reason.value, "detail": detail}
            for eid, reason, detail in r.details
        ],
    }


def create_router(service) -> APIRouter:
    router = APIRouter()

    @router.post("/ingest")
    async def ingest(request: Request, replay: bool = False) -> dict:
        try:
            body = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail="请求体必须是 JSON 事件或事件数组") from exc
        if isinstance(body, dict):
            raw = [body]
        elif isinstance(body, list):
            raw = body
        else:
            raise HTTPException(status_code=400, detail="请求体必须是对象或数组")
        try:
            result = await service.ingest(raw, replay=replay)
        except BackpressureError as exc:
            raise HTTPException(status_code=503, detail={
                "reason": exc.reason.value, "message": str(exc),
                "event_id": exc.event_id}) from exc
        except CheckpointRollbackError as exc:
            raise HTTPException(status_code=409, detail={
                "reason": exc.reason.value, "message": str(exc)}) from exc
        except IngestionError as exc:
            # 结构错误/内容冲突：整批拒绝，返回 422 且原因可区分
            return JSONResponse(status_code=422, content={"error": {
                "event_id": exc.event_id,
                "reason": exc.reason.value,
                "message": str(exc),
            }})
        return _result_dict(result)

    @router.get("/windows/{symbol}")
    async def windows(symbol: str, window_start_ms: int | None = None) -> dict:
        feats = service.query(symbol, window_start_ms)
        return {"symbol": symbol, "windows": [_feature_dict(f) for f in feats]}

    @router.get("/quarantine")
    async def quarantine(symbol: str | None = None) -> dict:
        items = service.quarantine_list(symbol)
        return {"count": len(items), "items": [_quarantine_dict(q) for q in items]}

    @router.get("/merges")
    async def merges(symbol: str | None = None) -> dict:
        """跨渠道合并/冲突审计：胜负渠道、event_id、最终归类。"""
        records = service.merge_records()
        if symbol is not None:
            records = [r for r in records if r.symbol == symbol]
        return {
            "count": len(records),
            "items": [
                {
                    "symbol": r.symbol, "trade_id": r.trade_id, "kind": r.kind,
                    "winner": r.winner, "loser": r.loser,
                    "winner_event_id": r.winner_event_id,
                    "loser_event_id": r.loser_event_id,
                    "decided_at_ms": r.decided_at_ms,
                }
                for r in records
            ],
        }

    @router.get("/channels")
    async def channels(symbol: str | None = None) -> dict:
        """多渠道推进观测：各渠道事件时间、独立水位、落后量、是否卡住。"""
        snap = service.channel_snapshot()
        if symbol is not None:
            body = snap.get(symbol)
            return {"symbol": symbol, "channels": body["channels"],
                    "symbol_watermark_ms": body["symbol_watermark_ms"],
                    "channel_count": body["channel_count"],
                    "stuck_count": body["stuck_count"]} if body else \
                {"symbol": symbol, "channels": [], "symbol_watermark_ms": None,
                 "channel_count": 0, "stuck_count": 0}
        return {"symbols": snap}

    @router.get("/checkpoints")
    async def checkpoints_get() -> dict:
        return {"checkpoints": service.checkpoints()}

    @router.post("/checkpoints")
    async def checkpoint_advance(payload: dict) -> dict:
        try:
            source = str(payload["source"])
            seq = int(payload["seq"])
        except (KeyError, TypeError, ValueError) as exc:
            raise HTTPException(status_code=400,
                                detail="需要 {'source': str, 'seq': int}") from exc
        try:
            moved = service.checkpoint(source, seq)
        except CheckpointRollbackError as exc:
            raise HTTPException(status_code=409, detail={
                "reason": exc.reason.value,
                "message": str(exc),
                "requested": exc.requested,
                "current": exc.current,
            }) from exc
        return {"source": source, "seq": service.checkpoint_get(source), "moved": moved}

    @router.get("/metrics")
    async def metrics() -> dict:
        return service.metrics_snapshot()

    @router.post("/benchmark")
    async def benchmark(payload: dict | None = None) -> dict:
        """本地可复现基准：合成确定性行情，回报耗时/内存是否满足配置上限。"""
        payload = payload or {}
        n = int(payload.get("count", service.config.benchmark_event_count))
        symbols = int(payload.get("symbols", 10))
        # 内存模式隔离运行，避免污染主服务状态
        import dataclasses as _dc
        from ..service import MarketDataService
        bench_cfg = _dc.replace(
            service.config,
            data_dir=":memory:",
            max_pending_events=max(2 * n + 100, 100),
        )
        bench = MarketDataService(bench_cfg)
        events = []
        for i in range(n):
            t = (i // 50) * 100  # 每 50 条推进 100ms
            events.append({
                "event_id": f"bench-{i}",
                "source": "bench",
                "seq": i,
                "symbol": f"S{i % symbols}",
                "price": 100.0 + (i % 7),
                "quantity": 1.0 + (i % 3),
                "event_time_ms": t,
            })
        # 一半正常、一半逆序投递，仍须正确去重/聚合
        events += list(reversed(events[: max(1, n // 20)]))
        t0 = time.perf_counter_ns()
        result = await bench.ingest(events)
        elapsed_ms = (time.perf_counter_ns() - t0) / 1e6
        snap = bench.metrics_snapshot()
        # 重放：整批重复投递，必须全部幂等
        t1 = time.perf_counter_ns()
        replay_result = await bench.ingest(events[:n])
        replay_ms = (time.perf_counter_ns() - t1) / 1e6
        budget = service.config.ingest_time_budget_ms
        mem_limit = service.config.max_pending_events
        return {
            "count": n,
            "elapsed_ms": elapsed_ms,
            "replay_ms": replay_ms,
            "budget_ms": budget,
            "within_time_budget": elapsed_ms <= budget,
            "accepted": result.accepted,
            "duplicate": result.duplicate,
            "replay_duplicate": replay_result.duplicate,
            "replay_accepted": replay_result.accepted,
            "max_pending_events": snap["max_pending_events"],
            "estimated_state_bytes": snap["estimated_state_bytes"],
            "memory_cap": mem_limit,
            "within_memory_cap": snap["max_pending_events"] <= mem_limit,
            "ns_per_event": snap["ingest"]["avg_ns_per_event"],
            "p99_ns_per_event": snap["ingest"]["p99_ns_per_event"],
            "published_windows": snap["published_windows"],
            "watermark_ms": snap["watermark_ms"],
        }

    return router

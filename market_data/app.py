"""FastAPI 应用装配。"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .api.routes import create_router
from .config import Config
from .service import MarketDataService


def create_app(config: Config | None = None) -> FastAPI:
    config = config or Config(
        data_dir=os.environ.get("MDI_DATA_DIR", ".mdi_data"),
    )
    service = MarketDataService(config)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        service.close()

    app = FastAPI(
        title="market-data-ingestion",
        version="0.1.0",
        description="本地可复现的行情增量接入与窗口特征聚合服务",
        lifespan=lifespan,
    )
    app.state.service = service
    app.include_router(create_router(service))

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok", "watermark_ms": service.watermark_ms}

    return app

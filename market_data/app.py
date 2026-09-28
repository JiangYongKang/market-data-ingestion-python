"""FastAPI 应用装配。"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .api.routes import create_router
from .config import Config
from .service import MarketDataService


def config_from_env() -> Config:
    """从环境变量构造配置。

    * ``MDI_DATA_DIR``：本地状态目录（默认 .mdi_data）；
    * ``MDI_MULTI_SOURCE``：多来源标的声明，形如
      ``"600000=venueA,venueB,venueC;000001=venueX,venueY"``
      （标的=逗号分隔渠道列表，多标的用分号；顺序即取舍优先级）。
    """
    multi: dict[str, list[str]] = {}
    raw = os.environ.get("MDI_MULTI_SOURCE", "").strip()
    if raw:
        for part in raw.split(";"):
            part = part.strip()
            if not part:
                continue
            symbol, _, srcs = part.partition("=")
            sources = [s.strip() for s in srcs.split(",") if s.strip()]
            if symbol.strip() and sources:
                multi[symbol.strip()] = sources
    return Config(
        data_dir=os.environ.get("MDI_DATA_DIR", ".mdi_data"),
        multi_source_symbols=multi,
    )


def create_app(config: Config | None = None) -> FastAPI:
    config = config or config_from_env()
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

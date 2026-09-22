"""pytest 公共夹具与判定日志。

单测日志必须打印：事件标识、事件时间、水位与判定依据。
这里将 ``market_data`` logger 的输出接到 pytest caplog 并在
失败时可见；同时提供统一的事件构造器与临时数据目录。
"""
from __future__ import annotations

import logging

import pytest

from market_data.config import Config
from market_data.service import decision_logger, MarketDataService


@pytest.fixture(autouse=True)
def _decision_log(caplog):
    """每个用例捕获 decision 日志（event_id/event_time/watermark/reason）。"""
    logger = decision_logger()
    logger.setLevel(logging.INFO)
    with caplog.at_level(logging.INFO, logger="market_data"):
        yield


@pytest.fixture()
def tmp_data_dir(tmp_path):
    return str(tmp_path / "mdi")


@pytest.fixture()
def config(tmp_data_dir):
    return Config(data_dir=tmp_data_dir)


@pytest.fixture()
def service(config):
    svc = MarketDataService(config)
    yield svc
    svc.close()


def make_event(eid: str, t: int, p: float = 10.0, q: float = 1.0,
               *, symbol: str = "A", source: str = "s", seq: int | None = None,
               **extra) -> dict:
    """构造一条行情事件原始字典。seq 缺省取事件号，保证默认稳定。"""
    if seq is None:
        seq = int("".join(ch for ch in eid if ch.isdigit()) or 0)
    d = {
        "event_id": eid,
        "source": source,
        "seq": seq,
        "symbol": symbol,
        "price": p,
        "quantity": q,
        "event_time_ms": t,
    }
    d.update(extra)
    return d


def advance_and_publish(svc, event_time_ms: int, *, source: str = "tick",
                        symbol: str = "__watermark_tick__") -> None:
    """投递一个推进水位的哨兵事件（独立标的，不影响业务窗口断言）。"""
    seq = 1_000_000 + event_time_ms
    payload = make_event(f"tick-{event_time_ms}", event_time_ms, 1.0, 1.0,
                         source=source, seq=seq,
                         symbol=symbol)
    svc.ingest_sync(payload)

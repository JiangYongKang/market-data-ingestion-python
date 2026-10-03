"""pytest 公共夹具与判定日志。

单测日志必须打印：事件标识、事件时间、水位与判定依据。
这里将 ``market_data`` logger 的输出接到 pytest caplog 并在
失败时可见；同时提供统一的事件构造器与临时数据目录。
"""
from __future__ import annotations

import logging
import os

import pytest

from market_data.config import Config
from market_data.service import MarketDataService, decision_logger

#: 测试文件 -> 业务域说明（用于测试结束后的分业务汇总）。
BUSINESS_AREAS: dict[str, str] = {
    "test_01_dedup.py": "幂等与内容冲突去重",
    "test_02_out_of_order.py": "乱序与水位线",
    "test_03_schema_evolution.py": "行情结构兼容演进",
    "test_04_checkpoint_replay.py": "检查点与重放",
    "test_05_concurrency.py": "并发一致性",
    "test_06_backpressure_benchmark.py": "背压与本地基准",
    "test_07_window_features.py": "窗口特征聚合",
    "test_08_http_api.py": "HTTP 接口",
    "test_09_retention.py": "保留与资源清理",
    "test_10_multi_source.py": "多来源接入与跨渠道合并",
    "test_11_multi_source_bugfix.py": "多来源修复回归（分标的推进/冲突投递形状/重启重放）",
}

_outcomes: dict[str, dict[str, int]] = {}


def pytest_runtest_logreport(report):
    if report.when != "call":
        return
    fname = os.path.basename(report.nodeid.split("::")[0])
    bucket = _outcomes.setdefault(fname, {"passed": 0, "failed": 0, "skipped": 0})
    if report.outcome in bucket:
        bucket[report.outcome] += 1


def pytest_terminal_summary(terminalreporter):
    """按业务域汇总用例与通过情况，便于从日志确认覆盖面。"""
    terminalreporter.write_sep("=", "业务覆盖汇总")
    for fname in sorted(_outcomes):
        area = BUSINESS_AREAS.get(fname, fname)
        o = _outcomes[fname]
        total = o["passed"] + o["failed"] + o["skipped"]
        status = "OK" if o["failed"] == 0 else "FAILED"
        terminalreporter.write_line(
            f"[{status}] {area}（{fname}）：共 {total} 项，"
            f"通过 {o['passed']}，失败 {o['failed']}，跳过 {o['skipped']}")


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

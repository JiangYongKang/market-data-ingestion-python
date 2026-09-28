"""全部配置项集中于此，均可构造时传入，缺省值确定。"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Config:
    # --- 窗口与水位 ---
    window_size_ms: int = 10_000           # 固定滚动窗口长度
    allowed_lateness_ms: int = 5_000       # 窗口关闭后仍允许修正的时间
    # --- 结构演进 ---
    reject_unknown_fields: bool = True     # 未知字段默认拒绝
    # --- 背压/资源上限 ---
    max_pending_events: int = 1_000_000    # 内存中未发布事件上限
    backpressure_strategy: str = "reject"  # reject | delay
    backpressure_delay_ms: int = 100       # delay 策略单次等待
    backpressure_max_retries: int = 50
    max_quarantine_size: int = 100_000     # 隔离区上限
    # --- 持久化 ---
    data_dir: str = ".mdi_data"            # 事件日志/位点目录；":memory:" 关闭落盘
    fsync: bool = False                    # 基准场景可关；生产建议开
    # --- 多来源合并 ---
    multi_source: bool = False            # 开启多路行情：每渠道独立事件时间推进
    merge_sources: tuple[str, ...] = ()   # 跨渠道成交取舍优先级（靠前优先；缺省按渠道名字典序）
    max_sources: int = 1024               # 渠道数量硬上限
    source_idle_timeout_ms: int = 30_000  # 渠道无数据多久视为停滞（处理时间）
    source_lag_alert_ms: int = 0          # 渠道落后告警阈值（0=不告警，仅观测）
    # --- 基准上限（供观测/校验） ---
    ingest_time_budget_ms: int = 10_000    # 基准批次总耗时预算
    benchmark_event_count: int = 100_000   # 本地基准规模

"""全部配置项集中于此，均可构造时传入，缺省值确定。"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Config:
    # --- 窗口与水位 ---
    window_size_ms: int = 10_000           # 固定滚动窗口长度
    allowed_lateness_ms: int = 5_000       # 窗口关闭后仍允许修正的时间
    # --- 多来源合并 ---
    # key=标的，value=该标的预期渠道 source 列表（声明顺序即取舍优先级，
    # 排在前面的渠道获胜）。未列入的标的按单来源路径处理，行为与旧版一致。
    multi_source_symbols: dict[str, list[str]] = field(default_factory=dict)
    max_channels_per_symbol: int = 16      # 单标的渠道数硬上限（含动态接入）
    channel_idle_timeout_ms: int = 600_000  # 渠道无事件多久视为卡住（仅观测告警）
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
    # --- 基准上限（供观测/校验） ---
    ingest_time_budget_ms: int = 10_000    # 基准批次总耗时预算
    benchmark_event_count: int = 100_000   # 本地基准规模

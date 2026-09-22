# 配置项与可观测指标

## 1. 配置项（`EngineConfig`，一个 frozen dataclass）

| 配置 | 默认值 | 说明 |
|---|---|---|
| `window_size_ms` | 1000 | 事件时间滚动窗口大小（毫秒） |
| `allowed_lateness_ms` | 500 | 允许迟到量：水位 = max(event_time) − 此值 |
| `supported_versions` | `("v1","v2")` | 可识别协议版本 |
| `unknown_field_policy` | `IGNORE` | 未知字段处理：`IGNORE`（剥离计数）/`REJECT` |
| `max_inflight_batches` | 64 | 在途批次数硬上限（背压门） |
| `backpressure_policy` | `DELAY` | 积压策略：`DELAY`（有界等待）/ `REJECT`（快速失败） |
| `backpressure_max_wait_ms` | 5000 | DELAY 下最长等待预算，超后拒绝并记因 |
| `max_dedupe_entries` | 1000000 | 去重指纹索引硬上限（满则背压拒绝，绝不静默淘汰） |
| `max_quarantine_per_symbol` | 1000 | 每标的隔离区容量，FIFO 淘汰 |
| `state_dir` | `None` | 持久化目录；`None` 表示纯内存 |
| `fsync` | `true` | 追加/快照落盘是否 fsync（测试可关） |
| `start_offset` | `None` | 引导重放前缀位点（仅新进程） |
| `bench_scale_events` | 20000 | 基准规模 |
| `bench_max_write_us` | 20000 | 基准“每事件耗时”门限（µs） |
| `bench_max_dedupe_kb` | 200000 (~200MB) | 去重索引驻留内存门限（KB） |
| `log_decisions` | `true` | 是否记录事件判定日志 |

环境变量（仅对默认引擎生效）：`INGESTION_STATE_DIR`、`INGESTION_FSYNC=0/1`。

## 2. 资源上限与原子性保证

- **无界占用被禁止**：在途批次数由独立信号量约束（不与状态锁耦合，
  避免背压阻塞查询）；去重容量在**分类阶段**预检，满则以
  `BACKPRESSURE_REJECTED` 整批中止，绝不驱逐旧指纹造成重复计数；
  隔离区按标的有界并计数淘汰。
- **失败不留半成品**：每批先零变更分类；提交顺序为
  日志追加 → 记住指纹 → 水位/窗口/隔离区应用 → 推进位点 → 原子快照。
  提交期任何异常都会把**聚合、去重、水位、隔离区、位点、计数器**
  恢复到批前快照，并 `rollbacks += 1`。
- **并发一致**：一把 RLock 串行化同一引擎的所有状态迁移与查询结果拷贝，
  同标的并发写入/重放/查询不会出现部分可见、重复计数或位点跳变。

## 3. 可观测信息（`GET /metrics`）

 counters：`accepted`、`duplicates_identical`、`duplicates_conflict`、
`quarantined`、`rejected`、`schema_unknown_fields`、
`schema_deprecated_fields`、`published_windows`、`recomputed_windows`、
`backpressure_delayed`（按策略延迟次数）、`backpressure_rejected`、
`rollbacks`、`quarantine_evicted`、`batches`。

时延分位：`batch_us_p50/p99/max`（每批墙钟）、
`write_us_p99`（每批应用阶段耗时；基准按“每事件预算×批大小”对照）。

资源：`inflight_batches`、`dedupe_entries`、`dedupe_memory_bytes`
（每条目按 256B 估算，恒定）。

## 4. 基准如何验证上限

基准是**可在本地复现的功能门限**，不是硬件微基准：

```bash
uv run pytest -m benchmark -q -s
# 自定义
BENCH_SCALE=20000 BENCH_MAX_WRITE_US=6000 BENCH_MAX_DEDUPE_KB=300000 \
  uv run pytest -m benchmark -q -s
```

断言：处理无超线性变慢、`write_us_p99` 不超过预算、去重索引随唯一事件
线性增长且每条目字节恒定、重复洪峰不增大索引、重复查询聚合逐位一致。

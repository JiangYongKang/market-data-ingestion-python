# 行情增量接入与窗口特征聚合（本地可复现）

一套**不依赖外部消息中间件、真实行情源或凭据**的本地行情接入服务：
批量/增量写入、事件去重、乱序与迟到处理、行情结构兼容演进、
固定窗口特征聚合（VWAP 与成交量加权波动）、检查点与重放、背压与资源上限。
所有状态保存在本地仅追加日志，进程重启或整体重放后结果**逐位可再现**。

## 快速开始

```bash
uv sync
uv run uvicorn main:app --reload        # 启动 HTTP 服务（默认数据目录 .mdi_data）
uv run pytest -q                        # 运行全部 53 项测试（含判定日志）
curl -X POST localhost:8000/ingest -H 'content-type: application/json' -d '[
  {"event_id":"e1","source":"venueA","seq":1,"symbol":"600000",
   "price":10.0,"quantity":3,"event_time_ms":1000},
  {"event_id":"e2","source":"venueA","seq":2,"symbol":"600000",
   "price":20.0,"quantity":1,"event_time_ms":5000}]'
curl 'localhost:8000/windows/600000'     # 查询已发布窗口特征
curl localhost:8000/metrics             # 观测：计数/水位/耗时/内存估计
curl -X POST localhost:8000/benchmark -H 'content-type: application/json' \
     -d '{"count":20000}'               # 本地基准（含幂等重放校验）
```

## 事件协议

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `event_id` | string | 是 | **稳定事件标识**，主键，重复投递据此识别 |
| `source` | string | 是 | 来源标识；位点按来源分别维护 |
| `seq` | int ≥ 0 | 是 | **来源序号**，与 event_id 构成双键身份 |
| `symbol` | string | 是 | 标的；窗口按标的独立聚合 |
| `price` | 有限数字 ≥ 0 | 是 | 成交价；`bool`/`NaN`/`Inf` 拒绝 |
| `quantity` | 有限数字 > 0 | 是 | 成交量 |
| `event_time_ms` | int ≥ 0 | 是 | 事件时间（毫秒），水位与窗口只依赖它 |
| `schema_version` | string | 否 | 缺省 `"1"`，仅接受当前版本 |
| `trade_id` / `venue` | string \| null | 否 | v1 新增字段，**缺省为 `null`=未知**，绝不以 0 等伪装业务值；聚合不使用 |

### 兼容演进规则（明确、不静默错列）

1. **新增字段**：必须先在 `market_data/models.py` 的 `DEFAULTS` 中声明稳定缺省值；
   旧生产者不带该字段时按缺省解析，语义确定；
2. **未知字段**：默认**拒绝**（`reject_unknown_fields=true`，原因
   `schema_unknown_field`）；可在配置中显式关闭改为忽略——拒绝或忽略都是显式选择；
3. **废弃字段**：`DEPRECATED` 中的字段（如 `exchange_code`）允许携带，
   解析时剥离并计入 `metrics.deprecated_seen`；
4. **类型不匹配 / 缺必填 / 版本不支持**：一律拒绝，原因分别为
   `schema_type_mismatch`、`schema_missing_field`、`schema_version_unsupported`；
5. 任何硬失败（结构错误、内容冲突、容量超限）导致**整批拒绝**，无任何副作用。

## 幂等与内容冲突

* 完全一致的重复投递：计入响应 `duplicate`，**不重复计入任何聚合**，原因 `duplicate_identical`；
* 同一身份（`event_id` 或 `(source, seq)`）内容不一致：拒绝整批，原因
  `duplicate_conflict`——与普通重复在状态码（HTTP 200 vs 422）、原因码、日志三处都可区分；
* 同 `(source, seq)` 绑定不同 `event_id` 也算身份冲突。

## 水位线与乱序规则

```
watermark_ms = max(已接纳事件 event_time_ms) - allowed_lateness_ms
```

* 窗口为左闭右开固定滚动窗口 `[w, w+window_size_ms)`；
* 批次按 `(event_time_ms, event_id)` 排序后**逐条以运行中水位判定**，
  因此同批跨窗口/乱序事件等价于按时间逐条到达，窗口内乱序可正确重算；
* 窗口聚合在发布时统一排序后用加权 Welford 算法计算，**结果只取决于事件集合，
  与到达顺序无关**；
* 窗口在 `window_end_ms - 1 <= watermark_ms` 时发布，发布即不可变；
* `event_time_ms <= watermark_ms` 或目标窗口已发布的事件进入**隔离区**
  （原因 `late_beyond_watermark`，带当时水位与判定说明），可经
  `GET /quarantine` 查询，绝不静默丢弃、绝不改变已发布结果。

## 窗口特征

* `vwap = Σ(price·quantity) / Σquantity`（成交量加权均价）；
* `volatility = sqrt(Σ quantity·(price-vwap)² / Σquantity)`
  （成交量加权的总体标准差；单价格窗口为 0）；
* 每条特征附 `event_ids`（按 `(event_time_ms, event_id)` 有序）与计数，
  结果可审计；已发布特征持久于 `published_windows.jsonl`，重启不丢、不改。

## 检查点与重放

* 位点按 `source` 维护，**只增不减**：`seq > 当前值` 推进，同值重复提交幂等无操作；
* 请求更小值：明确拒绝 `checkpoint_rollback`（HTTP 409），**不从中间继续、不产生重复**；
* 位点仅在批次全部事件成功落盘并提交后推进，绝不超前于实际状态；
* 重放/重启：从 `events/*.jsonl` 重放重建水位与打开窗口，已发布窗口以
  `published_windows.jsonl` 为唯一真相；event_id 去重保证重复消费不重复计数，
  事件集合不变保证特征逐位一致。

## 批次原子性与失败归类

写入采用三阶段：**只读预检 → 内存提交（快照回滚）→ 持久化副作用**。

| 情况 | 归类 | 批次效果 |
|---|---|---|
| 结构错误（未知字段/类型/版本/缺字段） | `schema_*`（422） | 整批拒绝 |
| 内容冲突重复 | `duplicate_conflict`（422） | 整批拒绝 |
| 完全一致重复 | 响应 `duplicate`（200） | 幂等忽略 |
| 超水位/已发布窗口迟到 | `quarantined`（200，可查询） | 进隔离区，位点推进 |
| 积压/隔离区超限 | `backpressure_rejected`（503/异常） | 整批拒绝 |
| 位点回退 | `checkpoint_rollback`（409） | 位点不变 |

硬失败时内存整体快照回滚，事件日志零写入、位点不动。持久化阶段事件按标的
单次 `write` 批量追加（整批原子可见）；即便此刻进程崩溃，未推进的位点与
重放去重也保证重试后不重复、不缺失。

## 配置项（`market_data/config.py`，均可构造注入）

| 配置 | 默认 | 说明 |
|---|---|---|
| `window_size_ms` | 10000 | 固定窗口长度 |
| `allowed_lateness_ms` | 5000 | 水位允许迟到；超出即隔离 |
| `reject_unknown_fields` | True | 未知字段拒绝（False=显式忽略） |
| `max_pending_events` | 1_000_000 | 未发布事件+隔离积压上限（背压阈值） |
| `backpressure_strategy` | `reject` | `reject` 立即拒绝 / `delay` 有限次等待 |
| `backpressure_delay_ms` / `backpressure_max_retries` | 100 / 50 | delay 策略参数（不无限等待） |
| `max_quarantine_size` | 100_000 | 隔离区容量硬上限 |
| `data_dir` | `.mdi_data` | 本地状态目录；`:memory:` 关闭落盘 |
| `fsync` | False | 事件落盘是否 fsync（生产建议 True） |
| `ingest_time_budget_ms` | 10000 | 基准耗时预算（`/benchmark` 校验） |
| `benchmark_event_count` | 100000 | 默认基准规模 |

环境变量 `MDI_DATA_DIR` 可覆盖数据目录。

## 观测与资源上限验证

`GET /metrics` 暴露：accepted/duplicate/conflicts/quarantined/rejected、
废弃字段计数、已发布窗口数、背压/回退拒绝数、当前/峰值积压、
状态内存保守估计 `estimated_state_bytes`、以及每事件耗时
`avg/p50/p99 ns_per_event` 与当前水位、隔离区大小。
`POST /benchmark {"count":N,"symbols":K}` 在内存隔离环境合成确定性行情
（含逆序投递与整段重放），返回 `within_time_budget`、`within_memory_cap`、
`replay_accepted=0` 等可直接断言的字段。

## 本地验证方法

```bash
uv run pytest -q                      # 53 项：去重/跨窗口乱序/结构演进/重放回退/并发/背压/基准/HTTP
uv run pytest tests/test_02_out_of_order.py -o log_cli=true   # 观察 event_id/时间/水位/判定日志
uv run python -m market_data.demo     # 端到端脚本演示（见该模块）
```

判定日志格式：
`decision event_id=… event_time_ms=… watermark_ms=… -> <accepted|duplicate|quarantined|…> reason=… | 依据`

## 数据目录布局

```
<data_dir>/
├── events/<symbol-hex>.jsonl   # 每标的仅追加事件日志
├── published_windows.jsonl     # 已发布窗口特征（不可变真相）
├── quarantine.jsonl            # 隔离记录（带原因/水位/说明）
└── checkpoints.json            # 各来源位点（临时文件+原子替换）
```

## 设计取舍说明

* 水位完全基于事件时间，与处理速度无关，因此结果可复现；处理时间仅记录在隔离元数据中；
* 隔离事件同样推进来源位点（它已被终态处理），避免重放方卡死在该序号反复投递；
* 崩溃发生在持久化阶段时，已到点但尚未写特征文件的窗口在重启后按事件日志重新发布，
  事件集合相同 → 特征逐位相同（见 `test_restart_*`）。

## 资源边界（为什么不会无界占用内存）

* **未发布积压**：只有水位允许时间跨度内的事件保留完整事件对象，其数量只
  取决于窗口/迟到配置，与历史事件总量无关；超过 `max_pending_events`
  按背压策略延迟或拒绝。
* **去重指纹**：窗口发布后，其事件的业务字段指纹被压缩为定长哨兵
  （`Deduplicator.mark_published`），仅保留 `event_id` 与 `(source,seq)`
  身份键继续去重——10 万事件基准状态估计约 13MB（压缩前约 27MB），
  已发布事件重投仍识别为幂等重复，不会二次计数。
* **隔离区**：`max_quarantine_size` 硬上限，满后按背压拒绝并给出原因。
* 以上数字均通过 `GET /metrics` 的 `pending_events`、
  `estimated_state_bytes`、`quarantine_size` 与 `/benchmark` 实时验证。

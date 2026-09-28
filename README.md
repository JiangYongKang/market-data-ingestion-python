# 行情增量接入与窗口特征聚合（本地可复现）

一套**不依赖外部消息中间件、真实行情源或凭据**的本地行情接入服务：
批量/增量写入、事件去重、乱序与迟到处理、行情结构兼容演进、
固定窗口特征聚合（VWAP 与成交量加权波动）、检查点与重放、背压与资源上限，
以及**同一标的多路行情的独立时间线与跨渠道成交合并/冲突隔离**。
所有状态保存在本地仅追加日志，进程重启或整体重放后结果**逐位可再现**。

## 快速开始

```bash
uv sync
uv run uvicorn main:app --reload        # 启动 HTTP 服务（默认数据目录 .mdi_data）
uv run pytest -q                        # 运行全部 78 项测试（含判定日志）
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
| `trade_id` / `venue` | string \| null | 否 | v1 新增字段，**缺省为 `null`=未知**，绝不以 0 等伪装业务值。多来源开启时，同标的相同非空 `trade_id` 视为同一笔成交参与跨渠道合并；`venue` 不参与聚合 |

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

## 多来源接入（同标的多渠道）

### 事件约定

* 在配置 `multi_source_symbols` 中**按标的声明**参与多来源合并的渠道
  列表，**声明顺序即取舍优先级**（index 小者胜）。未声明的标的完全
  沿用单来源路径，行为与旧版逐位一致（兼容性见下文）；
* 同一笔跨渠道成交必须携带**相同的非空 `trade_id`**。没有 `trade_id`
  就无法证明两渠道报的是同一笔成交，这类事件各自独立计入，绝不猜测合并；
* 每个渠道的 `(source, seq)` 序号体系互相独立；位点仍按 `source` 分别
  单调推进；`event_id` 仍是每条投递的稳定主键。

### 渠道间时间线互不影响

多来源标的不再使用全局水位，而是为每个 `(symbol, source)` 各维护一条
独立时间线：

```
channel_watermark(symbol, source) = 该渠道已见最大 event_time_ms - allowed_lateness_ms
symbol_watermark(symbol)          = min(该标的全部已注册渠道的 channel_watermark)
```

* 迟到判定只看事件**所属渠道自己**的水位：某渠道跑得很快不会把落后
  渠道的正常事件误判成迟到丢进隔离区；某渠道长时间没数据，其它渠道
  照常推进、正常事件照常进入窗口；
* **窗口关闭按最慢渠道**：窗口右边界必须越过 `symbol_watermark`（全部
  渠道水位的最小值）才发布。落后渠道没来齐，窗口不关闭，结果按各渠道
  实际到齐的事件计算，不会因为快渠道先到而提前定稿；
* 声明了但从未到达的渠道在观测中标记 `stuck=true`、`observed=false`，
  其水位为 -∞，会压住窗口——这是显式可见的"卡住"，不是静默丢弃。

### 合并与冲突处理规则

对每个合并键 `(symbol, trade_id)`：

1. **获胜副本（计入聚合）**：渠道优先级最高的副本（声明序 →
   动态渠道按 `source` 字典序，全序无并列）；
2. **一致合并 `merged_identical`**：组内所有副本的关键内容
   **价格 `price` 与数量 `quantity` 完全一致**时，除获胜副本外的其余
   副本标记为合并，响应计数 `merged`，审计写入 `merges.jsonl`
   （胜负渠道、双方 event_id）。成交量与价格特征**只计一次**；
3. **冲突隔离 `cross_source_conflict`**：只要组内存在任一副本的
   价格或数量对不上，除获胜副本外的**所有次级副本全部隔离**，隔离说明
   列出双方价格/数量。它与普通重复 `duplicate_identical`、超水位迟到
   `late_beyond_watermark` 在响应计数、原因码、隔离区、审计、日志五处
   都可区分；
4. **分歧只进不退、判定是副本集合的纯函数**：副本集合只会增长，已分歧
   的组不可能重新一致，因此乱序、跨批次、整段重放都得到同一结果；
5. **高优先级晚到**：窗口发布前，高优先级副本可替换当前获胜者——
   旧获胜者移出打开窗口（内容一致则记 `merged`，已分歧则转
   `cross_source_conflict` 隔离），窗口发布后判定冻结，再到的副本按
   自己渠道的水位判迟到并隔离（说明含"多来源判定冻结"）。

查询接口：

* `GET /merges?symbol=` —— 合并/冲突审计记录（kind、winner/loser 渠道、
  winner_event_id/loser_event_id）；
* `GET /channels?symbol=` —— 各渠道当前 `max_event_time_ms`、独立
  `watermark_ms`、`lag_ms`（距上次事件的处理时间间隔）、`observed`、
  `stuck`，以及标的合并水位 `symbol_watermark_ms`、渠道数与卡住数。

### 兼容范围

* 不配置 `multi_source_symbols` 时，**所有标的走单来源路径**：全局水位、
  既有 56 项单来源断言保持不变；多来源是"按标的显式开启"的增量能力；
* 协议字段未改变：`trade_id`/`venue` 在 schema v1 已存在（缺省 `null`），
  多来源只新增配置项、原因码（`merged_identical`/
  `cross_source_conflict`/`channel_limit_exceeded`）、状态文件
  `merges.jsonl` 与两个只读查询端点；`IngestResult` 新增带默认值的
  `merged` 字段，旧调用方不受影响。

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
* **多来源重放**：跨渠道合并组由"获胜副本（事件日志）+ 被合并/冲突副本
  （`merges.jsonl` 中保存的完整事件负载）"的副本集合，按确定性取舍规则
  完全重建；渠道时间线从全部终态副本按 `(symbol, source)` 重建。
  重放不改变已发布结果，被合并副本整段重投一律识别为幂等重复，
  同一笔成交不会被重新计算第二遍。

## 批次原子性与失败归类

写入采用三阶段：**只读预检 → 内存提交（快照回滚）→ 持久化副作用**。

| 情况 | 归类 | 批次效果 |
|---|---|---|
| 结构错误（未知字段/类型/版本/缺字段） | `schema_*`（422） | 整批拒绝 |
| 内容冲突重复 | `duplicate_conflict`（422） | 整批拒绝 |
| 完全一致重复 | 响应 `duplicate`（200） | 幂等忽略 |
| 跨渠道同一笔、内容一致 | 响应 `merged`（200，`/merges` 可查） | 只计获胜副本一次 |
| 跨渠道同一笔、价/量对不上 | `cross_source_conflict`（200，隔离区可查） | 获胜者计入，次级副本隔离 |
| 超水位/已发布窗口迟到 | `quarantined`（200，可查询） | 进隔离区，位点推进 |
| 单标的渠道数超上限 | `channel_limit_exceeded`（422/异常） | 整批拒绝，渠道不注册 |
| 积压/隔离区超限 | `backpressure_rejected`（503/异常） | 整批拒绝 |
| 位点回退 | `checkpoint_rollback`（409） | 位点不变 |

硬失败时内存整体快照回滚，事件日志零写入、位点不动。持久化阶段事件按标的
单次 `write` 批量追加（整批原子可见）；即便此刻进程崩溃，未推进的位点与
重放去重也保证重试后不重复、不缺失。

## 配置项（`market_data/config.py`，均可构造注入）

| 配置 | 默认 | 说明 |
|---|---|---|
| `window_size_ms` | 10000 | 固定窗口长度 |
| `allowed_lateness_ms` | 5000 | 水位允许迟到；超出即隔离（多来源按渠道各自计算） |
| `multi_source_symbols` | `{}` | 多来源声明：`{标的: [渠道,...]}`，顺序即取舍优先级；未声明标的保持单来源 |
| `max_channels_per_symbol` | 16 | 单标的渠道数硬上限（含动态接入），超出整批拒绝 `channel_limit_exceeded` |
| `channel_idle_timeout_ms` | 600000 | 渠道无事件多久在观测中标记 `stuck`（仅观测，不参与判定） |
| `reject_unknown_fields` | True | 未知字段拒绝（False=显式忽略） |
| `max_pending_events` | 1_000_000 | 未发布事件+隔离积压上限（背压阈值） |
| `backpressure_strategy` | `reject` | `reject` 立即拒绝 / `delay` 有限次等待 |
| `backpressure_delay_ms` / `backpressure_max_retries` | 100 / 50 | delay 策略参数（不无限等待） |
| `max_quarantine_size` | 100_000 | 隔离区容量硬上限 |
| `data_dir` | `.mdi_data` | 本地状态目录；`:memory:` 关闭落盘 |
| `fsync` | False | 事件落盘是否 fsync（生产建议 True） |
| `ingest_time_budget_ms` | 10000 | 基准耗时预算（`/benchmark` 校验） |
| `benchmark_event_count` | 100000 | 默认基准规模 |

环境变量 `MDI_DATA_DIR` 可覆盖数据目录；`MDI_MULTI_SOURCE` 可声明多来源
渠道（形如 `"600000=venueA,venueB;000001=venueX,venueY"`，
标的=逗号分隔渠道列表、多标的分号分隔，顺序即优先级）。

## 观测与资源上限验证

`GET /metrics` 暴露：accepted/duplicate/conflicts/quarantined/rejected、
废弃字段计数、已发布窗口数、背压/回退拒绝数、当前/峰值积压、
状态内存保守估计 `estimated_state_bytes`、以及每事件耗时
`avg/p50/p99 ns_per_event` 与当前水位、隔离区大小；多来源还包含
`merged`、`cross_source_conflicts`、`channel_rejected` 计数，
`merge_groups`/`merge_groups_open` 成交组数，以及 `channels` 明细
（每渠道推进位置、独立水位、`lag_ms`、`stuck`、标的最慢水位）。
`POST /benchmark {"count":N,"symbols":K}` 在内存隔离环境合成确定性行情
（含逆序投递与整段重放），返回 `within_time_budget`、`within_memory_cap`、
`replay_accepted=0` 等可直接断言的字段。

## 本地验证方法

```bash
uv run pytest -q                      # 78 项：单来源 56 项不回归 + 多来源 22 项
uv run pytest tests/test_10_multi_source.py              # 多来源合并/冲突/独立时间线/重启/容量
uv run pytest tests/test_02_out_of_order.py -o log_cli=true   # 观察 event_id/时间/水位/判定日志
uv run python -m market_data.demo     # 端到端脚本演示（含第 8 节多来源合并）
```

多来源本地手工验证：

```bash
MDI_MULTI_SOURCE='600000=venueA,venueB,venueC' uv run uvicorn main:app --reload
# 同一笔成交 T-100 三个渠道各报一次，第三个价格对不上
curl -X POST localhost:8000/ingest -H 'content-type: application/json' -d '[
  {"event_id":"a","source":"venueA","seq":1,"symbol":"600000","price":10.0,"quantity":2,"event_time_ms":1000,"trade_id":"T-100"},
  {"event_id":"b","source":"venueB","seq":1,"symbol":"600000","price":10.0,"quantity":2,"event_time_ms":2000,"trade_id":"T-100"},
  {"event_id":"c","source":"venueC","seq":1,"symbol":"600000","price":10.5,"quantity":2,"event_time_ms":3000,"trade_id":"T-100"}]'
# -> {"accepted":1,"merged":1,"quarantined":1,...}
curl localhost:8000/merges?symbol=600000      # 合并/冲突胜负留痕
curl 'localhost:8000/channels?symbol=600000'  # 各渠道推进位置/独立水位/卡住
```

判定日志格式：
`decision event_id=… event_time_ms=… watermark_ms=… -> <accepted|duplicate|quarantined|…> reason=… | 依据`

## 数据目录布局

```
<data_dir>/
├── events/<symbol-hex>.jsonl   # 每标的仅追加事件日志（获胜/独立成交）
├── published_windows.jsonl     # 已发布窗口特征（不可变真相）
├── quarantine.jsonl            # 隔离记录（迟到 + 跨渠道冲突，带原因/水位/说明）
├── merges.jsonl                # 跨渠道合并/冲突审计（胜负渠道 + 失败副本完整负载）
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
* **多来源渠道数**：`max_channels_per_symbol` 硬上限（默认 16），超出的
  渠道以 `channel_limit_exceeded` 整批拒绝、不注册、位点不动，防止渠道
  异常增长无界占用注册表与水位内存；动态渠道只能接入已声明多来源的标的。
* **合并组**：窗口发布后成交组业务副本被压缩为身份集合
  （`TradeMerger.mark_window_published`），内存占用只与未发布窗口内的
  成交数相关，不随历史无界增长；`metrics.merge_groups*` 与
  `estimated_state_bytes` 实时可见。
* 以上数字均通过 `GET /metrics` 的 `pending_events`、
  `estimated_state_bytes`、`quarantine_size`、`channels` 与 `/benchmark`
  实时验证。

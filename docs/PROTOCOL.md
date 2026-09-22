# 事件协议与结构演进

## 1. 写入形态

支持单条（增量）与批量两种写入：

- `POST /ingest/events`：请求体即一个事件对象；
- `POST /ingest/batch`：请求体 `{"events": [事件, ...]}`，**整批原子**。

每条事件必须携带稳定标识与来源序号：

| 字段 | 类型 | 约束 | 含义 |
|---|---|---|---|
| `event_id` | string | 非空 | 稳定业务标识，**幂等键** |
| `source` | string | 非空 | 来源/行情通道名 |
| `seq` | int | ≥ 0 | **按 source 单调**的来源序号 |
| `symbol` | string | 非空 | 标的 |
| `event_time_ms` | int | ≥ 0 | 事件时间（窗口/水位时钟） |
| `price` | number | 有限且 > 0 | 成交价 |
| `quantity` | number | 有限且 > 0 | 成交量 |
| `schema_version` | string | 可选，默认 `v1` | 协议版本 |

## 2. 身份与排序语义

- 业务身份 = `event_id`；来源顺序令牌 = `(source, seq)`；
  到达墙钟时间 `ingest_time_ms` 仅用于观测，不参与窗口正确性。
- 内容指纹（用于区分重复类型）包含
  `source, seq, symbol, event_time_ms, price, quantity, schema_version,
  notional_ccy, venue`，数值按 9 位小数归一后比较。

### 重复投递的两类判定（必须可区分）

1. **普通重复 `DUPLICATE_IDENTICAL`**：同 `event_id` 且指纹一致 →
   幂等空操作，不计入任何聚合、不推进位点；批量提交但只计 `duplicates`。
2. **内容冲突重复 `DUPLICATE_CONFLICT`**：同 `event_id` 但指纹不同 →
   **硬拒绝并整批回滚**，响应中给出存储指纹与新指纹（`stored=`/`new=`）。
3. **来源序号回退 `SEQ_REGRESSED`**：在**同一次有序投递（同一批）**内，
   某 `source` 的 `seq` 比该 source 在本批已出现的位置更小（回绕）→
   独立原因码硬拒绝并整批回滚。
   跨批次/跨线程的到达允许合法交叉乱序（由事件时间/水位与幂等指纹处理），
   因此不设跨批全局 seq 门限，避免把并发交叉误判为回退；不同 source 的
   序号空间相互独立。

同一未提交批次内重复出现的 id 按相同规则分类（批内幂等/批内冲突）。
真实重投携带相同 seq，因身份判定先于序号检查而正确幂等/冲突分类。

## 3. 结构兼容演进

| 版本 | 内容 |
|---|---|
| `v1` | 上述必填字段 |
| `v2` | 新增 `notional_ccy`、`venue` |

- **新增字段缺省语义确定且稳定**：
  无论报文是 v1 还是 v2，归一化后缺省值都一致——
  `notional_ccy` 缺省为 `"USD"`，`venue` 缺省为 `"UNKNOWN"`。
  因此“同一个业务事件”用不同线协议版本发送，归一化指纹完全相同。
- **未知字段**（策略 `unknown_field_policy`）：
  - `IGNORE`（默认，前向兼容）：剥离、绝不静默映射到已知列，
    并在指标 `schema_unknown_fields` 计数；
  - `REJECT`：以 `SCHEMA_UNKNOWN_FIELD` 明确拒绝。
- **废弃字段**：v2 中 v1 时代的别名 `ccy` 仍可读，作为 `notional_ccy`
  的回退值，并在 `schema_deprecated_fields` 计数；
  当 `ccy` 与 `notional_ccy` 同时出现且取值冲突时，以
  `SCHEMA_INVALID_VALUE` 拒绝，绝不静默二选一。
- **类型不匹配**永远以 `SCHEMA_TYPE_MISMATCH` 拒绝：
  布尔不被当作数字、`seq`/`event_time_ms` 必须为整型；
  `NaN`/`Inf`、非正数以 `SCHEMA_INVALID_VALUE` 拒绝；
  缺字段为 `SCHEMA_MISSING_FIELD`；版本不支持为
  `SCHEMA_UNSUPPORTED_VERSION`。

## 4. 示例

```jsonc
// v1
{"event_id":"e-1","source":"feed-a","seq":1,"symbol":"BTCUSD",
 "event_time_ms":1000,"price":100.5,"quantity":2}

// v2：新增字段，缺失则按稳定缺省补全
{"event_id":"e-2","source":"feed-a","seq":2,"symbol":"BTCUSD",
 "event_time_ms":1200,"price":101.0,"quantity":1,
 "schema_version":"v2","venue":"CNX"}          // notional_ccy -> "USD"
```

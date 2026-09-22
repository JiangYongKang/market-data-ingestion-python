# 水位、乱序与窗口规则

## 1. 滚动窗口

窗口大小由 `window_size_ms` 配置（默认 1000ms），窗口归属完全确定：

```
window_start = floor(event_time_ms / window_size_ms) * window_size_ms
window_end   = window_start + window_size_ms      // 左闭右开
```

窗口特征（可解释、可复现）：

- `event_count`、`total_quantity`；
- **成交量加权价 VWAP** = `Σ(price·quantity) / Σ(quantity)`；
- `mean_price`、**价格波动** `price_stddev`（成交价样本标准差，n<2 为 0）、
  `volatility_bps = stddev / mean · 10⁴`；
- `min_price`、`max_price`、首末事件时间；
- `event_ids`（有序，参与结果的数据明细，用于解释与复现）；
- `published`（是否冻结）、`recomputed`（是否曾因乱序重算）。

所有累加量（`Σp`、`Σp²`、`Σpq`、`Σq`）单遍维护，无需保存全部价格即可
复现同一 VWAP/方差；快照中持久化这些累加量，重启后逐位一致。

## 2. 水位与允许迟到

按标的独立维护，规则与到达墙钟无关、完全确定：

```
watermark(symbol) = max(已观察 event_time_ms) − allowed_lateness_ms
```

- 水位**只进不退**：观察到更老的事件时间不会使水位下降。
- 迟到判定：`event_time_ms < watermark(该标的)`（处理时刻）。
- 落在半开允许区间 `[watermark, watermark + allowed_lateness)` 的事件
  **不算迟到**，但属于乱序：会在窗口**发布之前正确重算**该窗口，
  结果标记 `recomputed=true`。
- 当窗口 `window_end <= watermark` 时窗口**发布并冻结**；
  此后针对该窗口的事件无法改变已发布结果。

## 3. 超过水位：隔离区（绝不静默丢弃）

晚于水位（或目标窗口已发布）的事件：

- 不改变任何已发布结果；
- 进入**有界、可查询的隔离区** `/quarantine`，记录
  `event_id / event_time_ms / 当时水位 / window_end / reason=
  LATE_BEYOND_WATERMARK / 原始 payload / 判定依据 detail`；
- 仍占用持久化位点（已被日志确认接收），但**不计入聚合 accepted**；
- 隔离区按标的有界（`max_quarantine_per_symbol`），超出按 FIFO 淘汰，
  淘汰数在 `quarantine_evicted` 中可观测。

边界约定：事件时间**等于**水位不算迟到（`<` 判定，边界 inclusive）。

## 4. 复现与重放

- 已发布结果在重复消费、进程重启后**逐位复现**：
  启动时读取原子快照 + 重放快照之后的日志尾（无快照则从 0 重放）；
  全量重放天然幂等（去重指纹保证不重复计数）。
- 崩溃时日志尾部半条记录会在打开时被识别并截断，不会产生半批事件。
- 检查点（= 已确认日志事件数）**只单调推进**：
  - 运行中的引擎请求 `offset <= 当前位点` → 拒绝 `CHECKPOINT_REGRESSED`
    （不会“从中间继续”，也不会产生重复）；
  - 请求超过日志长度的位点 → 拒绝 `CHECKPOINT_UNKNOWN`（不跳空）；
  - 需要重放到更早前缀：用**新引擎进程** + 同一 `state_dir` +
    `start_offset` 引导，确定性重建，而不是回退活引擎。

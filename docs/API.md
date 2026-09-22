# HTTP API 与失败原因码

基址 `http://127.0.0.1:8000`，交互式文档 `/docs`。

## 端点

| 方法/路径 | 说明 |
|---|---|
| `GET /healthz` | 存活检查 |
| `POST /ingest/events` | 写入单条事件 |
| `POST /ingest/batch` | 原子批量写入 `{"events":[...]}` |
| `GET /windows?symbol=&all=` | 窗口特征；默认仅已发布，`all=true` 含未决 |
| `GET /quarantine?symbol=&reason=` | 查询隔离区 |
| `GET /checkpoint` | 当前位点 |
| `POST /replay` | `{"offset":n}` 前向重定位（只进不退） |
| `GET /metrics` | 计数器/时延分位/资源占用 |

## 批量响应

```json
{
  "committed": true,
  "accepted": 2,
  "duplicates": 1,
  "conflicts": 0,
  "quarantined": 0,
  "rejected": 0,
  "checkpoint": 3,
  "duration_us": 142,
  "records": [
    {"event_id": "e-1", "accepted": true, "reason_code": null,
     "detail": "", "window_start_ms": 0, "watermark_ms": 500,
     "quarantine_id": null}
  ]
}
```

普通重复：`committed=true`、`accepted` 不增加、`reason_code=
DUPLICATE_IDENTICAL`。内容冲突/序号回退/结构错误：`committed=false`，
整批无副作用，每条记录给出独立 `reason_code` 与 `detail`。

## 状态码映射

| HTTP | 场景 | reason |
|---|---|---|
| 200 | 成功（含幂等重复、含进入隔离区的接收） | `DUPLICATE_IDENTICAL` / `LATE_BEYOND_WATERMARK`（记录级） |
| 400 | 请求体形状错误 | `SCHEMA_INVALID_VALUE` / `SCHEMA_TYPE_MISMATCH` |
| 422 | 结构/冲突/序号等领域拒绝 | `SCHEMA_*`、`DUPLICATE_CONFLICT`、`SEQ_REGRESSED` |
| 409 | 位点回退或未知位点 | `CHECKPOINT_REGRESSED` / `CHECKPOINT_UNKNOWN` |
| 429 | 背压拒绝 | `BACKPRESSURE_REJECTED` |

错误体统一为：

```json
{"error": "CheckpointError", "reason": "CHECKPOINT_REGRESSED",
 "detail": "refusing checkpoint rewind: requested=1 current=5; ..."}
```

## 端到端小试（curl）

```bash
curl -s localhost:8000/ingest/events -H 'content-type: application/json' \
  -d '{"event_id":"e1","source":"f","seq":1,"symbol":"BTCUSD",
       "event_time_ms":0,"price":100,"quantity":2}'
curl -s 'localhost:8000/windows?symbol=BTCUSD&all=true'
curl -s localhost:8000/metrics
```

## 判定日志

`ingestion.*` logger 在 INFO 级别输出单测与运行时判定依据，包含
**事件标识、事件时间、水位、窗口、原因码与判定 basis**，例如：

```
dedupe  event_id=e1 decision=DUPLICATE_CONFLICT basis=id-reseen fingerprint-stored=... fingerprint-new=...
watermark symbol=BTCUSD advanced 400 -> 900 basis=max_event_time=1400 lateness=500
quarantine id=7 event_id=late event_time_ms=100 watermark=1500 window_end=1000 reason=LATE_BEYOND_WATERMARK basis=...
```

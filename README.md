# 行情增量接入与窗口特征聚合（本地可复现）

一个**不依赖外部消息中间件、真实行情源或真实凭据**的本地行情增量接入服务：
支持批量/增量写入、事件幂等去重、乱序与迟到处理、行情结构兼容演进、
事件时间窗口特征（VWAP、波动率）、单调检查点与重放、背压与资源上限，
以及同标的并发一致性。持久化只用本地目录（append-only 日志 + 原子快照）。

- 语言/运行时：Python 3.14、FastAPI、纯标准库持久化（`pickle` 长度分帧）
- 并发模型：单把可重入锁保护全部状态迁移；背压用独立信号量
- 提交模型：每批“先分类（零变更）后提交”，任何失败整批回滚

## 快速开始

```bash
uv sync
# 启动（默认内存态；设置 INGESTION_STATE_DIR 启用本地持久化目录）
uv run uvicorn app.api:app --reload
# 或
uv run python main.py
```

健康检查：`GET http://127.0.0.1:8000/healthz`，交互式文档：`/docs`。

## 本地验证

```bash
# 全部单测（去重/跨窗口乱序/结构演进/重放回退/并发/背压/HTTP）
uv run pytest -q

# 仅基准门限（耗时与内存上限，可配置）
uv run pytest -m benchmark -q -s

# 调整基准规模与门限（环境变量，便于在不同机器上复现）
BENCH_SCALE=20000 BENCH_MAX_WRITE_US=6000 BENCH_MAX_DEDUPE_KB=300000 \
    uv run pytest -m benchmark -q -s
```

持久化与重启复现无需任何外部组件：

```bash
export INGESTION_STATE_DIR=./.state
uv run uvicorn app.api:app        # 写入后 Ctrl-C，再次启动即从日志/快照恢复
```

## 文档索引

- [事件协议与结构演进](docs/PROTOCOL.md)
- [水位、乱序与窗口规则](docs/WATERMARK.md)
- [配置项与可观测指标](docs/CONFIGURATION.md)
- [HTTP API 与失败原因码](docs/API.md)

## 失败原因码（可机器区分）

`RejectReason`：`DUPLICATE_IDENTICAL`（普通重复，幂等空操作）、
`DUPLICATE_CONFLICT`（同 id 内容冲突，硬拒绝）、`SEQ_REGRESSED`（来源序号回退）、
`LATE_BEYOND_WATERMARK`（晚于水位，入隔离区）、`SCHEMA_*`（结构问题）、
`BACKPRESSURE_*`（背压延迟/拒绝）、`CHECKPOINT_REGRESSED`/`CHECKPOINT_UNKNOWN`
（检查点回退/未知位点）。HTTP 上分别映射为 200/422/409/429。

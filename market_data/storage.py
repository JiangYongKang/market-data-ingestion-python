"""本地事件日志：按标的分片的仅追加 JSONL，无外部依赖。

* 路径：``<data_dir>/events/<symbol 的十六进制编码>.jsonl``，
  文件名编码确定、无注入与碰撞问题；
* 每条规范化事件序列化为单行 JSON，追加写入（可选 fsync）；
* 重放按文件内顺序产出；由于窗口聚合结果与到达顺序无关
  （见 :mod:`market_data.windows`），重放/重启后结果仍逐位一致；
* 事件标识去重保证重放不会重复计数，位点保证不会从中间状态续传。
"""
from __future__ import annotations

import json
import os

from .models import Event, QuarantinedEvent, WindowFeature


def _encode_symbol(symbol: str) -> str:
    return symbol.encode("utf-8").hex()


def _event_to_dict(e: Event) -> dict:
    return {
        "event_id": e.event_id,
        "source": e.source,
        "seq": e.seq,
        "symbol": e.symbol,
        "price": e.price,
        "quantity": e.quantity,
        "event_time_ms": e.event_time_ms,
        "schema_version": e.schema_version,
        "trade_id": e.trade_id,
        "venue": e.venue,
    }


class EventLog:
    def __init__(self, data_dir: str, fsync: bool = False) -> None:
        self._fsync = fsync
        self._dir: str | None = None
        if data_dir != ":memory:":
            self._dir = os.path.join(data_dir, "events")
            os.makedirs(self._dir, exist_ok=True)

    def _path_for(self, symbol: str) -> str:
        return os.path.join(self._dir, _encode_symbol(symbol) + ".jsonl")

    def append(self, event: Event) -> None:
        self.append_batch([event])

    def append_batch(self, events: list[Event]) -> None:
        """按标的分组，每组一次 ``write`` 系统调用提交，保证批次原子可见。

        本地文件系统上单次 ``write`` 对 O_APPEND 模式为原子写；
        配合 fsync，崩溃时只会看到整批或没有这批，不会出现半批。
        """
        if self._dir is None or not events:
            return
        by_symbol: dict[str, list[str]] = {}
        for event in events:
            line = json.dumps(_event_to_dict(event), sort_keys=True,
                              ensure_ascii=False, separators=(",", ":"))
            by_symbol.setdefault(event.symbol, []).append(line)
        for symbol, lines in by_symbol.items():
            with open(self._path_for(symbol), "a", encoding="utf-8") as fh:
                fh.write("".join(line + "\n" for line in lines))
                fh.flush()
                if self._fsync:
                    os.fsync(fh.fileno())

    def replay(self, symbol: str) -> list[Event]:
        if self._dir is None:
            return []
        path = self._path_for(symbol)
        if not os.path.exists(path):
            return []
        out: list[Event] = []
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    out.append(Event(**json.loads(line)))
        return out

    def replay_all(self) -> list[Event]:
        out: list[Event] = []
        for symbol in self.symbols():
            out.extend(self.replay(symbol))
        return out

    def symbols(self) -> list[str]:
        if self._dir is None or not os.path.isdir(self._dir):
            return []
        names = []
        for name in os.listdir(self._dir):
            if name.endswith(".jsonl"):
                names.append(bytes.fromhex(name[:-6]).decode("utf-8"))
        return sorted(names)


class PublishedWindowStore:
    """已发布窗口特征的仅追加存储（重启后查询/对账可复现）。

    文件 ``<data_dir>/published_windows.jsonl``，每行一条窗口特征，
    以 (symbol, window_start_ms) 去重写入；读取按 (symbol, window_start)
    排序，结果确定。
    """

    def __init__(self, data_dir: str) -> None:
        self._path: str | None = None
        self._existing: set[tuple[str, int]] = set()
        if data_dir != ":memory:":
            os.makedirs(data_dir, exist_ok=True)
            self._path = os.path.join(data_dir, "published_windows.jsonl")
            if os.path.exists(self._path):
                with open(self._path, encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if line:
                            rec = json.loads(line)
                            self._existing.add((rec["symbol"], rec["window_start_ms"]))

    @staticmethod
    def _feat_to_dict(f: WindowFeature) -> dict:
        return {
            "symbol": f.symbol,
            "window_start_ms": f.window_start_ms,
            "window_end_ms": f.window_end_ms,
            "count": f.count,
            "total_quantity": f.total_quantity,
            "vwap": f.vwap,
            "volatility": f.volatility,
            "event_ids": list(f.event_ids),
            "published": True,
        }

    def append_many(self, feats: list[WindowFeature]) -> None:
        if self._path is None:
            return
        fresh = [f for f in feats if (f.symbol, f.window_start_ms) not in self._existing]
        if not fresh:
            return
        with open(self._path, "a", encoding="utf-8") as fh:
            for f in fresh:
                fh.write(json.dumps(self._feat_to_dict(f), sort_keys=True,
                                    ensure_ascii=False, separators=(",", ":")) + "\n")
                self._existing.add((f.symbol, f.window_start_ms))
            fh.flush()
            os.fsync(fh.fileno())

    def load_all(self) -> list[WindowFeature]:
        if self._path is None or not os.path.exists(self._path):
            return []
        out: list[WindowFeature] = []
        with open(self._path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                out.append(WindowFeature(
                    symbol=rec["symbol"],
                    window_start_ms=rec["window_start_ms"],
                    window_end_ms=rec["window_end_ms"],
                    count=rec["count"],
                    total_quantity=rec["total_quantity"],
                    vwap=rec["vwap"],
                    volatility=rec["volatility"],
                    event_ids=tuple(rec["event_ids"]),
                    published=True,
                ))
        out.sort(key=lambda f: (f.symbol, f.window_start_ms))
        return out


class QuarantineStore:
    """隔离区仅追加持久化（重启后隔离记录仍可查询、位点语义连续）。"""

    def __init__(self, data_dir: str) -> None:
        self._path: str | None = None
        if data_dir != ":memory:":
            os.makedirs(data_dir, exist_ok=True)
            self._path = os.path.join(data_dir, "quarantine.jsonl")

    @staticmethod
    def _to_dict(q: QuarantinedEvent) -> dict:
        e = q.event
        return {
            "event": _event_to_dict(e),
            "reason": q.reason.value,
            "watermark_ms": q.watermark_ms,
            "detail": q.detail,
            "accepted_at_ms": q.accepted_at_ms,
        }

    def append_batch(self, items: list[QuarantinedEvent]) -> None:
        if self._path is None or not items:
            return
        payload = "".join(
            json.dumps(self._to_dict(q), sort_keys=True,
                       ensure_ascii=False, separators=(",", ":")) + "\n"
            for q in items)
        with open(self._path, "a", encoding="utf-8") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())

    def load_all(self) -> list[QuarantinedEvent]:
        if self._path is None or not os.path.exists(self._path):
            return []
        from .models import RejectReason
        out: list[QuarantinedEvent] = []
        with open(self._path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                out.append(QuarantinedEvent(
                    event=Event(**rec["event"]),
                    reason=RejectReason(rec["reason"]),
                    watermark_ms=rec["watermark_ms"],
                    detail=rec["detail"],
                    accepted_at_ms=rec["accepted_at_ms"],
                ))
        return out


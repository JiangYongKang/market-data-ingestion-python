"""跨渠道合并/冲突审计记录的仅追加存储。

文件 ``<data_dir>/merges.jsonl``，每行一条 :class:`MergeRecord`，
字段固定、按 key 排序序列化（确定）。用途：

* 审计：每个被合并/被冲突隔离的副本都留痕，标明胜负渠道、各自
  event_id 以及失败副本的完整事件负载；文件只追加，同一次级副本的
  状态可能从 merged 翻转为 conflict（分歧只进不退），此时追加一条新
  记录，读取时以同一 ``loser_event_id`` 的最后一条为准（追加 O(1)，
  不做整文件重写）；
* 重放：合并组由"获胜副本（事件日志）+ 失败副本（本文件负载）"的
  副本集合加确定性取舍规则完全重建，判定纯函数化、逐位可复现。
"""
from __future__ import annotations

import json
import os

from .models import Event, MergeRecord


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


def _record_to_dict(r: MergeRecord) -> dict:
    return {
        "symbol": r.symbol,
        "trade_id": r.trade_id,
        "kind": r.kind,
        "winner": r.winner,
        "loser": r.loser,
        "winner_event_id": r.winner_event_id,
        "loser_event_id": r.loser_event_id,
        "decided_at_ms": r.decided_at_ms,
        "loser_event": (
            _event_to_dict(r.loser_event_payload)
            if r.loser_event_payload is not None else None),
    }


class MergeRecordStore:
    def __init__(self, data_dir: str) -> None:
        self._path: str | None = None
        self._records: dict[str, MergeRecord] = {}  # loser_event_id -> 最新记录
        if data_dir != ":memory:":
            os.makedirs(data_dir, exist_ok=True)
            self._path = os.path.join(data_dir, "merges.jsonl")
            if os.path.exists(self._path):
                for rec in self._iter_file():
                    self._records[rec.loser_event_id] = rec

    @staticmethod
    def _from_dict(rec: dict) -> MergeRecord:
        le = rec.get("loser_event")
        return MergeRecord(
            symbol=rec["symbol"], trade_id=rec["trade_id"], kind=rec["kind"],
            winner=rec["winner"], loser=rec["loser"],
            winner_event_id=rec["winner_event_id"],
            loser_event_id=rec["loser_event_id"],
            decided_at_ms=rec["decided_at_ms"],
            loser_event_payload=Event(**le) if le else None,
        )

    def _iter_file(self):
        with open(self._path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    yield self._from_dict(json.loads(line))

    def append_batch(self, records: list[MergeRecord]) -> None:
        """追加记录；同一 loser_event_id 的 kind/winner 变化以新行覆盖。

        ``data_dir=":memory:"`` 时不落盘，但内存镜像照常保留
        （审计查询/同进程状态一致）。
        """
        if not records:
            return
        fresh: list[MergeRecord] = []
        for r in records:
            old = self._records.get(r.loser_event_id)
            if old is None or old.kind != r.kind or old.winner != r.winner:
                fresh.append(r)
                self._records[r.loser_event_id] = r
        if self._path is None or not fresh:
            return
        payload = "".join(
            json.dumps(_record_to_dict(r), sort_keys=True,
                       ensure_ascii=False, separators=(",", ":")) + "\n"
            for r in fresh)
        with open(self._path, "a", encoding="utf-8") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())

    def load_all(self) -> list[MergeRecord]:
        return [self._records[k] for k in sorted(self._records)]

    def load_loser_events(self) -> list[Event]:
        """全部失败副本的完整事件（用于重放重建合并组）。"""
        return [r.loser_event_payload for r in self.load_all()
                if r.loser_event_payload is not None]

"""跨渠道同一笔成交的合并：去双计、确定性取舍、内容冲突隔离。

触发条件
========
事件携带非空 ``trade_id``，且同一 ``(symbol, trade_id)`` 在此前由
**不同渠道**上报过。同渠道重复仍走 event_id/(source,seq) 双键去重。

取舍规则（确定、可复现）
========================
1. **主报渠道优先级**：按 ``config.merge_sources`` 声明的顺序取靠前
   渠道（未列入的渠道排在其后，渠道名字典序兜底）。高优先级渠道的
   上报到达时，若主报尚未发布，替换此前的低优先级主报；
2. **先到占位**：同优先级内以先到达（event_id 字典序兜底）为准，
   结果与重放顺序无关——重放按事件时间排序后仍收敛到同一主报；
3. 非主报的其余路标记为 :class:`MergedTradeRecord`（``cross_source_merged``），
   **不重复计入成交量与价格特征**。

冲突规则
========
同一 ``(symbol, trade_id)`` 不同渠道上报的**关键内容**（``price``、
``quantity``）对不上时，绝不静默按一方计算：该笔成交标记为冲突，
**所有**相关上报都以 ``merge_conflict`` 隔离——包括此前已入窗但尚未
发布的主报（确认冲突时从窗口清出，见 :meth:`CrossSourceMerger.evict_winner`），
结果与"分几次投递、一次投几路"无关；原因码与普通重复
（``duplicate_identical``）明确区分；冲突一旦成立不再解除。
已发布窗口不可变：窗口发布后到达的冲突副本只隔离新到上报，
已发布结果逐位不变。
``event_time_ms``、``venue`` 差异不属于冲突（各渠道时钟/场所命名可不同）。
"""
from __future__ import annotations

import copy

from .models import Event, MergedTradeRecord


def _trade_key(event: Event) -> tuple[str, str] | None:
    if event.trade_id:
        return (event.symbol, event.trade_id)
    return None


class CrossSourceMerger:
    def __init__(self, source_priority: tuple[str, ...]) -> None:
        self._priority: dict[str, int] = {
            src: i for i, src in enumerate(source_priority)
        }
        # trade_key -> 当前主报事件（仅保留未发布窗口所需信息）
        self._winners: dict[tuple[str, str], Event] = {}
        # 主报已随窗口发布的成交：保留身份与关键内容指纹，
        # 后到一致上报幂等并入，关键内容对不上仍可判冲突（绝不静默放过）。
        # key -> (winner_event_id, winner_source, price, quantity)
        self._published_winners: dict[tuple[str, str], tuple[str, str, float, float]] = {}
        # 主报到达前已到的其他渠道上报（罕见乱序：高优先级可能后来居上）
        self._pending: dict[tuple[str, str], list[Event]] = {}
        # 已被合并掉的上报留痕（含当批产生、待持久化）
        self._records: list[MergedTradeRecord] = []
        # 已判定内容冲突的成交（一旦成立不解除）
        self._conflicted: set[tuple[str, str]] = set()
        # 主报被高优先级替换、需要从窗口取出的事件 event_id -> event
        self._superseded: dict[str, Event] = {}

    def _rank(self, source: str) -> tuple[int, str]:
        return (self._priority.get(source, len(self._priority)), source)

    def is_merge_candidate(self, event: Event) -> bool:
        return event.trade_id is not None and bool(event.trade_id)

    def classify(self, event: Event) -> tuple[str, object]:
        """纯查询（不修改状态），供批次预检。

        返回 ``(verdict, payload)``：
        * ``("new", winner_event)``：该笔成交首报（event 本身将成为主报）；
        * ``("loser", winner_event)``：与已有主报关键内容一致，应被合并；
        * ``("replace", old_winner_event)``：更高优先级渠道，将替换主报；
        * ``("conflict", existing_event)``：关键内容对不上，应隔离。
        """
        key = _trade_key(event)
        if key is None:
            return "new", event
        if key in self._conflicted:
            return "conflict", self._winners.get(key)
        winner = self._winners.get(key)
        if winner is None:
            pub = self._published_winners.get(key)
            if pub is not None:
                _, _, price, qty = pub
                if (event.price, event.quantity) != (price, qty):
                    return "conflict", None
                return "loser_published", pub
            return "new", event
        if event.source == winner.source:
            # 同渠道同 trade_id：不是跨渠道合并场景，交回双键去重处理
            return "new", event
        if (event.price, event.quantity) != (winner.price, winner.quantity):
            return "conflict", winner
        if self._rank(event.source) < self._rank(winner.source):
            return "replace", winner
        return "loser", winner

    # ---------- 提交阶段登记 ----------
    def register_winner(self, event: Event) -> None:
        """登记主报（首报 / 高优先级替换后）。"""
        key = _trade_key(event)
        if key is None:
            return
        old = self._winners.get(key)
        if old is not None and event.source != old.source \
                and self._rank(event.source) < self._rank(old.source):
            # 高优先级渠道后来居上：旧主报降级为被合并，需从窗口取出
            self._superseded[old.event_id] = old
            self._records.append(MergedTradeRecord(
                trade_key=key, winner_event_id=event.event_id,
                winner_source=event.source, loser_event_id=old.event_id,
                loser_source=old.source, event_time_ms=old.event_time_ms,
                loser_event=old, loser_seq=old.seq))
        # 挂起的其他渠道上报：按内容/优先级分流
        buffered = self._pending.pop(key, [])
        self._winners[key] = event
        for other in buffered:
            self._absorb(key, event, other)

    def register_loser(self, event: Event) -> None:
        """登记一条被合并上报（已有主报且关键内容一致）。"""
        key = _trade_key(event)
        if key is None:
            return
        winner = self._winners.get(key)
        if winner is None:
            # 主报尚未提交（同批乱序）：暂存，待 register_winner 时分流
            self._pending.setdefault(key, []).append(event)
            return
        self._absorb(key, winner, event)

    def register_loser_published(self, event: Event) -> None:
        key = _trade_key(event)
        if key is None:
            return
        pub_winner_eid, pub_winner_src, _, _ = self._published_winners[key]
        self._records.append(MergedTradeRecord(
            trade_key=key, winner_event_id=pub_winner_eid,
            winner_source=pub_winner_src, loser_event_id=event.event_id,
            loser_source=event.source, event_time_ms=event.event_time_ms,
            loser_event=event, loser_seq=event.seq))

    def _absorb(self, key: tuple[str, str], winner: Event, other: Event) -> None:
        if other.source == winner.source:
            return
        if (other.price, other.quantity) != (winner.price, winner.quantity):
            # 提交期二次确认（pending 中可能混入冲突上报）
            self._conflicted.add(key)
            return
        self._records.append(MergedTradeRecord(
            trade_key=key, winner_event_id=winner.event_id,
            winner_source=winner.source, loser_event_id=other.event_id,
            loser_source=other.source, event_time_ms=other.event_time_ms,
            loser_event=other, loser_seq=other.seq))

    def mark_conflicted(self, trade_key: tuple[str, str]) -> None:
        self._conflicted.add(trade_key)

    def evict_winner(self, trade_key: tuple[str, str]) -> Event | None:
        """冲突成立时弹出当前主报（若尚未随窗口发布）。

        服务据此把该主报从未发布窗口移除并以 ``merge_conflict`` 隔离：
        同一笔成交的冲突一旦确认，先到的一路也不得计入成交量/价格特征。
        已发布窗口的主报不在 ``_winners`` 中（已压缩进 ``_published_winners``），
        不可变，返回 None。
        """
        return self._winners.pop(trade_key, None)

    def take_superseded(self) -> dict[str, Event]:
        """取出本批被替换、需要从打开窗口移除的旧主报（取走即清空）。"""
        out = self._superseded
        self._superseded = {}
        return out

    def drain_records(self) -> list[MergedTradeRecord]:
        """取走待持久化的合并记录（取走即清空）。"""
        out = self._records
        self._records = []
        return out

    def mark_winners_published(self, event_ids: set[str]) -> None:
        """窗口发布后：主报指纹压缩为身份+关键内容，继续支撑后续合并判定。"""
        for key, ev in list(self._winners.items()):
            if ev.event_id in event_ids:
                self._published_winners[key] = (
                    ev.event_id, ev.source, ev.price, ev.quantity)
                del self._winners[key]

    # ---------- 恢复 ----------
    def restore(self, records: list[MergedTradeRecord]) -> None:
        """重放恢复合并留痕（去重），状态本身由事件日志重放重建。"""
        existing = {(r.winner_event_id, r.loser_event_id) for r in self._records}
        for rec in records:
            k = (rec.winner_event_id, rec.loser_event_id)
            if k not in existing:
                self._records.append(rec)
                existing.add(k)

    def restore_conflicted(self, keys: list[tuple[str, str]]) -> None:
        self._conflicted.update(keys)

    def restore_published_winners(self, entries: dict[tuple[str, str],
                                                      tuple[str, str, float, float]]) -> None:
        self._published_winners.update(entries)

    def register_published_winner(self, event: Event) -> None:
        """重放恢复：事件日志中属于已发布窗口的带 trade_id 事件即该笔成交
        的已发布主报，压缩指纹回灌，保证重启后同一笔成交的迟到上报仍按
        合并/冲突判定，不会被当成新成交重新计入。"""
        key = _trade_key(event)
        if key is None:
            return
        self._published_winners.setdefault(
            key, (event.event_id, event.source, event.price, event.quantity))

    # ---------- 查询 ----------
    def is_conflicted(self, key: tuple[str, str]) -> bool:
        return key in self._conflicted

    def winner_of(self, key: tuple[str, str]) -> Event | None:
        return self._winners.get(key)

    def is_published_trade(self, key: tuple[str, str]) -> bool:
        return key in self._published_winners

    def published_winner_of(self, key: tuple[str, str]) \
            -> tuple[str, str, float, float] | None:
        return self._published_winners.get(key)

    def merged_records(self) -> list[MergedTradeRecord]:
        return list(self._records)

    def all_records(self) -> list[MergedTradeRecord]:
        return list(self._records)

    # ---------- 批次快照 / 回滚 ----------
    def snapshot(self):
        return (
            copy.deepcopy(self._winners),
            copy.deepcopy(self._published_winners),
            copy.deepcopy(self._pending),
            copy.deepcopy(self._records),
            set(self._conflicted),
            copy.deepcopy(self._superseded),
        )

    def restore_snapshot(self, snap) -> None:
        (self._winners, self._published_winners, self._pending,
         self._records, self._conflicted, self._superseded) = (
            copy.deepcopy(snap[0]), copy.deepcopy(snap[1]),
            copy.deepcopy(snap[2]), copy.deepcopy(snap[3]),
            set(snap[4]), copy.deepcopy(snap[5]))

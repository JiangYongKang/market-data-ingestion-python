"""跨渠道同一笔成交的合并与冲突隔离。

合并键
======
``(symbol, trade_id)``。只有携带**非空** ``trade_id`` 的事件才参与跨渠道
合并；没有 trade_id 就无法证明两渠道报的是同一笔成交，各自独立计入，
绝不猜测。

取舍规则（确定、与到达顺序无关）
================================
* 每个渠道对同一笔成交至多一个副本（同渠道重投由 event_id/(source,seq)
  幂等层先识别）；
* **获胜副本（winner）**：渠道优先级最高者。优先级取配置
  ``multi_source_symbols[symbol]`` 的声明顺序（index 小者胜）；未声明的
  动态渠道排在所有声明渠道之后，按 source 名字典序决胜。优先级全序，
  不存在并列；
* **一致合并（merged）**：组内所有副本的关键内容 ``(price, quantity)``
  完全一致时，除获胜副本外的其余副本标记为 :data:`RejectReason.MERGED_IDENTICAL`，
  只计一次成交量与价格特征；
* **冲突隔离（conflict）**：只要组内存在任何副本的关键内容对不上，
  除获胜副本外的**所有**次级副本一律隔离，原因
  :data:`RejectReason.CROSS_SOURCE_CONFLICT``，与普通重复、超水位迟到
  在原因码、隔离说明、审计记录三处都可区分。分歧状态只进不退：
  组内副本集合只会增长，已分歧的组不可能重新一致，因此判定结果
  是副本集合的纯函数，乱序/重放逐位可复现。

高优先级副本晚到
================
获胜副本可能在窗口发布前被更高优先级的后到副本替换：旧获胜副本从打开
窗口中移出、新获胜副本计入（内容一致时成交量/特征不变，仅审计替换）；
若因此产生分歧，旧获胜副本及此前已合并的副本一并转入冲突隔离。
窗口发布后判定冻结，任何更晚副本先被各渠道自己的水位判为迟到。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .models import Event


def _key_content(e: Event) -> tuple[float, float]:
    return (e.price, e.quantity)


@dataclass(slots=True)
class _Group:
    symbol: str
    trade_id: str
    #: source -> 当前副本（未发布窗口内保留完整事件）
    copies: dict[str, Event] = field(default_factory=dict)
    #: 窗口发布后压缩：仅保留身份，业务字段不再占用内存
    compact_losers: set[str] | None = None  # loser event_id 集合
    compact_winner_id: str | None = None

    @property
    def compacted(self) -> bool:
        return self.compact_winner_id is not None


@dataclass(frozen=True, slots=True)
class MergeEffect:
    """一个候选副本应用后的组内变化，供服务在提交阶段执行。

    * ``outcome="winner"``：候选成为获胜副本（``winner_before`` 非空表示
      发生了获胜者替换，服务须把旧获胜者移出窗口）；
    * ``outcome="merged"``：候选为一致次级副本，不计数；
    * ``outcome="conflict"``：候选为分歧次级副本，进冲突隔离。

    ``demoted`` 为本次判定中**状态发生变化**的既有副本，kind 取
    ``"merged" | "conflict"``：替换获胜者时旧获胜者与其带动翻转的旧副本；
    分歧首次出现时从 merged 翻转为 conflict 的旧副本。
    """

    candidate: Event
    outcome: str
    winner_before: Event | None
    winner_after: Event
    demoted: tuple[Event, ...] = ()
    demoted_kinds: tuple[str, ...] = ()


class TradeMerger:
    def __init__(self,
                 priority: dict[str, dict[str, int]] | None = None) -> None:
        # symbol -> {source: 声明序 index}
        self._declared: dict[str, dict[str, int]] = priority or {}
        self._groups: dict[tuple[str, str], _Group] = {}

    # ---------- 优先级 ----------
    def _rank(self, symbol: str, source: str) -> tuple[int, object]:
        """全序优先级键：声明渠道 (0, index)，动态渠道 (1, source)。"""
        idx = self._declared.get(symbol, {}).get(source)
        if idx is not None:
            return (0, idx)
        return (1, source)

    def _winner_of(self, g: _Group) -> Event:
        return min(g.copies.values(),
                   key=lambda e: self._rank(g.symbol, e.source))

    def winner_of(self, g: _Group) -> Event:
        """对外暴露当前获胜副本（重放重建窗口时使用）。"""
        return self._winner_of(g)

    def iter_open_groups(self):
        """遍历未压缩（未发布窗口内）的成交组，顺序按合并键确定。"""
        for key in sorted(self._groups):
            g = self._groups[key]
            if not g.compacted:
                yield g

    @staticmethod
    def _all_consistent(g: _Group, winner: Event) -> bool:
        wp, wq = _key_content(winner)
        return all(e.price == wp and e.quantity == wq
                   for e in g.copies.values())

    # ---------- 预检（纯读，不修改状态） ----------
    def preview(self, event: Event) -> MergeEffect | None:
        """返回候选事件的合并效果；无 trade_id 或组已压缩时返回 None。

        返回 None 的两种情形：事件不携带 trade_id（不参与合并）；
        对应成交组已随窗口发布而压缩（由上层按迟到/已发布窗口处理）。
        """
        if not event.trade_id:
            return None
        key = (event.symbol, event.trade_id)
        g = self._groups.get(key)
        if g is not None and g.compacted:
            return None
        # 既有获胜者与既有次级副本（用于状态翻转 diff）
        if g is None:
            return MergeEffect(
                candidate=event, outcome="winner", winner_before=None,
                winner_after=event)
        before_winner = self._winner_of(g)
        before_consistent = self._all_consistent(g, before_winner)
        before_losers = [e for src, e in g.copies.items()
                         if e.event_id != before_winner.event_id]

        copies = dict(g.copies)
        copies[event.source] = event
        after_winner = min(copies.values(),
                           key=lambda e: self._rank(event.symbol, e.source))
        after_consistent = True
        wp, wq = _key_content(after_winner)
        for e in copies.values():
            if e.price != wp or e.quantity != wq:
                after_consistent = False
                break

        demoted: list[Event] = []
        kinds: list[str] = []
        if after_winner.event_id == event.event_id:
            outcome = "winner"
            wb = None if before_winner.event_id == event.event_id \
                else before_winner
            if wb is not None:
                # 获胜者替换：旧获胜者必为次级；分歧时连同旧次级一起翻转
                demoted.append(wb)
                kinds.append("merged" if after_consistent else "conflict")
                if not after_consistent and before_consistent:
                    for e in before_losers:
                        demoted.append(e)
                        kinds.append("conflict")
        else:
            wb = before_winner
            outcome = "merged" if after_consistent else "conflict"
            # 候选未夺魁；若组此前一致、现在因候选而分歧，旧次级也翻转
            if not after_consistent and before_consistent:
                for e in before_losers:
                    demoted.append(e)
                    kinds.append("conflict")
        return MergeEffect(
            candidate=event, outcome=outcome, winner_before=wb,
            winner_after=after_winner,
            demoted=tuple(demoted), demoted_kinds=tuple(kinds),
        )

    # ---------- 提交 ----------
    def apply(self, effect: MergeEffect) -> None:
        """在服务提交阶段把效果落进合并组状态。"""
        e = effect.candidate
        g = self._groups.setdefault(
            (e.symbol, e.trade_id), _Group(e.symbol, e.trade_id))
        g.copies[e.source] = e

    # ---------- 恢复 ----------
    def restore_copy(self, event: Event) -> None:
        """重放恢复：直接登记一个副本（获胜关系由优先级重算）。"""
        if not event.trade_id:
            return
        g = self._groups.setdefault(
            (event.symbol, event.trade_id), _Group(event.symbol, event.trade_id))
        if g.compacted:
            g.compact_losers = None
            g.compact_winner_id = None
        g.copies[event.source] = event

    # ---------- 发布后压缩 ----------
    def mark_window_published(self, symbol: str, window_start_ms: int,
                              window_end_ms: int) -> int:
        """窗口发布后压缩其中成交组：仅保留身份集合，返回压缩组数。"""
        n = 0
        for (sym, tid), g in self._groups.items():
            if sym != symbol or g.compacted:
                continue
            winner = self._winner_of(g)
            if not (window_start_ms <= winner.event_time_ms < window_end_ms):
                continue
            loser_ids = {e.event_id for e in g.copies.values()
                         if e.event_id != winner.event_id}
            g.compact_winner_id = winner.event_id
            g.compact_losers = loser_ids
            g.copies = {}
            n += 1
        return n

    # ---------- 观测 ----------
    def group_count(self) -> int:
        return len(self._groups)

    def full_fingerprint_groups(self) -> int:
        """仍保留完整副本（处于未发布窗口）的成交组数。"""
        return sum(1 for g in self._groups.values() if not g.compacted)

    def copy_count(self) -> int:
        return sum(len(g.copies) for g in self._groups.values()
                   if not g.compacted)

    def groups_snapshot(self) -> dict[tuple[str, str], dict]:
        out: dict[tuple[str, str], dict] = {}
        for key, g in self._groups.items():
            if g.compacted:
                out[key] = {
                    "compacted": True,
                    "winner_event_id": g.compact_winner_id,
                    "loser_event_ids": sorted(g.compact_losers or ()),
                }
            else:
                w = self._winner_of(g)
                consistent = self._all_consistent(g, w)
                out[key] = {
                    "compacted": False,
                    "winner_source": w.source,
                    "winner_event_id": w.event_id,
                    "copies": {s: e.event_id for s, e in g.copies.items()},
                    "consistent": consistent,
                }
        return out

    def __len__(self) -> int:
        return len(self._groups)

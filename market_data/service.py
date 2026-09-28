"""行情接入服务门面。

职责与不变式
============
* **原子批次**：一批事件先完成全部解析与判定规划（不改状态），
  随后在同一临界区内提交。提交过程中任何异常都按内存快照回滚，
  绝不残留部分聚合或部分位点；
* **并发一致**：按标的加 ``asyncio`` 锁（多标的按名称排序加锁防死锁），
  临界区内以 ``threading.RLock`` 保护共享结构，保证同一标的的
  写入/重放/查询互斥、无部分可见、无重复计数、无位点跳变；
* **幂等**：event_id 与 (source,seq) 双键去重；一致重复忽略，
  内容冲突以 ``duplicate_conflict`` 拒绝，与 ``duplicate_identical`` 区分；
* **乱序/水位**：窗口内迟到修正未发布结果；超水位或命中已发布窗口
  的事件进隔离区并标注原因与水位，绝不静默丢弃；
* **位点单调**：每来源只在提交成功后推进；回退请求明确拒绝；
* **背压**：积压达 ``max_pending_events`` 时按 delay/reject 策略处理，
  隔离区也有容量上限，绝不无界占用内存；
* **可复现**：事件落仅追加日志，启动时重放重建；聚合与顺序无关，
  重复消费/重启后已发布结果逐位一致。
* **多来源**：声明为多来源的标的按 (symbol, source) 各维护独立
  时间线（渠道间互不误判迟到），窗口按最慢渠道水位关闭；同 trade_id
  的跨渠道副本按确定优先级只计一次，价/量对不上以
  cross_source_conflict 隔离并在 merges.jsonl 留痕；合并组与渠道
  状态均可由日志纯函数重建，渠道数有硬上限，组内存在界。
"""
from __future__ import annotations

import asyncio
import copy
import logging
import threading
import time

from .checkpoints import CheckpointStore
from .channels import ChannelRegistry
from .config import Config
from .dedup import Deduplicator
from .errors import BackpressureError, IngestionError
from .mergelog import MergeRecordStore
from .merge import TradeMerger
from .metrics import Metrics
from .models import IngestResult, MergeRecord, RejectReason
from .quarantine import Quarantine
from .schema import parse_event_safe
from .storage import EventLog, PublishedWindowStore, QuarantineStore
from .watermark import WatermarkManager
from .windows import WindowAggregator

logger = logging.getLogger("market_data")


def decision_logger() -> logging.Logger:
    """供测试挂接的判定日志器，消息含事件标识/事件时间/水位/判定依据。"""
    return logger


class MarketDataService:
    def __init__(self, config: Config | None = None) -> None:
        self.config = config or Config()
        self.metrics = Metrics()
        self._dedup = Deduplicator()
        self._watermark = WatermarkManager(self.config.allowed_lateness_ms)
        self._windows = WindowAggregator(self.config.window_size_ms)
        self._quarantine = Quarantine(self.config.max_quarantine_size)
        priority = {sym: {src: i for i, src in enumerate(srcs)}
                    for sym, srcs in self.config.multi_source_symbols.items()}
        self._channels = ChannelRegistry(
            self.config.multi_source_symbols,
            max_channels_per_symbol=self.config.max_channels_per_symbol,
            channel_idle_timeout_ms=self.config.channel_idle_timeout_ms)
        self._merger = TradeMerger(priority)
        self._log = EventLog(self.config.data_dir, fsync=self.config.fsync)
        self._published_store = PublishedWindowStore(self.config.data_dir)
        self._quarantine_store = QuarantineStore(self.config.data_dir)
        self._merge_store = MergeRecordStore(self.config.data_dir)
        self._checkpoints = CheckpointStore(self.config.data_dir)
        self._lock = threading.RLock()
        self._symbol_locks: dict[str, asyncio.Lock] = {}
        self._symbol_locks_guard = threading.Lock()
        self._closed = False
        self._recover()

    # ---------- 恢复 ----------
    def _recover(self) -> None:
        """从仅追加日志重放重建内存状态。

        真相划分：
        * 已发布窗口特征以 ``published_windows.jsonl`` 为唯一真相，
          直接回灌（不可变），事件日志中属于这些窗口的事件只占去重表；
        * 其余已落盘事件属于"已接纳但崩溃于阶段二"的批次，重放后按
          重建水位重新发布——事件集合与崩溃前一致，特征逐位相同，
          不重复（event_id 去重）也不缺失。

        多来源恢复
        ----------
        多来源标的按 (symbol, source) 各自重建渠道时间线，标的水位取各
        渠道最小值；跨渠道合并组由事件日志（获胜/已接纳副本）与
        ``merges.jsonl``（被合并/冲突隔离副本的完整负载）中的副本集合
        加确定性取舍规则重建，合并结果是副本集合的纯函数。
        """
        now_ms = time.time_ns() // 1_000_000
        already = self._published_store.load_all()
        self._windows.restore_published(already)
        # 隔离记录回灌：事件身份进入去重表（重复投递按幂等/冲突判定），
        # 记录本身保持可查询且不参与聚合。
        quarantined = self._quarantine_store.load_all()
        for q in quarantined:
            self._dedup.check(q.event)
        self._quarantine.restore(quarantined)
        # 合并审计记录中的失败副本将与事件日志一起重放（身份进入去重表，
        # 重投按幂等重复处理，绝不二次计数），无需单独占位。
        events = self._log.replay_all()
        loser_events = self._merge_store.load_loser_events()

        ordered = sorted(events + loser_events,
                         key=lambda e: (e.event_time_ms, e.event_id))
        multi_symbols = set(self.config.multi_source_symbols)
        # 第一遍：重建渠道时间线与跨渠道合并组（全部终态副本参与）。
        for ev in ordered:
            if ev.symbol not in multi_symbols:
                continue
            w0 = self._windows.window_start_for(ev.event_time_ms)
            if self._windows.is_published(ev.symbol, w0):
                continue
            self._channels.observe(ev.symbol, ev.source,
                                   ev.event_time_ms, now_ms)
            self._merger.restore_copy(ev)
        # 第二遍：身份占去重表；单来源重建全局水位；多来源只有合并组
        # 获胜副本进入聚合窗口（被合并/冲突副本绝不计数）。
        for ev in ordered:
            self._dedup.check(ev)
            if ev.symbol in multi_symbols:
                continue
            self._watermark.observe(ev.event_time_ms)
            w0 = self._windows.window_start_for(ev.event_time_ms)
            if self._windows.is_published(ev.symbol, w0):
                continue
            self._windows.add(ev)
        for g in self._merger.iter_open_groups():
            winner = self._merger.winner_of(g)
            w0 = self._windows.window_start_for(winner.event_time_ms)
            if not self._windows.is_published(winner.symbol, w0):
                self._windows.add(winner)
        # 发布：单来源标的按全局水位；多来源标的按各自最慢渠道水位。
        republished = self._windows.publish_due(self._watermark.watermark_ms)
        for sym in multi_symbols:
            swm = self._channels.symbol_watermark_ms(
                sym, self.config.allowed_lateness_ms)
            if swm is not None:
                republished += self._windows.publish_due(swm, symbol=sym)
        if republished:
            self._published_store.append_many(republished)
        for f in republished:
            self._merger.mark_window_published(
                f.symbol, f.window_start_ms, f.window_end_ms)
        # 恢复后压缩已发布事件的内容指纹（身份键保留）。
        for _symbol, _w0, feat in self._windows.all_published_pairs():
            self._dedup.mark_published(feat.event_ids)
        self._refresh_pending()

    def _symbol_lock(self, symbol: str) -> asyncio.Lock:
        with self._symbol_locks_guard:
            lk = self._symbol_locks.get(symbol)
            if lk is None:
                lk = asyncio.Lock()
                self._symbol_locks[symbol] = lk
            return lk

    # ---------- 三阶段批次处理 ----------
    def _ingest_batch(self, raw_events: list[dict]) -> IngestResult:
        """在已持有临界区锁的前提下处理一批事件。

        阶段一（只读预检）：解析全部事件、批次内/全局去重查询、按本批
        推进后的水位模拟迟到判定、隔离区与积压容量检查。**任何硬失败
        （结构错误、内容冲突、容量不足、隔离区满）立即中止，整批无
        任何副作用**；幂等重复与正常隔离不属于失败。
        阶段二（内存提交）：登记去重、推进水位、并入窗口、暂存隔离、
        发布到点窗口；异常时整体快照回滚。
        阶段三（持久化）：事件按标的批量原子追加 -> 发布特征追加 ->
        位点单调推进。崩溃后由重放 + 去重收敛，不重复不缺失。
        """
        # ===== 阶段一：预检（纯读） =====
        parsed: list = []
        hard_failures: list[tuple[str, RejectReason, str]] = []
        deprecated_total = 0
        for raw in raw_events:
            event, rej, deprecated = parse_event_safe(raw, self.config)
            deprecated_total += deprecated
            if rej is not None:
                hard_failures.append((rej[0] or "<unknown>", rej[1], rej[2]))
            else:
                parsed.append(event)
        # 批次内身份预检
        batch_fp: dict[str, object] = {}
        batch_seq: dict[tuple[str, int], str] = {}
        for ev in parsed:
            prev = batch_fp.get(ev.event_id)
            if prev is not None:
                if prev != ev:
                    hard_failures.append((ev.event_id, RejectReason.DUPLICATE_CONFLICT,
                        f"批次内 event_id={ev.event_id!r} 内容冲突"))
            else:
                batch_fp[ev.event_id] = ev
            other = batch_seq.get((ev.source, ev.seq))
            if other is not None and other != ev.event_id:
                hard_failures.append((ev.event_id, RejectReason.DUPLICATE_CONFLICT,
                    f"批次内 ({ev.source!r},{ev.seq}) 绑定了不同 event_id"))
            else:
                batch_seq[(ev.source, ev.seq)] = ev.event_id
        # 全局去重查询（inspect 不修改表）；确定每个事件 new/duplicate。
        # 批次内第二次及以上出现的同 event_id 事件按上一条的判定处理。
        verdict_of: dict[int, str] = {}
        seen_eid_verdict: dict[str, str] = {}
        for ev in parsed:
            prior = seen_eid_verdict.get(ev.event_id)
            if prior is not None:
                verdict_of[id(ev)] = "duplicate"  # 内容不一致已在上面记硬失败
                continue
            try:
                verdict = self._dedup.inspect(ev)
            except IngestionError as exc:
                hard_failures.append((ev.event_id, exc.reason, str(exc)))
                verdict = "new"
            verdict_of[id(ev)] = verdict
            seen_eid_verdict[ev.event_id] = verdict
        if hard_failures:
            for eid, reason, detail in sorted(hard_failures, key=lambda x: (str(x[0]), x[1].value)):
                self.metrics.rejected += 1
                if reason is RejectReason.DUPLICATE_CONFLICT:
                    self.metrics.conflicts += 1
                self._log_decision(eid, None, self._watermark.watermark_ms,
                                   "batch_rejected", reason, detail)
            first = hard_failures[0]
            raise IngestionError(first[1],
                f"批次被拒绝（{len(hard_failures)} 处硬失败），整批不生效；"
                f"首项 [{first[0]}] {first[1].value}: {first[2]}",
                event_id=first[0])

        # ===== 阶段一b：多来源渠道容量预检（纯读，硬失败整批拒绝） =====
        multi_symbols = set(self.config.multi_source_symbols)
        for ev in parsed:
            if ev.symbol in multi_symbols and verdict_of[id(ev)] != "duplicate":
                try:
                    self._channels.register(ev.symbol, ev.source,
                                            time.time_ns() // 1_000_000)
                except IngestionError as exc:
                    hard_failures.append((ev.event_id, exc.reason, str(exc)))
        if hard_failures:
            for eid, reason, detail in sorted(
                    hard_failures, key=lambda x: (str(x[0]), x[1].value)):
                self.metrics.rejected += 1
                if reason is RejectReason.DUPLICATE_CONFLICT:
                    self.metrics.conflicts += 1
                if reason is RejectReason.CHANNEL_LIMIT_EXCEEDED:
                    self.metrics.backpressure_rejected += 1
                    self.metrics.channel_rejected += 1
                self._log_decision(eid, None, self._watermark.watermark_ms,
                                   "batch_rejected", reason, detail)
            first = hard_failures[0]
            raise IngestionError(first[1],
                f"批次被拒绝（{len(hard_failures)} 处硬失败），整批不生效；"
                f"首项 [{first[0]}] {first[1].value}: {first[2]}",
                event_id=first[0])

        # 迟到分类 + 多来源合并规划。按事件时间升序逐条判定，事件 i 看到
        # 的水位只由已处理的事件 0..i-1 推进——与逐条实时到达等价。
        # * 单来源标的：全局水位 max(全部已见事件时间)-allowed_lateness；
        # * 多来源标的：只看事件所属渠道的运行中水位（渠道间互不影响）；
        # 重复事件不推进任何水位。
        ordered = sorted(parsed, key=lambda e: (e.event_time_ms, e.event_id))
        running_max = self._watermark.max_event_time_ms
        # 批次内各渠道运行中最大事件时间（初值为渠道当前推进位置），
        # 供本批逐条迟到判定使用。
        tentative_channels: dict[tuple[str, str], int] = {
            (st.symbol, st.source): st.max_event_time_ms
            for st in self._channels.channels()
            if st.max_event_time_ms is not None
        }
        accepted: list = []          # 进入窗口的获胜副本/无 trade_id 事件
        planned: list = []           # (event, kind, effect, wm_here) 提交计划
        quarantined_items: list = []  # 超水位/已发布窗口 (event, reason, wm, detail)
        conflict_items: list = []    # 跨渠道冲突 (event, effect, wm, detail)
        duplicates = 0
        merged_count = 0
        details: list[tuple[str, RejectReason, str]] = []
        q_seats_planned = 0  # 已在预检中预占的隔离席位（含冲突候选）
        # 批次内临时合并视图：按事件时间升序处理时，把本批已规划的副本
        # 先并入临时状态，使同一批次中多渠道副本（无论到达顺序）的判定
        # 与逐条实时到达等价；阶段一不改真实合并状态（原子性）。
        tentative = copy.deepcopy(self._merger)

        def _quarantine_seat() -> None:
            nonlocal q_seats_planned
            available = (self._quarantine._max - self._quarantine.size
                         - q_seats_planned)
            if available <= 0:
                self.metrics.backpressure_rejected += 1
                raise BackpressureError(
                    RejectReason.BACKPRESSURE_REJECTED,
                    f"隔离区已满（{self.config.max_quarantine_size}），整批拒绝")
            q_seats_planned += 1

        for ev in ordered:
            v = verdict_of[id(ev)]
            is_multi = ev.symbol in multi_symbols
            if is_multi:
                cur = tentative_channels.get((ev.symbol, ev.source))
                wm_here = (-(1 << 62)) if cur is None else \
                    cur - self.config.allowed_lateness_ms
            else:
                wm_here = (-(1 << 62)) if running_max is None else \
                    running_max - self.config.allowed_lateness_ms
            if v == "duplicate":
                duplicates += 1
                details.append((ev.event_id, RejectReason.DUPLICATE_IDENTICAL,
                                "与已接纳事件完全一致，幂等忽略"))
                self._log_decision(ev.event_id, ev.event_time_ms, wm_here,
                                   "duplicate", RejectReason.DUPLICATE_IDENTICAL,
                                   "identical redelivery")
                continue
            w0 = self._windows.window_start_for(ev.event_time_ms)
            late_by_wm = ev.event_time_ms <= wm_here
            published_hit = self._windows.is_published(ev.symbol, w0)
            # 合并组判定走批次内临时视图；组已随窗口发布压缩 -> 判定冻结
            fx = tentative.preview(ev) if is_multi else None
            group_frozen = (is_multi and bool(ev.trade_id) and fx is None
                            and (ev.symbol, ev.trade_id)
                            in tentative.groups_snapshot())
            if late_by_wm or published_hit or group_frozen:
                _quarantine_seat()
                if group_frozen:
                    detail = (f"成交 {ev.trade_id!r} 所属窗口 [{w0},"
                              f"{w0+self.config.window_size_ms}) 已发布，"
                              f"多来源判定冻结，事件时间 {ev.event_time_ms}")
                    reason = RejectReason.LATE_BEYOND_WATERMARK
                elif late_by_wm:
                    detail = (f"event_time_ms={ev.event_time_ms} <= "
                              f"watermark_ms={wm_here}，超过允许迟到窗口 "
                              f"{self.config.allowed_lateness_ms}ms"
                              + (f"（渠道 {ev.source!r} 独立水位）" if is_multi else ""))
                    reason = RejectReason.LATE_BEYOND_WATERMARK
                else:
                    detail = (f"窗口 [{w0},{w0+self.config.window_size_ms}) 已发布"
                              f"且不可变，事件时间 {ev.event_time_ms}")
                    reason = RejectReason.LATE_BEYOND_WATERMARK
                quarantined_items.append((ev, reason, wm_here, detail))
                details.append((ev.event_id, reason, detail))
                self._log_decision(ev.event_id, ev.event_time_ms, wm_here,
                                   "quarantined", reason, detail)
                continue

            # 非迟到：推进所属时间线（含被合并/冲突副本——它们是渠道终态）。
            if is_multi:
                key = (ev.symbol, ev.source)
                tentative_channels[key] = ev.event_time_ms \
                    if key not in tentative_channels \
                    else max(tentative_channels[key], ev.event_time_ms)
            else:
                running_max = ev.event_time_ms if running_max is None \
                    else max(running_max, ev.event_time_ms)

            if fx is None:
                # 无 trade_id（或单来源）：独立成交，直接计入窗口
                accepted.append(ev)
                planned.append((ev, "standalone", fx, wm_here))
            elif fx.outcome == "winner":
                accepted.append(ev)
                planned.append((ev, "winner", fx, wm_here))
                tentative.apply(fx)
            elif fx.outcome == "merged":
                merged_count += 1
                planned.append((ev, "merged", fx, wm_here))
                tentative.apply(fx)
                detail = (f"成交 {ev.trade_id!r} 已由渠道 "
                          f"{fx.winner_after.source!r} 的副本 "
                          f"{fx.winner_after.event_id!r} 计入，渠道 "
                          f"{ev.source!r} 副本 {ev.event_id!r} 关键内容一致，"
                          f"合并不重复计数")
                details.append((ev.event_id, RejectReason.MERGED_IDENTICAL, detail))
                self._log_decision(ev.event_id, ev.event_time_ms, wm_here,
                                   "merged", RejectReason.MERGED_IDENTICAL, detail)
            else:  # conflict：次级副本内容对不上 -> 单独隔离
                _quarantine_seat()
                detail = (f"成交 {ev.trade_id!r} 跨渠道关键内容不一致："
                          f"渠道 {ev.source!r} 副本 {ev.event_id!r} "
                          f"price={ev.price},quantity={ev.quantity} 与获胜渠道 "
                          f"{fx.winner_after.source!r} 副本 "
                          f"{fx.winner_after.event_id!r} "
                          f"price={fx.winner_after.price},"
                          f"quantity={fx.winner_after.quantity} 对不上；"
                          f"按取舍规则以获胜渠道为准，本副本隔离不计数")
                conflict_items.append((ev, fx, wm_here, detail))
                planned.append((ev, "conflict", fx, wm_here))
                tentative.apply(fx)
                details.append((ev.event_id, RejectReason.CROSS_SOURCE_CONFLICT,
                                detail))
                self._log_decision(ev.event_id, ev.event_time_ms, wm_here,
                                   "conflict_quarantined",
                                   RejectReason.CROSS_SOURCE_CONFLICT, detail)
        # 积压容量预估：窗口新增获胜/独立成交占窗口席位；
        # 冲突副本占隔离席位（含本批预占的迟到席位）。
        pending_after = (self._windows.pending_event_count()
                         + self._quarantine.size
                         + q_seats_planned + len(accepted))
        if pending_after > self.config.max_pending_events:
            self.metrics.backpressure_rejected += 1
            raise BackpressureError(
                RejectReason.BACKPRESSURE_REJECTED,
                f"提交后预估积压 {pending_after} 超过上限 "
                f"{self.config.max_pending_events}，整批拒绝")

        # ===== 阶段二：内存提交（快照回滚保护） =====
        snapshot = (copy.deepcopy(self._dedup.__dict__),
                    copy.deepcopy(self._windows.__dict__),
                    copy.deepcopy(self._quarantine.__dict__),
                    copy.deepcopy(self._merger.__dict__),
                    copy.deepcopy(self._channels.__dict__),
                    self._watermark.max_event_time_ms)
        now_ms = time.time_ns() // 1_000_000
        max_seq_by_source: dict[str, int] = {}
        merge_records: list[MergeRecord] = []

        def _advance_seq(ev) -> None:
            if ev.seq > max_seq_by_source.get(ev.source, -1):
                max_seq_by_source[ev.source] = ev.seq

        try:
            for ev, kind, _fx_pre, wm_here in planned:
                # 提交时对真实合并状态重新做纯查询：批次内判定基于批次
                # 开始时的临时快照（含本批已规划副本），而提交必须看到
                # 此前批次已提交的获胜者（跨批次替换据此识别旧获胜者）。
                fx = self._merger.preview(ev) if kind in (
                    "winner", "merged", "conflict") else None
                if kind in ("standalone", "winner"):
                    self._dedup.check(ev)
                    if ev.symbol in multi_symbols:
                        self._channels.observe(
                            ev.symbol, ev.source, ev.event_time_ms, now_ms)
                    else:
                        self._watermark.observe(ev.event_time_ms)
                    self._windows.add(ev)
                    w0 = self._windows.window_start_for(ev.event_time_ms)
                    self._log_decision(ev.event_id, ev.event_time_ms, wm_here,
                                       "accepted", None,
                                       f"window_start={w0} source={ev.source}")
                    _advance_seq(ev)
                if kind == "winner":
                    # 高优先级副本夺魁：旧获胜副本移出未发布窗口。
                    # preview 已把旧获胜者列入 demoted（一致=merged，
                    # 分歧=conflict），下面按 kind 统一处理并写审计。
                    demoted_ids = {d.event_id for d in fx.demoted}
                    if (fx.winner_before is not None
                            and fx.winner_before.event_id not in demoted_ids):
                        # 防御路径：未被 demoted 覆盖的旧获胜者按一致合并处理
                        self._windows.remove(ev.symbol,
                                             fx.winner_before.event_id)
                        merge_records.append(MergeRecord(
                            symbol=fx.winner_before.symbol,
                            trade_id=fx.winner_before.trade_id or ev.trade_id or "",
                            kind="merged", winner=ev.source,
                            loser=fx.winner_before.source,
                            winner_event_id=ev.event_id,
                            loser_event_id=fx.winner_before.event_id,
                            decided_at_ms=now_ms,
                            loser_event_payload=fx.winner_before))
                    # 旧获胜者及其带动翻转的旧副本
                    for old, old_kind in zip(fx.demoted, fx.demoted_kinds):
                        self._windows.remove(old.symbol, old.event_id)
                        if old_kind == "conflict":
                            detail = (f"成交 {old.trade_id!r} 在渠道 "
                                      f"{ev.source!r} 副本 {ev.event_id!r} 到达后"
                                      f"组内出现关键内容分歧，原副本 "
                                      f"{old.event_id!r}（渠道 {old.source!r}，"
                                      f"price={old.price},quantity={old.quantity}）"
                                      f"转为跨渠道冲突隔离，以渠道 {ev.source!r} "
                                      f"获胜副本为准")
                            conflict_items.append((old, None, wm_here, detail))
                            details.append((old.event_id,
                                            RejectReason.CROSS_SOURCE_CONFLICT,
                                            detail))
                            self._log_decision(old.event_id, old.event_time_ms,
                                               wm_here, "conflict_demoted",
                                               RejectReason.CROSS_SOURCE_CONFLICT,
                                               detail)
                        merge_records.append(MergeRecord(
                            symbol=old.symbol,
                            trade_id=old.trade_id or ev.trade_id or "",
                            kind=old_kind, winner=ev.source, loser=old.source,
                            winner_event_id=ev.event_id,
                            loser_event_id=old.event_id,
                            decided_at_ms=now_ms,
                            loser_event_payload=old))
                    self._merger.apply(fx)
                elif kind == "merged":
                    self._merger.apply(fx)
                    self._dedup.check(ev)  # 身份占去重表，重投按幂等重复
                    self._channels.observe(
                        ev.symbol, ev.source, ev.event_time_ms, now_ms)
                    _advance_seq(ev)
                    merge_records.append(MergeRecord(
                        symbol=ev.symbol, trade_id=ev.trade_id, kind="merged",
                        winner=fx.winner_after.source, loser=ev.source,
                        winner_event_id=fx.winner_after.event_id,
                        loser_event_id=ev.event_id, decided_at_ms=now_ms,
                        loser_event_payload=ev))
                elif kind == "conflict":
                    self._merger.apply(fx)
                    self._channels.observe(
                        ev.symbol, ev.source, ev.event_time_ms, now_ms)
                    _advance_seq(ev)
                    merge_records.append(MergeRecord(
                        symbol=ev.symbol, trade_id=ev.trade_id, kind="conflict",
                        winner=fx.winner_after.source, loser=ev.source,
                        winner_event_id=fx.winner_after.event_id,
                        loser_event_id=ev.event_id, decided_at_ms=now_ms,
                        loser_event_payload=ev))

            # 被隔离事件（超水位迟到 / 已发布窗口）：去重登记，
            # 多来源渠道时间线不推进——迟到不代表渠道真实进度。
            for ev, reason, wm, detail in quarantined_items:
                self._dedup.check(ev)
                _advance_seq(ev)
            # 冲突副本：终态处理，身份占去重表，渠道时间线推进。
            for ev, _fx, wm, detail in conflict_items:
                self._dedup.check(ev)
                if ev.symbol in multi_symbols:
                    self._channels.observe(
                        ev.symbol, ev.source, ev.event_time_ms, now_ms)
                _advance_seq(ev)

            # 发布：单来源按全局水位；多来源按各标的最慢渠道水位
            published = self._windows.publish_due(self._watermark.watermark_ms)
            for sym in multi_symbols:
                swm = self._channels.symbol_watermark_ms(
                    sym, self.config.allowed_lateness_ms)
                if swm is not None:
                    published += self._windows.publish_due(swm, symbol=sym)
            for f in published:
                if f.symbol in multi_symbols:
                    self._merger.mark_window_published(
                        f.symbol, f.window_start_ms, f.window_end_ms)
        except BaseException:
            self._dedup.__dict__ = copy.deepcopy(snapshot[0])
            self._windows.__dict__ = copy.deepcopy(snapshot[1])
            self._quarantine.__dict__ = copy.deepcopy(snapshot[2])
            self._merger.__dict__ = copy.deepcopy(snapshot[3])
            self._channels.__dict__ = copy.deepcopy(snapshot[4])
            self._watermark._max_event_time = snapshot[5]
            raise

        # ===== 阶段三：持久化副作用 =====
        from .models import QuarantinedEvent
        all_quarantined = quarantined_items + [
            (ev, RejectReason.CROSS_SOURCE_CONFLICT, wm, detail)
            for ev, _fx, wm, detail in conflict_items]
        q_records = [
            QuarantinedEvent(event=ev, reason=reason,
                             watermark_ms=wm, detail=detail,
                             accepted_at_ms=now_ms)
            for ev, reason, wm, detail in all_quarantined]
        self._log.append_batch(accepted)
        self._published_store.append_many(published)
        self._quarantine_store.append_batch(q_records)
        self._merge_store.append_batch(merge_records)
        # 已发布窗口的事件身份压缩指纹：去重能力保留，内存不无界增长。
        for f in published:
            self._dedup.mark_published(f.event_ids)
        for rec in q_records:
            self._quarantine.add(rec.event, rec.reason, rec.watermark_ms,
                                 rec.detail, rec.accepted_at_ms)
        for source, seq in max_seq_by_source.items():
            self._checkpoints.advance(source, seq)

        self.metrics.deprecated_seen += deprecated_total
        self.metrics.accepted += len(accepted)
        self.metrics.duplicates += duplicates
        self.metrics.merged += merged_count
        self.metrics.cross_source_conflicts += sum(
            1 for _ev, reason, _w, _d in all_quarantined
            if reason is RejectReason.CROSS_SOURCE_CONFLICT)
        self.metrics.quarantined += len(q_records)
        self.metrics.published_windows += len(published)
        self._refresh_pending()
        return IngestResult(
            accepted=len(accepted), duplicate=duplicates,
            quarantined=len(q_records), rejected=0,
            merged=merged_count,
            details=tuple(details),
        )

    # ---------- 对外入口 ----------
    async def ingest(self, raw_events: list[dict] | dict, *, replay: bool = False) -> IngestResult:
        if self._closed:
            raise RuntimeError("服务已关闭")
        if isinstance(raw_events, dict):
            raw_events = [raw_events]
        # 背压：在获取标的锁之前按当前积压决策（delay 策略异步等待，不阻塞事件循环）
        await self._await_capacity(len(raw_events))
        symbols = sorted({
            r.get("symbol", "?") for r in raw_events if isinstance(r, dict)
        })
        locks = [self._symbol_lock(s) for s in symbols]
        for lk in locks:
            await lk.acquire()
        start = time.perf_counter_ns()
        try:
            result = self._ingest_batch(raw_events)
        finally:
            for lk in reversed(locks):
                lk.release()
        elapsed = time.perf_counter_ns() - start
        self.metrics.record_batch(len(raw_events), elapsed)
        return result

    async def _await_capacity(self, incoming: int) -> None:
        strategy = self.config.backpressure_strategy
        retries = 0
        while True:
            pending = self._windows.pending_event_count() + self._quarantine.size
            if pending + incoming <= self.config.max_pending_events:
                return
            if strategy == "reject" or retries >= self.config.backpressure_max_retries:
                self.metrics.backpressure_rejected += 1
                raise BackpressureError(
                    RejectReason.BACKPRESSURE_REJECTED,
                    f"积压 {pending} + 本批 {incoming} 超过上限 "
                    f"{self.config.max_pending_events}（策略 {strategy}）")
            retries += 1
            await asyncio.sleep(self.config.backpressure_delay_ms / 1000.0)

    def ingest_sync(self, raw_events: list[dict] | dict, *, replay: bool = False) -> IngestResult:
        """同步/测试入口：语义与 :meth:`ingest` 相同（delay 退化为短时自旋）。"""
        if self._closed:
            raise RuntimeError("服务已关闭")
        if isinstance(raw_events, dict):
            raw_events = [raw_events]
        self._await_capacity_sync(len(raw_events))
        symbols = sorted({
            r.get("symbol", "?") for r in raw_events if isinstance(r, dict)
        })
        with self._lock:
            start = time.perf_counter_ns()
            result = self._ingest_batch(raw_events)
            elapsed = time.perf_counter_ns() - start
        self.metrics.record_batch(len(raw_events), elapsed)
        return result

    def _await_capacity_sync(self, incoming: int) -> None:
        retries = 0
        while True:
            pending = self._windows.pending_event_count() + self._quarantine.size
            if pending + incoming <= self.config.max_pending_events:
                return
            if (self.config.backpressure_strategy == "reject"
                    or retries >= self.config.backpressure_max_retries):
                self.metrics.backpressure_rejected += 1
                raise BackpressureError(
                    RejectReason.BACKPRESSURE_REJECTED,
                    f"积压 {pending} + 本批 {incoming} 超过上限 "
                    f"{self.config.max_pending_events}")
            retries += 1
            time.sleep(self.config.backpressure_delay_ms / 1000.0)

    # ---------- 查询 / 位点 / 观测 ----------
    def query(self, symbol: str, window_start_ms: int | None = None) -> list:
        with self._lock:
            return self._windows.get_published(symbol, window_start_ms)

    def query_provisional(self, symbol: str, window_start_ms: int):
        with self._lock:
            return self._windows.provisional(symbol, window_start_ms)

    def quarantine_list(self, symbol: str | None = None) -> list:
        with self._lock:
            return self._quarantine.list(symbol)

    def checkpoint(self, source: str, seq: int) -> bool:
        """显式位点推进/回退检查（回退抛 CheckpointRollbackError）。"""
        with self._lock:
            return self._checkpoints.advance(source, seq)

    def checkpoint_get(self, source: str) -> int:
        return self._checkpoints.get(source)

    def checkpoints(self) -> dict[str, int]:
        return self._checkpoints.all()

    def channel_snapshot(self) -> dict:
        """多来源渠道观测快照：各渠道推进到哪、落后多少、是否卡住。"""
        now_ms = time.time_ns() // 1_000_000
        return self._channels.snapshot(now_ms, self.config.allowed_lateness_ms)

    def merge_records(self) -> list:
        """跨渠道合并/冲突审计记录（重启后仍可查询）。"""
        return self._merge_store.load_all()

    @property
    def watermark_ms(self) -> int:
        return self._watermark.watermark_ms

    def metrics_snapshot(self) -> dict:
        with self._lock:
            self._refresh_pending()
            snap = self.metrics.snapshot()
            snap["watermark_ms"] = self._watermark.watermark_ms
            snap["quarantine_size"] = self._quarantine.size
            snap["dedup_size"] = len(self._dedup)
            now_ms = time.time_ns() // 1_000_000
            snap["channels"] = self._channels.snapshot(
                now_ms, self.config.allowed_lateness_ms)
            snap["merge_groups"] = self._merger.group_count()
            snap["merge_groups_open"] = self._merger.full_fingerprint_groups()
            return snap

    def close(self) -> None:
        with self._lock:
            self._closed = True

    # ---------- 内部工具 ----------
    def _refresh_pending(self) -> None:
        pending = self._windows.pending_event_count()
        self.metrics.pending_events = pending
        if pending > self.metrics.max_pending_events:
            self.metrics.max_pending_events = pending
        # 保守内存估计：完整指纹的打开窗口事件 ~200B；压缩后的历史身份 ~120B
        compact = len(self._dedup) - self._dedup.full_fingerprint_count
        # 多来源：未发布窗口内保留完整副本 ~200B/副本，压缩后的成交组 ~64B/组
        est = self._dedup.full_fingerprint_count * 200 + compact * 120 \
            + pending * 200 \
            + self._merger.copy_count() * 200 \
            + self._merger.group_count() * 64
        self.metrics.estimated_state_bytes = est

    def _log_decision(self, eid, event_time_ms, wm, kind, reason, detail) -> None:
        logger.info(
            "decision event_id=%s event_time_ms=%s watermark_ms=%s -> %s reason=%s | %s",
            eid, event_time_ms, wm, kind,
            reason.value if reason is not None else "-", detail,
        )

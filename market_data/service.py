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
"""
from __future__ import annotations

import asyncio
import copy
import logging
import threading
import time

from .checkpoints import CheckpointStore
from .channels import ChannelTimelines
from .config import Config
from .dedup import Deduplicator
from .errors import BackpressureError, IngestionError
from .merger import CrossSourceMerger, _trade_key
from .metrics import Metrics
from .models import Event, IngestResult, RejectReason
from .quarantine import Quarantine
from .schema import parse_event_safe
from .storage import EventLog, MergedTradeStore, PublishedWindowStore, QuarantineStore
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
        self._log = EventLog(self.config.data_dir, fsync=self.config.fsync)
        self._published_store = PublishedWindowStore(self.config.data_dir)
        self._quarantine_store = QuarantineStore(self.config.data_dir)
        self._merged_store = MergedTradeStore(self.config.data_dir)
        self._checkpoints = CheckpointStore(self.config.data_dir)
        # 多来源：每渠道独立事件时间推进 + 跨渠道成交合并。
        # 关闭时完全走单一全局水位旧路径（单来源行为不回归）。
        self._multi = bool(self.config.multi_source)
        self._channels: ChannelTimelines | None = None
        self._merger: CrossSourceMerger | None = None
        if self._multi:
            self._channels = ChannelTimelines(
                self.config.allowed_lateness_ms,
                self.config.source_idle_timeout_ms,
                self.config.max_sources,
                self.config.source_lag_alert_ms,
                clock=self._state_clock)
            self._merger = CrossSourceMerger(self.config.merge_sources)
        self._lock = threading.RLock()
        self._symbol_locks: dict[str, asyncio.Lock] = {}
        self._symbol_locks_guard = threading.Lock()
        self._closed = False
        self._recover()

    def _state_clock(self) -> int:
        """停滞检测使用的处理时间时钟（毫秒）；测试可替换为确定性时钟。"""
        return time.time_ns() // 1_000_000

    # ---------- 恢复 ----------
    def _recover(self) -> None:
        """从仅追加日志重放重建内存状态。

        真相划分：
        * 已发布窗口特征以 ``published_windows.jsonl`` 为唯一真相，
          直接回灌（不可变），事件日志中属于这些窗口的事件只占去重表；
        * 其余已落盘事件属于"已接纳但崩溃于阶段二"的批次，重放后按
          重建水位重新发布——事件集合与崩溃前一致，特征逐位相同，
          不重复（event_id 去重）也不缺失。
        """
        if self._multi:
            self._recover_multi()
            return
        already = self._published_store.load_all()
        self._windows.restore_published(already)
        # 隔离记录回灌：事件身份进入去重表（重复投递按幂等/冲突判定），
        # 记录本身保持可查询且不参与聚合。
        quarantined = self._quarantine_store.load_all()
        for q in quarantined:
            self._dedup.check(q.event)
        self._quarantine.restore(quarantined)
        events = self._log.replay_all()
        # 按事件时间排序重放：全部已接纳事件都参与水位重建（日志中不含
        # 被隔离事件）；只有未发布窗口的事件进入聚合桶，已发布窗口的
        # 事件仅在去重表占位。
        for ev in sorted(events, key=lambda e: (e.event_time_ms, e.event_id)):
            self._dedup.check(ev)
            self._watermark.observe(ev.event_time_ms)
            w0 = self._windows.window_start_for(ev.event_time_ms)
            if self._windows.is_published(ev.symbol, w0):
                continue
            self._windows.add(ev)
        # 水位由全部已接纳事件重建；到点但崩溃于持久化前的窗口在此发布，
        # 事件集合与崩溃前一致 -> 特征逐位相同，不重复不缺失。
        republished = self._windows.publish_due(self._watermark.watermark_ms)
        if republished:
            self._published_store.append_many(republished)
        # 恢复后压缩已发布事件的内容指纹（身份键保留）。
        for _symbol, _w0, feat in self._windows.all_published_pairs():
            self._dedup.mark_published(feat.event_ids)
        self._refresh_pending()

    def _recover_multi(self) -> None:
        """多来源模式重放：渠道水位各自重建，跨渠道合并不重复计算。

        * 已发布窗口仍是不可变真相，直接回灌；
        * 隔离记录（含 merge_conflict）身份入去重表、冲突态回灌；
        * 事件日志只含各笔成交的**主报**，按 (event_time,event_id) 排序
          重放进窗——取舍规则确定，主报集合与崩溃前一致；
        * 合并日志中的被合并次报不进窗，但恢复去重身份、推进所属渠道水位；
        * 渠道水位/参与关系由主报+次报共同恢复，窗口按各标的水位发布。
        """
        assert self._channels is not None and self._merger is not None
        already = self._published_store.load_all()
        self._windows.restore_published(already)

        quarantined = self._quarantine_store.load_all()
        conflict_keys: list[tuple[str, str]] = []
        for q in quarantined:
            self._dedup.check(q.event)
            if q.reason is RejectReason.MERGE_CONFLICT and q.event.trade_id:
                conflict_keys.append((q.event.symbol, q.event.trade_id))
        self._quarantine.restore(quarantined)
        self._merger.restore_conflicted(conflict_keys)

        # 合并记录回灌（留痕 + 被合并事件身份/渠道推进）。
        # 次报只推进其渠道水位与去重身份，不作为窗口关窗参与方。
        merged_records = self._merged_store.load_records()
        self._merger.restore(merged_records)
        for rec in merged_records:
            loser = rec.loser_event
            if loser is not None:
                self._dedup.check(loser)
                self._channels.restore(loser.source, loser.event_time_ms)

        events = self._log.replay_all()
        for ev in sorted(events, key=lambda e: (e.event_time_ms, e.event_id)):
            self._dedup.check(ev)
            self._channels.restore(ev.source, ev.event_time_ms)
            # 事件日志只含主报：这些渠道才参与该标的窗口关窗判定。
            self._channels.note_symbol_source(ev.symbol, ev.source)
            w0 = self._windows.window_start_for(ev.event_time_ms)
            if self._windows.is_published(ev.symbol, w0):
                continue
            self._windows.add(ev)

        # 到点窗口按各标的参与渠道水位发布（确定性顺序落盘）
        wm_by_symbol = self._publish_watermarks()
        republished = self._windows.publish_due_by_symbol(wm_by_symbol)
        if republished:
            self._published_store.append_many(republished)
        for _symbol, _w0, feat in self._windows.all_published_pairs():
            self._dedup.mark_published(feat.event_ids)
            self._merger.mark_winners_published(set(feat.event_ids))
        self._refresh_pending()

    def _publish_watermarks(self) -> dict[str, int]:
        """收集所有涉及标的的当前发布水位（多来源）。"""
        assert self._channels is not None
        return {sym: self._channels.publish_watermark_for(sym)
                for sym in self._windows.all_symbols()}

    def _symbol_lock(self, symbol: str) -> asyncio.Lock:
        with self._symbol_locks_guard:
            lk = self._symbol_locks.get(symbol)
            if lk is None:
                lk = asyncio.Lock()
                self._symbol_locks[symbol] = lk
            return lk

    # ---------- 多来源批次处理 ----------
    def _ingest_batch_multi(self, raw_events: list[dict]) -> IngestResult:
        """多来源批次：每渠道独立水位判定 + 跨渠道成交合并。

        三阶段（只读预检 -> 快照提交 -> 持久化），与单来源一致；差异：
        * 迟到判定只与**事件所属渠道**水位比较，渠道间互不影响；
        * 同一 (symbol, trade_id) 的跨渠道上报先合并判定：关键内容一致 ->
          非主报计入 merged 留痕、不进聚合；不一致 -> 该笔本批相关上报
          全部以 merge_conflict 隔离，绝不静默按一方算。
        """
        assert self._channels is not None and self._merger is not None
        channels, merger = self._channels, self._merger

        # ===== 阶段一-a：解析 + 双键身份预检（纯读，与旧路径相同） =====
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
        batch_fp: dict[str, Event] = {}
        batch_seq: dict[tuple[str, int], str] = {}
        for ev in parsed:
            prev = batch_fp.get(ev.event_id)
            if prev is not None and prev != ev:
                hard_failures.append((ev.event_id, RejectReason.DUPLICATE_CONFLICT,
                    f"批次内 event_id={ev.event_id!r} 内容冲突"))
            else:
                batch_fp.setdefault(ev.event_id, ev)
            other = batch_seq.get((ev.source, ev.seq))
            if other is not None and other != ev.event_id:
                hard_failures.append((ev.event_id, RejectReason.DUPLICATE_CONFLICT,
                    f"批次内 ({ev.source!r},{ev.seq}) 绑定了不同 event_id"))
            else:
                batch_seq[(ev.source, ev.seq)] = ev.event_id
        dedup_verdict: dict[int, str] = {}
        seen_eid: dict[str, str] = {}
        for ev in parsed:
            if ev.event_id in seen_eid:
                dedup_verdict[id(ev)] = "duplicate"
                continue
            try:
                verdict = self._dedup.inspect(ev)
            except IngestionError as exc:
                hard_failures.append((ev.event_id, exc.reason, str(exc)))
                verdict = "new"
            dedup_verdict[id(ev)] = verdict
            seen_eid[ev.event_id] = verdict
        if hard_failures:
            for eid, reason, detail in sorted(
                    hard_failures, key=lambda x: (str(x[0]), x[1].value)):
                self.metrics.rejected += 1
                if reason is RejectReason.DUPLICATE_CONFLICT:
                    self.metrics.conflicts += 1
                self._log_decision(eid, None, None, "batch_rejected", reason, detail)
            first = hard_failures[0]
            raise IngestionError(first[1],
                f"批次被拒绝（{len(hard_failures)} 处硬失败），整批不生效；"
                f"首项 [{first[0]}] {first[1].value}: {first[2]}",
                event_id=first[0])

        # ===== 阶段一-b：合并判定两遍（模拟状态，不落库） =====
        # 渠道数量硬上限：本批新渠道 + 已有渠道超限即整批拒绝，
        # 防止渠道标识异常增长无界占用内存。
        new_sources = {ev.source for ev in parsed} - set(channels.sources())
        if len(channels) + len(new_sources) > self.config.max_sources:
            self.metrics.backpressure_rejected += 1
            raise IngestionError(
                RejectReason.SOURCE_LIMIT_REJECTED,
                f"渠道数量将达 {len(channels) + len(new_sources)}，超过上限 "
                f"{self.config.max_sources}，整批拒绝",
                event_id=None)
        # 按 (event_time, event_id) 升序，与重放顺序一致 -> 取舍确定。
        ordered = sorted(parsed, key=lambda e: (e.event_time_ms, e.event_id))
        sim_winners: dict[tuple[str, str], Event] = dict(merger._winners)  # noqa: SLF001
        published_trades = merger._published_winners  # noqa: SLF001
        conflicted = set(merger._conflicted)  # noqa: SLF001
        mv_of: dict[int, str] = {}
        for ev in ordered:
            key = _trade_key(ev)
            if dedup_verdict[id(ev)] == "duplicate" or key is None:
                mv_of[id(ev)] = "normal"          # 交双键去重 / 无 trade_id
                continue
            if key in conflicted:
                mv_of[id(ev)] = "conflict"
                continue
            if key in published_trades:
                _, _, price, qty = published_trades[key]
                mv_of[id(ev)] = ("conflict" if (ev.price, ev.quantity) != (price, qty)
                                 else "loser_published")
                continue
            cur = sim_winners.get(key)
            if cur is None or ev.source == cur.source:
                # 首报；或同渠道同 trade_id（不是跨渠道合并，正常处理）
                if cur is None:
                    sim_winners[key] = ev
                mv_of[id(ev)] = "normal"
                continue
            if (ev.price, ev.quantity) != (cur.price, cur.quantity):
                mv_of[id(ev)] = "conflict"
                conflicted.add(key)
                continue
            if merger._rank(ev.source) < merger._rank(cur.source):  # noqa: SLF001
                mv_of[id(ev)] = "replace"         # 高优先级后来居上
                sim_winners[key] = ev
            else:
                mv_of[id(ev)] = "loser"

        # 冲突扩散：同一 key 在本批被判冲突，则该 key 本批**所有**上报都隔离
        # （包括此前在 sim 中临时成为主报的那一路），绝不按一方计入。
        for ev in ordered:
            key = _trade_key(ev)
            if key is not None and key in conflicted \
                    and dedup_verdict[id(ev)] != "duplicate":
                mv_of[id(ev)] = "conflict"

        # ===== 阶段一-c：渠道迟到判定（每渠道独立"运行中水位"） =====
        # 模拟本批按时间升序逐条到达：事件 i 看到的本渠道水位只由该渠道
        # 已处理的 0..i-1 推进，等价于实时逐条到达（同批乱序不被未来误伤）。
        running_max: dict[str, int] = {
            s: st.max_event_time_ms for s, st in channels._channels.items()  # noqa: SLF001
            if st.max_event_time_ms is not None}
        allowed = self.config.allowed_lateness_ms

        def running_late(source: str, t: int) -> bool:
            base = running_max.get(source)
            return base is not None and t <= base - allowed

        accepted: list[Event] = []
        loser_items: list[Event] = []
        quarantined_items: list[tuple[Event, RejectReason, str, int]] = []
        duplicates = 0
        merged_count = 0
        details: list[tuple[str, RejectReason, str]] = []
        q_seats = self._quarantine._max - self._quarantine.size  # noqa: SLF001
        for ev in ordered:
            ch_wm = channels.channel_watermark(ev.source)
            if dedup_verdict[id(ev)] == "duplicate":
                duplicates += 1
                details.append((ev.event_id, RejectReason.DUPLICATE_IDENTICAL,
                                "与已接纳事件完全一致，幂等忽略"))
                self._log_decision(ev.event_id, ev.event_time_ms, ch_wm,
                                   "duplicate", RejectReason.DUPLICATE_IDENTICAL,
                                   "identical redelivery")
                continue
            mv = mv_of[id(ev)]
            w0 = self._windows.window_start_for(ev.event_time_ms)
            if mv == "conflict":
                reason = RejectReason.MERGE_CONFLICT
                detail = (f"同一笔成交 (symbol={ev.symbol!r}, trade_id="
                          f"{ev.trade_id!r}) 在渠道 {ev.source!r} 与其他渠道上报的"
                          f"价格/数量不一致，整笔隔离待核对（不按任何一方计入）")
                if q_seats <= 0:
                    self.metrics.backpressure_rejected += 1
                    raise BackpressureError(
                        RejectReason.BACKPRESSURE_REJECTED,
                        f"隔离区已满（{self.config.max_quarantine_size}），整批拒绝",
                        event_id=ev.event_id)
                q_seats -= 1
                quarantined_items.append((ev, reason, detail, ch_wm))
                details.append((ev.event_id, reason, detail))
                self.metrics.conflicts += 1
                self._log_decision(ev.event_id, ev.event_time_ms, ch_wm,
                                   "quarantined", reason, detail)
                # 冲突事件仍代表该渠道已处理到该序号 -> 推进其运行中水位
                running_max[ev.source] = max(
                    running_max.get(ev.source, ev.event_time_ms), ev.event_time_ms)
                continue
            if running_late(ev.source, ev.event_time_ms) \
                    or self._windows.is_published(ev.symbol, w0):
                if q_seats <= 0:
                    self.metrics.backpressure_rejected += 1
                    raise BackpressureError(
                        RejectReason.BACKPRESSURE_REJECTED,
                        f"隔离区已满（{self.config.max_quarantine_size}），整批拒绝",
                        event_id=ev.event_id)
                q_seats -= 1
                if running_late(ev.source, ev.event_time_ms):
                    detail = (f"渠道 {ev.source!r} event_time_ms={ev.event_time_ms} <= "
                              f"本渠道 watermark_ms={ch_wm}，超过允许迟到 "
                              f"{self.config.allowed_lateness_ms}ms")
                else:
                    detail = (f"窗口 [{w0},{w0+self.config.window_size_ms}) 已发布"
                              f"且不可变，事件时间 {ev.event_time_ms}")
                quarantined_items.append(
                    (ev, RejectReason.LATE_BEYOND_WATERMARK, detail, ch_wm))
                details.append((ev.event_id, RejectReason.LATE_BEYOND_WATERMARK, detail))
                self._log_decision(ev.event_id, ev.event_time_ms, ch_wm,
                                   "quarantined", RejectReason.LATE_BEYOND_WATERMARK, detail)
                running_max[ev.source] = max(
                    running_max.get(ev.source, ev.event_time_ms), ev.event_time_ms)
                continue
            if mv in ("loser", "loser_published"):
                loser_items.append(ev)
                merged_count += 1
                details.append((ev.event_id, RejectReason.CROSS_SOURCE_MERGED,
                    f"同一笔成交 trade_id={ev.trade_id!r} 已由优先渠道上报，"
                    f"本路 {ev.source!r} 被合并且不重复计入"))
                self._log_decision(ev.event_id, ev.event_time_ms, ch_wm,
                                   "merged", RejectReason.CROSS_SOURCE_MERGED,
                                   f"merged into winner of trade {ev.trade_id}")
                running_max[ev.source] = max(
                    running_max.get(ev.source, ev.event_time_ms), ev.event_time_ms)
                continue
            # normal / replace：作为（新）主报入窗
            accepted.append(ev)
            running_max[ev.source] = max(
                running_max.get(ev.source, ev.event_time_ms), ev.event_time_ms)

        pending_after = (self._windows.pending_event_count()
                         + self._quarantine.size + len(quarantined_items)
                         + len(accepted) + len(loser_items))
        if pending_after > self.config.max_pending_events:
            self.metrics.backpressure_rejected += 1
            raise BackpressureError(
                RejectReason.BACKPRESSURE_REJECTED,
                f"提交后预估积压 {pending_after} 超过上限 "
                f"{self.config.max_pending_events}，整批拒绝")

        # ===== 阶段二：内存提交（快照回滚） =====
        snapshot = (copy.deepcopy(self._dedup.__dict__),
                    copy.deepcopy(self._windows.__dict__),
                    copy.deepcopy(self._quarantine.__dict__),
                    channels._snapshot(),   # noqa: SLF001（显式排除时钟）
                    merger.snapshot())
        now_ms = self._state_clock()
        max_seq_by_source: dict[str, int] = {}

        def _bump_seq(ev: Event) -> None:
            if ev.seq > max_seq_by_source.get(ev.source, -1):
                max_seq_by_source[ev.source] = ev.seq

        try:
            for ev in accepted:
                self._dedup.check(ev)
                # register_winner 内部在"高优先级替换"时生成旧主报合并记录
                merger.register_winner(ev)
                channels.observe(ev.source, ev.event_time_ms, now_ms=now_ms)
                channels.note_symbol_source(ev.symbol, ev.source)
                self._windows.add(ev)
                _bump_seq(ev)
            for ev in loser_items:
                self._dedup.check(ev)
                if mv_of[id(ev)] == "loser_published":
                    merger.register_loser_published(ev)
                else:
                    merger.register_loser(ev)
                channels.observe(ev.source, ev.event_time_ms, now_ms=now_ms)
                # 被合并副本不提供新成交 -> 不参与该标的窗口关窗判定，
                # 否则一个只转发冗余拷贝、随后停更的辅助渠道会无限拖住发布。
                _bump_seq(ev)
            # 被替换的旧主报：register_winner 已生成记录，这里负责把它移出窗口
            for old_ev in merger.take_superseded().values():
                w0o = self._windows.window_start_for(old_ev.event_time_ms)
                self._windows.remove_open_event(old_ev.symbol, w0o, old_ev.event_id)
                _bump_seq(old_ev)
            for ev, reason, _detail, _wm in quarantined_items:
                self._dedup.check(ev)
                channels.observe(ev.source, ev.event_time_ms, now_ms=now_ms)
                # 被隔离事件不参与关窗（迟到/冲突副本不代表新成交）
                _bump_seq(ev)
                if reason is RejectReason.MERGE_CONFLICT and ev.trade_id:
                    merger.mark_conflicted((ev.symbol, ev.trade_id))
            new_merged_records = merger.drain_records()
            wm_by_symbol = self._publish_watermarks()
            published = self._windows.publish_due_by_symbol(wm_by_symbol)
        except BaseException:
            self._dedup.__dict__ = copy.deepcopy(snapshot[0])
            self._windows.__dict__ = copy.deepcopy(snapshot[1])
            self._quarantine.__dict__ = copy.deepcopy(snapshot[2])
            channels._restore(snapshot[3])   # noqa: SLF001
            merger.restore_snapshot(snapshot[4])
            raise

        # ===== 阶段三：持久化副作用 =====
        from .models import QuarantinedEvent
        q_records = [
            QuarantinedEvent(event=ev, reason=reason, watermark_ms=wm,
                             detail=detail, accepted_at_ms=now_ms)
            for ev, reason, detail, wm in quarantined_items]
        self._log.append_batch(accepted)
        self._published_store.append_many(published)
        self._quarantine_store.append_batch(q_records)
        self._merged_store.append_batch(new_merged_records)
        for f in published:
            self._dedup.mark_published(f.event_ids)
            merger.mark_winners_published(set(f.event_ids))
        for rec in q_records:
            self._quarantine.add(rec.event, rec.reason, rec.watermark_ms,
                                 rec.detail, rec.accepted_at_ms)
        for source, seq in max_seq_by_source.items():
            self._checkpoints.advance(source, seq)

        self.metrics.deprecated_seen += deprecated_total
        self.metrics.accepted += len(accepted)
        self.metrics.duplicates += duplicates
        self.metrics.merged += merged_count
        self.metrics.quarantined += len(q_records)
        self.metrics.published_windows += len(published)
        self._refresh_pending()
        return IngestResult(
            accepted=len(accepted), duplicate=duplicates,
            quarantined=len(q_records), rejected=0,
            merged=merged_count, details=tuple(details),
        )

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
        if self._multi:
            return self._ingest_batch_multi(raw_events)
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

        # 迟到分类：按事件时间排序，用"运行中水位"逐条判定。
        # 这样同批内的乱序事件以彼此到达前的水位判定（较早事件不会被
        # 同批较晚事件推进的水位误伤），且允许窗口内迟到修正未发布窗口。
        # 重复事件不推进水位。
        ordered = sorted(parsed, key=lambda e: (e.event_time_ms, e.event_id))
        # 关键：按事件时间升序逐条判定，事件 i 看到的水位只由
        # 已处理的事件 0..i-1 推进 —— 与逐条实时到达等价，因此
        # 同批跨窗口/乱序事件不会被"未来"的同批事件误判迟到。
        running_max = self._watermark.max_event_time_ms
        accepted: list = []
        quarantined_items: list = []
        duplicates = 0
        details: list[tuple[str, RejectReason, str]] = []
        q_seats = self._quarantine._max - self._quarantine.size
        for ev in ordered:
            v = verdict_of[id(ev)]
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
            if ev.event_time_ms <= wm_here or self._windows.is_published(ev.symbol, w0):
                if q_seats <= 0:
                    self.metrics.backpressure_rejected += 1
                    raise BackpressureError(
                        RejectReason.BACKPRESSURE_REJECTED,
                        f"隔离区已满（{self.config.max_quarantine_size}），整批拒绝",
                        event_id=ev.event_id)
                q_seats -= 1
                if ev.event_time_ms <= wm_here:
                    detail = (f"event_time_ms={ev.event_time_ms} <= "
                              f"watermark_ms={wm_here}，超过允许迟到窗口 "
                              f"{self.config.allowed_lateness_ms}ms")
                else:
                    detail = (f"窗口 [{w0},{w0+self.config.window_size_ms}) 已发布"
                              f"且不可变，事件时间 {ev.event_time_ms}")
                quarantined_items.append((ev, detail, w0, wm_here))
                details.append((ev.event_id, RejectReason.LATE_BEYOND_WATERMARK, detail))
                self._log_decision(ev.event_id, ev.event_time_ms, wm_here,
                                   "quarantined", RejectReason.LATE_BEYOND_WATERMARK, detail)
                continue
            accepted.append(ev)
            running_max = ev.event_time_ms if running_max is None \
                else max(running_max, ev.event_time_ms)
        # 批次结束后水位推进到本批观察上界（升序处理后天然等于最大接纳事件时间）
        final_max = running_max
        pending_after = (self._windows.pending_event_count()
                         + self._quarantine.size + len(quarantined_items)
                         + len(accepted))
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
                    self._watermark.max_event_time_ms)
        now_ms = time.time_ns() // 1_000_000
        max_seq_by_source: dict[str, int] = {}
        try:
            for ev in accepted:
                self._dedup.check(ev)
                wm_before = self._watermark.watermark_ms
                self._watermark.observe(ev.event_time_ms)
                self._windows.add(ev)
                w0 = self._windows.window_start_for(ev.event_time_ms)
                self._log_decision(ev.event_id, ev.event_time_ms, wm_before,
                                   "accepted", None,
                                   f"window_start={w0} wm {wm_before}->"
                                   f"{self._watermark.watermark_ms}")
                if ev.seq > max_seq_by_source.get(ev.source, -1):
                    max_seq_by_source[ev.source] = ev.seq
            for ev, _detail, _w0, wm in quarantined_items:
                self._dedup.check(ev)
                if ev.seq > max_seq_by_source.get(ev.source, -1):
                    max_seq_by_source[ev.source] = ev.seq
            published = self._windows.publish_due(self._watermark.watermark_ms)
        except BaseException:
            self._dedup.__dict__ = copy.deepcopy(snapshot[0])
            self._windows.__dict__ = copy.deepcopy(snapshot[1])
            self._quarantine.__dict__ = copy.deepcopy(snapshot[2])
            self._watermark._max_event_time = snapshot[3]
            raise

        # ===== 阶段三：持久化副作用 =====
        from .models import QuarantinedEvent
        q_records = [
            QuarantinedEvent(event=ev, reason=RejectReason.LATE_BEYOND_WATERMARK,
                             watermark_ms=wm, detail=detail,
                             accepted_at_ms=now_ms)
            for ev, detail, _w0, wm in quarantined_items]
        self._log.append_batch(accepted)
        self._published_store.append_many(published)
        self._quarantine_store.append_batch(q_records)
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
        self.metrics.quarantined += len(quarantined_items)
        self.metrics.published_windows += len(published)
        self._refresh_pending()
        return IngestResult(
            accepted=len(accepted), duplicate=duplicates,
            quarantined=len(quarantined_items), rejected=0,
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

    def merged_trades(self) -> list[dict]:
        """跨渠道合并留痕：每笔被合并次报对应主报与渠道（重启后仍可查询）。"""
        out = []
        for rec in self._merged_store.load_records():
            out.append({
                "symbol": rec.trade_key[0],
                "trade_id": rec.trade_key[1],
                "winner_event_id": rec.winner_event_id,
                "winner_source": rec.winner_source,
                "loser_event_id": rec.loser_event_id,
                "loser_source": rec.loser_source,
                "event_time_ms": rec.event_time_ms,
            })
        return out

    def channels_snapshot(self) -> dict | None:
        """多来源观测：各渠道推进到哪、落后多少、是否停滞（单来源模式为 None）。"""
        if self._channels is None:
            return None
        with self._lock:
            return self._channels.snapshot_observability()

    def checkpoint(self, source: str, seq: int) -> bool:
        """显式位点推进/回退检查（回退抛 CheckpointRollbackError）。"""
        with self._lock:
            return self._checkpoints.advance(source, seq)

    def checkpoint_get(self, source: str) -> int:
        return self._checkpoints.get(source)

    def checkpoints(self) -> dict[str, int]:
        return self._checkpoints.all()

    @property
    def watermark_ms(self) -> int:
        # 多来源没有单一全局水位；返回所有渠道参与的最小发布水位供健康检查。
        if self._channels is not None:
            wms = [self._channels.publish_watermark_for(sym)
                   for sym in self._windows.all_symbols()]
            return min(wms) if wms else -(1 << 62)
        return self._watermark.watermark_ms

    def metrics_snapshot(self) -> dict:
        with self._lock:
            self._refresh_pending()
            snap = self.metrics.snapshot()
            snap["quarantine_size"] = self._quarantine.size
            snap["dedup_size"] = len(self._dedup)
            if self._channels is not None:
                ch = self._channels.snapshot_observability()
                snap["watermark_ms"] = ch["max_event_time_ms"]
                snap["multi_source"] = True
                snap["channels"] = ch
                stalled = [c["source"] for c in ch["channels"] if c["stalled"]]
                lagging = [c["source"] for c in ch["channels"] if c["lag_alert"]]
                snap["stalled_channels"] = stalled
                snap["lagging_channels"] = lagging
            else:
                snap["watermark_ms"] = self._watermark.watermark_ms
                snap["multi_source"] = False
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
        est = self._dedup.full_fingerprint_count * 200 + compact * 120 \
            + pending * 200
        self.metrics.estimated_state_bytes = est

    def _log_decision(self, eid, event_time_ms, wm, kind, reason, detail) -> None:
        logger.info(
            "decision event_id=%s event_time_ms=%s watermark_ms=%s -> %s reason=%s | %s",
            eid, event_time_ms, wm, kind,
            reason.value if reason is not None else "-", detail,
        )

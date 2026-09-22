"""The ingestion engine.

Concurrency model: one re-entrant lock guards every state transition, so
concurrent writes / replays / queries against the same symbol can never show
a partial update, double-count or jump a checkpoint. Reads return immutable
copies (dataclass tuples) taken under the lock.

Atomicity model (two-phase per batch):

1. *classify* every payload (schema -> seq -> dedupe) with **no mutation**.
   Any hard reject (schema error, conflicting duplicate, seq regression)
   aborts the whole batch: nothing is journaled or aggregated,
   ``committed=False`` and ``rollbacks`` is incremented.
2. *commit*: append accepted events to the journal, then apply
   watermark/window/quarantine in arrival order, remember dedupe
   fingerprints, advance the checkpoint once and snapshot. Any failure in
   this phase restores the pre-batch state snapshot -- no partial aggregate
   or checkpoint position survives.

Identical redeliveries are idempotent no-ops and do not abort the batch;
late-but-allowed events recompute open windows; late-beyond-watermark
events land in the queryable quarantine.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any

from .checkpoint import CheckpointManager
from .config import BackpressurePolicy, EngineConfig
from .dedupe import (
    DedupeDecision,
    DedupeIndex,
    DedupeStatus,
)
from .errors import (
    BackpressureError,
    CheckpointError,
    RejectReason,
)
from .journal import DurableStore
from .metrics import Metrics
from .models import (
    BatchResult,
    Checkpoint,
    Event,
    IngestRecord,
    QuarantineEntry,
    WindowResult,
)
from .quarantine import Quarantine
from .schema import ParseResult, ParseStatus, SchemaRegistry
from .watermark import WatermarkTracker
from .windows import WindowPublishedError, WindowStore

logger = logging.getLogger("ingestion.engine")


class IngestionEngine:
    def __init__(self, config: EngineConfig | None = None) -> None:
        self.config = config or EngineConfig()
        self._lock = threading.RLock()
        self._inflight_lock = threading.Lock()
        self._inflight = 0
        # Backpressure gate, deliberately independent of the state lock so a
        # blocked waiter never holds state visibility hostage.
        self._gate = threading.Semaphore(self.config.max_inflight_batches)
        self._closed = False
        self._journaled = 0  # number of events available in the durable log

        self.schema = SchemaRegistry(
            supported_versions=self.config.supported_versions,
            unknown_field_policy=self.config.unknown_field_policy,
        )
        self.dedupe = DedupeIndex(self.config.max_dedupe_entries)
        self.windows = WindowStore(self.config.window_size_ms)
        self.watermarks = WatermarkTracker(self.config.allowed_lateness_ms)
        self.metrics = Metrics()
        self.quarantine = Quarantine(
            self.config.max_quarantine_per_symbol, self.metrics
        )
        self.checkpoints = CheckpointManager()
        self.store: DurableStore | None = None
        if self.config.state_dir:
            self.store = DurableStore(
                self.config.state_dir, fsync=self.config.fsync
            )

    # ------------------------------------------------------------- lifecycle
    def start(self) -> IngestionEngine:
        with self._lock:
            if self.store is not None:
                self._journaled = self.store.open()
                all_events = self.store.read_all()
                if self.config.start_offset is not None:
                    # Boot-time deterministic rebuild from a journal prefix.
                    start_offset = self.config.start_offset
                    if start_offset < 0:
                        raise CheckpointError(
                            f"start_offset must be >= 0, got {start_offset}",
                            reason=RejectReason.SCHEMA_INVALID_VALUE,
                        )
                    if start_offset > len(all_events):
                        raise CheckpointError(
                            f"cannot start at offset {start_offset}: journal "
                            f"holds {len(all_events)} events",
                            reason=RejectReason.CHECKPOINT_UNKNOWN,
                        )
                    prefix = all_events[:start_offset]
                    for event in prefix:
                        self.dedupe.remember([event])
                        self._apply_event(
                            event,
                            raw_payload={"event_id": event.event_id},
                        )
                    if prefix:
                        self.checkpoints.advance(len(prefix))
                    logger.info(
                        "engine booted at replay offset=%s (journal=%s)",
                        start_offset, len(all_events),
                    )
                else:
                    snap = self.store.read_snapshot()
                    if snap is not None:
                        state, offset = snap
                        self._load_state(state)
                        self.checkpoints.restore({"offset": offset})
                        tail = all_events[offset:]
                    else:
                        tail = all_events
                        self._replay_journal_events(tail, advance=True)
                    logger.info(
                        "engine recovered checkpoint=%s journaled=%s",
                        self.checkpoints.offset, self._journaled,
                    )
            return self

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self.store is not None:
                self.store.close()

    def __enter__(self) -> IngestionEngine:
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ save
    def _collect_state(self) -> dict[str, Any]:
        return {
            "dedupe": self.dedupe.snapshot(),
            "windows": self.windows.snapshot(),
            "watermarks": self.watermarks.snapshot(),
            "quarantine": self.quarantine.snapshot(),
            "checkpoints": self.checkpoints.snapshot(),
            "journaled": self._journaled,
        }

    def _load_state(self, state: dict[str, Any], *, force: bool = False) -> None:
        self.dedupe.restore(state["dedupe"])
        self.windows.restore(state["windows"])
        self.watermarks.restore(state["watermarks"])
        self.quarantine.restore(state["quarantine"])
        self.checkpoints.restore(state["checkpoints"], force=force)
        self._journaled = int(state.get("journaled", self.checkpoints.offset))

    def _persist_snapshot(self) -> None:
        if self.store is not None:
            self.store.write_snapshot(
                self._collect_state(), self.checkpoints.offset
            )

    def _replay_journal_events(self, events: list[Event], *, advance: bool) -> None:
        """Deterministic re-application of journaled events (no re-journal)."""
        applied = 0
        for event in events:
            self._apply_event(event, raw_payload={"event_id": event.event_id})
            applied += 1
        if advance and applied:
            self.checkpoints.advance(applied)

    # --------------------------------------------------------- backpressure
    def _acquire_slot(self) -> None:
        if self.config.backpressure_policy is BackpressurePolicy.REJECT:
            if not self._gate.acquire(blocking=False):
                with self._inflight_lock:
                    inflight = self._inflight
                self.metrics.backpressure_rejected += 1
                raise BackpressureError(
                    f"inflight={inflight} >= cap="
                    f"{self.config.max_inflight_batches}; policy=REJECT"
                )
        else:
            if self._gate.acquire(blocking=False):
                acquired = True
            else:
                # Capacity is saturated: this caller now applies the DELAY
                # policy and waits up to the configured bounded budget.
                self.metrics.backpressure_delayed += 1
                wait_s = self.config.backpressure_max_wait_ms / 1000
                acquired = self._gate.acquire(timeout=wait_s)
            if not acquired:
                with self._inflight_lock:
                    inflight = self._inflight
                self.metrics.backpressure_rejected += 1
                raise BackpressureError(
                    f"backpressure wait budget exhausted after "
                    f"{self.config.backpressure_max_wait_ms}ms "
                    f"(inflight={inflight}, cap="
                    f"{self.config.max_inflight_batches})"
                )
        with self._inflight_lock:
            self._inflight += 1

    def _release_slot(self) -> None:
        with self._inflight_lock:
            self._inflight -= 1
        self._gate.release()

    # ---------------------------------------------------------------- writes
    def write_one(self, payload: dict[str, Any] | Event) -> BatchResult:
        return self.write_batch([payload])

    def write_batch(self, payloads: list[dict[str, Any] | Event]) -> BatchResult:
        start = time.perf_counter()
        # Backpressure is applied before touching shared state so a saturated
        # service rejects/waits without blocking queries or other symbols.
        self._acquire_slot()
        try:
            with self._lock:
                if self._closed:
                    raise RuntimeError("engine is closed")
                result = self._write_batch_locked(payloads)
        finally:
            self._release_slot()
        self.metrics.record_batch(
            int((time.perf_counter() - start) * 1_000_000)
        )
        return result

    def _write_batch_locked(
        self, payloads: list[dict[str, Any] | Event]
    ) -> BatchResult:
        t0 = time.perf_counter_ns()
        records: list[IngestRecord] = []
        parsed: list[tuple[ParseResult, Event, dict[str, Any]]] = []
        # Counter mutations during classification are also rolled back when
        # the batch aborts, so a failed batch leaves no partial observability.
        metrics_before = self.metrics.counters_snapshot()
        # Batch-local view: an id appearing twice within one not-yet-committed
        # batch is classified the same way as a redelivery.
        batch_seen: dict[str, tuple[Any, ...]] = {}
        # Per-source highest seq already seen *within this batch*.
        batch_seq: dict[str, int] = {}

        # ---- phase 1: classify, no mutation ---------------------------------
        for raw in payloads:
            if isinstance(raw, Event):
                event = raw
                result = ParseResult(status=ParseStatus.OK, event=event)
                raw_payload = {
                    "event_id": event.event_id,
                    "schema_version": event.schema_version,
                }
            else:
                result = self.schema.parse(raw)
                raw_payload = raw
                event = result.event  # type: ignore[assignment]

            if not result.ok:
                records.append(
                    IngestRecord(
                        event_id=(
                            str(raw_payload.get("event_id", "?"))
                            if isinstance(raw_payload, dict)
                            else "?"
                        ),
                        accepted=False,
                        reason=result.reason,
                        detail=result.detail,
                        watermark_ms=(
                            self.watermarks.get(
                                str(raw_payload.get("symbol", ""))
                            )
                            if isinstance(raw_payload, dict)
                            else None
                        ),
                    )
                )
                continue
            assert event is not None

            # Identity first: a genuine redelivery carries the SAME seq and
            # must be idempotent / conflict-classified before any ordering
            # check. Sequence regression is therefore only meaningful for a
            # brand-new event id.
            decision = self.dedupe.check(event)
            fp = event.content_fingerprint()
            if event.event_id in batch_seen:
                # duplicate id within the same batch: classify vs the first
                in_batch_fp = batch_seen[event.event_id]
                if in_batch_fp == fp:
                    decision = DedupeDecision(
                        DedupeStatus.DUPLICATE_IDENTICAL,
                        detail="duplicate event within the same batch",
                    )
                else:
                    decision = DedupeDecision(
                        DedupeStatus.DUPLICATE_CONFLICT,
                        detail=(
                            f"event_id {event.event_id!r} appears twice in the "
                            f"batch with different content"
                        ),
                    )
            if decision.status is DedupeStatus.DUPLICATE_IDENTICAL:
                self.metrics.duplicates_identical += 1
                records.append(
                    IngestRecord(
                        event_id=event.event_id,
                        accepted=True,
                        reason=RejectReason.DUPLICATE_IDENTICAL,
                        detail=decision.detail,
                        watermark_ms=self.watermarks.get(event.symbol),
                    )
                )
                continue
            if decision.status is DedupeStatus.DUPLICATE_CONFLICT:
                self.metrics.duplicates_conflict += 1
                records.append(
                    IngestRecord(
                        event_id=event.event_id,
                        accepted=False,
                        reason=RejectReason.DUPLICATE_CONFLICT,
                        detail=decision.detail,
                        watermark_ms=self.watermarks.get(event.symbol),
                    )
                )
                continue

            # Within one delivery batch, (source, seq) must be
            # non-decreasing: a backwards jump in the same stream indicates a
            # producer/transport rewind. Across batches/threads arrivals may
            # legitimately interleave out of order; event-time/watermark and
            # the idempotency index handle those, not a global seq gate.
            prior_in_batch = batch_seq.get(event.source)
            if prior_in_batch is not None and event.seq < prior_in_batch:
                records.append(
                    IngestRecord(
                        event_id=event.event_id,
                        accepted=False,
                        reason=RejectReason.SEQ_REGRESSED,
                        detail=(
                            f"source {event.source!r} seq regressed within "
                            f"batch: {event.seq} < {prior_in_batch}"
                        ),
                        watermark_ms=self.watermarks.get(event.symbol),
                    )
                )
                continue
            batch_seq[event.source] = event.seq

            # Hard resource bound, enforced *before* commit so a full index
            # aborts the batch atomically as backpressure (never evicts and
            # never double counts).
            if len(self.dedupe) + len(parsed) + 1 > self.dedupe.max_entries:
                self.metrics.backpressure_rejected += 1
                records.append(
                    IngestRecord(
                        event_id=event.event_id,
                        accepted=False,
                        reason=RejectReason.BACKPRESSURE_REJECTED,
                        detail=(
                            f"dedupe index capacity "
                            f"{self.dedupe.max_entries} reached"
                        ),
                        watermark_ms=self.watermarks.get(event.symbol),
                    )
                )
                continue

            batch_seen[event.event_id] = fp
            parsed.append((result, event, raw_payload))
            self.metrics.schema_unknown_fields += len(result.unknown_fields)
            self.metrics.schema_deprecated_fields += len(result.deprecated_fields)

        n_dup = sum(
            1 for r in records if r.reason is RejectReason.DUPLICATE_IDENTICAL
        )
        n_conflict = sum(
            1 for r in records if r.reason is RejectReason.DUPLICATE_CONFLICT
        )
        n_seq = sum(
            1 for r in records if r.reason is RejectReason.SEQ_REGRESSED
        )
        n_bp = sum(
            1 for r in records if r.reason is RejectReason.BACKPRESSURE_REJECTED
        )
        n_hard = sum(1 for r in records if not r.accepted)
        if n_hard:
            # Undo every counter mutation performed during classification...
            self.metrics.counters_restore(metrics_before)
            # ...then retain observability for the parts that genuinely
            # happened: idempotent duplicate sightings, the classified hard
            # rejections (incl. resource/backpressure), and the abort itself.
            self.metrics.duplicates_identical += n_dup
            self.metrics.duplicates_conflict += n_conflict
            self.metrics.backpressure_rejected += n_bp
            self.metrics.rejected += n_hard
            self.metrics.rollbacks += 1
            logger.info(
                "batch-abort hard_rejects=%s duplicates=%s conflicts=%s seq=%s "
                "backpressure=%s basis=pre-commit-classification",
                n_hard, n_dup, n_conflict, n_seq, n_bp,
            )
            return BatchResult(
                accepted=0,
                duplicates=n_dup,
                conflicts=n_conflict,
                quarantined=0,
                rejected=n_hard,
                records=tuple(records),
                checkpoint=self.checkpoints.offset,
                committed=False,
                duration_us=(time.perf_counter_ns() - t0) // 1000,
            )

        # ---- phase 2: commit -------------------------------------------------
        fresh_events = [e for _, e, _ in parsed]
        state_before = self._collect_state()
        try:
            if fresh_events:
                if self.store is not None:
                    self.store.append_events(fresh_events)
                    self._journaled += len(fresh_events)
                # Remember before applying aggregates: any later failure
                # rolls the whole batch back to ``state_before``.
                self.dedupe.remember(fresh_events)
                c_start = time.perf_counter_ns()
                new_records = self._commit_events(parsed)
                # Total application latency for the batch; per-event latency
                # budgets in benchmarks compare this against
                # per_event_budget * batch_size.
                self.metrics.record_write(
                    max(0, (time.perf_counter_ns() - c_start) // 1000)
                )
                records.extend(new_records)
                self.checkpoints.advance(len(fresh_events))
                self._persist_snapshot()
            # stable order: classification records then commit records
            records.sort(key=lambda r: r.event_id)
            n_quar = sum(
                1 for r in records if r.reason is RejectReason.LATE_BEYOND_WATERMARK
            )
            accepted = len(fresh_events) - n_quar
            return BatchResult(
                accepted=accepted,
                duplicates=n_dup,
                conflicts=0,
                quarantined=n_quar,
                rejected=0,
                records=tuple(records),
                checkpoint=self.checkpoints.offset,
                committed=True,
                duration_us=(time.perf_counter_ns() - t0) // 1000,
            )
        except Exception:
            self._load_state(state_before, force=True)
            self.metrics.counters_restore(metrics_before)
            self.metrics.rollbacks += 1
            logger.exception("batch commit failed; state rolled back")
            raise

    def _commit_events(
        self,
        parsed: list[tuple[ParseResult, Event, dict[str, Any]]],
    ) -> list[IngestRecord]:
        """Apply in arrival order: watermark then window/quarantine."""
        out: list[IngestRecord] = []
        self.metrics.accepted += len(parsed)
        for _result, event, payload in parsed:
            wm_before = self.watermarks.get(event.symbol)
            if self.watermarks.is_behind(event.symbol, event.event_time_ms):
                wm = wm_before
                window_end = (
                    (event.event_time_ms // self.config.window_size_ms + 1)
                    * self.config.window_size_ms
                )
                entry = self.quarantine.add(
                    event,
                    watermark_ms=wm,
                    window_end_ms=window_end,
                    reason=RejectReason.LATE_BEYOND_WATERMARK,
                    detail=(
                        f"event_time_ms={event.event_time_ms} < watermark={wm}"
                    ),
                    payload=payload,
                )
                self.metrics.quarantined += 1
                out.append(
                    IngestRecord(
                        event_id=event.event_id,
                        accepted=True,
                        reason=RejectReason.LATE_BEYOND_WATERMARK,
                        detail=entry.detail,
                        watermark_ms=wm,
                        quarantine_id=entry.quarantine_id,
                    )
                )
                continue

            wm = self.watermarks.observe(event.symbol, event.event_time_ms)
            window_start = (
                event.event_time_ms // self.config.window_size_ms
            ) * self.config.window_size_ms
            try:
                wr = self.windows.add(event)
                if wr.recomputed:
                    self.metrics.recomputed_windows += 1
            except WindowPublishedError:
                entry = self.quarantine.add(
                    event,
                    watermark_ms=wm,
                    window_end_ms=window_start + self.config.window_size_ms,
                    reason=RejectReason.LATE_BEYOND_WATERMARK,
                    detail=(
                        f"target window [{window_start},"
                        f"{window_start + self.config.window_size_ms}) "
                        f"already published at/under watermark={wm}"
                    ),
                    payload=payload,
                )
                self.metrics.quarantined += 1
                out.append(
                    IngestRecord(
                        event_id=event.event_id,
                        accepted=True,
                        reason=RejectReason.LATE_BEYOND_WATERMARK,
                        detail=entry.detail,
                        window_start_ms=window_start,
                        watermark_ms=wm,
                        quarantine_id=entry.quarantine_id,
                    )
                )
                continue

            frozen = self.windows.publish_up_to(event.symbol, wm)
            self.metrics.published_windows += len(frozen)
            out.append(
                IngestRecord(
                    event_id=event.event_id,
                    accepted=True,
                    window_start_ms=window_start,
                    watermark_ms=wm,
                )
            )
        return out

    def _apply_event(self, event: Event, *, raw_payload: dict[str, Any]) -> None:
        """Replay path: event is already deduped/journaled."""
        if self.watermarks.is_behind(event.symbol, event.event_time_ms):
            window_end = (
                (event.event_time_ms // self.config.window_size_ms + 1)
                * self.config.window_size_ms
            )
            self.quarantine.add(
                event,
                watermark_ms=self.watermarks.get(event.symbol),
                window_end_ms=window_end,
                reason=RejectReason.LATE_BEYOND_WATERMARK,
                detail=f"event_time_ms={event.event_time_ms} behind watermark on replay",
                payload=raw_payload,
            )
            return
        wm = self.watermarks.observe(event.symbol, event.event_time_ms)
        try:
            self.windows.add(event)
        except WindowPublishedError:
            window_start = (
                event.event_time_ms // self.config.window_size_ms
            ) * self.config.window_size_ms
            self.quarantine.add(
                event,
                watermark_ms=wm,
                window_end_ms=window_start + self.config.window_size_ms,
                reason=RejectReason.LATE_BEYOND_WATERMARK,
                detail="window already published on replay",
                payload=raw_payload,
            )
            return
        self.windows.publish_up_to(event.symbol, wm)

    # ---------------------------------------------------------------- queries
    def query_windows(
        self, symbol: str | None = None, include_unpublished: bool = False
    ) -> list[WindowResult]:
        with self._lock:
            if include_unpublished:
                return self.windows.list_all(symbol)
            return self.windows.list_published(symbol)

    def query_quarantine(
        self, symbol: str | None = None
    ) -> list[QuarantineEntry]:
        with self._lock:
            return self.quarantine.query(symbol)

    def current_checkpoint(self) -> Checkpoint:
        with self._lock:
            return self.checkpoints.current()

    # ----------------------------------------------------------------- replay
    def replay_from(self, offset: int) -> Checkpoint:
        """Reposition deterministically -- forward only.

        Rules:

        * ``offset <= current checkpoint`` is refused with
          ``CHECKPOINT_REGRESSED``: a running engine (including one that
          recovered from a snapshot) never rewinds or re-emits data.
        * ``offset > events available in the journal`` is refused with
          ``CHECKPOINT_UNKNOWN`` (no gap is ever skipped).
        * Otherwise state is rebuilt from the journal prefix
          ``[0, offset)`` and the checkpoint advances to ``offset``.

        Replaying an *ear*lier* prefix than the latest committed position is
        done by starting a fresh engine process over the same state
        directory (deterministic rebuild), never by rewinding a live engine.
        """
        with self._lock:
            self.checkpoints.require_forward(offset)
            if self.store is None:
                raise CheckpointError(
                    "replay requires a durable state_dir; engine is ephemeral",
                    reason=RejectReason.CHECKPOINT_UNKNOWN,
                )
            all_events = self.store.read_all()
            if offset > len(all_events):
                raise CheckpointError(
                    f"cannot replay to offset {offset}: journal holds "
                    f"{len(all_events)} events",
                    reason=RejectReason.CHECKPOINT_UNKNOWN,
                )

            # rebuild from scratch deterministically
            self.dedupe = DedupeIndex(self.config.max_dedupe_entries)
            self.windows = WindowStore(self.config.window_size_ms)
            self.watermarks = WatermarkTracker(self.config.allowed_lateness_ms)
            self.quarantine = Quarantine(
                self.config.max_quarantine_per_symbol, self.metrics
            )
            self.checkpoints = CheckpointManager()
            prefix = all_events[:offset]
            for event in prefix:
                self.dedupe.remember([event])
                self._apply_event(
                    event, raw_payload={"event_id": event.event_id}
                )
            if prefix:
                self.checkpoints.advance(len(prefix))
            self._persist_snapshot()
            logger.info(
                "replay-forward offset=%s basis=journal-prefix-rebuild", offset
            )
            return self.checkpoints.current()

    def metrics_snapshot(self) -> dict[str, int | float]:
        with self._lock:
            with self._inflight_lock:
                inflight = self._inflight
            return self.metrics.snapshot(
                dedupe_entries=len(self.dedupe),
                dedupe_bytes=self.dedupe.memory_bytes(),
                inflight=inflight,
            )

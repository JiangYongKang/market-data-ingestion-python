"""Out-of-order/late events, watermark rules, VWAP & volatility."""
from __future__ import annotations

import math
import statistics

from app.ingestion.config import EngineConfig
from app.ingestion.engine import IngestionEngine
from app.ingestion.errors import RejectReason
from tests.conftest import payload


def _engine(**kw) -> IngestionEngine:
    cfg = EngineConfig(window_size_ms=1000, allowed_lateness_ms=500, **kw)
    return IngestionEngine(cfg).start()


def test_vwap_and_volatility_are_explainable() -> None:
    eng = _engine()
    eng.write_batch([
        payload("a", seq=1, event_time_ms=0, price=100, quantity=2),
        payload("b", seq=2, event_time_ms=100, price=110, quantity=3),
        payload("c", seq=3, event_time_ms=200, price=120, quantity=5),
    ])
    eng.write_one(payload("z", seq=4, event_time_ms=2000, price=1, quantity=1))
    w = eng.query_windows("BTCUSD")[0]
    assert w.event_ids == ("a", "b", "c")
    assert math.isclose(w.vwap, (100 * 2 + 110 * 3 + 120 * 5) / 10)
    assert math.isclose(w.mean_price, 110.0)
    assert math.isclose(w.price_stddev, statistics.stdev([100, 110, 120]))
    assert math.isclose(w.volatility_bps, w.price_stddev / w.mean_price * 1e4)
    assert w.min_price == 100 and w.max_price == 120


def test_out_of_order_within_allowed_lateness_recomputes_open_window() -> None:
    # Window [0,1000) gets its first event at t=900. Then observe t=2000,
    # wm=1500 -> [0,1000) publishes (end 1000 <= 1500) and a late event for
    # it must quarantine. Instead use a tick keeping wm < 1000: after t=1400,
    # wm=900, a t=100 tick is >= wm (inside the allowance) and recomputes the
    # still-open window [0,1000).
    eng = _engine()
    eng.write_one(payload("first", seq=1, event_time_ms=900, price=100, quantity=1))
    eng.write_one(payload("ahead", seq=2, event_time_ms=1200, price=200, quantity=1))
    # wm is now 1200-500=700; window [0,1000) is still OPEN.
    assert eng.query_windows("BTCUSD") == []
    r = eng.write_one(payload("ooo", seq=3, event_time_ms=700, price=50, quantity=1))
    assert r.committed and r.quarantined == 0
    wins = {w.window_start_ms: w for w in
            eng.query_windows("BTCUSD", include_unpublished=True)}
    assert wins[0].event_ids == ("first", "ooo")
    assert wins[0].recomputed is True
    assert math.isclose(wins[0].vwap, (100 * 1 + 50 * 1) / 2)


def test_late_beyond_watermark_is_quarantined_and_does_not_mutate() -> None:
    eng = _engine()
    eng.write_one(payload("a", seq=1, event_time_ms=0, price=100, quantity=1))
    eng.write_one(payload("b", seq=2, event_time_ms=500, price=110, quantity=1))
    eng.write_one(payload("c", seq=3, event_time_ms=2000, price=1, quantity=1))
    published = eng.query_windows("BTCUSD")
    assert [w.window_start_ms for w in published] == [0]
    frozen = published[0]

    r = eng.write_one(payload("late1", seq=4, event_time_ms=100, price=1, quantity=1))
    assert r.quarantined == 1
    rec = r.records[0]
    assert rec.reason is RejectReason.LATE_BEYOND_WATERMARK
    assert rec.quarantine_id is not None
    assert rec.watermark_ms == 1500
    q = eng.query_quarantine("BTCUSD")
    assert len(q) == 1
    assert q[0].event_id == "late1" and q[0].event_time_ms == 100
    assert q[0].watermark_ms == 1500 and q[0].window_end_ms == 1000

    again = eng.query_windows("BTCUSD")
    assert again[0] == frozen
    assert again[0].event_count == 2


def test_watermark_never_retreats(caplog) -> None:
    eng = _engine()
    eng.write_one(payload("a", seq=1, event_time_ms=3000))
    eng.write_one(payload("b", seq=2, event_time_ms=100))
    q = eng.query_quarantine("BTCUSD")
    assert len(q) == 1 and q[0].reason is RejectReason.LATE_BEYOND_WATERMARK
    assert any("watermark" in m and "advanced" in m for m in
               (rec.message for rec in caplog.records))


def test_boundary_event_time_equal_to_watermark_is_not_late() -> None:
    # After observing t=1400, wm=900: window [0,1000) remains OPEN.
    # An event at exactly t=900 (== watermark) must be accepted into it.
    eng = _engine()
    eng.write_one(payload("a", seq=1, event_time_ms=1400, price=20, quantity=1))
    r = eng.write_one(payload("b", seq=2, event_time_ms=900, price=10, quantity=1))
    assert r.committed and r.quarantined == 0
    wins = {w.window_start_ms: w for w in
            eng.query_windows("BTCUSD", include_unpublished=True)}
    assert wins[0].event_ids == ("b",)
    # an event one ms below the watermark is late
    r2 = eng.write_one(payload("c", seq=3, event_time_ms=899, price=10, quantity=1))
    assert r2.quarantined == 1


def test_cross_window_disorder_multiple_symbols() -> None:
    eng = _engine()
    rows = [
        ("x1", "X", 2000, 50, 1),
        ("y1", "Y", 2000, 500, 2),
        ("x0", "X", 100, 40, 3),
        ("y0", "Y", 100, 400, 4),
        ("xf", "X", 4000, 1, 5),
        ("yf", "Y", 4000, 1, 6),
        ("xpub", "X", 5500, 1, 7),
        ("ypub", "Y", 5500, 1, 8),
    ]
    for eid, sym, t, p, seq in rows:
        eng.write_one(payload(eid, seq=seq, symbol=sym,
                              event_time_ms=t, price=p, quantity=1))
    # First arrival per symbol is t=2000 => wm=1500; the later t=100 events
    # are behind 1500 and must be quarantined for BOTH symbols independently.
    qids = {(q.symbol, q.event_id) for q in eng.query_quarantine()}
    assert ("X", "x0") in qids and ("Y", "y0") in qids
    # Published windows only contain on-time events, per symbol
    xw = [w for w in eng.query_windows("X")]
    yw = [w for w in eng.query_windows("Y")]
    assert [w.event_ids for w in xw] == [("x1",), ("xf",)]
    assert [w.event_ids for w in yw] == [("y1",), ("yf",)]
    assert xw[0].vwap == 50 and yw[0].vwap == 500

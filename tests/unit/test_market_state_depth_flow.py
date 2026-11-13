import json
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from types import MappingProxyType

import pytest

from tests.market_fixtures import START, instrument
from tradeforge.domain.common import DataQuality
from tradeforge.domain.market import StreamState
from tradeforge.market.order_book import LocalOrderBook
from tradeforge.market.streams import MarketEvent, StreamSpec
from tradeforge.market.trade_flow import TradeFlow


def event(
    kind="depth",
    *,
    first=99,
    last=101,
    previous=98,
    generation=1,
    bids=None,
    asks=None,
    second=1,
    maker=False,
    qty="2",
):
    at = START + timedelta(seconds=second)
    row = {"s": "BTCUSDT", "E": int(at.timestamp() * 1000)}
    if kind.startswith("depth"):
        row.update(
            U=first,
            u=last,
            pu=previous,
            b=bids if bids is not None else [["100", "3"]],
            a=asks if asks is not None else [],
        )
    else:
        row.update(a=last, p="100", q=qty, m=maker, T=int(at.timestamp() * 1000))
    return MarketEvent(
        StreamSpec("btcusdt@" + kind),
        "BTCUSDT",
        at,
        at,
        generation,
        last,
        last,
        MappingProxyType({}),
        json.dumps(row),
    )


def snapshot(last=100):
    return {
        "lastUpdateId": last,
        "E": int(START.timestamp() * 1000),
        "bids": [["100", "1"], ["99", "2"]],
        "asks": [["102", "1"]],
    }


def test_partial_depth_frames_replace_the_book_without_a_rest_snapshot():
    book = LocalOrderBook(instrument())
    book.begin(1)
    assert book.ingest(event("depth20@500ms", last=101, bids=[["100", "3"]], asks=[["102", "5"]]))
    first = book.snapshot(START + timedelta(seconds=1))
    assert first.stream_state is StreamState.READY
    assert book.ingest(
        event(
            "depth20@500ms",
            first=105,
            last=106,
            previous=104,
            second=2,
            bids=[["99", "2"]],
            asks=[["103", "4"]],
        )
    )
    second = book.snapshot(START + timedelta(seconds=2))
    assert second.stream_state is StreamState.READY
    assert second.bids == ((Decimal("99"), Decimal("2")),)
    assert second.asks == ((Decimal("103"), Decimal("4")),)
    assert second.last_update_id == 106
    assert book.state_at(START + timedelta(seconds=8)) is StreamState.STALE


def test_buffered_stream_event_bridges_a_later_rest_snapshot():
    """Production order: the stream event is buffered before the snapshot.

    The REST snapshot is fetched after the stream event was received, so its
    event time is newer. Synchronisation is proven by update-id continuity
    only; an event-time comparison would discard the bridge event and the
    book could never become READY.
    """
    book = LocalOrderBook(instrument())
    book.begin(1)
    assert book.ingest(event(first=99, last=101, previous=98, second=1))
    later = {
        "lastUpdateId": 100,
        "E": int((START + timedelta(seconds=5)).timestamp() * 1000),
        "bids": [["100", "1"], ["99", "2"]],
        "asks": [["102", "1"]],
    }
    book.install(later, START + timedelta(seconds=6), generation=1)
    view = book.snapshot(START + timedelta(seconds=6))
    assert view.stream_state is StreamState.READY
    assert view.last_update_id == 101


def test_snapshot_bridge_accepts_a_direct_update_id_successor():
    """The first event after a snapshot may continue it exactly."""
    book = LocalOrderBook(instrument())
    book.begin(1)
    book.install(snapshot(100), START + timedelta(seconds=2), generation=1)
    assert book.snapshot(START + timedelta(seconds=2)).stream_state is StreamState.UNSYNCED
    assert book.ingest(event(first=101, last=101, previous=100, second=3))
    view = book.snapshot(START + timedelta(seconds=3))
    assert view.stream_state is StreamState.READY
    assert view.last_update_id == 101


def test_depth_snapshot_bridge_continuity_gap_and_rebuild():
    book = LocalOrderBook(instrument())
    book.begin(1)
    book.ingest(event())
    book.install(snapshot(), START + timedelta(seconds=2), generation=1)
    view = book.snapshot(START + timedelta(seconds=2))
    assert view.stream_state is StreamState.READY
    assert view.last_update_id == 101 and view.bids[0][1] == 3
    book.ingest(event(first=102, last=102, previous=101, second=3, bids=[["99", "0"]]))
    assert len(book.snapshot(START + timedelta(seconds=3)).bids) == 1
    book.ingest(event(first=104, last=104, previous=103, second=4))
    assert book.snapshot(START + timedelta(seconds=4)).stream_state is StreamState.UNSYNCED
    book.install(snapshot(104), START + timedelta(seconds=5), generation=1)
    assert book.snapshot(START + timedelta(seconds=5)).stream_state is StreamState.READY


def test_snapshot_without_bridge_is_not_ready_and_generation_fenced():
    book = LocalOrderBook(instrument())
    book.begin(2)
    book.install(snapshot(), START, generation=2)
    assert book.snapshot(START).stream_state is StreamState.UNSYNCED
    assert not book.ingest(event(generation=1))
    assert book.snapshot(START + timedelta(seconds=1)).stream_state is StreamState.UNSYNCED
    with pytest.raises(ValueError, match="GENERATION"):
        book.install(snapshot(), START, generation=1)


@pytest.mark.parametrize("seconds", [1, 2, 30])
def test_bounded_book_preserves_full_snapshot_evidence_and_freshness(seconds):
    book = LocalOrderBook(instrument())
    book.begin(1)
    book.ingest(event())
    book.install(snapshot(), START + timedelta(seconds=2), generation=1)
    at = START + timedelta(seconds=seconds)
    full = book.snapshot(at)
    limited = book.snapshot(at, limit=1)
    assert limited.bids == full.bids[:1] and limited.asks == full.asks[:1]
    assert limited.model_dump(exclude={"bids", "asks"}) == full.model_dump(exclude={"bids", "asks"})
    assert book.state_at(at) is full.stream_state


@pytest.mark.parametrize("limit", [0, -1, True, 10001])
def test_invalid_book_handoff_limit_is_rejected(limit):
    with pytest.raises(ValueError, match="BOOK_DEPTH_LIMIT"):
        LocalOrderBook(instrument()).snapshot(START, limit=limit)


def test_handoff_limit_can_exceed_the_configured_book_capacity():
    book = LocalOrderBook(instrument(), max_levels=4)
    book.begin(1)
    book.ingest(event())
    book.install(snapshot(), START + timedelta(seconds=2), generation=1)
    at = START + timedelta(seconds=2)
    assert book.snapshot(at, limit=20) == book.snapshot(at)


def test_depth_duplicate_crossing_overflow_and_stale():
    book = LocalOrderBook(instrument(), max_pending=2, max_levels=4)
    book.begin(1)
    book.ingest(event())
    book.install(snapshot(), START + timedelta(seconds=1), generation=1)
    assert not book.ingest(event())
    assert book.snapshot(START + timedelta(seconds=30)).stream_state is StreamState.STALE
    book.ingest(event(first=102, last=102, previous=101, bids=[["103", "2"]], second=2))
    assert book.snapshot(START + timedelta(seconds=2)).stream_state is StreamState.UNSYNCED
    book.begin(2)
    for n in range(3):
        book.ingest(
            event(last=200 + n, first=200 + n, previous=199 + n, generation=2, second=3 + n)
        )
    assert len(book.pending) <= 2
    assert book.snapshot(START + timedelta(seconds=6)).stream_state is StreamState.UNSYNCED


def test_flow_survives_a_stream_generation_change_without_losing_evidence():
    """A reconnect is not missing evidence while the trade ids stay contiguous.

    The aggregate trade id is contiguous per symbol, so the first trade after
    a reconnect proves that no trade was missed. Restarting the availability
    window there would hide a healthy flow for a whole period.
    """
    flow = TradeFlow(instrument(), capacity=8, period_seconds=10)
    flow.begin(1, START)
    assert flow.ingest(event("aggTrade", last=1, second=1))
    assert flow.ingest(event("aggTrade", last=2, second=2, maker=True, qty="0.5"))
    flow.begin(2, START + timedelta(seconds=3))
    assert flow.ingest(event("aggTrade", last=3, second=4, generation=2))
    assert flow.snapshot(START + timedelta(seconds=12)).validity is DataQuality.OK
    # A real discontinuity is still reported as a gap.
    assert flow.ingest(event("aggTrade", last=5, second=5, generation=2))
    assert flow.snapshot(START + timedelta(seconds=12)).validity is DataQuality.GAP


def test_trade_flow_exact_signed_dedup_missing_and_point_in_time():
    flow = TradeFlow(instrument(), capacity=8, period_seconds=10)
    flow.begin(1, START)
    flow.ingest(event("aggTrade", last=1, second=1))
    flow.ingest(event("aggTrade", last=2, second=2, maker=True, qty="0.5"))
    assert not flow.ingest(event("aggTrade", last=2, second=2, maker=True, qty="0.5"))
    first = flow.snapshot(START + timedelta(seconds=1))
    assert first.signed_taker_flow == Decimal("2") and first.count == 1
    result = flow.snapshot(START + timedelta(seconds=11))
    assert result.signed_taker_flow == Decimal("-0.5")
    assert result.validity is DataQuality.OK
    flow.ingest(event("aggTrade", last=4, second=12))
    assert flow.snapshot(START + timedelta(seconds=12)).validity is DataQuality.GAP
    assert flow.snapshot(START + timedelta(seconds=50)).validity is DataQuality.STALE


def test_flow_overflow_and_bad_payload_fail_closed():
    flow = TradeFlow(instrument(), capacity=1, period_seconds=10)
    flow.begin(1, START)
    for i in range(1, 3):
        flow.ingest(event("aggTrade", last=i, second=i))
    assert flow.snapshot(START + timedelta(seconds=11)).validity is DataQuality.OK
    assert flow.snapshot(START + timedelta(seconds=2)).validity is DataQuality.GAP
    with pytest.raises(ValueError):
        flow.ingest(event("aggTrade", last=3, second=3, maker=1))
    assert flow.snapshot(START + timedelta(seconds=3)).validity is not DataQuality.OK


def test_previous_window_includes_late_receipt_known_at_evaluation():
    flow = TradeFlow(instrument(), period_seconds=10)
    flow.begin(1, START)
    boundary = START + timedelta(seconds=10)
    now = boundary + timedelta(milliseconds=500)
    flow.ingest(replace(event("aggTrade", last=1, second=10), received_at=now))
    assert flow.snapshot(boundary).validity is DataQuality.STALE
    previous = flow.snapshot(boundary, known_at=now)
    assert previous.validity is DataQuality.OK
    assert previous.source_end == boundary and previous.observed_at == now
    assert flow.snapshot(boundary + timedelta(seconds=10)).count == 0

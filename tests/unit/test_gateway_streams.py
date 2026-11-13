import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tradeforge.market.streams import StreamSpec, decode_events
from tradeforge.market.subscription_budget import MarketQueue, plan_subscriptions

NOW = datetime(2026, 10, 16, 12, tzinfo=UTC)
MS = int(NOW.timestamp() * 1000)


def ticker(symbol="BTCUSDT", **updates):
    return {
        "e": "24hrTicker",
        "E": MS,
        "s": symbol,
        "c": "100",
        "o": "99",
        "h": "101",
        "l": "98",
        "v": "10",
        "q": "1000",
        "P": "1",
        **updates,
    }


def mark(symbol="BTCUSDT", **updates):
    return {
        "e": "markPriceUpdate",
        "E": MS,
        "s": symbol,
        "p": "100",
        "i": "100",
        "r": "0.0001",
        "T": MS + 10000,
        **updates,
    }


def decode(rows, stream="!ticker@arr", **kw):
    return decode_events(
        json.dumps({"stream": stream, "data": rows}),
        route="market",
        received_at=NOW,
        generation=1,
        sequence=7,
        **kw,
    )


def test_global_wire_envelope_and_exact_money():
    (event,) = decode([ticker()], eligible=frozenset({"BTCUSDT"}))
    assert event.symbol == "BTCUSDT"
    assert event.event_time == event.received_at == NOW
    assert (event.generation, event.sequence, event.schema_version) == (1, 7, 1)
    assert event.values["c"] == Decimal("100")
    assert event.stream.route == "market"
    assert event.exchange_sequence is None  # Never invent exchange continuity for ticker.
    assert decode([ticker("OTHERUSDT")], eligible=frozenset({"BTCUSDT"})) == ()


@pytest.mark.parametrize(
    "changes",
    [
        {"c": "NaN"},
        {"c": "-0"},
        {"c": 100},
        {"c": "-1"},
        {"v": "-1"},
        {"E": MS + 5000},
        {"E": True},
        {"market_type": "COIN_M"},
        {"st": 2},
        {"e": "ACCOUNT_UPDATE"},
    ],
)
def test_invalid_global_input_rejected(changes):
    with pytest.raises(ValueError):
        decode([ticker(**changes)], eligible=frozenset({"BTCUSDT"}))


def test_source_staleness_duplicate_keys_and_mixed_market():
    # A market array carries one row per symbol. A symbol that has not traded
    # keeps its own old timestamp; that row is excluded and the valid peers
    # of the same frame must survive.
    peers = decode(
        [ticker("BTCUSDT"), ticker("ETHUSDT", E=MS - 60000)],
        eligible=frozenset({"BTCUSDT", "ETHUSDT"}),
    )
    assert [event.symbol for event in peers] == ["BTCUSDT"]
    # A single-symbol stream with a stale event is a broken stream.
    with pytest.raises(ValueError):
        decode(
            [
                {
                    "e": "kline",
                    "E": MS - 60000,
                    "s": "BTCUSDT",
                    "k": {
                        "t": MS - 900000,
                        "T": MS - 1,
                        "s": "BTCUSDT",
                        "i": "15m",
                        "o": "100",
                        "c": "101",
                        "h": "102",
                        "l": "99",
                        "v": "10",
                        "x": True,
                    },
                }
            ],
            stream="btcusdt@kline_15m",
            eligible=frozenset({"BTCUSDT"}),
        )
    # A clock that is trusted within a skew may not reject every event of
    # that stream. The tolerance must match the declared clock policy.
    future = decode([ticker(E=MS + 2000)], eligible=frozenset({"BTCUSDT"}), max_future=3)
    assert len(future) == 1
    with pytest.raises(ValueError):
        decode([ticker(E=MS + 2000)], eligible=frozenset({"BTCUSDT"}))
    with pytest.raises(ValueError):
        decode([ticker(E=MS + 5000)], eligible=frozenset({"BTCUSDT"}), max_future=3)
    with pytest.raises(ValueError):
        decode_events(
            '{"stream":"!ticker@arr","stream":"x","data":[]}',
            route="market",
            received_at=NOW,
            generation=1,
            sequence=1,
            eligible=frozenset({"BTCUSDT"}),
        )
    # Known COIN-M row must never be admitted under the same symbol.
    with pytest.raises(ValueError):
        decode([ticker(), ticker(st=2)], eligible=frozenset({"BTCUSDT"}))


def test_documented_merged_um_cm_array_retains_all_um_members():
    events = decode([ticker(st=1), ticker("BTCUSD_PERP", st=2)], eligible=frozenset({"BTCUSDT"}))
    assert [e.symbol for e in events] == ["BTCUSDT"]


def test_routes_and_private_stream_rejection():
    assert StreamSpec("btcusdt@bookTicker").route == "public"
    assert StreamSpec("btcusdt@depth@100ms").route == "public"
    assert StreamSpec("btcusdt@aggTrade").route == "market"
    assert StreamSpec("btcusdt@kline_15m").route == "market"
    for bad in ("ACCOUNT_UPDATE", "key@account", "btcusdt@unknown", "BTCUSDT@ticker"):
        with pytest.raises(ValueError):
            StreamSpec(bad)
    with pytest.raises(ValueError):
        decode_events(
            json.dumps({"stream": "!ticker@arr", "data": [ticker()]}),
            route="public",
            received_at=NOW,
            generation=1,
            sequence=1,
            eligible=frozenset({"BTCUSDT"}),
        )


def test_budget_covers_every_member_and_prioritizes_protected():
    symbols = frozenset(f"C{i}USDT" for i in range(125))
    plan = plan_subscriptions(
        symbols,
        watched=tuple(sorted(symbols)),
        deep=("C0USDT", "C1USDT"),
        protected=("C124USDT",),
        max_sockets=8,
        streams_per_socket=128,
    )
    assert plan.eligible == symbols
    names = {s.name for group in plan.groups for s in group}
    assert {
        "!ticker@arr",
        "!markPrice@arr@1s",
        "c124usdt@depth@100ms",
        "c124usdt@aggTrade",
    } <= names
    # Every eligible pair receives its mandatory closed 15m stream.
    assert {f"{symbol.lower()}@kline_15m" for symbol in symbols} <= names
    assert all(len(g) <= 128 for g in plan.groups)
    for group in plan.groups:
        assert len({s.route for s in group}) == 1
    # A budget too small for the mandatory sweep fails closed; optional feeds
    # are the only work that may be omitted.
    constrained = plan_subscriptions(
        symbols, watched=tuple(sorted(symbols)), max_sockets=8, streams_per_socket=32
    )
    required = {f"{symbol.lower()}@kline_15m" for symbol in symbols}
    assert required <= {s.name for group in constrained.groups for s in group}
    assert constrained.omitted
    with pytest.raises(ValueError):
        plan_subscriptions(symbols, protected=tuple(symbols), max_sockets=1)
    with pytest.raises(ValueError):
        plan_subscriptions(symbols, watched=("WRONGUSDT",))


def test_queue_sheds_only_market_observations_in_priority_order():
    (global_event,) = decode([ticker()], eligible=frozenset({"BTCUSDT"}))
    queue = MarketQueue(2)
    assert queue.put(global_event, priority=0)
    assert queue.put(global_event, priority=1)
    assert queue.put(global_event, priority=3)
    assert queue.dropped == 1
    assert [queue.get().priority, queue.get().priority] == [3, 1]
    with pytest.raises(ValueError):
        queue.put(object(), priority=3)
    assert len(queue) == 0


def test_queue_bounds_payload_bytes_as_well_as_item_count():
    (event,) = decode([ticker()], eligible=frozenset({"BTCUSDT"}))
    size = len(event.payload_json.encode())
    queue = MarketQueue(100, max_bytes=size + 1)
    queue.put(event, priority=1)
    queue.put(event, priority=2)
    assert len(queue) == 1
    assert queue.dropped == 1
    assert queue.byte_size <= size + 1
    oversized = MarketQueue(100, max_bytes=1)
    assert not oversized.put(event, priority=3)


def test_mark_funding_and_old_source_handling():
    (event,) = decode(
        [mark(r="-0.001")], stream="!markPrice@arr@1s", eligible=frozenset({"BTCUSDT"})
    )
    assert event.values["r"] == Decimal("-0.001")
    assert event.event_time <= NOW + timedelta(seconds=1)


@pytest.mark.parametrize(
    "suffix,row,exchange_id",
    [
        ("bookTicker", {"e": "bookTicker", "b": "99", "a": "100", "B": "1", "A": "2", "u": 5}, 5),
        (
            "depth@100ms",
            {"e": "depthUpdate", "b": [["99", "0"]], "a": [["100", "1"]], "U": 5, "u": 6, "pu": 4},
            6,
        ),
        ("aggTrade", {"e": "aggTrade", "p": "100", "q": "2", "a": 8}, 8),
        (
            "kline_1m",
            {
                "e": "kline",
                "k": {
                    "s": "BTCUSDT",
                    "i": "1m",
                    "x": False,
                    "o": "100",
                    "h": "101",
                    "l": "99",
                    "c": "100",
                    "v": "1",
                },
            },
            None,
        ),
    ],
)
def test_watch_deep_wire_streams_preserve_exchange_identity(suffix, row, exchange_id):
    spec = StreamSpec(f"btcusdt@{suffix}")
    (event,) = decode_events(
        json.dumps({"stream": spec.name, "data": dict(row, s="BTCUSDT", E=MS)}),
        route=spec.route,
        received_at=NOW,
        generation=1,
        sequence=1,
        eligible=frozenset({"BTCUSDT"}),
    )
    assert event.exchange_sequence == exchange_id
    # Partial bars are transported as observations, never materialized as ClosedBar.
    assert event.symbol == "BTCUSDT"


def test_gateway_can_skip_open_candles_before_parsing_prices():
    from tradeforge.market.streams import decode_frame

    kwargs = {
        "route": "market",
        "received_at": NOW,
        "generation": 1,
        "sequence": 1,
        "eligible": frozenset({"BTCUSDT"}),
        "closed_only": True,
    }
    candle = {"s": "BTCUSDT", "i": "5m", "x": False}
    row = {"s": "BTCUSDT", "E": MS, "e": "kline", "k": candle}
    assert decode_frame({"stream": "btcusdt@kline_5m", "data": row}, **kwargs) == ()
    candle.update(x=True, o="100", h="101", l="99", c="100", v="1")
    assert len(decode_frame({"stream": "btcusdt@kline_5m", "data": row}, **kwargs)) == 1

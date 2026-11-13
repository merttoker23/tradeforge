"""A discretionary rebalance must not interrupt the retained market streams."""

import asyncio
import json

from tests.market_fixtures import START, instrument
from tradeforge.market.canonical_feed import CanonicalFeed
from tradeforge.market.socket_manager import SocketManager, WireFrame
from tradeforge.market.streams import StreamSpec


def connector(commands, events):
    class Socket:
        async def send_json(self, command):
            commands.append(command)

        async def receive(self):
            if events:
                return events.pop(0)
            await asyncio.sleep(0.01)
            return WireFrame("TEXT", json.dumps({"id": 1, "result": None}))

        async def pong(self, value):
            pass

    from contextlib import asynccontextmanager

    class Connector:
        @asynccontextmanager
        async def open(self, route):
            yield Socket()

    return Connector().open


def test_live_delta_subscribes_and_unsubscribes_without_reconnecting():
    commands: list[dict] = []
    applied: list[tuple[tuple[str, ...], tuple[str, ...], int]] = []
    stop = asyncio.Event()

    async def scenario():
        manager = SocketManager(
            (StreamSpec("btcusdt@kline_15m"), StreamSpec("btcusdt@aggTrade")),
            connector(commands, []),
            eligible=lambda: frozenset({"BTCUSDT", "ETHUSDT"}),
            on_event=lambda event: None,
            on_state=lambda state, generation: None,
            on_streams=lambda added, removed, generation: applied.append(
                (tuple(s.name for s in added), tuple(s.name for s in removed), generation)
            ),
        )
        task = asyncio.create_task(manager.run(stop))
        await asyncio.sleep(0.05)
        manager.update((StreamSpec("btcusdt@kline_15m"), StreamSpec("ethusdt@aggTrade")))
        await asyncio.sleep(0.4)
        stop.set()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())
    methods = [command["method"] for command in commands]
    assert methods[0] == "SUBSCRIBE"
    assert "SUBSCRIBE" in methods and "UNSUBSCRIBE" in methods
    subscribed = [
        name
        for command in commands
        if command["method"] == "SUBSCRIBE"
        for name in command["params"]
    ]
    unsubscribed = [
        name
        for command in commands
        if command["method"] == "UNSUBSCRIBE"
        for name in command["params"]
    ]
    assert "ethusdt@aggTrade" in subscribed
    assert "btcusdt@aggTrade" in unsubscribed
    # The retained stream is never unsubscribed and no connection is restarted.
    assert "btcusdt@kline_15m" not in unsubscribed
    assert methods.count("SUBSCRIBE") == 2
    # The owner is told exactly which streams changed.
    assert applied and applied[0][0] == ("ethusdt@aggTrade",)
    assert applied[0][1] == ("btcusdt@aggTrade",)


def test_subscription_waits_for_a_trusted_clock():
    """A skewed clock must not receive market evidence.

    The event decoder rejects an event whose exchange timestamp falls outside
    the trusted clock window. Subscribing before the clock is trusted would
    reject every event and restart the feed in a loop.
    """
    commands: list[dict] = []
    stop = asyncio.Event()
    trusted = [False]

    async def scenario():
        manager = SocketManager(
            (StreamSpec("btcusdt@kline_15m"),),
            connector(commands, []),
            eligible=lambda: frozenset({"BTCUSDT"}),
            on_event=lambda event: None,
            on_state=lambda state, generation: None,
            clock_ready=lambda: trusted[0],
        )
        task = asyncio.create_task(manager.run(stop))
        await asyncio.sleep(0.1)
        assert commands == []
        trusted[0] = True
        await asyncio.sleep(0.1)
        stop.set()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())
    assert [command["method"] for command in commands] == ["SUBSCRIBE"]
    assert commands[0]["params"] == ["btcusdt@kline_15m"]


def test_invalid_pair_does_not_disconnect_valid_peer():
    async def scenario():
        stop = asyncio.Event()
        invalid = []
        accepted = []
        btc, eth = StreamSpec("btcusdt@bookTicker"), StreamSpec("ethusdt@bookTicker")
        frames = [
            WireFrame("TEXT", json.dumps({"id": 1, "result": None})),
            WireFrame("TEXT", json.dumps({"stream": btc.name, "data": {}})),
            WireFrame("TEXT", json.dumps({"stream": btc.name, "data": {}})),
            WireFrame(
                "TEXT",
                json.dumps(
                    {
                        "stream": eth.name,
                        "data": {
                            "e": "bookTicker",
                            "s": "ETHUSDT",
                            "u": 1,
                            "E": int(START.timestamp() * 1000),
                            "b": "10",
                            "a": "11",
                            "B": "1",
                            "A": "1",
                        },
                    }
                ),
            ),
        ]

        def accept(event):
            accepted.append(event.symbol)
            stop.set()

        manager = SocketManager(
            (btc, eth),
            connector([], frames),
            eligible=lambda: frozenset({"BTCUSDT", "ETHUSDT"}),
            on_event=accept,
            on_state=lambda *_: None,
            on_invalid_stream=lambda stream, generation: invalid.append((stream, generation)),
            clock=lambda: START,
        )
        await asyncio.wait_for(manager.run(stop), timeout=1)
        assert invalid == [(btc, 1)]
        assert accepted == ["ETHUSDT"]
        assert manager.reconnects == 0

    asyncio.run(scenario())


def test_canonical_delta_keeps_the_retained_book_and_flow():
    feed = CanonicalFeed(clock=lambda: START)
    feed.configure((instrument(),), ("BTCUSDT",))
    feed.socket_state((StreamSpec("btcusdt@depth"), StreamSpec("btcusdt@aggTrade")), "SYNCING", 1)
    book = feed.books["BTCUSDT"]
    flow = feed.flows["BTCUSDT"]
    generation, flow_generation = book.generation, flow.generation

    # A rebalance that keeps the symbol changes nothing for it.
    feed.socket_streams((), (), 2)
    assert feed.books["BTCUSDT"] is book
    assert feed.flows["BTCUSDT"] is flow
    assert book.generation == generation
    assert flow.generation == flow_generation

    # A newly selected symbol starts a fresh local book and flow.
    feed.configure((instrument(), instrument("ETHUSDT")), ("BTCUSDT", "ETHUSDT"))
    feed.socket_streams((StreamSpec("ethusdt@depth"), StreamSpec("ethusdt@aggTrade")), (), 2)
    assert feed.books["ETHUSDT"].generation > 0
    assert feed.flows["ETHUSDT"].generation > 0
    assert feed.books["BTCUSDT"] is book
    assert feed.flows["BTCUSDT"] is flow


def test_removed_stream_loses_its_generation():
    feed = CanonicalFeed(clock=lambda: START)
    feed.configure((instrument(),), ("BTCUSDT",))
    feed.socket_state((StreamSpec("btcusdt@aggTrade"),), "SYNCING", 1)
    assert "btcusdt@aggTrade" in feed._generations
    feed.socket_streams((), (StreamSpec("btcusdt@aggTrade"),), 1)
    assert "btcusdt@aggTrade" not in feed._generations


def test_socket_group_identity_is_its_anchor_stream():
    """A group's identity survives a change of its composition."""
    from tradeforge.market.market_gateway import MarketGateway

    anchor = (StreamSpec("!ticker@arr"), StreamSpec("btcusdt@kline_15m"))
    assert MarketGateway._group_key(anchor) == "!ticker@arr"
    # The same anchor with a different composition is still the same group.
    extended = (*anchor, StreamSpec("ethusdt@aggTrade"))
    assert MarketGateway._group_key(extended) == MarketGateway._group_key(anchor)
    # A different anchor is a different group and must not reuse its socket.
    other = (StreamSpec("ethusdt@kline_15m"),)
    assert MarketGateway._group_key(other) != MarketGateway._group_key(anchor)

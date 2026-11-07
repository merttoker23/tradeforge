import asyncio
from dataclasses import replace
from datetime import timedelta

from tests.contract.test_market_state_wire import kline
from tests.market_fixtures import START, instrument
from tests.unit.test_market_state_depth_flow import event, snapshot
from tradeforge.domain.common import DataQuality
from tradeforge.domain.market import StreamState, Timeframe
from tradeforge.market.canonical_feed import CanonicalFeed


def test_feed_discontinuity_fences_generation_and_preserves_closed_history():
    feed = CanonicalFeed()
    feed.configure((instrument(),), ("BTCUSDT",))
    feed.socket_state((kline(True).stream,), "SYNCING", 1)
    feed.consume(kline(True))
    at = START + timedelta(minutes=1)
    assert feed.state.view(instrument(), Timeframe.M1, at).quality is DataQuality.OK
    feed.socket_state((kline(True).stream,), "STALE", 1)
    assert not feed.consume(kline(True))
    assert len(feed.state.view(instrument(), Timeframe.M1, at).bars) == 1
    feed.socket_state((event().stream,), "SYNCING", 1)
    feed.consume(event())
    feed.books["BTCUSDT"].install(snapshot(), START + timedelta(seconds=2), generation=1)
    assert feed.books["BTCUSDT"].state is StreamState.READY
    feed.lost(event())
    assert feed.books["BTCUSDT"].state is StreamState.UNSYNCED
    feed.configure((), ())
    assert not feed.books and not feed.flows


def test_selection_addition_preserves_unchanged_deep_generation_and_book():
    feed = CanonicalFeed()
    feed.configure((instrument(),), ("BTCUSDT",))
    feed.socket_state((event().stream,), "SYNCING", 1)
    assert feed.consume(event())
    book = feed.books["BTCUSDT"]
    book.install(snapshot(), START + timedelta(seconds=2), generation=1)
    assert book.state is StreamState.READY
    feed.configure((instrument(), instrument("ETHUSDT")), ("BTCUSDT",))
    assert feed.books["BTCUSDT"] is book
    assert feed._generations[event().stream.name] == 1
    assert feed.consume(event(first=102, last=102, previous=101))
    feed.configure((instrument("ETHUSDT"),), ())
    assert not feed.consume(event(first=103, last=103, previous=102))
    assert not feed.books and not feed.flows


def test_feed_worker_repairs_and_persists_outside_ingestion():
    async def scenario():
        feed = CanonicalFeed()
        feed.configure((instrument(),), ())
        feed.socket_state((kline(True).stream,), "SYNCING", 1)
        assert not feed.consume(replace(kline(True), generation=0))
        assert feed.consume(kline(True))
        saved = []

        class Recovery:
            async def bars(self, key, timeframe, start, end, **kwargs):
                return True

            async def depth(self, book):
                return False

        def save(view):
            saved.append(view)

        await feed.maintain(Recovery(), save, START + timedelta(minutes=1))
        assert saved and saved[0].bars

    asyncio.run(scenario())


def test_dirty_generation_survives_new_bar_during_worker_write():
    async def scenario():
        feed = CanonicalFeed()
        feed.configure((instrument(),), ())
        stream = kline(True).stream
        feed.socket_state((stream,), "SYNCING", 1)
        feed.consume(kline(True))

        class Recovery:
            async def bars(self, *args, **kwargs):
                return False

            async def depth(self, *args):
                return False

        def save(view):
            from tests.market_fixtures import bar

            feed.state.ingest_bar(bar(1, Timeframe.M1))
            feed._dirty.add(("BTCUSDT", Timeframe.M1))

        await feed.maintain(Recovery(), save, START + timedelta(minutes=1))
        assert ("BTCUSDT", Timeframe.M1) in feed._dirty

    asyncio.run(scenario())


def test_symbol_churn_respects_active_budget():
    feed = CanonicalFeed(max_symbols=1)
    for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        feed.configure((instrument(symbol),), ())
        from tradeforge.market.streams import StreamSpec

        for tf in (Timeframe.M1, Timeframe.M5, Timeframe.M15):
            feed.socket_state((StreamSpec(f"{symbol.lower()}@kline_{tf}"),), "SYNCING", 1)
        assert len(feed.state._series) <= 3


def test_new_loss_during_repair_keeps_repair_pending():
    async def scenario():
        feed = CanonicalFeed()
        feed.configure((instrument(),), ())
        feed.socket_state((kline(True).stream,), "SYNCING", 1)

        class Recovery:
            async def bars(self, *args, **kwargs):
                feed.lost(kline(True))
                return True

            async def depth(self, *args):
                return False

        await feed.maintain(Recovery(), lambda view: None, START + timedelta(minutes=1))
        assert ("BTCUSDT", Timeframe.M1) in feed._repair

    asyncio.run(scenario())


def test_other_socket_reset_invalidates_cleared_depth_queue():
    from tradeforge.market.clock_sync import ClockSync
    from tradeforge.market.market_gateway import MarketGateway
    from tradeforge.market.streams import StreamSpec

    gateway = MarketGateway(ClockSync())
    gateway.canonical.configure((instrument(),), ("BTCUSDT",))
    gateway.canonical.socket_state((event().stream,), "SYNCING", 1)
    gateway.canonical.consume(event())
    book = gateway.canonical.books["BTCUSDT"]
    book.install(snapshot(), START + timedelta(seconds=2), generation=1)
    gateway.queue.put(event(first=102, last=102, previous=101), priority=2)
    gateway.socket_state((StreamSpec("!ticker@arr"),), "STALE", 1)
    assert book.state is StreamState.UNSYNCED


def test_worker_discovers_silent_bar_gap_without_socket_disconnect():
    async def scenario():
        from tests.market_fixtures import bar
        from tradeforge.market.streams import StreamSpec

        at = bar(2).observed_at
        feed = CanonicalFeed(clock=lambda: at)
        feed.configure((instrument(),), ())
        feed.socket_state((StreamSpec("btcusdt@kline_15m"),), "SYNCING", 1)
        feed._repair.clear()
        feed.state.mark(instrument(), Timeframe.M15, DataQuality.OK, START)
        feed.state.ingest_bar(bar(0))
        feed.state.ingest_bar(bar(2))
        repaired = []

        class Recovery:
            async def bars(self, key, tf, start, end, **kwargs):
                repaired.append(tf)
                return False

            async def depth(self, book):
                return False

        await feed.maintain(Recovery(), lambda view: None, at)
        assert repaired == [Timeframe.M15]

    asyncio.run(scenario())

"""A slow candle repair must not starve the selected order book."""

import asyncio

from tests.market_fixtures import START, instrument
from tradeforge.domain.market import Timeframe
from tradeforge.market.canonical_feed import CanonicalFeed
from tradeforge.market.streams import StreamSpec


def test_depth_snapshot_runs_before_slow_unrelated_bar_repair():
    async def scenario():
        feed = CanonicalFeed(clock=lambda: START)
        feed.configure((instrument(),), ("BTCUSDT",))
        feed.socket_state(
            (StreamSpec("btcusdt@depth"), StreamSpec("btcusdt@kline_5m")), "SYNCING", 1
        )
        depth = asyncio.Event()

        class Recovery:
            async def bars(self, *args, **kwargs):
                await asyncio.Event().wait()

            async def depth(self, book):
                depth.set()
                return False

        task = asyncio.create_task(feed.maintain(Recovery(), lambda view: None, START))
        try:
            await asyncio.wait_for(depth.wait(), 0.2)
            assert ("BTCUSDT", Timeframe.M5) in feed._repair
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())


def test_storage_failure_is_safe_and_a_successful_retry_recovers():
    from tests.market_fixtures import bar

    feed = CanonicalFeed(clock=lambda: bar().as_of)
    feed.configure((instrument(),), ())
    feed.state.ingest_bar(bar())
    feed._dirty.add(("BTCUSDT", Timeframe.M15))

    def fail(view):
        raise ValueError("PRIVATE_DATABASE_SENTINEL")

    asyncio.run(feed.maintain(None, fail, bar().as_of))
    assert feed.storage_ready is False
    assert getattr(feed, "storage_error", None) == "INPUT_INVALID"
    assert "PRIVATE_DATABASE_SENTINEL" not in str(feed.storage_error)
    asyncio.run(feed.maintain(None, lambda view: None, bar().as_of))
    assert feed.storage_ready is True and feed.storage_error is None


def test_depth_can_recover_again_while_candle_worker_is_still_blocked():
    async def scenario():
        from types import SimpleNamespace

        from apps.market_gateway import runtime

        feed = CanonicalFeed(clock=lambda: START)
        feed.configure((instrument(),), ("BTCUSDT",))
        feed.socket_state(
            (StreamSpec("btcusdt@depth"), StreamSpec("btcusdt@kline_5m")), "SYNCING", 1
        )
        seen = 0
        entered = asyncio.Event()

        class Recovery:
            async def bars(self, *args, **kwargs):
                entered.set()
                await asyncio.Event().wait()

            async def depth(self, book):
                nonlocal seen
                seen += 1
                return False

        gateway = SimpleNamespace(canonical=feed)
        repair = asyncio.create_task(
            feed.maintain(Recovery(), lambda view: None, START, recover_depth=False)
        )
        try:
            await asyncio.wait_for(entered.wait(), 0.5)
            await runtime.depth_cycle(gateway, None, Recovery(), require_admission=False)
            await runtime.depth_cycle(gateway, None, Recovery(), require_admission=False)
            assert seen == 2 and not repair.done()
        finally:
            repair.cancel()
            await asyncio.gather(repair, return_exceptions=True)

    asyncio.run(scenario())

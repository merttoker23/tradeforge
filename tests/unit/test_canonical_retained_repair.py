"""Repair retained evidence without dropping gaps or relaxing source freshness."""

import asyncio
import json
from datetime import timedelta

import pytest

from tests.market_fixtures import START, bar, instrument
from tradeforge.domain.common import DataQuality
from tradeforge.domain.market import Timeframe
from tradeforge.market.canonical_feed import CanonicalFeed
from tradeforge.market.exchange_info import Reply
from tradeforge.market.recovery import MarketRecovery, PublicMarketClient
from tradeforge.market.streams import StreamSpec


def test_repair_fills_retained_gap_older_than_default_tail():
    async def scenario():
        at = bar(129, Timeframe.M5).as_of
        feed = CanonicalFeed(capacity=256, clock=lambda: at)
        feed.configure((instrument(),), ())
        feed.socket_state((StreamSpec("btcusdt@kline_5m"),), "SYNCING", 1)
        original = tuple(bar(i, Timeframe.M5) for i in range(130) if i != 10)
        for sample in original:
            feed.state.ingest_bar(sample)
        requested = []

        async def get(path, params):
            assert path == "/fapi/v1/klines"
            start = int(params["startTime"])
            end = int(params["endTime"]) + 1
            requested.append((start, end))
            rows = [
                [opened, "100", "110", "90", "101", "2", opened + 299999, "0", 1, "0", "0", "0"]
                for opened in range(start, end, 300000)
            ]
            return Reply(200, {}, json.dumps(rows).encode())

        client = PublicMarketClient(get=get, clock=lambda: at, min_interval=0)
        assert feed.state.view(instrument(), Timeframe.M5, at).quality is not DataQuality.OK
        await feed.maintain(MarketRecovery(feed.state, client), lambda view: None, at)
        view = feed.state.view(instrument(), Timeframe.M5, at)
        assert view.quality is DataQuality.OK
        assert len(view.bars) == 130
        assert all(sample in view.evidence for sample in original)
        missing = int((START + timedelta(minutes=50)).timestamp() * 1000)
        assert any(start <= missing < end for start, end in requested)
        assert feed.symbol_ready("BTCUSDT", at)
        assert not feed._repair

    asyncio.run(scenario())


@pytest.mark.parametrize("unrelated", [True, False])
def test_recovery_fences_only_target_stream_changes(unrelated):
    async def scenario():
        at = bar(100).as_of
        feed = CanonicalFeed(capacity=128, clock=lambda: at)
        feed.configure((instrument(),), ())
        stream = StreamSpec("btcusdt@kline_15m")
        feed.socket_state((stream,), "SYNCING", 1)

        async def get(path, params):
            feed.socket_state(
                (StreamSpec("!ticker@arr"),) if unrelated else (stream,), "SYNCING", 2
            )
            start = int(params["startTime"])
            end = int(params["endTime"]) + 1
            rows = [
                [opened, "100", "110", "90", "101", "2", opened + 899999, "0", 1, "0", "0", "0"]
                for opened in range(start, end, 900000)
            ]
            return Reply(200, {}, json.dumps(rows).encode())

        client = PublicMarketClient(get=get, clock=lambda: at, min_interval=0)
        await feed.maintain(MarketRecovery(feed.state, client), lambda view: None, at)
        assert feed.symbol_ready("BTCUSDT", at) is unrelated
        assert bool(feed._repair) is not unrelated
        if unrelated:
            assert len(feed.state.view(instrument(), Timeframe.M15, at).bars) == 96
        else:
            assert feed.state.view(instrument(), Timeframe.M15, at).bars == ()

    asyncio.run(scenario())


def test_success_reply_does_not_clear_incomplete_repair():
    async def scenario():
        at = bar().as_of
        feed = CanonicalFeed(clock=lambda: at)
        feed.configure((instrument(),), ())
        feed.socket_state((StreamSpec("btcusdt@kline_15m"),), "SYNCING", 1)

        class IncompleteRecovery:
            async def bars(self, *args, **kwargs):
                return True

            async def depth(self, book):
                return False

        await feed.maintain(IncompleteRecovery(), lambda view: None, at)
        assert ("BTCUSDT", Timeframe.M15) in feed._repair
        assert not feed.symbol_ready("BTCUSDT", at)

    asyncio.run(scenario())


@pytest.mark.parametrize("timeframe", [Timeframe.M5, Timeframe.M15])
def test_canonical_rejection_identifies_the_incomplete_timeframe(timeframe):
    at = bar(1, timeframe).as_of
    feed = CanonicalFeed(clock=lambda: at)
    feed.configure((instrument(),), ())
    feed.socket_state((StreamSpec(f"btcusdt@kline_{timeframe.value}"),), "SYNCING", 1)
    feed.state.ingest_bar(bar(1, timeframe))
    feed.state.mark(instrument(), timeframe, DataQuality.OK, at)
    feed._repair.clear()
    assert feed.bar_rejections("BTCUSDT", at) == ()
    stale = at + timedelta(seconds=timeframe.seconds + 6)
    assert feed.bar_rejections("BTCUSDT", stale) == (f"CANONICAL_{timeframe.value.upper()}_STALE",)
    assert not feed.symbol_ready("BTCUSDT", stale)

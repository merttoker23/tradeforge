import asyncio
import json
from datetime import timedelta

import pytest

from tests.market_fixtures import START, instrument
from tradeforge.domain.common import DataQuality
from tradeforge.domain.market import Timeframe
from tradeforge.market.exchange_info import FetchError, Reply
from tradeforge.market.market_state import MarketState
from tradeforge.market.recovery import MarketRecovery, PublicMarketClient


def rows(count=3, missing=None):
    return [
        [
            int((START + timedelta(minutes=i)).timestamp() * 1000),
            "100",
            "110",
            "90",
            "101",
            "2",
            int((START + timedelta(minutes=i + 1)).timestamp() * 1000) - 1,
            "0",
            2,
            "0",
            "0",
            "0",
        ]
        for i in range(count)
        if i != missing
    ]


def test_backfill_receipt_time_and_partial_exclusion():
    async def scenario():
        calls = []
        at = START + timedelta(minutes=2, seconds=1)

        async def get(path, params):
            calls.append((path, params))
            return Reply(200, {}, json.dumps(rows()).encode())

        client = PublicMarketClient(get=get, clock=lambda: at, min_interval=0)
        state = MarketState(8)
        recovery = MarketRecovery(state, client)
        assert await recovery.bars(instrument(), Timeframe.M1, START, START + timedelta(minutes=2))
        assert state.view(instrument(), Timeframe.M1, at - timedelta(seconds=1)).bars == ()
        view = state.view(instrument(), Timeframe.M1, at)
        assert len(view.bars) == 2 and view.quality is DataQuality.OK
        assert all(b.observed_at == at for b in view.bars)
        assert calls[0][0] == "/fapi/v1/klines"

    asyncio.run(scenario())


def test_missing_backfill_is_fail_closed_and_retry_recovers():
    async def scenario():
        at = START + timedelta(minutes=3, seconds=1)
        missing = [1]

        async def get(path, params):
            return Reply(200, {}, json.dumps(rows(missing=missing[0])).encode())

        state = MarketState(8)
        recovery = MarketRecovery(
            state, PublicMarketClient(get=get, clock=lambda: at, min_interval=0)
        )
        end = START + timedelta(minutes=3)
        assert not await recovery.bars(instrument(), Timeframe.M1, START, end)
        assert state.view(instrument(), Timeframe.M1, at).quality is not DataQuality.OK
        missing[0] = None
        assert await recovery.bars(instrument(), Timeframe.M1, START, end)
        assert state.view(instrument(), Timeframe.M1, at).quality is DataQuality.OK

    asyncio.run(scenario())


def test_rate_limit_and_timeouts_are_bounded():
    async def scenario():
        calls = []

        async def banned(path, params):
            calls.append(path)
            return Reply(429, {"Retry-After": "60"}, b"redacted")

        client = PublicMarketClient(get=banned, min_interval=0)
        with pytest.raises(FetchError, match="RATE_LIMIT"):
            await client.fetch("/fapi/v1/depth", {"symbol": "BTCUSDT", "limit": "1000"})
        with pytest.raises(FetchError, match="RATE_LIMIT"):
            await client.fetch("/fapi/v1/depth", {"symbol": "BTCUSDT", "limit": "1000"})
        assert len(calls) == 1

        async def timeout(path, params):
            calls.append(path)
            raise TimeoutError

        async def no_sleep(seconds):
            pass

        client = PublicMarketClient(get=timeout, sleep=no_sleep, min_interval=0)
        with pytest.raises(FetchError, match="UNAVAILABLE"):
            await client.fetch("/fapi/v1/depth", {"symbol": "BTCUSDT", "limit": "1000"})
        assert len(calls) == 4

    asyncio.run(scenario())


def test_generation_guard_prevents_obsolete_backfill_publication():
    async def scenario():
        valid = [True]
        at = START + timedelta(minutes=3, seconds=1)

        async def get(path, params):
            valid[0] = False
            return Reply(200, {}, json.dumps(rows()).encode())

        state = MarketState(8)
        recovery = MarketRecovery(
            state, PublicMarketClient(get=get, clock=lambda: at, min_interval=0)
        )
        assert not await recovery.bars(
            instrument(), Timeframe.M1, START, START + timedelta(minutes=3), valid=lambda: valid[0]
        )
        assert not state.view(instrument(), Timeframe.M1, at).bars

    asyncio.run(scenario())


def test_http_date_retry_after_is_honored():
    async def scenario():
        from email.utils import format_datetime

        async def get(path, params):
            return Reply(
                418, {"Retry-After": format_datetime(START + timedelta(hours=1), usegmt=True)}, b""
            )

        client = PublicMarketClient(
            get=get, clock=lambda: START, monotonic=lambda: 100, min_interval=0
        )
        with pytest.raises(FetchError):
            await client.fetch("/fapi/v1/depth", {})
        assert client._cooldown >= 3700

    asyncio.run(scenario())

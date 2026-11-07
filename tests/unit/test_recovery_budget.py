"""Public recovery overlaps network latency without exceeding its shared request budget."""

import asyncio
import time
from datetime import UTC, datetime

import pytest

from tradeforge.market.exchange_info import FetchError, Reply
from tradeforge.market.recovery import PublicMarketClient


def test_concurrent_recovery_keeps_aggregate_start_budget_and_bounded_inflight():
    async def scenario():
        starts, active, peak = [], 0, 0

        async def get(path, params):
            nonlocal active, peak
            starts.append(time.monotonic())
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.06)
            active -= 1
            return Reply(200, {}, b"[]")

        client = PublicMarketClient(
            get=get,
            min_interval=0.02,
            max_concurrency=2,
            clock=lambda: datetime(2026, 11, 3, tzinfo=UTC),
        )
        await asyncio.gather(*(client.fetch("/fapi/v1/klines", {}) for _ in range(4)))
        assert peak == 2
        assert all(b - a >= 0.018 for a, b in zip(starts, starts[1:], strict=False))

    asyncio.run(scenario())


def test_early_timer_wakeup_cannot_start_request_before_shared_deadline():
    async def scenario():
        mono, starts, sleeps = [10.0], [], []

        async def get(path, params):
            starts.append(mono[0])
            return Reply(200, {}, b"[]")

        async def early_sleep(delay):
            sleeps.append(delay)
            mono[0] += delay / 2 if len(sleeps) == 1 else delay

        client = PublicMarketClient(
            get=get,
            min_interval=1,
            max_concurrency=2,
            monotonic=lambda: mono[0],
            sleep=early_sleep,
        )
        for _ in range(3):
            await client.fetch("/fapi/v1/klines", {})
        assert starts == [10.0, 11.0, 12.0]

    asyncio.run(scenario())


def test_overlapping_rate_limit_responses_cannot_shorten_longer_cooldown():
    async def scenario():
        mono, count = [0.0], 0

        async def get(path, params):
            nonlocal count
            index = count
            count += 1
            if index >= 2:
                return Reply(200, {}, b"[]")
            await asyncio.sleep(0.01 * (index + 1))
            return Reply(429, {"Retry-After": "120" if index == 0 else "60"}, b"[]")

        client = PublicMarketClient(
            get=get, min_interval=0, max_concurrency=2, monotonic=lambda: mono[0]
        )
        outcomes = await asyncio.gather(
            client.fetch("/fapi/v1/klines", {}),
            client.fetch("/fapi/v1/klines", {}),
            return_exceptions=True,
        )
        assert all(isinstance(outcome, FetchError) for outcome in outcomes)
        mono[0] = 90
        with pytest.raises(FetchError, match="MARKET_RATE_LIMITED"):
            await client.fetch("/fapi/v1/klines", {})

    asyncio.run(scenario())

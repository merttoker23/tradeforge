"""Retries for a missing/new listing must not suppress completed market analysis."""

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

from tests.market_fixtures import instrument
from tradeforge.market.runtime import GatewayPersistence


def test_partial_sweep_stays_complete_while_failed_symbols_retry(monkeypatch):
    at = datetime(2026, 10, 23, tzinfo=UTC)
    observation = SimpleNamespace(
        universe=SimpleNamespace(eligible=(instrument("BTCUSDT"), instrument("ETHUSDT"))),
        version_hash="known",
    )
    gateway = SimpleNamespace(
        clock=lambda: at,
        monotonic=lambda: 1,
        clock_sync=SimpleNamespace(ready=lambda *args: True),
        view=SimpleNamespace(ready=True, observation=observation),
    )
    worker = GatewayPersistence(gateway, None)
    retry_states = []
    stop = asyncio.Event()

    async def recovery(self, key, *args, **kwargs):
        if key.symbol == "ETHUSDT":
            retry_states.append(worker.history_complete)
            return False
        return True

    async def pause(event, seconds):
        if len(retry_states) >= 2:
            stop.set()

    monkeypatch.setattr("tradeforge.market.runtime.MarketRecovery.bars", recovery)
    monkeypatch.setattr("tradeforge.market.runtime.save_view", lambda *args, **kwargs: None)
    monkeypatch.setattr("tradeforge.market.runtime.pause", pause)
    asyncio.run(worker.history(stop))
    assert retry_states == [False, True]
    assert worker.history_reason == "HISTORY_PARTIAL"


def test_slow_member_does_not_block_other_history_and_progress_is_visible(monkeypatch):
    async def scenario():
        at = datetime(2026, 10, 23, tzinfo=UTC)
        observation = SimpleNamespace(
            universe=SimpleNamespace(eligible=(instrument("BTCUSDT"), instrument("ETHUSDT"))),
            version_hash="known",
        )
        gateway = SimpleNamespace(
            clock=lambda: at,
            monotonic=lambda: 1,
            clock_sync=SimpleNamespace(ready=lambda *args: True),
            view=SimpleNamespace(ready=True, observation=observation),
        )
        worker = GatewayPersistence(gateway, None)
        fast = asyncio.Event()
        release = asyncio.Event()
        stop = asyncio.Event()

        async def recovery(self, key, *args, **kwargs):
            if key.symbol == "BTCUSDT":
                await release.wait()
                return False
            fast.set()
            return True

        monkeypatch.setattr("tradeforge.market.runtime.MarketRecovery.bars", recovery)
        monkeypatch.setattr("tradeforge.market.runtime.save_view", lambda *a, **kw: None)
        task = asyncio.create_task(worker.history(stop))
        try:
            await asyncio.wait_for(fast.wait(), 0.5)
            for _ in range(10):
                await asyncio.sleep(0.01)
                if worker.history_progress["history_completed"] == 1:
                    break
            assert worker.history_progress["history_completed"] == 1
            assert worker.history_progress["history_inflight"] == 1
            assert worker.history_complete is False
            assert worker.history_period is not None
        finally:
            stop.set()
            release.set()
            await asyncio.wait_for(task, 1)
        assert worker.history_progress["history_failed"] == 1
        assert worker.history_progress["history_attempted"] == 2
        assert worker.history_progress["history_last_failure_symbol"] == "BTCUSDT"
        assert worker.history_progress["history_last_completed_symbol"] == "ETHUSDT"
        assert worker.history_progress["history_last_member_ms"] >= 0
        assert (
            worker.history_progress["history_last_failure_reason"] == "HISTORY_RECOVERY_INCOMPLETE"
        )

    asyncio.run(scenario())


def test_slow_first_member_does_not_hold_completed_slots_in_a_fixed_batch(monkeypatch):
    async def scenario():
        at = datetime(2026, 10, 23, tzinfo=UTC)
        keys = tuple(instrument(f"C{i}USDT") for i in range(5))
        gateway = SimpleNamespace(
            clock=lambda: at,
            monotonic=lambda: 1,
            clock_sync=SimpleNamespace(ready=lambda *args: True),
            view=SimpleNamespace(
                ready=True,
                observation=SimpleNamespace(
                    universe=SimpleNamespace(eligible=keys),
                    version_hash="known",
                ),
            ),
        )
        worker = GatewayPersistence(gateway, None)
        release, fifth, stop = asyncio.Event(), asyncio.Event(), asyncio.Event()
        inflight, maximum = 0, 0
        seen = []

        async def recovery(self, key, *args, **kwargs):
            nonlocal inflight, maximum
            seen.append(key.symbol)
            inflight += 1
            maximum = max(maximum, inflight)
            try:
                if key.symbol == "C0USDT":
                    await release.wait()
                    return False
                if key.symbol == "C4USDT":
                    fifth.set()
                return True
            finally:
                inflight -= 1

        monkeypatch.setattr("tradeforge.market.runtime.MarketRecovery.bars", recovery)
        monkeypatch.setattr("tradeforge.market.runtime.save_view", lambda *a, **kw: None)
        task = asyncio.create_task(worker.history(stop))
        try:
            await asyncio.wait_for(fifth.wait(), 1)
            assert not release.is_set()
            assert "C0USDT" in seen and "C4USDT" in seen
            assert maximum <= 4
        finally:
            stop.set()
            release.set()
            await asyncio.wait_for(task, 1)

    asyncio.run(scenario())

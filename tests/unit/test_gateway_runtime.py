"""History must be clock-safe and retry transient failures within the period."""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from tradeforge.market.clock_sync import ClockSync
from tradeforge.market.instrument_registry import RegistryView
from tradeforge.market.market_gateway import MarketGateway
from tradeforge.market.runtime import GatewayPersistence


def test_unsafe_clock_cannot_start_rest_history_or_quotes(monkeypatch):
    async def scenario():
        gateway = MarketGateway(ClockSync())
        worker = GatewayPersistence(gateway, None)
        stop = asyncio.Event()

        async def paused(stop, seconds):
            stop.set()

        monkeypatch.setattr("tradeforge.market.runtime.pause", paused)
        await worker.history(stop)
        assert worker.history_reason == "CLOCK_UNSAFE"
        assert worker.history_complete is False
        assert worker.markets([], datetime.now(UTC)) == ()

    asyncio.run(scenario())


def test_history_retries_failed_symbol_without_repeating_success(monkeypatch):
    from tests.intelligence_fixtures import universe

    async def scenario():
        now = datetime.now(UTC)
        sync = ClockSync()
        sync.observe(now, now, now, 1)
        gateway = MarketGateway(sync, clock=lambda: now, monotonic=lambda: 1)
        u = universe()
        gateway.view = RegistryView(
            SimpleNamespace(universe=u, version_hash=u.version_hash), True, "OK"
        )
        gateway.eligible = frozenset(k.symbol for k in u.eligible)
        worker = GatewayPersistence(gateway, None)
        stop = asyncio.Event()
        attempts = dict.fromkeys(gateway.eligible, 0)

        async def recover(self, key, *args, **kwargs):
            attempts[key.symbol] += 1
            return key.symbol != "BTCUSDT" or attempts[key.symbol] > 1

        async def paused(stop, seconds):
            if worker.history_reason == "HISTORY_SYNCED":
                stop.set()

        monkeypatch.setattr("tradeforge.market.runtime.MarketRecovery.bars", recover)
        monkeypatch.setattr("tradeforge.market.runtime.save_view", lambda *args, **kwargs: None)
        monkeypatch.setattr("tradeforge.market.runtime.pause", paused)
        await asyncio.wait_for(worker.history(stop), timeout=3)
        assert attempts == {"BTCUSDT": 2, "ETHUSDT": 1, "SOLUSDT": 1}
        assert worker.history_complete
        assert worker.history_period <= now - timedelta(seconds=2)

    asyncio.run(scenario())


def test_completed_sweep_recovers_after_clock_interruption(monkeypatch):
    from tests.intelligence_fixtures import universe

    async def scenario():
        now = datetime.now(UTC)
        sync = ClockSync()
        sync.observe(now, now, now, 1)
        gateway = MarketGateway(sync, clock=lambda: now, monotonic=lambda: 1)
        u = universe()
        gateway.view = RegistryView(
            SimpleNamespace(universe=u, version_hash=u.version_hash), True, "OK"
        )
        worker = GatewayPersistence(gateway, None)
        stop = asyncio.Event()
        interrupted = False

        async def recover(*args, **kwargs):
            return True

        async def paused(stop, seconds):
            nonlocal interrupted
            if worker.history_reason == "HISTORY_SYNCED" and not interrupted:
                interrupted = True
                sync.invalidate()
            elif worker.history_reason == "CLOCK_UNSAFE":
                sync.observe(now, now, now, 1)
            elif worker.history_reason == "HISTORY_SYNCED" and interrupted:
                stop.set()

        monkeypatch.setattr("tradeforge.market.runtime.MarketRecovery.bars", recover)
        monkeypatch.setattr("tradeforge.market.runtime.save_view", lambda *args, **kwargs: None)
        monkeypatch.setattr("tradeforge.market.runtime.pause", paused)
        await asyncio.wait_for(worker.history(stop), timeout=3)
        assert worker.history_complete and worker.history_reason == "HISTORY_SYNCED"

    asyncio.run(scenario())

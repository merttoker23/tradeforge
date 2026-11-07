"""Account proof remains truthful across refresh and slow exchange responses."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event

from tests.integration.test_protection_recovery import prepared


def test_refresh_retains_previous_proof_until_its_original_expiry(seeded, monkeypatch):
    _, _, client, recovery, at = prepared(seeded)
    monkeypatch.setattr("tradeforge.domain.events.clock", lambda _: at + timedelta(minutes=1))
    recovery.reconcile()
    now = at + timedelta(seconds=5)
    recovery.clock = lambda: now
    entered, release = Event(), Event()
    request = client.request

    def blocked(method, path, params=None, **kwargs):
        if path == "/fapi/v3/account":
            entered.set()
            assert release.wait(5)
        return request(method, path, params, **kwargs)

    client.request = blocked
    with ThreadPoolExecutor(max_workers=1) as pool:
        attempt = pool.submit(recovery.reconcile)
        try:
            assert entered.wait(5)
            assert recovery.health()["reasons"] == []
            assert recovery.ready, recovery.health()
            health = recovery.health()
            assert health["reconciled_at"] == at.isoformat()
            assert health["reconciliation_in_progress"] is True
            now = at + timedelta(seconds=16)
            assert not recovery.ready
            assert "ACCOUNT_RECONCILIATION_STALE" in recovery.health()["reasons"]
        finally:
            release.set()
            attempt.result(timeout=5)


def test_reconciliation_uses_observation_time_without_renewing_at_completion(seeded, monkeypatch):
    _, _, client, recovery, at = prepared(seeded)
    now = at
    monkeypatch.setattr("tradeforge.domain.events.clock", lambda _: at + timedelta(minutes=1))
    recovery.clock = lambda: now
    request = client.request

    def slow(method, path, params=None, **kwargs):
        nonlocal now
        result = request(method, path, params, **kwargs)
        now += timedelta(milliseconds=500)
        return result

    client.request = slow
    recovery.reconcile()
    assert recovery.ready
    assert recovery.health()["reconciled_at"] == at.isoformat()
    assert recovery.health()["reconciled_age_seconds"] >= 4
    now = at + timedelta(seconds=16)
    assert not recovery.ready


def test_failed_refresh_revokes_prior_proof(seeded):
    _, _, client, recovery, _ = prepared(seeded)
    recovery.reconcile()
    assert recovery.ready
    client.unavailable = True
    recovery.reconcile()
    assert not recovery.ready
    assert recovery.health()["reconciled_at"] is None
    assert "ACCOUNT_RECONCILIATION_UNAVAILABLE" in recovery.health()["reasons"]


def test_serve_keeps_reconciling_while_market_handoff_is_blocked(seeded, monkeypatch):
    import asyncio

    from apps.execution_guardian import runtime
    from tradeforge.execution.private_stream import PrivateStream
    from tradeforge.runtime import market_link

    db, _, client, _, _ = prepared(seeded)
    client.environment = "testnet"

    async def scenario():
        entered = asyncio.Event()
        blocked = asyncio.Event()
        original = runtime.protection_loop

        async def fast_protection(recovery, stream):
            await original(recovery, stream, interval=0.01)

        async def idle(*args):
            await blocked.wait()

        class OfflineStream(PrivateStream):
            async def run(self, client):
                self.connected = True
                await blocked.wait()

        async def handoff(*args):
            entered.set()
            await blocked.wait()
            raise AssertionError("test handoff is only released through cancellation")

        monkeypatch.setattr(runtime, "PrivateStream", OfflineStream)
        monkeypatch.setattr(runtime, "project_demo", idle)
        monkeypatch.setattr(runtime, "protection_loop", fast_protection)
        monkeypatch.setattr(market_link, "fetch_memory", handoff)
        service = asyncio.create_task(
            runtime.serve(db, client, 0, internal_token="offline-test", enable_entries=False)
        )
        try:
            await asyncio.wait_for(entered.wait(), 10)
            before = sum(path == "/fapi/v3/account" for _, path, _ in client.calls)
            deadline = asyncio.get_running_loop().time() + 10
            while (
                sum(path == "/fapi/v3/account" for _, path, _ in client.calls) < before + 2
                and asyncio.get_running_loop().time() < deadline
            ):
                await asyncio.sleep(0.01)
            assert sum(path == "/fapi/v3/account" for _, path, _ in client.calls) >= before + 2
            assert not service.done()
        finally:
            service.cancel()
            await asyncio.gather(service, return_exceptions=True)

    asyncio.run(scenario())


def test_observed_unknown_order_revokes_proof_before_refresh_finishes(seeded):
    _, intent, client, recovery, _ = prepared(seeded)
    recovery.reconcile()
    assert recovery.ready
    client.extra_orders = [{"clientOrderId": "external", "symbol": intent.symbol}]
    entered, release = Event(), Event()
    request = client.request
    account_reads = 0

    def blocked_final(method, path, params=None, **kwargs):
        nonlocal account_reads
        if path == "/fapi/v3/account":
            account_reads += 1
            if account_reads == 2:
                entered.set()
                assert release.wait(5)
        return request(method, path, params, **kwargs)

    client.request = blocked_final
    with ThreadPoolExecutor(max_workers=1) as pool:
        attempt = pool.submit(recovery.reconcile)
        try:
            assert entered.wait(5)
            assert not recovery.ready
            assert "UNKNOWN_EXCHANGE_ORDER" in recovery.health()["reasons"]
            assert recovery.health()["reconciliation_in_progress"] is True
        finally:
            release.set()
            attempt.result(timeout=5)
    assert client.extra_orders == [{"clientOrderId": "external", "symbol": intent.symbol}]
    assert not client.exits

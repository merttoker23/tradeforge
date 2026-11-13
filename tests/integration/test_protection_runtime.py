"""Guardian readiness follows exchange recovery, stream freshness and durable risk."""

import asyncio
from datetime import UTC, datetime, timedelta

from tests.integration.test_protection_recovery import prepared


def test_protection_requires_recovery_and_reports_unprotected_duration(seeded):
    _, _, client, recovery, _ = prepared(seeded)
    client.fail_stop = True
    client.close_partial = True
    recovery.reconcile()
    assert recovery.ready is False
    health = recovery.health()
    assert health["unprotected"][0]["seconds"] >= 0


def test_private_stream_account_and_algo_events_reconcile_before_ready(seeded):
    from tradeforge.execution.private_stream import PrivateStream

    db, _, _, recovery, at = prepared(seeded)
    calls = []

    def recover():
        calls.append("REST")
        recovery.reconcile()
        return recovery.ready

    stream = PrivateStream(db, lambda _: calls.append("ORDER"), recover=recover)
    stream.on_event({"e": "ACCOUNT_UPDATE", "E": int(at.timestamp() * 1000), "a": {}})
    stream.on_event({"e": "ALGO_UPDATE", "E": int(at.timestamp() * 1000), "o": {"X": "NEW"}})
    assert calls == ["REST", "REST"]
    assert recovery.ready


def test_stale_private_connection_is_not_ready_and_fresh_reconnect_resets_ordering(seeded):
    from tradeforge.execution.private_stream import PrivateStream

    db, _, _, recovery, at = prepared(seeded)
    now = datetime.now(UTC)
    stream = PrivateStream(db, lambda _: None, clock=lambda: now)
    stream.ready = True
    assert stream.ready
    now += timedelta(seconds=61)
    assert not stream.ready


def test_socket_connect_never_sets_ready_before_reconciliation(seeded):
    from tradeforge.execution.private_stream import PrivateStream

    db, _, _, recovery, at = prepared(seeded)
    seen = []
    stream = None

    def recover():
        seen.append(stream.ready)
        return False

    class EmptySocket:
        def __aiter__(self):
            return self

        async def __anext__(self):
            seen.append(stream.ready)
            raise StopAsyncIteration

    stream = PrivateStream(db, lambda _: None, recover=recover)
    asyncio.run(stream.consume(EmptySocket()))
    assert seen == [False, False]
    assert not stream.ready


def test_quiet_connected_stream_recovers_after_periodic_rest_success(seeded):
    from tradeforge.execution.private_stream import PrivateStream

    db, _, _, recovery, at = prepared(seeded)
    stream = PrivateStream(db, lambda _: None, recover=lambda: False)
    stream.connected = True
    stream.ready = False
    recovery.reconcile()
    stream.mark_recovered(recovery.ready)
    assert stream.ready
    stream.gap()
    stream.mark_recovered(True)
    assert not stream.ready

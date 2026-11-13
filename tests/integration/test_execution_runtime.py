"""Loopback HTTP/WS, guardian isolation and durable stream-gap acceptance."""

import asyncio
import json
from datetime import timedelta

import pytest
from aiohttp import ClientSession, web
from sqlalchemy import text

from tests.execution_fixtures import setup_execution


def test_embedded_execution_rejects_unsafely_disabled_account_stream():
    from types import SimpleNamespace

    from tradeforge.runtime.auto_execution import execution_cycle

    results = []
    recovery = SimpleNamespace(ready=False, reconcile=lambda: results.append("reconcile"))
    stream = SimpleNamespace(ready=False, mark_recovered=lambda ready: results.append(ready))
    worker = SimpleNamespace(run_once=lambda: results.append("DISPATCH"))
    commands = SimpleNamespace(run_once=lambda *, ready: results.append(ready()))
    execution_cycle(recovery, stream, worker, commands)
    assert results == ["reconcile", False, False]


def test_listen_key_renewal_failure_immediately_closes_stream_and_latches_halt(seeded):
    from tradeforge.execution.broker import AckUnknown
    from tradeforge.execution.private_stream import PrivateStream

    db, _, _, _ = setup_execution(seeded)
    stream = PrivateStream(db, lambda _: None)
    stream.ready = True

    class Client:
        def request(self, *args, **kwargs):
            raise AckUnknown("untrusted secret transport message")

    class Socket:
        closed = False

        async def close(self):
            self.closed = True

    socket = Socket()
    assert asyncio.run(stream.renew(Client(), socket, "never-log-key")) is False
    assert socket.closed and not stream.ready
    with db.transaction() as c:
        assert c.scalar(text("SELECT kill_switch FROM risk_state")) is True


def test_private_stream_expiry_and_disconnect_latch_kill_switch_without_payload_logging(seeded):
    from tradeforge.execution.private_stream import PrivateStream

    db, _, _, _ = setup_execution(seeded)

    async def run():
        async def ws(request):
            response = web.WebSocketResponse()
            await response.prepare(request)
            await response.send_json({"e": "listenKeyExpired", "listenKey": "do-not-log"})
            await response.close()
            return response

        app = web.Application()
        app.router.add_get("/private/ws/offline", ws)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        stream = PrivateStream(db, lambda value: None)
        try:
            async with ClientSession() as session:
                async with session.ws_connect(
                    f"http://127.0.0.1:{port}/private/ws/offline"
                ) as socket:
                    await stream.consume(socket)
            assert not stream.ready
            with db.transaction() as c:
                assert c.scalar(text("SELECT kill_switch FROM risk_state")) is True
                bodies = c.execute(text("SELECT payload FROM audit_log")).scalars().all()
                assert "do-not-log" not in json.dumps(bodies)
        finally:
            await runner.cleanup()

    asyncio.run(run())


def test_private_stream_out_of_order_event_halts_but_duplicates_are_harmless(seeded):
    from tradeforge.execution.private_stream import PrivateStream
    from tradeforge.persistence.db import EntryHalt

    db, intent, _, at = setup_execution(seeded)
    seen = []
    stream = PrivateStream(db, seen.append)
    event = {
        "e": "ORDER_TRADE_UPDATE",
        "E": int(at.timestamp() * 1000),
        "T": int(at.timestamp() * 1000),
        "o": {"c": intent.client_order_id},
    }
    stream.on_event(event)
    stream.on_event(event)
    assert seen == [intent.client_order_id, intent.client_order_id]
    with pytest.raises(EntryHalt):
        stream.on_event({**event, "E": int((at - timedelta(seconds=1)).timestamp() * 1000)})
    assert not stream.ready


def test_db_context_requires_fresh_explicit_snapshot(seeded):
    from tradeforge.execution.context import load_context, save_context
    from tradeforge.persistence.db import EntryHalt

    db, intent, context, at = setup_execution(seeded)
    with pytest.raises(EntryHalt, match="CONTEXT_MISSING"):
        load_context(db, intent)
    save_context(db, context)
    assert load_context(db, intent) == context


def test_protective_algo_cancel_event_latches_entry_halt(seeded):
    from tradeforge.execution.private_stream import PrivateStream
    from tradeforge.persistence.db import EntryHalt

    db, _, _, at = setup_execution(seeded)
    stream = PrivateStream(db, lambda _: None)
    stream.ready = True
    with pytest.raises(EntryHalt):
        stream.on_event(
            {
                "e": "ALGO_UPDATE",
                "E": int(at.timestamp() * 1000),
                "o": {"X": "CANCELED", "caid": "sl_offline"},
            }
        )
    assert not stream.ready
    with db.transaction() as c:
        assert c.scalar(text("SELECT kill_switch FROM risk_state")) is True


def test_standalone_execution_http_surface_is_removed():
    from tradeforge.runtime import auto_execution

    assert not hasattr(auto_execution, "create_app")
    assert callable(auto_execution.serve)

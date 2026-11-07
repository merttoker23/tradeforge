"""Loopback private-stream recovery without Binance credentials or real orders."""

import asyncio
from datetime import UTC, datetime

from aiohttp import web

from tests.execution_fixtures import setup_execution


def test_disconnect_reconnect_gets_new_key_resets_event_ordering_and_answers_ping(
    seeded, monkeypatch
):
    import tradeforge.execution.private_stream as private

    db, intent, _, at = setup_execution(seeded)

    async def run():
        received = asyncio.Event()
        pong = asyncio.Event()
        loop = asyncio.get_running_loop()
        seen = []
        keys = []
        deleted = []
        event_time = int(datetime.now(UTC).timestamp() * 1000)

        class Client:
            environment = "testnet"

            def request(self, method, path, **kwargs):
                assert path == "/fapi/v1/listenKey"
                if method == "POST":
                    keys.append(f"offline_{len(keys) + 1}")
                    return {"listenKey": keys[-1]}
                if method == "DELETE":
                    deleted.append(keys[-1])
                    return {}
                raise AssertionError(method)

        async def socket(request):
            ws = web.WebSocketResponse(autoping=False)
            await ws.prepare(request)
            second = request.match_info["key"] == "offline_2"
            await ws.send_json(
                {
                    "e": "ORDER_TRADE_UPDATE",
                    "E": event_time - (1 if second else 0),
                    "o": {"c": intent.client_order_id},
                }
            )
            if second:
                await ws.ping(b"probe")
                async for message in ws:
                    if message.type is web.WSMsgType.PONG and message.data == b"probe":
                        pong.set()
            else:
                await ws.close()
            return ws

        def reconcile(client_id):
            seen.append(client_id)
            if len(seen) == 2:
                loop.call_soon_threadsafe(received.set)

        app = web.Application()
        app.router.add_get("/private/ws/{key}", socket)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        monkeypatch.setattr(private, "PRIVATE_WS", f"http://127.0.0.1:{port}/private/ws/")
        stream = private.PrivateStream(db, reconcile, recover=lambda: True)
        task = asyncio.create_task(stream.run(Client()))
        try:
            await asyncio.wait_for(received.wait(), 5)
            await asyncio.wait_for(pong.wait(), 5)
            assert stream.ready and stream.connected
            assert keys == ["offline_1", "offline_2"]
            assert deleted == ["offline_1"]
            assert seen == [intent.client_order_id, intent.client_order_id]
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await runner.cleanup()
        assert not stream.ready and not stream.connected
        assert deleted == keys

    asyncio.run(run())

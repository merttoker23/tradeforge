import asyncio
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from aiohttp import web

from tradeforge.market.socket_manager import AiohttpConnector, SocketManager
from tradeforge.market.streams import StreamSpec


def test_real_mock_ws_ack_ping_disconnect_and_graceful_resubscribe():
    async def scenario():
        stop = asyncio.Event()
        states, events, requests, pongs = [], [], [], []
        connections = 0

        async def ws_handler(request):
            nonlocal connections
            connections += 1
            ws = web.WebSocketResponse(autoping=False)
            await ws.prepare(request)
            command = await ws.receive_json()
            requests.append(command)
            assert command["method"] == "SUBSCRIBE"
            await ws.send_json({"result": None, "id": command["id"]})
            await ws.ping(b"same-payload")
            message = await ws.receive(timeout=2)
            pongs.append(message.data)
            now = datetime.now(UTC)
            row = {
                "e": "24hrTicker",
                "E": int(now.timestamp() * 1000),
                "s": "BTCUSDT",
                "c": "100",
                "o": "99",
                "h": "101",
                "l": "98",
                "v": "1",
                "q": "100",
                "P": "1",
            }
            await ws.send_json({"stream": "!ticker@arr", "data": [row]})
            if connections == 1:
                await ws.close()
            else:
                await stop.wait()
                await ws.close()
            return ws

        app = web.Application()
        app.router.add_get("/market/stream", ws_handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]

        def record(event):
            events.append(event)
            if len(events) == 2:
                stop.set()

        try:
            async with AiohttpConnector(base_url=f"http://127.0.0.1:{port}") as connector:
                manager = SocketManager(
                    (StreamSpec("!ticker@arr"),),
                    connector.open,
                    eligible=lambda: frozenset({"BTCUSDT"}),
                    on_event=record,
                    on_state=lambda state, generation: states.append((state, generation)),
                    retry_base=0.01,
                    jitter=lambda: 0,
                )
                await asyncio.wait_for(manager.run(stop), 5)
            assert len(requests) == 2
            assert pongs == [b"same-payload", b"same-payload"]
            assert [e.generation for e in events] == [1, 2]
            assert ("STALE", 1) in states
            assert manager.reason == "WS_DISCONNECTED_1000"
            assert events[0].sequence > 0
            assert all(r["params"] == ["!ticker@arr"] for r in requests)
        finally:
            await runner.cleanup()

    asyncio.run(scenario())


def test_rate_limit_cooldown_and_stop_are_bounded():
    from tradeforge.market.socket_manager import StreamFailure

    async def scenario():
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def unavailable(route):
            nonlocal attempts
            attempts += 1
            raise StreamFailure("WS_RATE_LIMITED", retry_after=100)
            yield

        manager = SocketManager(
            (StreamSpec("!ticker@arr"),),
            unavailable,
            eligible=lambda: frozenset({"BTCUSDT"}),
            on_event=lambda event: None,
            on_state=lambda state, generation: None,
            retry_base=0.001,
        )
        task = asyncio.create_task(manager.run(stop))
        await asyncio.sleep(0.05)
        assert attempts == 1
        stop.set()
        await asyncio.wait_for(task, 1)
        assert manager.failures == 1

    asyncio.run(scenario())


def test_rotation_reconnects_and_unacknowledged_socket_never_publishes():
    from tradeforge.market.socket_manager import WireFrame

    async def scenario():
        stop, events = asyncio.Event(), []
        opens = 0

        class Socket:
            async def send_json(self, value):
                self.id = value["id"]

            async def pong(self, data):
                pass

            async def receive(self):
                await asyncio.sleep(0.005)
                return WireFrame("TEXT", json.dumps({"id": self.id + 1, "result": None}))

        @asynccontextmanager
        async def connect(route):
            nonlocal opens
            opens += 1
            if opens == 3:
                stop.set()
            yield Socket()

        manager = SocketManager(
            (StreamSpec("!ticker@arr"),),
            connect,
            eligible=lambda: frozenset({"BTCUSDT"}),
            on_event=events.append,
            on_state=lambda state, generation: None,
            rotate_after=0.04,
            ack_timeout=0.02,
            retry_base=0.001,
            jitter=lambda: 0,
        )
        await asyncio.wait_for(manager.run(stop), 2)
        assert opens == 3 and not events

    asyncio.run(scenario())


def test_clock_http_ban_honors_retry_after_without_second_request():
    import pytest

    from tradeforge.market.socket_manager import StreamFailure

    async def scenario():
        calls = []

        async def handler(request):
            calls.append(request.path)
            return web.Response(status=418, headers={"Retry-After": "600"})

        app = web.Application()
        app.router.add_get("/fapi/v1/time", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            async with AiohttpConnector(base_url=f"http://127.0.0.1:{port}") as connector:
                for _ in range(2):
                    with pytest.raises(StreamFailure) as error:
                        await connector.server_time()
                    assert error.value.retry_after > 590
            assert calls == ["/fapi/v1/time"]
        finally:
            await runner.cleanup()

    asyncio.run(scenario())


def test_acknowledged_socket_rotates_before_lifespan_and_throttles_controls():
    from time import monotonic

    from tradeforge.market.socket_manager import WireFrame

    async def scenario():
        stop, sent = asyncio.Event(), []
        opens = 0

        class Socket:
            async def send_json(self, value):
                sent.append(monotonic())
                self.id = value["id"]

            async def receive(self):
                await asyncio.sleep(0.001)
                return WireFrame("TEXT", json.dumps({"id": self.id, "result": None}))

            async def pong(self, value):
                pass

        @asynccontextmanager
        async def connect(route):
            nonlocal opens
            opens += 1
            if opens == 3:
                stop.set()
            yield Socket()

        manager = SocketManager(
            (StreamSpec("!ticker@arr"),),
            connect,
            eligible=lambda: frozenset({"BTCUSDT"}),
            on_event=lambda event: None,
            on_state=lambda state, generation: None,
            rotate_after=0.02,
            retry_base=0.001,
            jitter=lambda: 0,
        )
        await asyncio.wait_for(manager.run(stop), 3)
        assert opens == 3
        assert all(b - a >= 0.24 for a, b in zip(sent, sent[1:], strict=False))
        assert manager.failures == 0

    asyncio.run(scenario())


def test_connector_refuses_nonpublic_endpoints_and_handshake_rate_limit():
    import pytest

    from tradeforge.market.socket_manager import StreamFailure

    for url in (
        "https://elsewhere.invalid",
        "https://fstream.binance.com/private",
        "http://user:password@127.0.0.1:1234",
    ):
        with pytest.raises(ValueError):
            AiohttpConnector(base_url=url)

    async def scenario():
        async def handler(request):
            return web.Response(status=429, headers={"Retry-After": "120"})

        app = web.Application()
        app.router.add_get("/market/stream", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            async with AiohttpConnector(base_url=f"http://127.0.0.1:{port}") as connector:
                with pytest.raises(ValueError):
                    async with connector.open("private"):
                        pass
                with pytest.raises(StreamFailure) as error:
                    async with connector.open("market"):
                        pass
                assert error.value.retry_after == 120
        finally:
            await runner.cleanup()

    asyncio.run(scenario())

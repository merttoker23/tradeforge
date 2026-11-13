"""Buffered WebSocket reads must leave time for probes, handoffs and cancellation."""

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import pytest

from tests.market_fixtures import START
from tradeforge.market.socket_manager import SocketManager
from tradeforge.market.socket_manager import WireFrame as Frame
from tradeforge.market.streams import StreamSpec


@pytest.mark.parametrize("kind", ["event", "ack", "wrong_ack", "ignored", "pre_ack", "timeout"])
def test_buffered_socket_yields_before_draining_all_frames(kind):
    async def scenario():
        stop = asyncio.Event()
        reads, heartbeat, events = 0, [], []
        loop = asyncio.get_running_loop()

        class Socket:
            request = None

            async def send_json(self, payload):
                self.request = payload

            async def receive(self):
                nonlocal reads
                reads += 1
                if reads == 1:
                    loop.call_soon(lambda: heartbeat.append(reads))
                if reads == 64:
                    stop.set()
                if kind == "timeout":
                    raise TimeoutError
                if kind == "ignored":
                    return Frame("PONG", b"ok")
                if kind in {"ack", "wrong_ack"} or (kind == "event" and reads == 1):
                    return Frame(
                        "TEXT",
                        json.dumps(
                            {"id": self.request["id"] + (kind == "wrong_ack"), "result": None}
                        ),
                    )
                return Frame(
                    "TEXT",
                    json.dumps(
                        {
                            "stream": "!ticker@arr",
                            "data": [
                                {
                                    "e": "24hrTicker",
                                    "E": int(START.timestamp() * 1000),
                                    "s": "BTCUSDT",
                                    "c": "100",
                                    "o": "99",
                                    "h": "101",
                                    "l": "98",
                                    "v": "2",
                                    "q": "200",
                                    "P": "0",
                                    "st": 1,
                                }
                            ],
                        }
                    ),
                )

            async def pong(self, data):
                pass

        @asynccontextmanager
        async def connect(route):
            yield Socket()

        manager = SocketManager(
            (StreamSpec("!ticker@arr"),),
            connect,
            eligible=lambda: frozenset({"BTCUSDT"}),
            on_event=events.append,
            on_state=lambda state, generation: None,
            clock=lambda: START,
            monotonic=lambda: 0,
        )
        await manager.run(stop)
        await asyncio.sleep(0)
        assert heartbeat and heartbeat[0] < 64
        if kind != "event":
            assert events == []
        else:
            assert events

    asyncio.run(scenario())


def test_each_connection_that_delivers_market_data_restarts_backoff(monkeypatch):
    from tradeforge.market import socket_manager

    async def scenario():
        stop = asyncio.Event()
        delays, events = [], []
        now = datetime.now(UTC)

        class Socket:
            async def send_json(self, payload):
                self.request_id = payload["id"]
                self.frames = iter(
                    (
                        {"id": self.request_id, "result": None},
                        {
                            "stream": "!ticker@arr",
                            "data": [
                                {
                                    "e": "24hrTicker",
                                    "E": int(now.timestamp() * 1000),
                                    "s": "BTCUSDT",
                                    "c": "100",
                                    "o": "99",
                                    "h": "101",
                                    "l": "98",
                                    "v": "2",
                                    "q": "200",
                                    "P": "0",
                                }
                            ],
                        },
                    )
                )

            async def receive(self):
                frame = next(self.frames, None)
                return Frame("TEXT", json.dumps(frame)) if frame else Frame("CLOSE")

        @asynccontextmanager
        async def connect(route):
            yield Socket()

        async def record_backoff(stop, delay):
            delays.append(delay)
            if len(delays) == 3:
                stop.set()

        monkeypatch.setattr(socket_manager, "pause", record_backoff)
        manager = SocketManager(
            (StreamSpec("!ticker@arr"),),
            connect,
            eligible=lambda: frozenset({"BTCUSDT"}),
            on_event=events.append,
            on_state=lambda state, generation: None,
            clock=lambda: now,
            jitter=lambda: 0,
            retry_base=0.001,
        )
        await asyncio.wait_for(manager.run(stop), 2)
        assert len(events) == 3
        assert delays == [0.002, 0.002, 0.002]

    asyncio.run(scenario())


def test_global_socket_reconnects_when_acknowledged_but_one_feed_stays_silent(monkeypatch):
    from tradeforge.market import socket_manager

    async def scenario():
        stop = asyncio.Event()
        elapsed = 0.0
        events = []

        class Socket:
            request_id = None

            async def send_json(self, payload):
                self.request_id = payload["id"]

            async def receive(self):
                nonlocal elapsed
                if self.request_id is not None:
                    request_id, self.request_id = self.request_id, None
                    return Frame("TEXT", json.dumps({"id": request_id, "result": None}))
                elapsed += 1
                row = {
                    "e": "24hrTicker",
                    "E": int(START.timestamp() * 1000),
                    "s": "BTCUSDT",
                    "c": "100",
                    "o": "99",
                    "h": "101",
                    "l": "98",
                    "v": "2",
                    "q": "200",
                    "P": "0",
                }
                if elapsed >= 10:
                    stop.set()
                return Frame("TEXT", json.dumps({"stream": "!ticker@arr", "data": [row]}))

        @asynccontextmanager
        async def connect(route):
            yield Socket()

        async def stop_after_failure(stop, delay):
            stop.set()

        monkeypatch.setattr(socket_manager, "pause", stop_after_failure)
        manager = SocketManager(
            (StreamSpec("!ticker@arr"), StreamSpec("!markPrice@arr@1s")),
            connect,
            eligible=lambda: frozenset({"BTCUSDT"}),
            on_event=events.append,
            on_state=lambda state, generation: None,
            clock=lambda: START,
            monotonic=lambda: elapsed,
            data_timeout=4,
        )
        await asyncio.wait_for(manager.run(stop), 2)
        assert events
        assert manager.reconnects == 1
        assert manager.reason == "WS_DATA_TIMEOUT"

    asyncio.run(scenario())

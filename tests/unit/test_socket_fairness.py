"""Buffered WebSocket reads must leave time for probes, handoffs and cancellation."""

import asyncio
import json
from contextlib import asynccontextmanager

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

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import timedelta

from tests.contract.test_market_state_wire import kline
from tests.market_fixtures import START, instrument
from tests.universe_fixtures import payload, symbol
from tradeforge.domain.market import Timeframe
from tradeforge.market.clock_sync import ClockSync
from tradeforge.market.instrument_registry import InstrumentRegistry
from tradeforge.market.market_gateway import MarketGateway
from tradeforge.market.socket_manager import WireFrame


def test_gateway_runner_consumes_explicit_native_watch_streams(migrated):
    at = START + timedelta(minutes=1)
    registry = InstrumentRegistry(migrated)
    registry.record(payload(symbol("BTCUSDT")), at, timedelta(hours=1))
    view = registry.view(at)

    async def scenario():
        stop = asyncio.Event()
        gateway = MarketGateway(ClockSync(), clock=lambda: at)
        subscriptions = []

        class Refresher:
            def tick(self):
                return view

        class Socket:
            acked = False
            sent = False

            async def send_json(self, value):
                self.command = value
                subscriptions.extend(value["params"])

            async def pong(self, value):
                pass

            async def receive(self):
                await asyncio.sleep(0.005)
                if not self.acked:
                    self.acked = True
                    return WireFrame("TEXT", json.dumps({"id": self.command["id"], "result": None}))
                if "btcusdt@kline_1m" in self.command["params"] and not self.sent:
                    self.sent = True
                    row = json.loads(kline(True).payload_json)
                    row.update(e="kline", E=int(at.timestamp() * 1000), s="BTCUSDT")
                    return WireFrame(
                        "TEXT", json.dumps({"stream": "btcusdt@kline_1m", "data": row})
                    )
                await asyncio.sleep(0.01)
                return WireFrame("TEXT", json.dumps({"id": self.command["id"], "result": None}))

        class Connector:
            async def server_time(self):
                return at

            @asynccontextmanager
            async def open(self, route):
                yield Socket()

        def consumed(event):
            if "@kline_" in event.stream.name:
                stop.set()

        await asyncio.wait_for(
            gateway.run(Refresher(), Connector(), stop, watched=("BTCUSDT",), consume=consumed), 3
        )
        assert "btcusdt@kline_5m" in subscriptions
        assert len(gateway.canonical.state.view(instrument(), Timeframe.M1, at).bars) == 1

    asyncio.run(scenario())

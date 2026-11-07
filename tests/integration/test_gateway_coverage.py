import json
from datetime import UTC, datetime, timedelta

from tests.universe_fixtures import payload as exchange_info
from tests.universe_fixtures import symbol
from tradeforge.market.clock_sync import ClockSync
from tradeforge.market.instrument_registry import InstrumentRegistry, RegistryView
from tradeforge.market.market_gateway import MarketGateway
from tradeforge.market.streams import GLOBAL_STREAMS, StreamSpec, decode_events


def test_125_member_coverage_disconnect_generation_recovery_and_metadata_changes(migrated):
    now = datetime.now(UTC) - timedelta(seconds=2)
    mono = [10.0]
    clock = ClockSync()
    clock.observe(now, now, now, mono[0])
    gateway = MarketGateway(clock, clock=lambda: now, monotonic=lambda: mono[0], capacity=1024)
    registry = InstrumentRegistry(migrated)
    names = [f"C{i}USDT" for i in range(125)]
    registry.record(exchange_info(*[symbol(name) for name in names]), now, timedelta(hours=1))
    gateway.update_universe(registry.view(now))
    streams = tuple(StreamSpec(name) for name in GLOBAL_STREAMS)
    gateway.socket_state(streams, "SYNCING", 1)
    assert gateway.status()["covered"] == 0

    def feed(name, generation=1, members=names):
        rows = []
        for member in members:
            row = {"s": member, "E": int(now.timestamp() * 1000)}
            if name == "!ticker@arr":
                row.update(e="24hrTicker", c="100", o="99", h="101", l="98", v="1", q="100", P="1")
            else:
                row.update(
                    e="markPriceUpdate",
                    p="100",
                    i="100",
                    r="0.001",
                    T=int(now.timestamp() * 1000) + 1000,
                )
            rows.append(row)
        events = decode_events(
            json.dumps({"stream": name, "data": rows}),
            route="market",
            received_at=now,
            generation=generation,
            sequence=1,
            eligible=gateway.eligible,
        )
        return [gateway.ingest(event) for event in events]

    for stream in GLOBAL_STREAMS:
        feed(stream)
    assert gateway.status()["covered"] == 125
    assert gateway.status()["data_ready"] is True
    assert gateway.status()["trading_ready"] is False
    assert not any(feed("!ticker@arr"))  # Duplicate doesn't create new observations.
    gateway.socket_state(streams, "STALE", 1)
    assert gateway.status()["state"] == "STALE"
    gateway.socket_state(streams, "SYNCING", 2)
    for stream in GLOBAL_STREAMS:
        assert not any(feed(stream, generation=1))
        feed(stream, generation=2)
    assert gateway.status()["data_ready"]
    # A newly listed member is represented immediately, with zero invented data.
    now += timedelta(milliseconds=100)
    names.append("NEWUSDT")
    registry.record(exchange_info(*[symbol(name) for name in names]), now, timedelta(hours=1))
    gateway.update_universe(registry.view(now))
    assert gateway.status()["eligible"] == 126
    assert not gateway.status()["data_ready"]
    gateway.socket_state(streams, "SYNCING", 3)
    for stream in GLOBAL_STREAMS:
        feed(stream, generation=3)
    assert gateway.status()["data_ready"]
    mono[0] += 20
    now += timedelta(seconds=20)
    assert not gateway.status()["data_ready"]
    gateway.update_universe(RegistryView(None, False, "DB_UNAVAILABLE"))
    assert not gateway.status()["data_ready"]


def test_queue_loss_and_clock_expiry_close_gate(migrated):
    now = datetime.now(UTC) - timedelta(seconds=2)
    clock = ClockSync()
    clock.observe(now, now, now, 1)
    registry = InstrumentRegistry(migrated)
    registry.record(exchange_info(symbol("BTCUSDT")), now, timedelta(hours=1))
    gateway = MarketGateway(clock, clock=lambda: now, monotonic=lambda: 1, capacity=1)
    gateway.update_universe(registry.view(now))
    streams = tuple(StreamSpec(s) for s in GLOBAL_STREAMS)
    gateway.socket_state(streams, "SYNCING", 1)
    for name, row in (
        (
            GLOBAL_STREAMS[0],
            {
                "e": "24hrTicker",
                "c": "100",
                "o": "99",
                "h": "101",
                "l": "98",
                "v": "1",
                "q": "100",
                "P": "1",
            },
        ),
        (
            GLOBAL_STREAMS[1],
            {
                "e": "markPriceUpdate",
                "p": "100",
                "i": "100",
                "r": "0.001",
                "T": int(now.timestamp() * 1000) + 1000,
            },
        ),
    ):
        (event,) = decode_events(
            json.dumps(
                {"stream": name, "data": [dict(row, s="BTCUSDT", E=int(now.timestamp() * 1000))]}
            ),
            route="market",
            received_at=now,
            generation=1,
            sequence=1,
            eligible=gateway.eligible,
        )
        gateway.ingest(event)
    assert gateway.status()["dropped"] == 1
    assert not gateway.status()["data_ready"]
    clock.invalidate()
    assert gateway.status()["reason"] == "CLOCK_UNSAFE"


def test_newer_exchange_sequence_with_same_millisecond_is_accepted(migrated):
    from dataclasses import replace

    now = datetime.now(UTC) - timedelta(seconds=2)
    clock = ClockSync()
    clock.observe(now, now, now, 1)
    registry = InstrumentRegistry(migrated)
    registry.record(exchange_info(symbol("BTCUSDT")), now, timedelta(hours=1))
    gateway = MarketGateway(clock, clock=lambda: now, monotonic=lambda: 1)
    gateway.update_universe(registry.view(now), deep=("BTCUSDT",))
    stream = StreamSpec("btcusdt@aggTrade")
    gateway.socket_state((stream,), "SYNCING", 1)
    row = {
        "e": "aggTrade",
        "s": "BTCUSDT",
        "E": int(now.timestamp() * 1000),
        "p": "100",
        "q": "1",
        "a": 10,
    }
    (event,) = decode_events(
        json.dumps({"stream": stream.name, "data": row}),
        route="market",
        received_at=now,
        generation=1,
        sequence=1,
        eligible=gateway.eligible,
    )
    assert gateway.ingest(event)
    newer = replace(event, exchange_sequence=11, sequence=2, payload_json='{"a":11}')
    assert gateway.ingest(newer)
    assert not gateway.ingest(event)


def test_metadata_replan_waits_for_all_retiring_sockets_without_worker_failure(migrated):
    import asyncio
    from contextlib import asynccontextmanager

    from tradeforge.market.socket_manager import WireFrame

    now = datetime.now(UTC) - timedelta(seconds=2)
    registry = InstrumentRegistry(migrated)
    registry.record(exchange_info(symbol("BTCUSDT")), now, timedelta(hours=1))
    first = registry.view(now)
    registry.record(
        exchange_info(symbol("BTCUSDT"), symbol("NEWUSDT")),
        now + timedelta(milliseconds=1),
        timedelta(hours=1),
    )
    second = registry.view(now + timedelta(milliseconds=1))

    class Gateway(MarketGateway):
        def update_universe(self, view):
            super().update_universe(view, deep=("BTCUSDT",))

    class Refresher:
        calls = 0

        def tick(self):
            self.calls += 1
            return first if self.calls == 1 else second

    class Socket:
        async def send_json(self, value):
            self.request_id = value["id"]

        async def receive(self):
            await asyncio.sleep(0.005)
            return WireFrame("TEXT", json.dumps({"id": self.request_id, "result": None}))

        async def pong(self, value):
            pass

    class Connector:
        opens = 0

        async def server_time(self):
            return datetime.now(UTC)

        @asynccontextmanager
        async def open(self, route):
            self.opens += 1
            try:
                yield Socket()
            finally:
                await asyncio.sleep(0.15 if route == "public" else 0.01)

    async def scenario():
        gateway, connector = Gateway(ClockSync()), Connector()
        stop = asyncio.Event()

        async def finish():
            await asyncio.sleep(1.8)
            stop.set()

        finisher = asyncio.create_task(finish())
        try:
            await asyncio.wait_for(gateway.run(Refresher(), connector, stop), 4)
            # Global streams have a dedicated socket, separate from deep/watch.
            assert connector.opens == 6
        finally:
            await finisher

    asyncio.run(scenario())

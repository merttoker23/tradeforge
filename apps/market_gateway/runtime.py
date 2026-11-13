"""Explicit local public-feed runner. Offline foundation remains the default."""

import argparse
import asyncio
import json
import sys
from collections.abc import Callable
from pathlib import Path

from aiohttp import web

from tradeforge.bootstrap import main as foundation_main
from tradeforge.config.policy import RuntimeMode
from tradeforge.config.settings import load_settings
from tradeforge.market.clock_sync import ClockSync
from tradeforge.market.exchange_info import ExchangeInfoClient
from tradeforge.market.instrument_registry import InstrumentRegistry, RegistryView
from tradeforge.market.market_gateway import MarketGateway
from tradeforge.market.recovery import MarketRecovery, PublicMarketClient
from tradeforge.market.runtime import GatewayPersistence
from tradeforge.market.socket_manager import AiohttpConnector, pause
from tradeforge.market.storage import save_view
from tradeforge.market.universe_refresh import UniverseRefresher
from tradeforge.persistence.db import Database
from tradeforge.runtime.config import local_database, local_token_file
from tradeforge.runtime.signals import shutdown_signals


def create_app(
    gateway: MarketGateway, *, chart_demand: Callable[[str, str], None] | None = None
) -> web.Application:
    settings = load_settings()

    async def health(request: web.Request) -> web.Response:
        if request.path == "/health":
            return web.json_response(
                {
                    "alive": True,
                    "service": "market_gateway",
                    "phase": "PHASE_05",
                    "mode": settings.mode.value,
                    "state": "ALIVE",
                    "live": False,
                    "orders_enabled": False,
                    "trading_ready": False,
                },
                headers={"Cache-Control": "no-store"},
            )
        state = gateway.status()
        body = {
            **state,
            "alive": True,
            "service": "market_gateway",
            "phase": "PHASE_05",
            "mode": settings.mode.value,
            "live": False,
            "orders_enabled": False,
        }
        status = 200 if request.path == "/health" or state["data_ready"] else 503
        return web.json_response(body, status=status, headers={"Cache-Control": "no-store"})

    app = web.Application(client_max_size=1024)
    app.router.add_get("/health", health)
    app.router.add_get("/readiness", health)

    async def demand(request: web.Request) -> web.Response:
        transport = request.transport
        peer = transport.get_extra_info("peername") if transport else None
        bound = transport.get_extra_info("sockname") if transport else None
        if (
            not peer
            or peer[0] != "127.0.0.1"
            or not bound
            or request.host != f"127.0.0.1:{bound[1]}"
            or "Origin" in request.headers
            or "Sec-Fetch-Site" in request.headers
        ):
            raise web.HTTPForbidden()
        if chart_demand is None:
            raise web.HTTPServiceUnavailable()
        if set(request.query) != {"symbol", "timeframe"}:
            raise web.HTTPBadRequest()
        try:
            chart_demand(request.query["symbol"], request.query["timeframe"])
        except ValueError as error:
            if str(error) == "CHART_RATE_LIMITED":
                raise web.HTTPTooManyRequests() from None
            raise web.HTTPBadRequest() from None
        return web.json_response({"status": "REQUESTED"}, status=202)

    app.router.add_get("/chart-demand", demand)
    return app


async def canonical_cycle(
    gateway: MarketGateway,
    database: Database,
    recovery: MarketRecovery,
    *,
    require_admission: bool,
) -> bool:
    if require_admission:
        from tradeforge.runtime.maintenance import observation_admission

        try:
            admitted = await asyncio.to_thread(observation_admission, database)
        except Exception:
            admitted = False
        if not admitted:
            gateway.canonical.admission_ready = False
            return False
    gateway.canonical.admission_ready = True
    await gateway.canonical.maintain(
        recovery,
        lambda view: save_view(database, view),
        gateway.clock(),
        recover_depth=False,
    )
    return gateway.canonical.storage_ready


async def depth_cycle(
    gateway: MarketGateway,
    database: Database,
    recovery: MarketRecovery,
    *,
    require_admission: bool,
) -> None:
    if require_admission:
        from tradeforge.runtime.maintenance import observation_admission

        try:
            admitted = await asyncio.to_thread(observation_admission, database)
        except Exception:
            admitted = False
        gateway.canonical.admission_ready = admitted
        if not admitted:
            return
    await gateway.canonical.maintain_depth(recovery)


async def serve(
    database: Database,
    port: int,
    *,
    watched: tuple[str, ...] = (),
    deep: tuple[str, ...] = (),
    host: str = "127.0.0.1",
    internal_token: str | None = None,
    loopback: bool = False,
    full_depth: bool = False,
    depth_batch_size: int | None = None,
) -> None:
    from tradeforge.runtime.binance_environment import select_environment

    active = select_environment(database)
    settings = load_settings()
    if settings.mode is RuntimeMode.TESTNET and active.name != "testnet":
        raise ValueError("TESTNET_ENVIRONMENT_MISMATCH")
    if internal_token is not None or settings.mode is RuntimeMode.TESTNET:
        database.pin_environment(active.revision)
    gateway = MarketGateway(
        ClockSync(max_rtt=5.0, max_skew=3.0),
        environment=active.name,
        full_depth=full_depth,
        depth_batch_size=depth_batch_size,
    )
    refresher = UniverseRefresher(
        InstrumentRegistry(database),
        ExchangeInfoClient(
            environment=active.name,
            timeout=20 if active.name == "testnet" else 5,
        ),
    )
    stop = asyncio.Event()
    recovery = MarketRecovery(
        gateway.canonical.state, PublicMarketClient(max_concurrency=2, environment=active.name)
    )
    persistence = GatewayPersistence(
        gateway, database, require_admission=internal_token is not None
    )
    app = create_app(gateway, chart_demand=persistence.request_chart)
    from tradeforge.runtime.event_handoff import MarketEventHub, add_market_events_route
    from tradeforge.runtime.market_link import add_loopback_memory_route, add_memory_route

    market_events = MarketEventHub()

    if internal_token is None:
        # Standalone services share public market evidence over loopback only.
        # No token file, environment variable, or OS-specific setup is needed.
        if host != "127.0.0.1" or port != 8101:
            raise ValueError("LOCAL_MARKET_HANDOFF_REQUIRES_LOOPBACK_8101")
        add_loopback_memory_route(app, gateway, persistence)
    else:
        # The authenticated Compose/VPS handoff stays strict. A co-located
        # local dashboard has no token file, so its verified loopback GET
        # requests may share this route alongside the bearer calls.
        add_memory_route(app, gateway, persistence, internal_token, loopback=loopback)
    add_market_events_route(app, market_events, token=internal_token, loopback=loopback)
    runner = web.AppRunner(app, access_log=None)
    from tradeforge.runtime.surveillance import RuntimeSurveillance

    surveillance = RuntimeSurveillance(gateway, database)

    async def canonical_worker() -> None:
        while not stop.is_set():
            try:
                await canonical_cycle(
                    gateway, database, recovery, require_admission=internal_token is not None
                )
            except Exception as error:
                # A timed-out public REST call or a short DB stall must not
                # restart the process and drop every Binance socket with it.
                gateway.record_cycle_error("CANONICAL", error)
            await pause(stop, 2)

    async def depth_worker() -> None:
        while not stop.is_set():
            try:
                await depth_cycle(
                    gateway, database, recovery, require_admission=internal_token is not None
                )
            except Exception as error:
                gateway.record_cycle_error("DEPTH", error)
            await pause(stop, 1)

    async def recovery_workers() -> None:
        # Share one bounded, paced REST pool. A slow candle request cannot hold
        # the depth recovery lane; both still obey the same request budget.
        async with recovery.client:
            tasks = (asyncio.create_task(canonical_worker()), asyncio.create_task(depth_worker()))
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    await task
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def enforce_environment() -> None:
        # Fence existing sockets and RAM state; Compose restarts all clients on revision.
        while not stop.is_set():
            await asyncio.sleep(1)
            try:
                current = await asyncio.to_thread(select_environment, database)
            except Exception as error:
                if str(error) == "BINANCE_ENVIRONMENT_RESTART_REQUIRED":
                    stop.set()
                    raise
                # A transient database outage is not an environment switch.
                # Keep the last verified environment and retry next second.
                gateway.record_cycle_error("ENVIRONMENT", error)
                continue
            if current.revision != active.revision:
                gateway.update_universe(RegistryView(None, False, "ENVIRONMENT_CHANGED"))
                stop.set()
                raise RuntimeError("BINANCE_ENVIRONMENT_RESTART_REQUIRED")

    worker = asyncio.create_task(recovery_workers())

    helpers = [
        asyncio.create_task(enforce_environment()),
        asyncio.create_task(persistence.telemetry(stop)),
        asyncio.create_task(persistence.history(stop)),
        asyncio.create_task(persistence.quotes(stop)),
        asyncio.create_task(persistence.charts(stop)),
        asyncio.create_task(surveillance.run(stop)),
    ]
    with shutdown_signals(stop):
        await runner.setup()
        try:
            await web.TCPSite(runner, host, port).start()
            async with AiohttpConnector(base_url=active.stream_base) as connector:
                print(
                    json.dumps(
                        {
                            "service": "market_gateway",
                            "phase": "PHASE_05",
                            "mode": settings.mode.value,
                            "environment": active.name,
                            "live": False,
                            "orders_enabled": False,
                        }
                    ),
                    flush=True,
                )
                ingestion = asyncio.create_task(
                    gateway.run(
                        refresher,
                        connector,
                        stop,
                        watched=watched,
                        deep=deep,
                        selection=surveillance.selection,
                        consume=market_events.publish,
                    )
                )
                try:
                    done, _ = await asyncio.wait(
                        (worker, ingestion, *helpers), return_when=asyncio.FIRST_COMPLETED
                    )
                    for task in done:
                        await task
                    if stop.is_set():
                        return
                    raise RuntimeError("CANONICAL_WORKER_STOPPED")
                finally:
                    ingestion.cancel()
                    await asyncio.gather(ingestion, return_exceptions=True)
        finally:
            stop.set()
            worker.cancel()
            for helper in helpers:
                helper.cancel()
            await asyncio.gather(worker, *helpers, return_exceptions=True)
            await runner.cleanup()


def main() -> int:
    if "--stream" not in sys.argv[1:]:
        return foundation_main("market_gateway", 8101)
    parser = argparse.ArgumentParser(description="Tradeforge credential-free local market feeds")
    parser.add_argument(
        "--stream", action="store_true", help="Explicitly connect public data feeds"
    )
    parser.add_argument(
        "--database-url-file",
        type=Path,
        required=True,
        help="Ignored secret file containing a local PostgreSQL URL",
    )
    parser.add_argument("--port", type=int, default=8101)
    parser.add_argument(
        "--full-depth",
        action="store_true",
        help="Subscribe every eligible pair to the strategy evidence streams",
    )
    parser.add_argument(
        "--depth-batch-size",
        type=int,
        help="Rotate strategy subscriptions in batches while scanning the entire universe",
    )
    parser.add_argument(
        "--watch",
        action="append",
        default=[],
        metavar="SYMBOL",
        help="Explicit native 1m/5m/15m observations; repeat per symbol",
    )
    parser.add_argument(
        "--deep",
        action="append",
        default=[],
        metavar="SYMBOL",
        help="Explicit depth/aggTrade plus native bars; repeat per symbol",
    )
    parser.add_argument(
        "--gateway-token-file",
        type=Path,
        help="Internal handoff token; keeps admission gating across services",
    )
    args = parser.parse_args()
    try:
        settings = load_settings()
        from tradeforge.domain.instrument import InstrumentKey

        for symbol in (*args.watch, *args.deep):
            InstrumentKey(
                symbol=symbol,
                exchange="BINANCE",
                market_type="USD_M",
                quote="USDT",
                margin_asset="USDT",
                contract_type="PERPETUAL",
            )
        if len(set((*args.watch, *args.deep))) > 256:
            raise ValueError("CANONICAL_SYMBOL_BUDGET")
        if (
            settings.mode not in {RuntimeMode.PAPER, RuntimeMode.TESTNET}
            or not 0 < args.port <= 65535
        ):
            raise ValueError("GATEWAY_LOCAL_MODE_INVALID")
        token = local_token_file(args.gateway_token_file) if args.gateway_token_file else None
        database = local_database(args.database_url_file)
    except Exception:
        parser.error("GATEWAY_LOCAL_CONFIG_INVALID")
    # Local Testnet needs a bounded evidence tier even when surveillance has
    # no ranked candidates yet. An explicit full-depth request keeps its scope.
    local_testnet_batch = settings.mode is RuntimeMode.TESTNET and not args.full_depth
    depth_batch_size = args.depth_batch_size
    if local_testnet_batch and depth_batch_size is None:
        depth_batch_size = 24
    try:
        asyncio.run(
            serve(
                database,
                args.port,
                watched=tuple(args.watch),
                deep=tuple(args.deep),
                internal_token=token,
                loopback=True,
                full_depth=args.full_depth or local_testnet_batch,
                depth_batch_size=depth_batch_size,
            )
        )
    except KeyboardInterrupt:
        pass
    except ValueError as error:
        parser.error(
            str(error) if str(error) == "TESTNET_ENVIRONMENT_MISMATCH" else "GATEWAY_FAILED_CLOSED"
        )
    except Exception:
        parser.error("GATEWAY_FAILED_CLOSED")
    finally:
        database.close()
    return 0

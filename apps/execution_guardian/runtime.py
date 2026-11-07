"""Explicit local testnet process. Default invocation stays the offline foundation."""

import argparse
import asyncio
import json
import sys
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

from aiohttp import ClientError, ClientSession, web
from sqlalchemy import text
from sqlalchemy.engine import make_url

from tradeforge.bootstrap import main as foundation_main
from tradeforge.config.policy import RuntimeMode
from tradeforge.config.settings import load_settings
from tradeforge.dashboard.control import GuardianCommands
from tradeforge.domain.execution import ExecutionIntent
from tradeforge.execution.binance_client import BinanceTestnetClient
from tradeforge.execution.broker import AckUnknown, ExchangeRejected, ExecutionContext
from tradeforge.execution.context import load_context, verify_exchange_context
from tradeforge.execution.order_manager import OrderManager
from tradeforge.execution.outbox_worker import OutboxWorker
from tradeforge.execution.position_lifecycle import PositionLifecycle
from tradeforge.execution.preflight import preflight
from tradeforge.execution.private_stream import PrivateStream
from tradeforge.persistence.db import Database, EntryHalt, PersistenceError
from tradeforge.risk.storage import RiskService
from tradeforge.runtime.demo import load_demo_credentials, private_ignored_file
from tradeforge.runtime.demo_projection import project_demo
from tradeforge.runtime.progress import RuntimeProgress
from tradeforge.runtime.storage import save_runtime


def analysis_ready(db: Database, at: datetime) -> bool:
    """Only a fresh quant observation can establish broad analysis readiness."""
    with db.transaction() as connection:
        return (
            connection.scalar(
                text(
                    "SELECT payload->'analysis_ready' FROM runtime_observations "
                    "WHERE service='quant_core' AND observed_at<=:at AND expires_at>:at"
                ),
                {"at": at},
            )
            is True
        )


def create_app(
    manager: OrderManager,
    stream_ready: Callable[[], bool],
    recovery: PositionLifecycle | None = None,
    entries_enabled: Callable[[], bool] = lambda: False,
    progress: Callable[[], dict[str, object]] | None = None,
    stream_status: Callable[[], dict[str, object]] | None = None,
) -> web.Application:

    async def health(request: web.Request) -> web.Response:
        try:
            db_ready = await asyncio.to_thread(manager.db.probe)
            risk = await asyncio.to_thread(RiskService(manager.db).state)
            ready = bool(
                db_ready.db_writable
                and stream_ready()
                and not risk.kill_switch
                and recovery is not None
                and recovery.ready
            )
        except (PersistenceError, ValueError):
            ready = False
        body = {
            "service": "execution_guardian",
            "phase": "PHASE_15",
            "mode": "testnet",
            "environment": "BINANCE_FUTURES_DEMO",
            "live": False,
            "alive": True,
            "ready": ready,
            "orders_enabled": ready and entries_enabled(),
            "execution_capability": ready,
            "stream_ready": stream_ready(),
            "reason": "CONTEXT_CHECK_REQUIRED" if ready else "EXECUTION_HALTED",
            "protection": recovery.health() if recovery else {"ready": False},
            "private_stream": stream_status() if stream_status else {},
        }
        if progress is not None:
            dispatch = progress()
            body.update(
                healthy=True,
                data_ready=dispatch.get("data_ready") is True,
                analysis_ready=dispatch.get("analysis_ready") is True,
                execution_ready=ready and dispatch.get("execution_ready") is True,
                stage=dispatch.get("stage"),
                last_progress_at=dispatch.get("last_progress_at"),
                last_error=dispatch.get("last_error"),
                error_count=dispatch.get("error_count"),
                reason=dispatch.get("reason"),
            )
            body["orders_enabled"] = body["execution_ready"] and entries_enabled()
        return web.json_response(
            body,
            status=200 if request.path == "/health" or ready else 503,
            headers={"Cache-Control": "no-store"},
        )

    app = web.Application(client_max_size=1024)
    app.router.add_get("/health", health)
    app.router.add_get("/readiness", health)
    return app


def guardian_cycle(
    recovery: PositionLifecycle,
    stream: PrivateStream,
    worker: OutboxWorker,
    commands: GuardianCommands,
) -> None:
    # Protection runs independently and before the optional dashboard inbox.
    recovery.reconcile()
    stream.mark_recovered(recovery.ready)
    commands.run_once(ready=lambda: recovery.ready and stream.ready)
    if stream.ready and recovery.ready:
        worker.run_once()


async def protection_loop(
    recovery: PositionLifecycle, stream: PrivateStream, *, interval: float = 10
) -> None:
    """Refresh protection on its own start-to-start cadence and worker thread."""
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="guardian-protection")
    pending: asyncio.Future[None] | None = None

    def refresh() -> None:
        recovery.reconcile()
        stream.mark_recovered(recovery.ready)

    try:
        while True:
            started = time.monotonic()
            pending = asyncio.get_running_loop().run_in_executor(executor, refresh)
            # Cancellation must not abandon an in-flight protection side effect.
            await asyncio.shield(pending)
            await asyncio.sleep(max(0.05, interval - (time.monotonic() - started)))
    finally:
        try:
            if pending is not None and not pending.done():
                await asyncio.shield(pending)
        finally:
            executor.shutdown(wait=True, cancel_futures=True)


async def serve(
    db: Database,
    client: BinanceTestnetClient,
    port: int,
    *,
    host: str = "127.0.0.1",
    internal_token: str | None = None,
    enable_entries: bool = True,
    trial_enabled: bool = False,
    expected_revision: int | None = None,
) -> None:
    if trial_enabled and (
        not enable_entries or internal_token is None or client.environment != "testnet"
    ):
        raise ValueError("TESTNET_TRIAL_AUTHORITY_REQUIRED")
    stream: PrivateStream
    autonomous = internal_token is not None
    with db.transaction() as c:
        first_start = c.scalar(text("SELECT 1 FROM risk_state LIMIT 1")) is None
    recovery = PositionLifecycle(db, client, latch_failure=not autonomous)
    market: dict[str, object] = {}

    def context(intent: ExecutionIntent) -> ExecutionContext:
        if not recovery.ready or not stream.ready:
            raise EntryHalt("ACCOUNT_RECONCILIATION_REQUIRED")
        if autonomous and (
            not market.get("ready")
            or not 0
            <= (datetime.now(UTC) - datetime.fromisoformat(str(market["as_of"]))).total_seconds()
            <= 5
        ):
            raise EntryHalt("PUBLIC_MARKET_RECOVERY_REQUIRED")
        frame = load_context(db, intent)
        if stream.account_event_at is not None and frame.account.as_of < stream.account_event_at:
            raise EntryHalt("ACCOUNT_EVIDENCE_STALE")
        return verify_exchange_context(client, frame, stream_ready=stream.ready)

    manager = OrderManager(db, client, context, protection=lambda _: recovery.reconcile())

    def reconcile(client_id: str) -> None:
        with db.transaction() as c:
            identifier = c.scalar(
                text("SELECT intent_id FROM orders WHERE client_order_id=:id"), {"id": client_id}
            )
        if identifier is None:
            recovery.invalidate()
            recovery.reconcile()
            raise EntryHalt("UNKNOWN_EXCHANGE_ORDER")
        with recovery.lock:
            manager.execute(identifier)

    def recover() -> bool:
        recovery.reconcile()
        return recovery.ready

    stream = PrivateStream(db, reconcile, recover=recover, latch_gap=not autonomous)
    worker = OutboxWorker(db, manager)
    commands = GuardianCommands(db)
    progress = RuntimeProgress()
    runner = web.AppRunner(
        create_app(
            manager,
            lambda: stream.ready,
            recovery,
            entries_enabled=lambda: enable_entries and autonomous and bool(market.get("ready")),
            progress=progress.snapshot,
            stream_status=stream.health,
        ),
        access_log=None,
    )
    await runner.setup()

    async def dispatch() -> None:
        nonlocal first_start, enable_entries, trial_enabled
        from tradeforge.decision.trial import TestnetTrialPolicy
        from tradeforge.execution.account_sync import cleanup_flat_testnet
        from tradeforge.runtime.demo_decisions import DemoDecisions
        from tradeforge.runtime.maintenance import observe_storage, retain
        from tradeforge.runtime.market_link import fetch_memory
        from tradeforge.runtime.settlement import settle_closed_results
        from tradeforge.runtime.trading import initial_demo_activation

        decisions = DemoDecisions(
            db, client, trial_policy=TestnetTrialPolicy(enabled=trial_enabled)
        )
        default_authority = enable_entries, trial_enabled
        retained = float("-inf")
        session = ClientSession(trust_env=False)
        try:
            while True:
                started = time.monotonic()
                progress.advance("ENTRY_ADMISSION" if autonomous else "RECONCILIATION")
                try:
                    # Filled/canceled entries retain protection work even with entries disabled.
                    if not autonomous:
                        await asyncio.to_thread(guardian_cycle, recovery, stream, worker, commands)
                        progress.complete(
                            data_ready=False,
                            analysis_ready=False,
                            execution_ready=False,
                            reason="LOCAL_GUARDIAN_ONLY",
                        )
                    else:
                        market["ready"] = False
                        ready = recovery.ready and stream.ready
                        if ready:
                            progress.advance("SETTLEMENT")
                            await asyncio.to_thread(settle_closed_results, db, client)

                        def entry_ready() -> bool:
                            return recovery.ready and stream.ready

                        def run_commands() -> None:
                            # Acquire before the command's risk transaction to keep lock order.
                            with recovery.lock:
                                commands.run_once(
                                    ready=entry_ready,
                                    testnet=client.environment == "testnet",
                                    cleanup=lambda: cleanup_flat_testnet(client),
                                )

                        await asyncio.to_thread(run_commands)
                        enable_entries, trial_enabled = await asyncio.to_thread(
                            commands.testnet_authority, default_authority
                        )
                        decisions.trial_policy = TestnetTrialPolicy(enabled=trial_enabled)
                        if ready and first_start and enable_entries:
                            await asyncio.to_thread(
                                initial_demo_activation, db, first_start=True, ready=True
                            )
                            first_start = False
                        progress.advance("MARKET_HANDOFF")
                        assert internal_token is not None
                        data = await fetch_memory(session, internal_token)
                        market.update(
                            ready=data["status"].get("data_ready") is True, as_of=data["as_of"]
                        )
                        progress.advance("STORAGE_ADMISSION")
                        if time.monotonic() - retained >= 900:
                            retained = time.monotonic()
                            await asyncio.to_thread(retain, db)
                        storage_ok = await asyncio.to_thread(observe_storage, db)
                        risk = await asyncio.to_thread(RiskService(db).state)
                        analyzed = bool(
                            data["status"].get("analysis_coverage_ready") is True
                            and await asyncio.to_thread(analysis_ready, db, datetime.now(UTC))
                        )
                        admitted = bool(
                            enable_entries
                            and recovery.ready
                            and stream.ready
                            and market["ready"]
                            and analyzed
                            and storage_ok
                            and not risk.kill_switch
                        )
                        if admitted:
                            progress.advance("DECISION_EVALUATION")
                            await asyncio.to_thread(decisions.tick, data)
                            progress.advance("OUTBOX_DISPATCH")

                            def run_worker() -> None:
                                # Recheck after waiting: protection may have revoked admission.
                                with recovery.lock:
                                    if recovery.ready and stream.ready:
                                        worker.run_once()

                            await asyncio.to_thread(run_worker)
                        admitted = admitted and recovery.ready and stream.ready
                        reason = (
                            "DEMO_ENTRIES_DISABLED"
                            if not enable_entries
                            else "PRIVATE_STREAM_NOT_READY"
                            if not stream.transport_ready
                            else "ACCOUNT_RECONCILIATION_REQUIRED"
                            if not recovery.ready
                            else "PRIVATE_STREAM_NOT_READY"
                            if not stream.ready
                            else "PUBLIC_MARKET_RECOVERY_REQUIRED"
                            if not market["ready"]
                            else "BROAD_ANALYSIS_NOT_READY"
                            if not analyzed
                            else "STORAGE_ADMISSION_HALTED"
                            if not storage_ok
                            else "RISK_KILL_SWITCH"
                            if risk.kill_switch
                            else "READY"
                        )
                        blockers = []
                        if not enable_entries:
                            blockers.append("DEMO_ENTRIES_DISABLED")
                        if not stream.ready:
                            blockers.append("PRIVATE_STREAM_NOT_READY")
                        if not recovery.ready:
                            protection_reasons = recovery.health()["reasons"]
                            blockers.extend(
                                protection_reasons
                                if isinstance(protection_reasons, list) and protection_reasons
                                else ["ACCOUNT_RECONCILIATION_STALE"]
                            )
                        if not market["ready"]:
                            blockers.append("PUBLIC_MARKET_RECOVERY_REQUIRED")
                        if not analyzed:
                            blockers.append("BROAD_ANALYSIS_NOT_READY")
                        if not storage_ok:
                            blockers.append("STORAGE_ADMISSION_HALTED")
                        if risk.kill_switch:
                            blockers.append("RISK_KILL_SWITCH")
                        progress.state.update(
                            entries_enabled=enable_entries,
                            trial_enabled=trial_enabled,
                            stream_ready=stream.ready,
                            private_stream=stream.health(),
                            protection=recovery.health(),
                            protection_ready=recovery.ready,
                            blockers=blockers,
                        )
                        progress.complete(
                            data_ready=bool(market["ready"]),
                            analysis_ready=analyzed,
                            execution_ready=admitted,
                            reason=reason,
                        )
                except (
                    EntryHalt,
                    PersistenceError,
                    ValueError,
                    ClientError,
                    TimeoutError,
                    OSError,
                    AckUnknown,
                    ExchangeRejected,
                    KeyError,
                    TypeError,
                    ArithmeticError,
                ) as error:
                    progress.fail(error)
                finally:
                    progress.state.update(decisions.status())
                    at = datetime.now(UTC)
                    try:
                        await asyncio.to_thread(
                            save_runtime,
                            db,
                            "execution_guardian",
                            at,
                            at + timedelta(seconds=15),
                            progress.snapshot(),
                        )
                    except PersistenceError as error:
                        progress.fail(error)
                interval = 2 if autonomous else 10
                deadline = time.monotonic() + max(0.05, interval - (time.monotonic() - started))
                await asyncio.sleep(max(0, deadline - time.monotonic()))
                progress.state["loop_lag_ms"] = round(max(0, time.monotonic() - deadline) * 1000, 3)
        finally:
            await session.close()

    tasks = []

    try:
        await web.TCPSite(runner, host, port).start()
        tasks = [
            asyncio.create_task(dispatch()),
            asyncio.create_task(stream.run(client)),
            asyncio.create_task(project_demo(db, client)),
        ]
        if autonomous:
            tasks.append(asyncio.create_task(protection_loop(recovery, stream)))
        if expected_revision is not None:
            from tradeforge.runtime.binance_environment import select_environment

            async def fence_environment() -> None:
                while True:
                    await asyncio.sleep(1)
                    current = await asyncio.to_thread(select_environment, db)
                    if current.revision != expected_revision or current.name != "testnet":
                        raise RuntimeError("BINANCE_ENVIRONMENT_RESTART_REQUIRED")

            tasks.append(asyncio.create_task(fence_environment()))
        print(
            json.dumps(
                {
                    "service": "execution_guardian",
                    "mode": "testnet",
                    "live": False,
                    "orders_enabled": False,
                    "reason": "EXPLICIT_RISK_AND_CONTEXT_REQUIRED",
                }
            ),
            flush=True,
        )
        if expected_revision is None:
            await asyncio.gather(*tasks)
        else:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                await task
            raise RuntimeError("GUARDIAN_PROCESS_STOPPED")
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await runner.cleanup()


def run_local(home: Path, port: int, *, observer_home: Path | None = None) -> None:
    settings = load_settings()
    if settings.mode is not RuntimeMode.TESTNET or settings.live:
        raise ValueError("EXPLICIT_TESTNET_EXECUTION_REQUIRED")
    root = Path(__file__).resolve().parents[2]
    url = make_url(private_ignored_file(home / "guardian.url", root))
    if (
        url.host not in {"127.0.0.1", "localhost", "::1"}
        or url.query
        or url.database != "tradeforge_demo"
        or url.username != "tf_demo_guardian"
    ):
        raise ValueError("DEMO_DATABASE_REQUIRED")
    credentials = load_demo_credentials(home, observer_home=observer_home)
    # Read-only account diagnostics precede any possible protection/execution write.
    diagnostic = preflight(
        BinanceTestnetClient(
            credentials,
            mode="testnet",
            opted_in=True,
            read_only=True,
        )
    )
    if any(
        diagnostic[k] != "OK" for k in ("connection", "account", "balance", "positions", "clock")
    ) or set(diagnostic["reason_codes"]) - {
        "EXISTING_EXPOSURE_REQUIRES_RECONCILIATION",
        "KEY_PERMISSIONS_UNVERIFIED",
        "EXPLICIT_TESTNET_EXECUTION_REQUIRED",
    }:
        raise ValueError("DEMO_PREFLIGHT_FAILED")
    db = Database(url)
    try:
        with db.transaction() as c:
            bound = c.scalar(
                text(
                    "SELECT payload->>'environment' FROM audit_log "
                    "WHERE event_type='execution.environment.v1' LIMIT 1"
                )
            )
            if bound != "testnet" or c.scalar(text("SELECT count(*) FROM runtime_market")):
                raise ValueError("DEMO_DATABASE_ENVIRONMENT_MISMATCH")
        if not db.probe().db_writable:
            raise ValueError("DB_UNAVAILABLE")
        RiskService(db).set_kill_switch(True, reason="GUARDIAN_STARTUP_RECONCILIATION")
        client = BinanceTestnetClient(credentials, mode="testnet", opted_in=True, read_only=False)
        asyncio.run(serve(db, client, port))
    finally:
        db.close()


def main() -> int:
    if "--testnet" not in sys.argv[1:]:
        return foundation_main("execution_guardian", 8103)
    parser = argparse.ArgumentParser(description="Explicit loopback TESTNET guardian; LIVE locked")
    parser.add_argument("--testnet", action="store_true")
    parser.add_argument("--enable-orders", action="store_true")
    parser.add_argument("--database-file", type=Path, required=True)
    parser.add_argument("--key-file", type=Path, required=True)
    parser.add_argument("--secret-file", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8103)
    parser.add_argument("--observer-home", type=Path)
    args = parser.parse_args()
    try:
        if not args.enable_orders or not 1 <= args.port <= 65535:
            raise ValueError
        home = args.database_file.parent
        if (
            args.database_file.name != "guardian.url"
            or args.key_file != home / "binance-demo.key"
            or args.secret_file != home / "binance-demo.secret"
        ):
            raise ValueError
        run_local(home, args.port, observer_home=args.observer_home)
    except KeyboardInterrupt:
        return 0
    except Exception:
        parser.error("TESTNET_GUARDIAN_UNAVAILABLE")
    return 0

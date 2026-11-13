"""Embedded autonomous Testnet execution. Runs inside quant_core; no separate service."""

import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

from aiohttp import ClientError, ClientSession
from sqlalchemy import text

from tradeforge.dashboard.control import ExecutionCommands
from tradeforge.domain.execution import ExecutionIntent
from tradeforge.execution.binance_client import BinanceTestnetClient
from tradeforge.execution.broker import AckUnknown, ExchangeRejected, ExecutionContext
from tradeforge.execution.context import load_context, verify_exchange_context
from tradeforge.execution.order_manager import OrderManager
from tradeforge.execution.outbox_worker import OutboxWorker
from tradeforge.execution.position_lifecycle import PositionLifecycle
from tradeforge.execution.private_stream import PrivateStream
from tradeforge.persistence.db import Database, EntryHalt, PersistenceError
from tradeforge.risk.storage import RiskService
from tradeforge.runtime.demo_projection import project_demo
from tradeforge.runtime.event_handoff import fetch_market_events
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


def execution_cycle(
    recovery: PositionLifecycle,
    stream: PrivateStream,
    worker: OutboxWorker,
    commands: ExecutionCommands,
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
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="auto-protection")
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
    *,
    internal_token: str,
    enable_entries: bool = True,
    trial_enabled: bool = False,
    expected_revision: int | None = None,
    gateway_base: str | None = None,
) -> None:
    if client.environment != "testnet" or not enable_entries:
        raise ValueError("EXPLICIT_TESTNET_AUTO_EXECUTION_REQUIRED")
    stream: PrivateStream
    autonomous = bool(internal_token)
    if not autonomous:
        raise ValueError("INTERNAL_MARKET_TOKEN_REQUIRED")
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
    commands = ExecutionCommands(db)
    progress = RuntimeProgress()
    # No second web server, route or port: the execution worker is embedded in quant_core.

    async def dispatch() -> None:
        nonlocal first_start, enable_entries, trial_enabled, gateway_base
        from tradeforge.decision.trial import TestnetTrialPolicy
        from tradeforge.execution.account_sync import cleanup_flat_testnet
        from tradeforge.runtime.demo_decisions import DemoDecisions
        from tradeforge.runtime.maintenance import observe_storage, retain
        from tradeforge.runtime.market_link import fetch_memory, require_market_environment
        from tradeforge.runtime.settlement import settle_closed_results
        from tradeforge.runtime.trading import initial_demo_activation

        decisions = DemoDecisions(
            db, client, trial_policy=TestnetTrialPolicy(enabled=trial_enabled)
        )
        default_authority = enable_entries, trial_enabled
        retained = float("-inf")
        storage_checked = float("-inf")
        storage_ok = False
        session = ClientSession(trust_env=False)
        cursor, epoch = 0, ""
        wake_symbols: frozenset[str] | None = None
        last_full_decision_scan = float("-inf")
        try:
            while True:
                started = time.monotonic()
                progress.advance("ENTRY_ADMISSION" if autonomous else "RECONCILIATION")
                try:
                    # Filled/canceled entries retain protection work even with entries disabled.
                    if not autonomous:
                        await asyncio.to_thread(execution_cycle, recovery, stream, worker, commands)
                        progress.complete(
                            data_ready=False,
                            analysis_ready=False,
                            execution_ready=False,
                            reason="LOCAL_EXECUTION_DISABLED",
                        )
                    else:
                        market.update(ready=False, source=None)
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
                        # A saved start command authorizes entries. The operator
                        # must also opt in to the separately capped trial policy.
                        trial_enabled = trial_enabled and default_authority[1]
                        decisions.trial_policy = TestnetTrialPolicy(enabled=trial_enabled)
                        if ready and first_start and enable_entries:
                            await asyncio.to_thread(
                                initial_demo_activation, db, first_start=True, ready=True
                            )
                            first_start = False
                        progress.advance("STORAGE_ADMISSION")
                        if time.monotonic() - storage_checked >= 5:
                            storage_ok = False
                            storage_ok = await asyncio.to_thread(observe_storage, db)
                            storage_checked = time.monotonic()
                        if time.monotonic() - retained >= 900:
                            retained = time.monotonic()
                            await asyncio.to_thread(retain, db)
                        progress.advance("MARKET_HANDOFF")
                        assert internal_token is not None
                        full_scan = time.monotonic() - last_full_decision_scan >= 15
                        selected_symbols = None if full_scan else wake_symbols
                        data = await fetch_memory(
                            session,
                            internal_token,
                            base=gateway_base,
                            summary_only=True,
                        )
                        require_market_environment(data, "testnet")
                        market.update(
                            ready=data["status"].get("data_ready") is True,
                            as_of=data["as_of"],
                            source=data["status"].get("market_data_source"),
                        )
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
                            if selected_symbols is None or selected_symbols:
                                pending = await asyncio.to_thread(
                                    decisions.pending, selected_symbols
                                )
                                if pending:
                                    symbols = frozenset(
                                        record.discovery.candidate.instrument.symbol
                                        for record in pending
                                    )
                                    data = await fetch_memory(
                                        session, internal_token, base=gateway_base, symbols=symbols
                                    )
                                    require_market_environment(data, "testnet")
                                    market.update(
                                        ready=data["status"].get("data_ready") is True,
                                        as_of=data["as_of"],
                                        source=data["status"].get("market_data_source"),
                                    )
                                await asyncio.to_thread(
                                    decisions.tick, data, symbols=selected_symbols, pending=pending
                                )
                                if selected_symbols is None:
                                    last_full_decision_scan = time.monotonic()
                            progress.advance("OUTBOX_DISPATCH")

                            def run_worker() -> None:
                                # Recheck after waiting: protection may have revoked admission.
                                with recovery.lock:
                                    if recovery.ready and stream.ready:
                                        worker.drain(limit=8)

                            await asyncio.to_thread(run_worker)
                        admitted = (
                            admitted and market["ready"] is True and recovery.ready and stream.ready
                        )
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
                            account_reconciled=recovery.ready,
                            market_ready=bool(market["ready"]),
                            market_data_source=market["source"] if market["ready"] else None,
                            risk_allowed=not risk.kill_switch,
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
                    progress.state["blockers"] = [progress.state["reason"]]
                finally:
                    progress.state.update(decisions.status())
                    at = datetime.now(UTC)
                    try:
                        await asyncio.to_thread(
                            save_runtime,
                            db,
                            "execution_core",
                            at,
                            at + timedelta(seconds=15),
                            progress.snapshot(),
                        )
                    except PersistenceError as error:
                        progress.fail(error)
                if autonomous:
                    try:
                        packet = await fetch_market_events(
                            session,
                            internal_token,
                            after=cursor,
                            epoch=epoch,
                            wait=2.0,
                            base=gateway_base,
                        )
                        cursor, epoch = int(packet["sequence"]), str(packet["epoch"])
                        wake_symbols = (
                            None
                            if packet["overflow"]
                            or any(
                                isinstance(event, dict) and event.get("kind") == "FRAME"
                                for event in packet["events"]
                            )
                            else frozenset(
                                str(event["symbol"])
                                for event in packet["events"]
                                if isinstance(event, dict) and "symbol" in event
                            )
                        )
                        # Coalesce bursts; prevent repeated expensive exchange
                        # preflight on every single order-book packet.
                        elapsed = time.monotonic() - started
                        if elapsed < 1.0:
                            await asyncio.sleep(1.0 - elapsed)
                    except (OSError, ValueError, TimeoutError, KeyError, TypeError):
                        wake_symbols = None
                        await asyncio.sleep(1.0)
                else:
                    await asyncio.sleep(10)
                progress.state["loop_lag_ms"] = round(
                    max(0, time.monotonic() - started - 2) * 1000, 3
                )
        finally:
            await session.close()

    tasks = []

    try:
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
                    try:
                        current = await asyncio.to_thread(select_environment, db)
                    except PersistenceError as error:
                        if str(error) == "BINANCE_ENVIRONMENT_RESTART_REQUIRED":
                            raise
                        continue
                    if current.revision != expected_revision or current.name != "testnet":
                        raise RuntimeError("BINANCE_ENVIRONMENT_RESTART_REQUIRED")

            tasks.append(asyncio.create_task(fence_environment()))
        print(
            json.dumps(
                {
                    "service": "execution_core",
                    "mode": "testnet",
                    "live": False,
                    "orders_enabled": False,
                    "reason": "AUTONOMOUS_RISK_AND_CONTEXT_REQUIRED",
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
            raise RuntimeError("EXECUTION_PROCESS_STOPPED")
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

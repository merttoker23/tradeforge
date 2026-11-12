"""Explicit continuous local analysis; offline foundation remains available."""

import argparse
import asyncio
import json
import time
from copy import deepcopy
from datetime import timedelta
from pathlib import Path

from aiohttp import ClientSession, web

from tradeforge.domain.market import MarketSnapshot
from tradeforge.intelligence.runtime import ANALYSIS_COUNTERS, QuantWorker
from tradeforge.market.socket_manager import pause
from tradeforge.persistence.db import Database, PersistenceError
from tradeforge.runtime.analysis import AutonomousAnalysis
from tradeforge.runtime.config import local_database, local_token_file
from tradeforge.runtime.event_handoff import fetch_market_events
from tradeforge.runtime.market_link import alpha_inputs, fetch_memory
from tradeforge.runtime.signals import shutdown_signals
from tradeforge.runtime.storage import save_runtime


def preserve_analysis(state: dict, failed: dict) -> dict:
    """Carry the last known analysis counters into a failed cycle's state."""
    for key in ANALYSIS_COUNTERS:
        if key in state:
            failed[key] = state[key]
    return failed


def create_app(worker: QuantWorker) -> web.Application:
    from tradeforge.config.settings import load_settings

    async def health(request: web.Request) -> web.Response:
        state = worker.status()
        return web.json_response(
            {
                "service": "quant_core",
                "mode": load_settings().mode.value,
                "phase": "PHASE_16A",
                **state,
            },
            status=200 if request.path == "/health" or state["data_ready"] else 503,
            headers={"Cache-Control": "no-store"},
        )

    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_get("/readiness", health)
    return app


async def run_cycle(
    worker: QuantWorker,
    analysis: AutonomousAnalysis | None,
    session: ClientSession | None,
    internal_token: str | None,
    *,
    changed_symbols: frozenset[str] | None = None,
    refresh_features: bool = True,
    gateway_base: str | None = None,
) -> None:
    started = time.perf_counter()
    last_evaluation = worker.state.get("last_evaluation")
    last_input_check = worker.state.get("last_input_check")
    worker.progress.state.setdefault("loop_lag_ms", 0.0)
    worker.progress.advance("MARKET_HANDOFF")
    try:
        # Standalone uses fixed local loopback; Compose keeps its authenticated
        # service-to-service handoff. Neither mode needs additional user setup.
        worker.market_samples = ()
        if session is None:
            feed = None
        elif refresh_features or changed_symbols is None:
            feed = await fetch_memory(session, internal_token, base=gateway_base, summary_only=True)
        else:
            feed = await fetch_memory(
                session, internal_token, base=gateway_base, symbols=changed_symbols
            )
        if feed is not None:
            worker.market_samples = tuple(
                MarketSnapshot.model_validate_json(json.dumps(m)) for m in feed["markets"]
            )
        if refresh_features:
            await asyncio.to_thread(worker.tick, persist=False)
        if (
            refresh_features
            and worker.state.get("processed", 0) > 0
            and worker.detector.latest is not None
        ):
            worker.progress.state.update(
                last_analysis_at=worker.clock().isoformat(),
                last_analysis_source_at=worker.detector.latest.frame.source_at.isoformat(),
            )
        deep_status = feed.get("deep_status", []) if feed else []
        worker.state.update(
            # Per-pair deep results are cumulative. A pass without fresh
            # strategy evidence must not erase the last completed evaluation.
            input_rejections=[
                {"symbol": row["symbol"], "reason": row["reason"]}
                for row in deep_status[:16]
                if row.get("ready") is not True
            ],
            selected_deep_count=len(feed.get("deep_status", feed.get("deep", []))) if feed else 0,
            ready_deep_count=sum(row.get("ready") is True for row in deep_status),
        )
        if (
            feed is not None
            and worker.detector.latest is not None
            and worker.status()["analysis_ready"]
        ):
            worker.progress.advance("ALPHA_INPUTS")
            diagnostics: dict[str, object] = {}
            symbols = changed_symbols
            if deep_status:
                assert analysis is not None
                # Joining and validating the full-universe feature proof costs
                # time for every pair. A 24-pair join outlives the five-second
                # quote/book TTL, even when the gateway selects only 24 pairs.
                # Fetch fresh microstructure and dispatch one ready pair at a
                # time; the cursor still gives every ready pair a turn.
                symbols = analysis.next_batch(
                    frozenset(
                        row["symbol"] for row in deep_status if row.get("ready") is True
                    ),
                    limit=1,
                )
            # The analysis cycle is longer than the market evidence's freshness
            # window. The alpha inputs must be measured against a market
            # snapshot that is still fresh at the evaluation time, otherwise
            # every price is rejected as stale before it is even considered.
            current = (
                feed if symbols == frozenset()
                else await fetch_memory(session, internal_token, base=gateway_base, symbols=symbols)
            )
            inputs = await asyncio.to_thread(
                alpha_inputs,
                worker.db,
                worker.detector.latest,
                current,
                evaluated_at=worker.clock(),
                diagnostics=diagnostics,
                symbols=symbols,
            )
            worker.state.update(diagnostics)
            if symbols is not None:
                inputs = tuple(
                    sample for sample in inputs
                    if sample.instrument.symbol in symbols
                )
            last_input_check = deepcopy(
                {
                    "as_of": worker.clock().isoformat(),
                    "input_rejections": diagnostics.get("input_rejections", []),
                }
            )
            if inputs:
                worker.progress.advance("ALPHA_EVALUATION")
                assert analysis is not None
                if symbols is None:
                    await asyncio.to_thread(analysis.evaluate, inputs)
                else:
                    await asyncio.to_thread(
                        analysis.evaluate, inputs, symbols=symbols
                    )
                worker.state.update(analysis.state)
                worker.progress.state["last_evaluation_at"] = worker.clock().isoformat()
                # Display history is separate from this pass's readiness and
                # results. Empty/failed passes must not erase a completed batch.
                last_evaluation = deepcopy(
                    {
                        "as_of": worker.progress.state["last_evaluation_at"],
                        "symbol_results": analysis.state["symbol_results"],
                        "symbol_summaries": analysis.state["symbol_summaries"],
                        "reason_counts": analysis.state["reason_counts"],
                        "evaluated_count": analysis.state["evaluated_count"],
                        "analyzed_symbols": analysis.state["analyzed_symbols"],
                        "evaluation_total": analysis.state["evaluation_total"],
                        "last_analysis_at": analysis.state["last_analysis_at"],
                        "input_rejections": diagnostics.get("input_rejections", []),
                    }
                )
        if worker.progress.state["healthy"]:
            worker.progress.advance("DISPATCH_COMPLETE")
        worker.state.update(worker.progress.state)
    except Exception as error:
        stage = worker.progress.state["stage"]
        worker.progress.fail(error)
        worker.progress.state["healthy"] = False
        reason = (
            "MARKET_HANDOFF_UNAVAILABLE"
            if stage == "MARKET_HANDOFF"
            else "QUANT_ANALYSIS_UNAVAILABLE"
        )
        worker.state = preserve_analysis(worker.state, worker._state(reason, False, 0))
    worker.state["last_evaluation"] = last_evaluation
    worker.state["last_input_check"] = last_input_check
    worker.state["cycle_ms"] = round((time.perf_counter() - started) * 1000, 3)
    try:
        worker.progress.advance("RUNTIME_STORAGE")
        now = worker.clock()
        await asyncio.to_thread(
            save_runtime, worker.db, "quant_core", now, now + timedelta(seconds=15), worker.state
        )
    except Exception as error:
        worker.progress.fail(error)
        worker.progress.state["healthy"] = False
        worker.state = preserve_analysis(
            worker.state, worker._state("QUANT_STORAGE_UNAVAILABLE", False, 0)
        )
        worker.state["last_evaluation"] = last_evaluation
        worker.state["last_input_check"] = last_input_check


async def serve(
    db: Database,
    port: int,
    *,
    host: str = "127.0.0.1",
    internal_token: str | None = None,
    gateway_base: str | None = None,
) -> None:
    from tradeforge.runtime.binance_environment import select_environment

    active = select_environment(db) if internal_token is not None else None
    if active is not None:
        db.pin_environment(active.revision)
    execution_task: asyncio.Task[None] | None = None
    if active is not None:
        import os
        from tradeforge.config.policy import RuntimeMode
        from tradeforge.config.settings import load_settings
        from tradeforge.runtime.binance_environment import load_credentials
        from tradeforge.execution.binance_client import BinanceTestnetClient, Credentials
        from tradeforge.runtime.auto_execution import serve as execute

        mode = load_settings().mode
        enabled = os.environ.get("TF_AUTO_ORDERS", "false")
        if enabled not in {"false", "true"}:
            raise ValueError("AUTO_ORDERS_CONFIG_INVALID")
        if enabled == "true":
            if mode is not RuntimeMode.TESTNET or active.name != "testnet":
                raise ValueError("TESTNET_AUTO_ORDERS_REQUIRED")
            if not internal_token:
                raise ValueError("AUTO_EXECUTION_MASTER_REQUIRED")
            pair = load_credentials(db, internal_token, "testnet")
            if pair is None:
                raise ValueError("AUTO_EXECUTION_CREDENTIALS_MISSING")
            client = BinanceTestnetClient(
                Credentials.model_validate({"api_key": pair[0], "api_secret": pair[1]}),
                mode="testnet", opted_in=True, read_only=False,
            )
            execution_task = asyncio.create_task(
                execute(db, client, internal_token=internal_token,
                        enable_entries=True, trial_enabled=True,
                        expected_revision=active.revision, gateway_base=gateway_base)
            )
    worker = QuantWorker(db, require_gateway=True)
    runner = web.AppRunner(create_app(worker), access_log=None)
    stop = asyncio.Event()
    with shutdown_signals(stop):
        await runner.setup()
        try:
            await web.TCPSite(runner, host, port).start()
            analysis = AutonomousAnalysis(db)
            async with ClientSession(trust_env=False) as session:
                expected = time.monotonic()
                last_run = float('-inf')
                last_full_run = float('-inf')
                cursor, epoch = 0, ''
                pending_symbols: set[str] = set()
                while not stop.is_set():
                    if execution_task is not None and execution_task.done():
                        await execution_task  # Failed execution must not run silently.
                        raise RuntimeError("AUTO_EXECUTION_STOPPED")
                    if active is not None:
                        try:
                            current = await asyncio.to_thread(select_environment, db)
                        except PersistenceError as error:
                            if str(error) == "BINANCE_ENVIRONMENT_RESTART_REQUIRED":
                                raise
                            worker.progress.advance("ENVIRONMENT")
                            worker.progress.fail(error)
                            worker.progress.state["healthy"] = False
                            worker.state = worker._state("QUANT_STORAGE_UNAVAILABLE", False, 0)
                            await pause(stop, 1)
                            continue
                        if current.revision != active.revision:
                            raise RuntimeError("BINANCE_ENVIRONMENT_RESTART_REQUIRED")
                    if last_run != float('-inf'):
                        try:
                            packet = await fetch_market_events(
                                session, internal_token, after=cursor, epoch=epoch, wait=0.25,
                                base=gateway_base,
                            )
                            cursor = int(packet['sequence'])
                            epoch = str(packet['epoch'])
                            if packet['overflow']:
                                pending_symbols.clear()
                                refresh_all = True
                                frame_changed = True
                            else:
                                refresh_all = False
                                frame_changed = any(
                                    isinstance(event, dict) and event.get('kind') == 'FRAME'
                                    for event in packet['events']
                                )
                                pending_symbols.update(
                                    str(event['symbol'])
                                    for event in packet['events']
                                    if isinstance(event, dict) and 'symbol' in event
                                )
                        except (OSError, ValueError, TimeoutError, KeyError, TypeError):
                            # Event hints are never authority; periodic recovery
                            # continues to verify the canonical source of truth.
                            refresh_all = True
                            frame_changed = True
                            await pause(stop, 1.0)
                        if time.monotonic() - last_run < 1.0:
                            await pause(stop, 1.0 - (time.monotonic() - last_run))
                    else:
                        refresh_all = True
                        frame_changed = True
                    started = time.monotonic()
                    worker.progress.state['loop_lag_ms'] = round(
                        max(0, started - expected) * 1000, 3
                    )
                    expected = started
                    # A periodic resynchronization checks new subscriptions
                    # and notifications missed during a process restart.
                    periodic_reconcile = started - last_full_run >= 5
                    selection = (
                        None if refresh_all or frame_changed or periodic_reconcile
                        else frozenset(pending_symbols)
                    )
                    pending_symbols.clear()
                    await run_cycle(
                        worker, analysis, session, internal_token,
                        changed_symbols=selection,
                        gateway_base=gateway_base,
                    )
                    last_run = time.monotonic()
                    if selection is None:
                        last_full_run = last_run
        finally:
            if execution_task is not None:
                execution_task.cancel()
                await asyncio.gather(execution_task, return_exceptions=True)
            await runner.cleanup()


def loopback_base(value: str) -> str:
    """Only a plain http loopback URL may carry the local market handoff."""
    from urllib.parse import urlsplit

    parts = urlsplit(value.strip())
    host = parts.hostname or ""
    port = parts.port or 8101
    if (
        parts.scheme != "http"
        or host not in {"127.0.0.1", "localhost"}
        or parts.username is not None
        or parts.password is not None
        or parts.path not in ("", "/")
        or parts.query
        or parts.fragment
        or not 0 < port <= 65535
    ):
        raise ValueError("GATEWAY_BASE_LOOPBACK_REQUIRED")
    return f"http://{host}:{port}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Local closed-market intelligence worker")
    parser.add_argument("--database-file", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8102)
    parser.add_argument(
        "--gateway-token-file",
        type=Path,
        help="Internal handoff token; also decrypts saved testnet credentials",
    )
    parser.add_argument(
        "--gateway-base",
        help="Explicit market handoff URL (loopback http only); overrides Compose DNS",
    )
    args = parser.parse_args()
    db = None
    try:
        token = (
            local_token_file(args.gateway_token_file) if args.gateway_token_file else None
        )
        base = loopback_base(args.gateway_base) if args.gateway_base else None
        db = local_database(args.database_file)
        if not 0 <= args.port <= 65535:
            raise ValueError
        asyncio.run(serve(db, args.port, internal_token=token, gateway_base=base))
    except KeyboardInterrupt:
        pass
    except Exception:
        parser.error("QUANT_LOCAL_RUNTIME_UNAVAILABLE")
    finally:
        if db is not None:
            db.close()
    return 0

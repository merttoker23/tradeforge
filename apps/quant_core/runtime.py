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
from tradeforge.intelligence.runtime import QuantWorker
from tradeforge.market.socket_manager import pause
from tradeforge.persistence.db import Database
from tradeforge.runtime.analysis import AutonomousAnalysis
from tradeforge.runtime.config import local_database
from tradeforge.runtime.market_link import alpha_inputs, fetch_memory
from tradeforge.runtime.signals import shutdown_signals
from tradeforge.runtime.storage import save_runtime


def create_app(worker: QuantWorker) -> web.Application:
    async def health(request: web.Request) -> web.Response:
        state = worker.status()
        return web.json_response(
            {"service": "quant_core", "mode": "paper", "phase": "PHASE_16A", **state},
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
        feed = await fetch_memory(session, internal_token) if session is not None else None
        if feed is not None:
            worker.market_samples = tuple(
                MarketSnapshot.model_validate_json(json.dumps(m)) for m in feed["markets"]
            )
        await asyncio.to_thread(worker.tick, persist=False)
        if worker.state.get("processed", 0) > 0 and worker.detector.latest is not None:
            worker.progress.state.update(
                last_analysis_at=worker.clock().isoformat(),
                last_analysis_source_at=worker.detector.latest.frame.source_at.isoformat(),
            )
        worker.state.update(
            candidate_count=0,
            reason_counts={},
            evaluated_count=0,
            analysis_symbols=0,
            symbol_results=[],
            input_rejections=[
                {"symbol": row["symbol"], "reason": row["reason"]}
                for row in (feed.get("deep_status", []) if feed else [])[:16]
                if row.get("ready") is not True
            ],
            selected_deep_count=len(feed.get("deep_status", feed.get("deep", []))) if feed else 0,
            ready_deep_count=len(feed.get("deep", [])) if feed else 0,
        )
        if (
            feed is not None
            and worker.detector.latest is not None
            and worker.status()["analysis_ready"]
        ):
            worker.progress.advance("ALPHA_INPUTS")
            diagnostics: dict[str, object] = {}
            inputs = await asyncio.to_thread(
                alpha_inputs,
                worker.db,
                worker.detector.latest,
                feed,
                evaluated_at=worker.clock(),
                diagnostics=diagnostics,
            )
            worker.state.update(diagnostics)
            last_input_check = deepcopy(
                {
                    "as_of": worker.clock().isoformat(),
                    "input_rejections": diagnostics.get("input_rejections", []),
                }
            )
            worker.progress.advance("ALPHA_EVALUATION")
            assert analysis is not None
            await asyncio.to_thread(analysis.evaluate, inputs)
            worker.state.update(analysis.state)
            if inputs:
                worker.progress.state["last_evaluation_at"] = worker.clock().isoformat()
                # Display history is separate from this cycle's readiness and
                # results. Empty/failed cycles must not erase a completed batch.
                last_evaluation = deepcopy(
                    {
                        "as_of": worker.progress.state["last_evaluation_at"],
                        "symbol_results": analysis.state["symbol_results"],
                        "reason_counts": analysis.state["reason_counts"],
                        "evaluated_count": analysis.state["evaluated_count"],
                        "input_rejections": diagnostics.get("input_rejections", []),
                    }
                )
        worker.state.update(worker.progress.state)
        if worker.state.get("healthy") is True:
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
        worker.state = worker._state(reason, False, 0)
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
        worker.state = worker._state("QUANT_STORAGE_UNAVAILABLE", False, 0)
        worker.state["last_evaluation"] = last_evaluation
        worker.state["last_input_check"] = last_input_check


async def serve(
    db: Database, port: int, *, host: str = "127.0.0.1", internal_token: str | None = None
) -> None:
    from tradeforge.runtime.binance_environment import select_environment

    active = select_environment(db) if internal_token is not None else None
    if active is not None:
        db.pin_environment(active.revision)
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
                while not stop.is_set():
                    if active is not None:
                        current = await asyncio.to_thread(select_environment, db)
                        if current.revision != active.revision:
                            raise RuntimeError("BINANCE_ENVIRONMENT_RESTART_REQUIRED")
                    started = time.monotonic()
                    worker.progress.state["loop_lag_ms"] = round(
                        max(0, started - expected) * 1000, 3
                    )
                    expected = started + 2
                    await run_cycle(worker, analysis, session, internal_token)
                    elapsed = time.monotonic() - started
                    await pause(stop, max(0.1, 2 - elapsed))
        finally:
            await runner.cleanup()


def main() -> int:
    parser = argparse.ArgumentParser(description="Local closed-market intelligence worker")
    parser.add_argument("--database-file", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8102)
    args = parser.parse_args()
    db = None
    try:
        db = local_database(args.database_file)
        if not 0 <= args.port <= 65535:
            raise ValueError
        asyncio.run(serve(db, args.port))
    except KeyboardInterrupt:
        pass
    except Exception:
        parser.error("QUANT_LOCAL_RUNTIME_UNAVAILABLE")
    finally:
        if db is not None:
            db.close()
    return 0

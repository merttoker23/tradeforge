"""Worker data readiness is independent of liveness and old successful analysis."""

from datetime import timedelta

import pytest

from tests.intelligence_fixtures import engine, frame
from tradeforge.intelligence.runtime import QuantWorker
from tradeforge.runtime.analysis import AutonomousAnalysis


def test_strategy_batches_reach_all_517_ready_pairs():
    analysis = AutonomousAnalysis(None)
    symbols = frozenset(f"TF{i:04d}USDT" for i in range(517))
    observed = set()
    for _ in range(22):
        batch = analysis.next_batch(symbols)
        assert len(batch) == 24
        observed.update(batch)
    assert observed == symbols


def test_small_deep_tier_is_evaluated_before_microstructure_expires(monkeypatch):
    """Even a 24-pair tier must not age all quotes during serial input validation."""
    import asyncio
    from types import SimpleNamespace

    from apps.quant_core import runtime
    from tradeforge.persistence.db import Database

    at = [frame(1).as_of]
    db = Database("postgresql+psycopg://unused@127.0.0.1:1/unused")
    worker = QuantWorker(db, clock=lambda: at[0])
    worker.detector.update(frame(1))
    analysis = AutonomousAnalysis(db, clock=lambda: at[0])
    ready = frozenset({"BTCUSDT", "ETHUSDT", "SOLUSDT"})
    evaluated = []

    async def handoff(*args, **kwargs):
        selected = kwargs.get("symbols")
        selected = ready if selected is None else ready.intersection(selected)
        return {
            "as_of": at[0], "markets": [],
            "deep_status": [{"symbol": s, "ready": True} for s in sorted(selected)],
        }

    def tick(**kwargs):
        worker.state = worker._state("ANALYZED", True, 0)

    def inputs(db, features, feed, **kwargs):
        selected = kwargs.get("symbols")
        selected = ready if selected is None else ready.intersection(selected)
        # Full-universe feature validation costs time for each joined pair.
        at[0] += timedelta(seconds=3 * len(selected))
        return tuple(SimpleNamespace(instrument=SimpleNamespace(symbol=s),
                                     source_at=feed["as_of"]) for s in sorted(selected))

    def evaluate(samples, **kwargs):
        for sample in samples:
            if (at[0] - sample.source_at).total_seconds() <= 5:
                evaluated.append(sample.instrument.symbol)

    monkeypatch.setattr(runtime, "fetch_memory", handoff)
    monkeypatch.setattr(runtime, "alpha_inputs", inputs)
    monkeypatch.setattr(runtime, "save_runtime", lambda *args: None)
    monkeypatch.setattr(worker, "tick", tick)
    monkeypatch.setattr(analysis, "evaluate", evaluate)
    try:
        for _ in ready:
            asyncio.run(runtime.run_cycle(worker, analysis, object(), "offline-token"))
        assert evaluated == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    finally:
        db.close()


@pytest.mark.parametrize("reason", ["UNIVERSE_MISSING", "UNIVERSE_STALE", "WAITING_HISTORY_SWEEP"])
def test_fresh_previous_analysis_cannot_mask_current_input_failure(monkeypatch, reason):
    from tradeforge.persistence.db import Database

    detector = engine()
    for index in range(1, 9):
        detector.update(frame(index))
    latest = detector.latest
    assert latest is not None and latest.state.label.value not in {"UNKNOWN", "TRANSITION"}
    db = Database("postgresql+psycopg://unused@127.0.0.1:1/unused")
    try:
        worker = QuantWorker(db, clock=lambda: latest.frame.as_of + timedelta(seconds=1))
        worker.loaded, worker.detector = True, detector
        monkeypatch.setattr(worker, "_frame", lambda now: (None, reason))
        monkeypatch.setattr("tradeforge.intelligence.runtime.save_runtime", lambda *args: None)
        state = worker.tick()
        assert state["alive"] and state["data_ready"] is False
        assert state["reason"] == reason
    finally:
        db.close()


def test_health_server_cannot_reuse_a_stalled_tick():
    from datetime import UTC, datetime

    from tradeforge.persistence.db import Database

    at = [datetime.now(UTC)]
    db = Database("postgresql+psycopg://unused@127.0.0.1:1/unused")
    try:
        worker = QuantWorker(db, clock=lambda: at[0])
        worker.state["data_ready"] = True
        at[0] += timedelta(seconds=16)
        assert worker.status()["alive"] is True
        assert worker.status()["data_ready"] is False
        assert worker.status()["reason"] == "QUANT_TICK_STALE"
    finally:
        db.close()


def test_quant_stage_failure_has_safe_diagnostics_and_revokes_authority(monkeypatch):
    from tradeforge.persistence.db import Database

    db = Database("postgresql+psycopg://unused@127.0.0.1:1/unused")
    worker = QuantWorker(db)
    try:

        def fail(now):
            raise ValueError("PASSWORD_SENTINEL")

        monkeypatch.setattr(worker, "_load", fail)
        state = worker.tick()
        assert state["stage"] == "QUANT_LOAD"
        assert state["healthy"] is False
        assert state["last_error"] == "INPUT_INVALID"
        assert state["error_count"] == 1
        assert state["analysis_ready"] is False
        assert state["execution_ready"] is False
        assert "PASSWORD_SENTINEL" not in str(state)
    finally:
        db.close()


def test_handoff_failure_reports_stage_and_never_reuses_old_market_samples(monkeypatch):
    import asyncio

    from apps.quant_core import runtime
    from tradeforge.persistence.db import Database

    assert callable(getattr(runtime, "run_cycle", None))
    db = Database("postgresql+psycopg://unused@127.0.0.1:1/unused")
    worker = QuantWorker(db)
    worker.market_samples = (frame(1).observations[0].market,)

    async def fail(*args, **kwargs):
        raise OSError("PASSWORD_SENTINEL")

    monkeypatch.setattr(runtime, "fetch_memory", fail)
    monkeypatch.setattr(runtime, "save_runtime", lambda *args: None)
    try:
        asyncio.run(runtime.run_cycle(worker, None, object(), "offline-token"))
        assert worker.market_samples == ()
        assert worker.state["stage"] == "MARKET_HANDOFF"
        assert worker.state["last_error"] == "TRANSPORT_UNAVAILABLE"
        assert worker.state["data_ready"] is False
        assert worker.state["analysis_ready"] is False
        assert worker.state["execution_ready"] is False
        assert "PASSWORD_SENTINEL" not in str(worker.state)
    finally:
        db.close()


def test_alpha_stage_failure_is_persisted_as_safe_metadata(monkeypatch):
    import asyncio

    from apps.quant_core import runtime
    from tradeforge.persistence.db import Database

    db = Database("postgresql+psycopg://unused@127.0.0.1:1/unused")
    worker = QuantWorker(db)
    detector = engine()
    detector.update(frame(1))
    worker.detector = detector
    captured = []

    async def handoff(*args, **kwargs):
        return {"markets": []}

    def tick(**kwargs):
        worker.progress.complete(
            data_ready=True, analysis_ready=True, execution_ready=False, reason="ANALYZED"
        )
        worker.state = worker._state("ANALYZED", True, 0)

    def fail(*args, **kwargs):
        raise ValueError("PASSWORD_SENTINEL")

    monkeypatch.setattr(runtime, "fetch_memory", handoff)
    monkeypatch.setattr(worker, "tick", tick)
    monkeypatch.setattr(runtime, "alpha_inputs", fail)
    monkeypatch.setattr(runtime, "save_runtime", lambda *args: captured.append(args[-1]))
    try:
        asyncio.run(runtime.run_cycle(worker, None, object(), "offline-token"))
        assert worker.state["stage"] == "ALPHA_INPUTS"
        assert worker.state["last_error"] == "INPUT_INVALID"
        assert captured[0]["stage"] == "ALPHA_INPUTS"
        assert captured[0]["loop_lag_ms"] == 0
        assert worker.state["data_ready"] is False
        assert worker.state["execution_ready"] is False
        assert "PASSWORD_SENTINEL" not in str(worker.state)
    finally:
        db.close()


def test_unknown_regime_is_explained_instead_of_just_waiting_for_a_bar(monkeypatch):
    from tradeforge.persistence.db import Database

    detector = engine()
    detector.update(frame(1))
    latest = detector.latest
    db = Database("postgresql+psycopg://unused@127.0.0.1:1/unused")
    try:
        worker = QuantWorker(db, clock=lambda: latest.frame.as_of + timedelta(seconds=1))
        worker.loaded, worker.detector = True, detector
        monkeypatch.setattr(worker, "_frame", lambda now: (None, "WAITING_CLOSED_BAR"))
        state = worker.tick(persist=False)
        assert state["analysis_ready"] is True
        assert state["regime_label"] == "UNKNOWN"
        assert state["regime_tradeable"] is False
    finally:
        db.close()


def test_same_period_enrichment_preserves_regime_confirmation_without_double_count(monkeypatch):
    from tradeforge.domain.market import RegimeLabel
    from tradeforge.intelligence.regime import RegimeEngine
    from tradeforge.persistence.db import Database

    samples = iter(((frame(1), False), (frame(1), True), (frame(2), False), (frame(2), True)))
    at = [frame(1).as_of]
    db = Database("postgresql+psycopg://unused@127.0.0.1:1/unused")
    worker = QuantWorker(db, clock=lambda: at[0])
    worker.loaded, worker.detector = True, engine(cooldown=0)

    def next_frame(now):
        sample, worker.rebuild_current = next(samples)
        at[0] = sample.as_of
        return sample, "ANALYZED"

    monkeypatch.setattr(worker, "_frame", next_frame)
    monkeypatch.setattr(worker, "_persist", lambda *args: None)

    def classify(self, sample, features, reasons):
        reasons.append("RANGE_EVIDENCE")
        return RegimeLabel.RANGE, {}

    monkeypatch.setattr(RegimeEngine, "_classify", classify)
    try:
        labels = []
        for _ in range(4):
            worker.tick(persist=False)
            labels.append(worker.detector.latest.state.label.value)
        assert labels == ["TRANSITION", "TRANSITION", "RANGE", "RANGE"]
    finally:
        db.close()


@pytest.mark.parametrize("interruption", ["empty", "not_ready", "handoff", "evaluation", "storage"])
def test_last_completed_evaluation_survives_cycles_without_new_results(monkeypatch, interruption):
    import asyncio
    from copy import deepcopy
    from types import SimpleNamespace

    from apps.quant_core import runtime
    from tradeforge.persistence.db import Database

    at = [frame(1).as_of]
    db = Database("postgresql+psycopg://unused@127.0.0.1:1/unused")
    worker = QuantWorker(db, clock=lambda: at[0])
    worker.detector.update(frame(1))
    captured = []
    inputs = [(object(),)]
    ready = [True]
    failing = [False]
    rejections = [[{"symbol": "ETHUSDT", "reason": "BOOK_UNAVAILABLE"}]]

    async def handoff(*args, **kwargs):
        if failing[0] and interruption == "handoff":
            raise OSError("offline")
        return {"markets": [], "deep": [], "deep_status": []}

    def tick(**kwargs):
        worker.progress.complete(
            data_ready=ready[0],
            analysis_ready=ready[0],
            execution_ready=False,
            reason="WAITING_CLOSED_BAR" if ready[0] else "WAITING_HISTORY_SWEEP",
        )
        worker.state = worker._state("WAITING_CLOSED_BAR", ready[0], 0)

    def alpha_inputs(*args, diagnostics, **kwargs):
        diagnostics["input_rejections"] = rejections[0]
        return inputs[0]

    analysis = SimpleNamespace(state={})

    def evaluate(samples):
        if failing[0] and interruption == "evaluation":
            raise ValueError("evaluation failed")
        analysis.state = {
            "evaluated_count": 2 if samples else 0,
            "reason_counts": {"SPREAD_GATE": 2} if samples else {},
            "symbol_results": [{"symbol": "BTCUSDT", "state": "NO_TRADE"}] if samples else [],
            "symbol_summaries": [
                {"symbol": "BTCUSDT", "decision": "NO_TRADE", "reasons": ["SPREAD_GATE"]}
            ]
            if samples
            else [],
            "analyzed_symbols": 1 if samples else 0,
            "evaluation_total": 2 if samples else 0,
            "analysis_cycle_total": 1 if samples else 0,
            "last_analysis_at": at[0].isoformat() if samples else None,
        }
        return 0

    analysis.evaluate = evaluate

    def save(*args):
        if failing[0] and interruption == "storage":
            raise OSError("storage unavailable")
        captured.append(deepcopy(args[-1]))

    monkeypatch.setattr(runtime, "fetch_memory", handoff)
    monkeypatch.setattr(worker, "tick", tick)
    monkeypatch.setattr(runtime, "alpha_inputs", alpha_inputs)
    monkeypatch.setattr(runtime, "save_runtime", save)
    try:
        asyncio.run(runtime.run_cycle(worker, analysis, object(), "offline-token"))
        completed = deepcopy(worker.state.get("last_evaluation"))
        assert completed is not None
        assert completed["evaluated_count"] == 2
        assert completed["as_of"] == at[0].isoformat()
        assert completed["input_rejections"] == [
            {"symbol": "ETHUSDT", "reason": "BOOK_UNAVAILABLE"}
        ]
        input_check = deepcopy(worker.state.get("last_input_check"))
        assert input_check is not None
        assert input_check["input_rejections"] == rejections[0]
        at[0] += timedelta(seconds=2)
        failing[0] = True
        inputs[0] = () if interruption in {"empty", "storage"} else inputs[0]
        ready[0] = interruption != "not_ready"
        asyncio.run(runtime.run_cycle(worker, analysis, object(), "offline-token"))
        assert worker.state["last_evaluation"] == completed
        assert captured[-1]["last_evaluation"] == completed
        # The last completed evaluation's counters describe the analysed
        # pairs. A failed or empty cycle is not new evidence about them.
        assert worker.state.get("evaluated_count", 0) == 2
        assert worker.state.get("analyzed_symbols", 0) == 1
        assert worker.state["execution_ready"] is False
        assert worker.state["last_input_check"]["input_rejections"] == rejections[0]
        if interruption == "not_ready" or interruption == "handoff":
            assert worker.state["last_input_check"] == input_check
        if interruption != "empty":
            assert worker.state["analysis_ready"] is False

        failing[0], ready[0], inputs[0] = False, True, (object(),)
        rejections[0] = []
        at[0] += timedelta(seconds=2)
        asyncio.run(runtime.run_cycle(worker, analysis, object(), "offline-token"))
        assert worker.state["last_evaluation"]["as_of"] == at[0].isoformat()
        assert worker.state["last_evaluation"] != completed
        assert worker.state["last_input_check"] == {
            "as_of": at[0].isoformat(),
            "input_rejections": [],
        }
    finally:
        db.close()

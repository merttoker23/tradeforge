"""Current synchronized coverage can be analyzed while a global recovery is still running."""

from datetime import timedelta

import pytest
from sqlalchemy import event

from tests.integration.test_quant_history_cache import seed
from tests.intelligence_fixtures import observation
from tradeforge.intelligence.runtime import QuantWorker
from tradeforge.market.market_state import BarView
from tradeforge.market.storage import save_view
from tradeforge.runtime.storage import save_runtime


def test_gateway_heartbeat_replaced_during_tick_is_read_at_current_evaluation_time(migrated):
    seeded_at, _ = seed(migrated, count=3)
    at = [seeded_at + timedelta(seconds=3)]

    def pulse():
        save_runtime(
            migrated,
            "market_gateway",
            at[0],
            at[0] + timedelta(seconds=15),
            {"clock_safe": True, "history_complete": True},
        )

    pulse()
    replaced = False

    def replace_heartbeat(connection, cursor, statement, parameters, context, executemany):
        nonlocal replaced
        if not replaced and "SELECT payload,observed_at FROM runtime_observations" in statement:
            replaced = True
            at[0] += timedelta(milliseconds=100)
            pulse()

    worker = QuantWorker(migrated, clock=lambda: at[0], require_gateway=True)
    event.listen(migrated.sql_engine, "before_cursor_execute", replace_heartbeat)
    try:
        state = worker.tick()
    finally:
        event.remove(migrated.sql_engine, "before_cursor_execute", replace_heartbeat)
    assert replaced
    assert state["processed"] == 1
    assert worker.detector.latest.frame.as_of == at[0]


@pytest.mark.parametrize("offset", [-16, 1])
def test_gateway_heartbeat_rejects_stale_or_future_evidence(migrated, offset):
    seeded_at, _ = seed(migrated, count=3)
    now = seeded_at + timedelta(seconds=3)
    observed = now + timedelta(seconds=offset)
    save_runtime(
        migrated,
        "market_gateway",
        observed,
        observed + timedelta(seconds=15),
        {"clock_safe": True, "history_complete": True},
    )
    worker = QuantWorker(migrated, clock=lambda: now, require_gateway=True)
    assert worker.tick()["reason"] == "GATEWAY_TELEMETRY_UNSAFE"
    assert worker.detector.latest is None


@pytest.mark.parametrize("missing,processed", [(1, 1), (2, 0), (5, 0)])
def test_incomplete_sweep_uses_only_sufficient_current_period_members(migrated, missing, processed):
    _, previous = seed(migrated, count=5, periods=39)
    samples = [observation(40, s.instrument.symbol) for s in previous]
    now = samples[0].current.as_of + timedelta(seconds=3)
    current = samples[0].current.close_at
    for sample in samples[missing:]:
        save_view(
            migrated,
            BarView(
                sample.instrument, sample.current.timeframe, now, (sample.current,), sample.quality
            ),
        )
    save_runtime(
        migrated,
        "market_gateway",
        now,
        now + timedelta(seconds=15),
        {
            "clock_safe": True,
            "history_complete": False,
            "history_reason": "HISTORY_SYNCING",
        },
    )
    worker = QuantWorker(migrated, clock=lambda: now, require_gateway=True)
    state = worker.tick()
    assert state["processed"] == processed
    assert state["history_current_members"] == 5 - missing
    assert state["history_current_coverage"] == (5 - missing) / 5
    if processed:
        latest = worker.detector.latest
        assert latest.frame.source_at == current
        assert len(latest.frame.observations) == 4
        assert all(row.current.close_at == current for row in latest.frame.observations)
    else:
        assert state["reason"] == "WAITING_HISTORY_SWEEP"
        assert state["analysis_ready"] is False
        assert worker.detector.latest is None


def test_new_stress_members_rebuild_same_bar_without_counting_return_twice(migrated):
    seeded_at, samples = seed(migrated, count=3)
    at = [seeded_at + timedelta(seconds=3)]
    worker = QuantWorker(migrated, clock=lambda: at[0])
    assert worker.tick()["processed"] == 1
    previous = worker.detector.latest
    assert dict(previous.state.metrics)["stress_coverage"] == 0
    counts = [f.sample_count for f in previous.features]
    at[0] += timedelta(seconds=1)
    worker.market_samples = tuple(s.market for s in samples)
    assert worker.tick()["processed"] == 1
    enriched = worker.detector.latest
    assert enriched.frame.source_at == previous.frame.source_at
    assert dict(enriched.state.metrics)["stress_coverage"] == 1
    assert [f.sample_count for f in enriched.features] == counts
    at[0] += timedelta(seconds=1)
    assert worker.tick()["processed"] == 0


def test_cost_refresh_reuses_only_identical_closed_history_seed(migrated, monkeypatch):
    from copy import deepcopy

    from tests.risk_fixtures import change
    from tradeforge.intelligence import runtime
    from tradeforge.intelligence.regime import RegimeEngine
    from tradeforge.intelligence.warmup import seed_history

    seeded_at, samples = seed(migrated, count=3)
    at = [seeded_at + timedelta(seconds=3)]
    calls = []

    def counted(*args):
        calls.append(True)
        return seed_history(*args)

    monkeypatch.setattr(runtime, "seed_history", counted)
    worker = QuantWorker(migrated, clock=lambda: at[0])
    worker.market_samples = tuple(s.market for s in samples[:2])
    assert worker.tick()["processed"] == 1
    assert len(calls) == 1
    counts = [f.sample_count for f in worker.detector.latest.features]
    at[0] += timedelta(seconds=1)
    worker.market_samples = tuple(s.market for s in samples)
    frame, _ = worker._frame(at[0])
    reference = deepcopy(worker._before_current or RegimeEngine(worker.detector.policy))
    seed_history(reference, worker.history, frame)
    expected = reference.update(frame)
    assert worker.tick()["processed"] == 1
    assert len(calls) == 1
    assert worker.detector.latest == expected
    assert [f.sample_count for f in worker.detector.latest.features] == counts

    at[0] += timedelta(seconds=1)
    revised = change(
        samples[0].current,
        observed_at=at[0],
        as_of=at[0],
        high=samples[0].current.high * 2,
    )
    save_view(
        migrated,
        BarView(samples[0].instrument, revised.timeframe, at[0], (revised,), samples[0].quality),
    )
    assert worker.tick()["processed"] == 1
    assert len(calls) == 2


def test_alternating_partial_cost_members_do_not_rebuild_every_tick(migrated):
    seeded_at, samples = seed(migrated, count=3)
    at = [seeded_at + timedelta(seconds=3)]
    worker = QuantWorker(migrated, clock=lambda: at[0])
    worker.market_samples = tuple(s.market for s in samples[:2])
    assert worker.tick()["processed"] == 1
    source = worker.detector.latest.frame.source_at
    at[0] += timedelta(seconds=1)
    worker.market_samples = tuple(s.market for s in samples[1:])
    assert worker.tick()["processed"] == 0
    assert worker.detector.latest.frame.source_at == source


@pytest.mark.parametrize("funding", ["0.003", "0.01"])
def test_new_acute_cost_stress_bypasses_same_period_enrichment_throttle(migrated, funding):
    from decimal import Decimal

    from tests.risk_fixtures import change

    seeded_at, samples = seed(migrated, count=3)
    at = [seeded_at + timedelta(seconds=3)]
    worker = QuantWorker(migrated, clock=lambda: at[0])
    worker.market_samples = tuple(s.market for s in samples[:2])
    worker.tick()
    at[0] += timedelta(seconds=1)
    worker.market_samples = (change(samples[2].market, funding=Decimal(funding)),)
    assert worker.tick()["processed"] == 1
    assert "FUNDING_STRESS" in worker.detector.latest.reason_codes

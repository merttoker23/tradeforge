"""Safety invalidation is immediate; discretionary rotation is rate bounded."""

from datetime import timedelta

import pytest

from tests.market_fixtures import instrument
from tests.unit.test_surveillance_ranking import evidence, revise


def allocator(**kwargs):
    from tradeforge.intelligence.opportunity import SurveillancePolicy
    from tradeforge.intelligence.subscription_allocator import SubscriptionAllocator

    return SubscriptionAllocator(SurveillancePolicy(**kwargs))


def names(result):
    return {s.name for group in result.plan.groups for s in group}


def test_default_budgets_and_global_streams_preserve_all_eligible():
    worker, snapshot = allocator(), evidence()
    result = worker.update(snapshot, now=snapshot.frame.as_of, positions=())
    assert set(result.universe.eligible) == set(snapshot.frame.universe.eligible)
    assert len(result.universe.watched) == 3
    assert len(result.universe.deep) == 3
    assert {"!ticker@arr", "!markPrice@arr@1s"} <= names(result)
    assert not result.trading_allowed
    assert result.status == "WATCHING"
    assert result.subscribe == names(result)
    assert result.unsubscribe == frozenset()


def test_pause_and_pressure_never_drop_position_feeds_including_delisted():
    worker, snapshot = allocator(), evidence()
    position = instrument("RETIREDUSDT")
    result = worker.update(
        snapshot, now=snapshot.frame.as_of, positions=(position,), queue_pressure=1.0, paused=True
    )
    assert not result.universe.watched and not result.universe.deep
    assert result.plan.protected == {"RETIREDUSDT"}
    assert {"retiredusdt@depth@100ms", "retiredusdt@bookTicker", "retiredusdt@aggTrade"} <= names(
        result
    )
    assert set(result.universe.eligible) == set(snapshot.frame.universe.eligible)
    assert result.status == "NO_TRADE"
    result.verify_monitoring(names(result))
    with pytest.raises(ValueError, match="POSITION_MONITORING_LOST"):
        result.verify_monitoring(names(result) - {"retiredusdt@depth@100ms"})
    with pytest.raises(ValueError, match="POSITION_MONITORING_LOST"):
        result.verify_monitoring(names(result), lost={"retiredusdt@aggTrade"})


def test_unknown_positions_and_mandatory_capacity_overflow_fail_closed():
    worker, snapshot = allocator(max_sockets=1, streams_per_socket=2), evidence()
    with pytest.raises(ValueError, match="POSITION_STATE_UNKNOWN"):
        worker.update(snapshot, now=snapshot.frame.as_of, positions=None)
    with pytest.raises(ValueError, match="PROTECTED_MARKET_BUDGET_EXCEEDED"):
        worker.update(snapshot, now=snapshot.frame.as_of, positions=(instrument("BTCUSDT"),))
    assert worker.latest is None


def test_socket_budget_drops_whole_optional_allocations_not_partial_feeds():
    worker, snapshot = allocator(max_sockets=2, streams_per_socket=4), evidence()
    result = worker.update(snapshot, now=snapshot.frame.as_of, positions=())
    assert not result.plan.omitted
    assert len(result.plan.groups) <= 2
    assert all(len(group) <= 4 for group in result.plan.groups)
    for key in result.universe.watched:
        assert {
            f"{key.symbol.lower()}@{suffix}"
            for suffix in ("bookTicker", "kline_1m", "kline_5m", "kline_15m")
        } <= names(result)


def challenge(snapshot, held):
    features = [f.model_dump() for f in snapshot.features]
    challenger = next(
        f["instrument"]["symbol"] for f in features if f["instrument"]["symbol"] != held.symbol
    )
    for f in features:
        f["relative_return"] = 1.0 if f["instrument"]["symbol"] == challenger else 0.0
    changed = revise(snapshot, features=features)
    # A new snapshot is necessary for changed evidence; same timestamp conflict rejects.
    from tests.intelligence_fixtures import frame
    from tradeforge.intelligence.models import FeatureSnapshot
    from tradeforge.intelligence.regime import RegimeEngine

    next_frame = frame(8, moves=(0.008, 0.016, -0.008), previous=snapshot.frame)
    # Preserve real point-in-time checks with a newer immutable frame.
    data = changed.model_dump()
    data["frame"] = next_frame
    data["state"] = RegimeEngine(snapshot.policy).update(next_frame).state
    data["previous_hash"] = "0" * 64
    changed = FeatureSnapshot.model_validate(data)
    return changed, challenger


def test_dwell_cadence_hysteresis_and_cooldown_bound_rotation():
    worker, snapshot = allocator(watch=1, deep=1, hysteresis=0.0, dwell_seconds=1800), evidence()
    now = snapshot.frame.as_of
    first = worker.update(snapshot, now=now, positions=())
    held = first.universe.watched[0]
    changed, challenger = challenge(snapshot, held)
    later = changed.frame.as_of
    dwelling = worker.update(changed, now=later, positions=())
    assert dwelling.universe.watched == first.universe.watched
    assert not dwelling.subscribe and not dwelling.unsubscribe
    later += timedelta(seconds=900)
    rotated = worker.update(changed, now=later, positions=())
    assert rotated.universe.watched[0].symbol == challenger
    assert held not in rotated.universe.watched
    stable = worker.update(changed, now=later + timedelta(seconds=1), positions=())
    assert stable.universe.watched == rotated.universe.watched
    assert not stable.subscribe and not stable.unsubscribe
    # Pressure immediately removes discretionary deep even during dwell.
    pressure = worker.update(
        changed, now=later + timedelta(seconds=2), positions=(), queue_pressure=0.9
    )
    assert not pressure.universe.deep
    recovered = worker.update(changed, now=later + timedelta(seconds=3), positions=())
    assert not recovered.universe.deep


def test_hysteresis_prevents_rotation_despite_a_better_challenger_after_dwell():
    worker, snapshot = allocator(watch=1, deep=1, hysteresis=1.0, dwell_seconds=0), evidence()
    first = worker.update(snapshot, now=snapshot.frame.as_of, positions=())
    changed, challenger = challenge(snapshot, first.universe.watched[0])
    later = worker.update(changed, now=changed.frame.as_of, positions=())
    best = next(r for r in later.ranked if r.instrument.symbol == challenger)
    held = next(r for r in later.ranked if r.instrument == first.universe.watched[0])
    assert best.score > held.score
    assert later.universe.watched == first.universe.watched
    assert not later.subscribe and not later.unsubscribe


def test_failed_position_update_does_not_commit_partial_allocator_state():
    worker, snapshot = allocator(max_positions=1), evidence()
    first = worker.update(snapshot, now=snapshot.frame.as_of, positions=())
    with pytest.raises(ValueError, match="POSITION_BUDGET_EXCEEDED"):
        worker.update(
            snapshot,
            now=snapshot.frame.as_of + timedelta(seconds=30),
            positions=(instrument("BTCUSDT"), instrument("ETHUSDT")),
        )
    assert worker.latest is first
    retry = worker.update(snapshot, now=snapshot.frame.as_of + timedelta(seconds=30), positions=())
    assert not retry.subscribe and not retry.unsubscribe


def test_missing_data_immediately_removes_optional_but_preserves_position():
    worker, snapshot = allocator(watch=1, deep=1), evidence()
    first = worker.update(snapshot, now=snapshot.frame.as_of, positions=(instrument("SOLUSDT"),))
    stale = worker.update(
        snapshot, now=snapshot.frame.as_of + timedelta(hours=1), positions=(instrument("SOLUSDT"),)
    )
    assert stale.universe.deep == (instrument("SOLUSDT"),)
    assert stale.universe.watched == (instrument("SOLUSDT"),)
    assert stale.plan.protected == first.plan.protected
    stale.verify_monitoring(names(stale))
    assert stale.status == "NO_TRADE"


def test_identical_retry_has_no_subscription_delta_and_time_rollback_is_rejected():
    worker, snapshot = allocator(), evidence()
    now = snapshot.frame.as_of
    first = worker.update(snapshot, now=now, positions=())
    second = worker.update(snapshot, now=now, positions=())
    assert first.universe == second.universe
    assert not second.subscribe and not second.unsubscribe
    with pytest.raises(ValueError, match="CLOCK_ROLLBACK"):
        worker.update(snapshot, now=now - timedelta(seconds=1), positions=())


def test_new_evaluation_time_is_a_distinct_immutable_universe_event():
    worker, snapshot = allocator(), evidence()
    now = snapshot.frame.as_of
    first = worker.update(snapshot, now=now, positions=())
    later = worker.update(snapshot, now=now + timedelta(seconds=1), positions=())
    assert first.universe.as_of != later.universe.as_of
    assert first.universe.event_id != later.universe.event_id
    assert first.universe.version_hash != later.universe.version_hash
    assert not later.subscribe and not later.unsubscribe

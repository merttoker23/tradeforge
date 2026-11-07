from datetime import timedelta

import pytest
from pydantic import ValidationError

from tests.intelligence_fixtures import engine, frame, observation, universe
from tests.market_fixtures import START
from tradeforge.domain.common import DataQuality
from tradeforge.domain.market import RegimeLabel


def changed(model, **updates):
    return type(model).model_validate({**model.model_dump(), **updates})


@pytest.mark.parametrize(
    "moves,want",
    [
        ((0.005, 0.006, 0.004), RegimeLabel.TREND_EXPANSION),
        ((-0.005, -0.006, -0.004), RegimeLabel.TREND_EXPANSION),
        ((0.0, 0.0, 0.0), RegimeLabel.VOL_COMPRESSION),
    ],
)
def test_trend_and_flat_fixtures(moves, want):
    detector = engine()
    snapshots = [detector.update(frame(n, moves)) for n in range(1, 9)]
    assert snapshots[0].state.label is RegimeLabel.UNKNOWN
    assert snapshots[-1].state.label is want
    assert snapshots[-1].trading_allowed is False
    assert dict(snapshots[-1].state.metrics)["coverage"] == 1


def test_panic_bypasses_warmup_and_cooldown_and_recovery_is_conservative():
    detector = engine(cooldown=5)
    for n in range(1, 9):
        detector.update(frame(n))
    panic = detector.update(
        frame(9, (-0.15, -0.2, -0.18), spread=150, funding=-0.01, previous=detector.latest.frame)
    )
    assert panic.state.label is RegimeLabel.DISORDER
    assert "LIQUIDITY_STRESS" in panic.reason_codes
    assert "FUNDING_STRESS" in panic.reason_codes
    for n in range(10, 14):
        assert detector.update(frame(n, previous=detector.latest.frame)).state.label in (
            RegimeLabel.DISORDER,
            RegimeLabel.TRANSITION,
        )
    first = engine().update(frame(1, spread=150))
    assert first.state.label is RegimeLabel.DISORDER


def test_range_and_churn_protection():
    detector = engine()
    for n in range(1, 9):
        moves = (0.003, 0.004, 0.002) if n % 2 else (-0.003, -0.004, -0.002)
        result = detector.update(
            frame(n, moves, previous=detector.latest.frame if detector.latest else None)
        )
    assert result.state.label is RegimeLabel.RANGE
    for n in range(9, 15):
        result = detector.update(frame(n, previous=detector.latest.frame))
    assert result.state.label in (RegimeLabel.RANGE, RegimeLabel.TRANSITION)
    assert "HYSTERESIS" in result.reason_codes


def test_missing_stale_and_low_coverage_fail_closed_immediately():
    detector = engine()
    for n in range(1, 8):
        detector.update(frame(n))
    low = frame(8, (0.005,), symbols=("BTCUSDT",))
    result = detector.update(low)
    assert result.state.label is RegimeLabel.UNKNOWN
    assert dict(result.state.metrics)["coverage"] == pytest.approx(1 / 3)
    assert "LOW_COVERAGE" in result.reason_codes
    stale = changed(frame(9), as_of=frame(9).as_of + timedelta(hours=1))
    assert detector.update(stale).state.label is RegimeLabel.UNKNOWN
    assert "STALE_INPUT" in detector.latest.reason_codes
    missing = changed(frame(14), observations=())
    assert detector.update(missing).state.label is RegimeLabel.UNKNOWN


def test_beta_uses_only_aligned_varying_btc_history_and_missing_resets_pairs():
    detector = engine()
    for n, btc in enumerate((0.001, -0.002, 0.003, -0.004, 0.005), 1):
        result = detector.update(
            frame(
                n, (btc, 2 * btc, -btc), previous=detector.latest.frame if detector.latest else None
            )
        )
    eth = next(f for f in result.features if f.instrument.symbol == "ETHUSDT")
    assert eth.beta == pytest.approx(2)
    assert eth.relative_return == pytest.approx(0, abs=1e-14)
    result = detector.update(
        frame(6, (0.004, 0.002), symbols=("ETHUSDT", "SOLUSDT"), previous=detector.latest.frame)
    )
    assert all(f.beta is f.relative_return is None for f in result.features)
    assert "BTC_HISTORY_MISSING" in result.reason_codes
    for n in range(7, 9):
        result = detector.update(frame(n, previous=detector.latest.frame))
    assert all(f.beta is None for f in result.features)


def test_duplicate_reorder_conflict_replay_and_gap_warmup():
    detector, replay = engine(), engine()
    for n in range(1, 8):
        row = frame(n)
        result = detector.update(row)
        assert detector.update(row) == result
        assert replay.update(changed(row, observations=tuple(reversed(row.observations)))) == result
    with pytest.raises(ValueError, match="CONFLICT"):
        detector.update(frame(7, (0.01, 0.02, 0.03)))
    with pytest.raises(ValueError, match="ORDER"):
        detector.update(frame(6))
    gap = detector.update(frame(10))
    assert gap.state.label is RegimeLabel.UNKNOWN
    assert "DATA_GAP" in gap.reason_codes
    assert all(f.sample_count == 1 for f in gap.features)


def test_point_in_time_delisting_retains_old_evidence_and_limits_ram():
    detector = engine(max_symbols=3)
    old = detector.update(frame(1))
    retired = universe(("BTCUSDT", "ETHUSDT"), START + timedelta(minutes=40))
    result = detector.update(
        changed(
            frame(2, (0.001, 0.002), symbols=("BTCUSDT", "ETHUSDT"), previous=old.frame),
            universe=retired,
        )
    )
    assert dict(result.state.metrics)["coverage"] == 1
    assert len(old.frame.universe.eligible) == 3
    assert len(result.frame.universe.eligible) == detector.series_count == 2
    assert old.digest != result.digest
    restored = detector.update(
        changed(
            frame(3, previous=result.frame), universe=universe(at=START + timedelta(minutes=50))
        )
    )
    assert next(f for f in restored.features if f.instrument.symbol == "SOLUSDT").sample_count == 1


def test_invalid_future_mixed_timeframe_identity_and_nonfinite_are_rejected():
    row = frame(1)
    with pytest.raises(ValidationError):
        changed(row, as_of=row.as_of - timedelta(seconds=1))
    with pytest.raises(ValidationError):
        changed(row, universe=universe(at=row.as_of + timedelta(hours=1)))
    with pytest.raises(ValidationError):
        changed(row, observations=(observation(2),))
    with pytest.raises(ValidationError):
        changed(row, observations=(row.observations[0], row.observations[0]))
    with pytest.raises(ValidationError):
        changed(row.observations[0], previous=row.observations[1].previous)
    from tradeforge.intelligence.models import RegimePolicy

    with pytest.raises(ValidationError):
        RegimePolicy(minimum_coverage=float("nan"))


def test_missing_liquidity_and_invalid_quality_are_reasoned_unknown():
    detector = engine()
    row = frame(1)
    no_market = tuple(changed(x, market=None) for x in row.observations)
    result = detector.update(changed(row, observations=no_market))
    assert result.state.label is RegimeLabel.UNKNOWN
    assert "STRESS_DATA_MISSING" in result.reason_codes
    rows = tuple(changed(x, quality=DataQuality.UNKNOWN) for x in frame(2).observations)
    result = detector.update(changed(frame(2), observations=rows))
    assert result.state.label is RegimeLabel.UNKNOWN
    assert not result.features


def test_policy_and_history_change_snapshot_identity_and_roundtrip():
    from tradeforge.intelligence.models import FeatureSnapshot

    a, b = engine(), engine(trend_efficiency=0.8)
    row = frame(1)
    left, right = a.update(row), b.update(row)
    assert left.policy_hash != right.policy_hash
    assert left.digest != right.digest
    assert FeatureSnapshot.model_validate_json(left.model_dump_json()) == left
    a.update(frame(2))
    assert a.update(frame(3)).digest != engine().update(frame(3)).digest


def test_invalid_numeric_conversion_is_rejected_before_engine_mutation():
    from decimal import Decimal

    row = observation(2)
    huge = changed(row.market, funding=Decimal("1e999"))
    with pytest.raises(ValidationError, match="NONFINITE"):
        changed(row, market=huge)


def test_policy_budget_timeframe_and_stale_market_boundaries():
    from tradeforge.domain.market import Timeframe
    from tradeforge.intelligence.models import RegimePolicy
    from tradeforge.intelligence.regime import RegimeEngine

    with pytest.raises(ValueError, match="BUDGET"):
        engine(max_symbols=2).update(frame(1))
    with pytest.raises(ValueError, match="TIMEFRAME"):
        RegimeEngine(RegimePolicy(timeframe=Timeframe.M1)).update(frame(1))
    with pytest.raises(ValidationError):
        RegimePolicy(window=2, minimum_history=8)
    row = frame(3)
    stale_rows = tuple(
        changed(x, market=observation(1, x.instrument.symbol).market) for x in row.observations
    )
    result = engine().update(changed(row, observations=stale_rows))
    assert result.state.label is RegimeLabel.UNKNOWN
    assert "STRESS_DATA_MISSING" in result.reason_codes
    assert all(f.spread_bps is f.funding is None for f in result.features)


def test_overlapping_bar_revision_cannot_invent_continuous_trend_or_beta():
    detector = engine()
    for n in range(1, 8):
        old = detector.update(frame(n))
    rows = tuple(
        observation(8, x.instrument.symbol, previous_close=x.previous.close)
        for x in old.frame.observations
    )
    result = detector.update(changed(frame(8), observations=rows))
    assert result.state.label is RegimeLabel.UNKNOWN
    assert "OVERLAP_REVISION" in result.reason_codes
    assert all(f.sample_count == 1 and f.beta is None for f in result.features)
    assert old.state.label is RegimeLabel.TREND_EXPANSION


def test_old_or_conflicting_universe_cannot_resurrect_known_delisting():
    detector = engine()
    detector.update(frame(1))
    membership = universe(("BTCUSDT", "ETHUSDT"), START + timedelta(minutes=40))
    accepted = detector.update(
        changed(frame(2, (0.005, 0.006), symbols=("BTCUSDT", "ETHUSDT")), universe=membership)
    )
    with pytest.raises(ValueError, match="UNIVERSE_ROLLBACK"):
        detector.update(frame(3))
    conflicting = universe(at=membership.observed_at)
    with pytest.raises(ValueError, match="CONFLICTING_UNIVERSE"):
        detector.update(changed(frame(3), universe=conflicting))
    assert detector.latest == accepted
    assert detector.series_count == 2


def test_global_correlation_excludes_btc_self_correlation():
    detector = engine()
    for n, btc in enumerate((0.001, -0.002, 0.003, -0.004), 1):
        row = frame(
            n,
            (btc, -btc),
            symbols=("BTCUSDT", "ETHUSDT"),
            previous=detector.latest.frame if detector.latest else None,
        )
        result = detector.update(changed(row, universe=universe(("BTCUSDT", "ETHUSDT"))))
    metrics = dict(result.state.metrics)
    assert metrics["btc_correlation"] == pytest.approx(-1.0)
    assert metrics["correlation_coverage"] == 1.0

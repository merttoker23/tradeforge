"""Literal scoring and safety gates, independent of an exchange connection."""

from datetime import timedelta

import pytest
from pydantic import ValidationError

from tests.intelligence_fixtures import engine, frame
from tradeforge.intelligence.models import FeatureSnapshot


def evidence():
    worker = engine()
    prior = None
    for n in range(1, 8):
        prior = frame(n, moves=(0.001 * n, 0.002 * n, -0.001 * n), previous=prior)
        snapshot = worker.update(prior)
    return snapshot


def revise(snapshot, *, features=None, rows=None):
    """Recompute provenance after deliberately changed fixture evidence."""
    from tradeforge.intelligence.models import digest

    data = snapshot.model_dump()
    if features is not None:
        data["features"] = tuple(features)
    if rows is not None:
        payload = snapshot.frame.model_dump()
        payload["observations"] = tuple(rows)
        data["frame"] = type(snapshot.frame).model_validate(payload)
        data["state"]["input_version"] = digest(
            {
                "frame": data["frame"].model_dump(mode="json"),
                "policy": snapshot.policy_hash,
                "previous": snapshot.previous_hash,
                "algorithm": snapshot.algorithm_version,
            }
        )
    return FeatureSnapshot.model_validate(data)


def test_layer1_covers_every_eligible_symbol_and_scores_are_ordinal():
    from tradeforge.intelligence.opportunity import SurveillancePolicy
    from tradeforge.intelligence.ranking import rank_market

    snapshot = evidence()
    rows = rank_market(snapshot, SurveillancePolicy(), now=snapshot.frame.as_of)
    assert {r.instrument for r in rows} == set(snapshot.frame.universe.eligible)
    assert all(r.score is not None and 0 <= r.score <= 1 for r in rows)
    assert all(r.reason_codes == ("RANKED",) for r in rows)
    assert all(r.source_feature_hash == snapshot.digest for r in rows)
    assert [r.score for r in rows] == sorted((r.score for r in rows), reverse=True)
    assert not any("confidence" in r.model_dump() for r in rows)


def test_missing_relative_strength_or_market_has_no_score():
    from tradeforge.intelligence.opportunity import SurveillancePolicy
    from tradeforge.intelligence.ranking import rank_market

    snapshot = evidence()
    features = [f.model_dump() for f in snapshot.features]
    features[0]["relative_return"] = None
    rows = [r.model_dump() for r in snapshot.frame.observations]
    rows[1]["market"] = None
    snapshot = revise(snapshot, features=features, rows=rows)
    ranked = {
        r.instrument.symbol: r
        for r in rank_market(snapshot, SurveillancePolicy(), now=snapshot.frame.as_of)
    }
    assert ranked["BTCUSDT"].score is None
    assert "RELATIVE_STRENGTH_UNKNOWN" in ranked["BTCUSDT"].reason_codes
    assert ranked["ETHUSDT"].score is None
    assert "MARKET_MISSING" in ranked["ETHUSDT"].reason_codes
    assert ranked["SOLUSDT"].score is not None


def test_stale_snapshot_and_future_evaluation_fail_closed():
    from tradeforge.intelligence.opportunity import SurveillancePolicy
    from tradeforge.intelligence.ranking import rank_market

    snapshot = evidence()
    rows = rank_market(
        snapshot, SurveillancePolicy(), now=snapshot.frame.as_of + timedelta(hours=1)
    )
    assert all(r.score is None and "STALE_INPUT" in r.reason_codes for r in rows)
    with pytest.raises(ValueError, match="FUTURE_INPUT"):
        rank_market(snapshot, SurveillancePolicy(), now=snapshot.frame.as_of - timedelta(seconds=1))


def test_pump_volume_burst_is_excluded_even_with_high_relative_strength():
    from decimal import Decimal

    from tradeforge.intelligence.opportunity import SurveillancePolicy
    from tradeforge.intelligence.ranking import rank_market

    snapshot = evidence()
    features = [f.model_dump() for f in snapshot.features]
    features[1].update(log_return=0.04, relative_return=0.04)
    rows = [r.model_dump() for r in snapshot.frame.observations]
    rows[1]["current"]["volume"] = Decimal("1000")
    snapshot = revise(snapshot, features=features, rows=rows)
    row = next(
        r
        for r in rank_market(snapshot, SurveillancePolicy(), now=snapshot.frame.as_of)
        if r.instrument.symbol == "ETHUSDT"
    )
    assert row.score is None
    assert "PUMP_OR_DUMP" in row.reason_codes


@pytest.mark.parametrize(
    "patch",
    [{"watch": 0, "deep": 1}, {"rotation_limit": 0}, {"hysteresis": float("nan")}, {"watch": True}],
)
def test_policy_rejects_unsafe_budgets_and_nonfinite_values(patch):
    from tradeforge.intelligence.opportunity import SurveillancePolicy

    with pytest.raises(ValidationError):
        SurveillancePolicy(**patch)


def test_exact_volume_threshold_is_not_rounded_to_an_illiquid_rejection():
    from decimal import Decimal

    from tradeforge.intelligence.opportunity import SurveillancePolicy
    from tradeforge.intelligence.ranking import rank_market

    snapshot = evidence()
    rows = [r.model_dump() for r in snapshot.frame.observations]
    rows[0]["current"]["volume"] = Decimal("1")
    rows[0]["previous"]["volume"] = Decimal("10")
    snapshot = revise(snapshot, rows=rows)
    ranked = rank_market(snapshot, SurveillancePolicy(), now=snapshot.frame.as_of)
    btc = next(r for r in ranked if r.instrument.symbol == "BTCUSDT")
    assert btc.score is not None


def test_decimal_pump_threshold_is_inclusive_at_configured_boundary():
    from decimal import Decimal

    from tradeforge.intelligence.opportunity import SurveillancePolicy
    from tradeforge.intelligence.ranking import rank_market

    snapshot = evidence()
    rows = [r.model_dump() for r in snapshot.frame.observations]
    rows[0]["current"]["volume"] = Decimal("83")
    rows[0]["previous"]["volume"] = Decimal("10")
    features = [f.model_dump() for f in snapshot.features]
    features[0]["log_return"] = 0.04
    snapshot = revise(snapshot, rows=rows, features=features)
    ranked = rank_market(
        snapshot, SurveillancePolicy(pump_volume_ratio=8.3), now=snapshot.frame.as_of
    )
    btc = next(r for r in ranked if r.instrument.symbol == "BTCUSDT")
    assert btc.score is None
    assert "PUMP_OR_DUMP" in btc.reason_codes


def test_hand_calculated_score_components_and_symmetric_price_shock():
    from tradeforge.intelligence.opportunity import SurveillancePolicy
    from tradeforge.intelligence.ranking import rank_market

    snapshot = evidence()
    features = [f.model_dump() for f in snapshot.features]
    for feature in features:
        feature.update(
            volatility=0.01, relative_return=0.015, log_return=0.015, spread_bps=3.0, funding=0.0
        )
    snapshot = revise(snapshot, features=features)
    rows = rank_market(snapshot, SurveillancePolicy(), now=snapshot.frame.as_of)
    # ETH has the highest quote volume: rank midpoint is (2 + 3) / 6 = 5/6.
    eth = next(r for r in rows if r.instrument.symbol == "ETHUSDT")
    assert dict(eth.components) == pytest.approx(
        {
            "normalized_volume": 5 / 6,
            "tradability": 0.9,
            "relative_strength": 0.5,
            "abnormal_price": 0.5,
            "abnormal_volume": 0.5,
            "volatility_suitability": 1.0,
            "costs": 0.95,
        }
    )
    assert eth.score == pytest.approx(0.7404761904761905)
    features[0]["log_return"] = -0.08
    features[1]["log_return"] = 0.08
    shock = revise(snapshot, features=features)
    rejected = rank_market(shock, SurveillancePolicy(), now=shock.frame.as_of)
    assert {r.instrument.symbol for r in rejected if "PRICE_SHOCK" in r.reason_codes} == {
        "BTCUSDT",
        "ETHUSDT",
    }


def test_missing_observation_and_stale_market_are_per_symbol_not_global():
    from tradeforge.domain.common import DataQuality
    from tradeforge.intelligence.opportunity import SurveillancePolicy
    from tradeforge.intelligence.ranking import rank_market

    snapshot = evidence()
    rows = [r.model_dump() for r in snapshot.frame.observations][1:]
    rows[0]["market"]["quality"] = DataQuality.STALE
    snapshot = revise(snapshot, rows=rows)
    ranked = {
        r.instrument.symbol: r
        for r in rank_market(snapshot, SurveillancePolicy(), now=snapshot.frame.as_of)
    }
    assert ranked["BTCUSDT"].score is None
    assert ranked["ETHUSDT"].score is None
    assert ranked["SOLUSDT"].score is not None

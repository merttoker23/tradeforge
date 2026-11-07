import json
from decimal import Decimal

import pytest

from tests.intelligence_fixtures import engine, frame, observation, universe
from tests.unit.test_intelligence_regime import changed
from tradeforge.domain.market import RegimeLabel
from tradeforge.intelligence.models import FeatureSnapshot, RegimePolicy, digest
from tradeforge.intelligence.opportunity import SurveillancePolicy
from tradeforge.intelligence.ranking import rank_market

SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", *(f"ALT{index}USDT" for index in range(7)))


def market_frame(index, stresses, previous=None):
    move = 0.002 if index % 2 else 0.004
    current = frame(index)
    prices = (
        {row.instrument.symbol: row.current.close for row in previous.observations}
        if previous
        else {}
    )
    rows = []
    for symbol in SYMBOLS:
        updates = stresses.get(symbol, {})
        rows.append(
            observation(
                index,
                symbol,
                move=updates.get("move", move),
                spread=updates.get("spread", 2.0),
                funding=updates.get("funding", 0.0001),
                previous_close=prices.get(symbol),
            )
        )
    return changed(current, universe=universe(SYMBOLS), observations=tuple(rows))


@pytest.mark.parametrize(
    "stress,reason",
    [
        ({"funding": -0.015}, "FUNDING_STRESS"),
        ({"funding": 0.015}, "FUNDING_STRESS"),
        ({"spread": 150.0}, "LIQUIDITY_STRESS"),
        ({"move": 0.06}, "RETURN_SHOCK"),
        ({"move": -0.06}, "RETURN_SHOCK"),
    ],
)
def test_isolated_alt_stress_does_not_block_healthy_market(stress, reason):
    detector = engine()
    for index in range(1, 9):
        result = detector.update(
            market_frame(
                index, {"ALT0USDT": stress}, detector.latest.frame if detector.latest else None
            )
        )
    assert result.state.label is RegimeLabel.TREND_EXPANSION
    assert reason in result.reason_codes
    assert dict(result.state.metrics)["stress_breadth"] == pytest.approx(0.1)
    ranked = rank_market(result, SurveillancePolicy(), now=result.frame.as_of)
    assert any(row.score is not None for row in ranked if row.instrument.symbol != "ALT0USDT")
    outlier = next(row for row in ranked if row.instrument.symbol == "ALT0USDT")
    if "funding" in stress:
        assert outlier.score is None
        assert "FUNDING_STRESS" in outlier.reason_codes
    if "spread" in stress:
        assert outlier.score is None
        assert "COST_OR_LIQUIDITY_STRESS" in outlier.reason_codes
    if "move" in stress:
        assert outlier.score is None
        assert "PRICE_SHOCK" in outlier.reason_codes


@pytest.mark.parametrize(
    "stresses",
    [
        {"ALT0USDT": {"funding": -0.015}, "ALT1USDT": {"funding": 0.015}},
        {"ALT0USDT": {"spread": 150.0}, "ALT1USDT": {"move": -0.06}},
    ],
)
def test_widespread_stress_bypasses_warmup(stresses):
    result = engine().update(market_frame(1, stresses))
    assert result.state.label is RegimeLabel.DISORDER
    assert dict(result.state.metrics)["stress_breadth"] == pytest.approx(0.2)


def test_multiple_stress_signals_on_one_asset_are_not_multiple_market_members():
    result = engine().update(
        market_frame(1, {"ALT0USDT": {"funding": -0.015, "spread": 150.0, "move": -0.06}})
    )
    assert result.state.label is RegimeLabel.UNKNOWN
    assert dict(result.state.metrics)["stress_breadth"] == pytest.approx(0.1)


def test_benchmark_shock_remains_an_immediate_global_block():
    result = engine().update(market_frame(1, {"BTCUSDT": {"move": -0.06}}))
    assert result.state.label is RegimeLabel.DISORDER
    assert "RETURN_SHOCK" in result.reason_codes


def test_sparse_stressed_evidence_uses_whole_universe_and_fails_closed():
    sample = market_frame(1, {"ALT0USDT": {"funding": -0.015}})
    rows = tuple(row for row in sample.observations if row.instrument.symbol == "ALT0USDT")
    result = engine().update(changed(sample, observations=rows))
    assert result.state.label is RegimeLabel.UNKNOWN
    assert "LOW_COVERAGE" in result.reason_codes
    assert "STRESS_DATA_MISSING" in result.reason_codes


def test_default_stress_breadth_can_load_existing_policy_hashes():
    legacy = engine().update(frame(1))
    payload = legacy.model_dump(mode="json")
    payload["policy"].pop("minimum_stress_breadth", None)
    restored = FeatureSnapshot.model_validate_json(json.dumps(payload))
    assert restored.policy.policy_hash == legacy.policy_hash
    assert restored.policy.minimum_stress_breadth == 0.2
    stricter = RegimePolicy(minimum_stress_breadth=0.1)
    assert stricter.policy_hash != RegimePolicy().policy_hash


def test_custom_stress_breadth_controls_global_gate():
    result = engine(minimum_stress_breadth=0.1).update(
        market_frame(1, {"ALT0USDT": {"funding": Decimal("-0.015")}})
    )
    assert result.state.label is RegimeLabel.DISORDER


def test_new_regime_algorithm_has_distinct_identity_and_reads_legacy_evidence():
    current = engine().update(market_frame(1, {"ALT0USDT": {"funding": -0.015}}))
    legacy_input = digest(
        {
            "frame": current.frame.model_dump(mode="json"),
            "policy": current.policy_hash,
            "previous": current.previous_hash,
            "algorithm": 1,
        }
    )
    legacy = current.model_dump(mode="json")
    legacy["algorithm_version"] = 1
    legacy["state"]["input_version"] = legacy_input
    restored = FeatureSnapshot.model_validate_json(json.dumps(legacy))
    assert restored.input_version == legacy_input
    assert current.algorithm_version == 2
    assert current.input_version != restored.input_version


@pytest.mark.parametrize("family", ["momentum", "reversal"])
@pytest.mark.parametrize("direction", ["LONG", "SHORT"])
@pytest.mark.parametrize(
    "source,field,value,reason",
    [
        ("market", "funding", Decimal("-0.015"), "FUNDING_STRESS"),
        ("market", "funding", Decimal("0.015"), "FUNDING_STRESS"),
        ("feature", "funding", -0.015, "FUNDING_STRESS"),
        ("feature", "log_return", -0.06, "RETURN_SHOCK"),
        ("feature", "spread_bps", 150.0, "SPREAD_GATE"),
    ],
)
def test_already_deep_symbol_cannot_emit_candidate_after_isolated_stress(
    family, direction, source, field, value, reason
):
    from tests.unit.test_momentum_alpha import evidence
    from tests.unit.test_reversal_alpha import sample
    from tradeforge.alpha.momentum import MomentumAlpha
    from tradeforge.alpha.reversal import ReversalAlpha
    from tradeforge.domain.decision import Side

    side = Side(direction)
    sample_input = evidence(side) if family == "momentum" else sample(side)
    alpha_type = MomentumAlpha if family == "momentum" else ReversalAlpha
    assert (
        alpha_type().evaluate(sample_input, side=side, now=sample_input.as_of).candidate is not None
    )
    if source == "market":
        sample_input = changed(sample_input, market=changed(sample_input.market, **{field: value}))
    else:
        features = changed(
            sample_input.features,
            features=tuple(
                changed(row, **{field: value}) if row.instrument == sample_input.instrument else row
                for row in sample_input.features.features
            ),
        )
        sample_input = changed(sample_input, features=features)
    result = alpha_type().evaluate(sample_input, side=side, now=sample_input.as_of)
    assert result.candidate is None
    assert reason in result.reason_codes

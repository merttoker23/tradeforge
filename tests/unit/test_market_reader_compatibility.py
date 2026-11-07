"""Reader-first deployment must preserve old payloads and accept new durable evidence."""

import json
from contextlib import contextmanager

import pytest

from tests.unit.test_momentum_alpha import change, evidence
from tests.unit.test_reversal_alpha import sample as reversal_sample
from tradeforge.alpha.momentum import MomentumAlpha
from tradeforge.alpha.reversal import ReversalAlpha
from tradeforge.domain.decision import Side
from tradeforge.domain.market import MarketSnapshot
from tradeforge.intelligence.models import FeatureSnapshot, feature_input_version
from tradeforge.intelligence.runtime import QuantWorker


def component_market(market, missing_volume=False):
    return change(
        market,
        book_source_at=market.source_at,
        mark_source_at=market.source_at,
        volume_source_at=None if missing_volume else market.source_at,
        volume=None if missing_volume else market.volume,
    )


def test_legacy_payload_has_no_new_keys_and_keeps_exact_round_trip():
    sample = evidence()
    raw = sample.market.model_dump_json()
    assert {"book_source_at", "mark_source_at", "volume_source_at"}.isdisjoint(json.loads(raw))
    assert MarketSnapshot.model_validate_json(raw).model_dump_json() == raw
    feature_raw = sample.features.model_dump_json()
    loaded = FeatureSnapshot.model_validate_json(feature_raw)
    assert loaded.model_dump_json() == feature_raw
    assert loaded.digest == sample.features.digest
    assert loaded.input_version == sample.features.input_version


@pytest.mark.parametrize("missing_volume", [False, True])
def test_new_durable_snapshot_resumes_quant_reader_without_rewriting_evidence(missing_volume):
    sample = evidence()
    old = sample.features
    frame = change(
        old.frame,
        observations=tuple(
            change(row, market=component_market(row.market, missing_volume))
            for row in old.frame.observations
        ),
    )
    version = feature_input_version(
        frame,
        old.policy_hash,
        old.previous_hash,
        old.warmup_hash,
        algorithm_version=old.algorithm_version,
    )
    snapshot = change(old, frame=frame, state=change(old.state, input_version=version))
    raw = snapshot.model_dump(mode="json")

    class ReadOnlySnapshot:
        @contextmanager
        def transaction(self):
            yield self

        def scalar(self, query, parameters):
            assert "SELECT payload FROM regime_snapshots" in str(query)
            return raw

    worker = QuantWorker(ReadOnlySnapshot(), clock=lambda: frame.as_of)
    worker._load(frame.as_of)
    assert worker.loaded is True
    assert worker.detector.latest.model_dump(mode="json") == raw
    assert worker.detector.latest.digest == snapshot.digest
    for row in worker.detector.latest.frame.observations:
        assert row.market.book_source_at == row.market.mark_source_at == row.market.source_at
        assert row.market.costs_fresh(frame.as_of, 15)
        assert row.market.volume_fresh(frame.as_of, 15) is not missing_volume


@pytest.mark.parametrize("family", ["momentum", "reversal"])
def test_reader_accepting_cost_only_candidate_input_cannot_grant_alpha_authority(family):
    sample = evidence() if family == "momentum" else reversal_sample()
    current = change(sample, market=component_market(sample.market, missing_volume=True))
    loaded = type(current).model_validate_json(current.model_dump_json())
    alpha = MomentumAlpha() if family == "momentum" else ReversalAlpha()
    result = alpha.evaluate(loaded, side=Side.LONG, now=loaded.as_of)
    assert result.candidate is None
    assert result.reason_codes == ("MARKET_UNAVAILABLE",)

"""A refused real candidate keeps its exact reason and immutable proof reference."""

import json
from datetime import timedelta

import pytest

from tests.integration.test_demo_pipeline_readiness import OfflinePipeline
from tradeforge.decision.trial import TestnetTrialPolicy as TrialPolicy
from tradeforge.risk.governor import RiskAssessment


@pytest.mark.parametrize("stage", ["risk", "statistics"])
def test_refused_candidate_is_durably_linked_to_its_gate_proof(migrated, monkeypatch, stage):
    pipeline = OfflinePipeline(
        migrated, monkeypatch, trial_policy=TrialPolicy(enabled=stage != "statistics")
    )
    pipeline.clock.at += timedelta(seconds=61 if stage == "risk" else 1)
    pipeline.demo.tick(pipeline.feed())
    rows = pipeline.events("runtime.admission_rejected.v1")
    assert len(rows) == 1
    refusal = rows[0]
    assert refusal["decision_id"] == str(pipeline.record.decision_id)
    assert refusal["candidate_input_hash"] == pipeline.record.discovery.candidate.input_hash
    assert refusal["as_of"] == pipeline.clock().isoformat()
    if stage == "risk":
        assessment = RiskAssessment.model_validate_json(
            json.dumps(pipeline.events("risk.assessment.v1")[-1]["body"])
        )
        assert refusal["risk_assessment_id"] == assessment.digest
        assert refusal["reason_codes"] == ["RISK_REJECTED", "FACTOR_UNKNOWN"]
    else:
        assert "INSUFFICIENT_STATISTICAL_EVIDENCE" in refusal["reason_codes"]
        assert refusal["edge_analysis_version"]
    pipeline.assert_no_publication()

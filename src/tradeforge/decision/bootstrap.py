"""Explain evidence gaps without granting, enlarging or resetting order authority."""

from tradeforge.decision.edge import EdgeAnalysis
from tradeforge.decision.trial import TestnetTrialPolicy


def bootstrap_diagnostics(edge: EdgeAnalysis, trial: TestnetTrialPolicy) -> dict[str, object]:
    """Counts are bucket-specific; the trial ceiling is global across all buckets.

    Even this optimistic ceiling assumes every trial fills, closes and yields an
    independent label. No simulated outcome or bucket statistic supplies the
    missing, pre-entry probability forecast.
    """
    required = edge.policy.min_train + edge.policy.min_oos
    blockers = []
    if trial.max_entries < required:
        blockers.append("TRIAL_CANNOT_MEET_SAMPLE_MINIMUM")
    if edge.oos.brier_score is None or edge.oos.calibration_error is None:
        blockers.append("PROBABILITY_FORECASTS_UNAVAILABLE")
    return {
        "evidence_environment": edge.bucket.environment,
        "proof_state": edge.proof_state.value,
        "calibration_label": edge.calibration_label,
        "train_effective_samples": edge.train.effective_samples,
        "oos_effective_samples": edge.oos.effective_samples,
        "train_required": edge.policy.min_train,
        "oos_required": edge.policy.min_oos,
        "train_missing": max(0, edge.policy.min_train - edge.train.effective_samples),
        "oos_missing": max(0, edge.policy.min_oos - edge.oos.effective_samples),
        "minimum_total_effective_samples": required,
        "maximum_trial_entries": trial.max_entries,
        "trial_enabled": trial.enabled,
        "trial_can_self_validate": trial.max_entries >= required,
        "bootstrap_blockers": blockers,
        "edge_reasons": list(edge.reason_codes),
        "execution_authorized": False,
    }

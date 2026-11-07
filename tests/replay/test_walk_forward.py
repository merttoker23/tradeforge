from datetime import timedelta
from decimal import Decimal as D

import pytest

from tests.replay.fixtures import T


def sample(start, end, pnl="1", policy="a", available=None):
    from tradeforge.research.walk_forward import PolicyOutcome

    return PolicyOutcome(
        policy_version=policy,
        started_at=T + timedelta(seconds=start),
        ended_at=T + timedelta(seconds=end),
        available_at=T + timedelta(seconds=available if available is not None else end),
        net_pnl=D(pnl),
    )


def test_walk_forward_selection_uses_only_past_labels_with_embargo_and_no_tuning_on_test():
    from tradeforge.research.walk_forward import Fold, evaluate_fold

    fold = Fold(
        train_start=T,
        train_end=T + timedelta(seconds=10),
        test_start=T + timedelta(seconds=12),
        test_end=T + timedelta(seconds=20),
        embargo_seconds=2,
    )
    rows = (
        sample(1, 3, "2"),
        sample(1, 3, "1", policy="b"),
        sample(9, 13, "999", policy="b"),  # overlapping label: purged
        sample(5, 6, "999", policy="b", available=15),  # late revision: purged
        sample(13, 15, "-3"),
        sample(13, 15, "999", policy="b"),
    )
    result = evaluate_fold(rows, fold)
    assert result.selected_policy == "a"
    assert result.test_net_pnl == D("-3") and result.status == "NEGATIVE"
    assert result.purged_count == 2
    assert result.live_eligible is False
    with pytest.raises(ValueError, match="EMBARGO"):
        Fold(
            train_start=T,
            train_end=T + timedelta(seconds=10),
            test_start=T + timedelta(seconds=11),
            test_end=T + timedelta(seconds=20),
            embargo_seconds=2,
        )


def test_future_test_label_is_excluded_and_positive_sample_stays_unproven():
    from tradeforge.research.walk_forward import Fold, evaluate_fold

    fold = Fold(
        train_start=T,
        train_end=T + timedelta(seconds=10),
        test_start=T + timedelta(seconds=12),
        test_end=T + timedelta(seconds=20),
        embargo_seconds=2,
    )
    result = evaluate_fold(
        (sample(1, 3), sample(13, 15, "2"), sample(16, 19, "999", available=21)), fold
    )
    assert result.test_net_pnl == D("2")
    assert result.status == "UNPROVEN"
    with pytest.raises(ValueError, match="INSUFFICIENT"):
        evaluate_fold((), fold)


def test_walk_forward_folds_do_not_reuse_out_of_sample_periods():
    from tradeforge.research.walk_forward import Fold, walk_forward

    one = Fold(
        train_start=T,
        train_end=T + timedelta(seconds=10),
        test_start=T + timedelta(seconds=12),
        test_end=T + timedelta(seconds=20),
        embargo_seconds=2,
    )
    two = Fold(
        train_start=T,
        train_end=T + timedelta(seconds=20),
        test_start=T + timedelta(seconds=22),
        test_end=T + timedelta(seconds=30),
        embargo_seconds=2,
    )
    rows = (sample(1, 3), sample(13, 15, "-1"), sample(23, 25, "-2"))
    results = walk_forward(rows, (one, two))
    assert len(results) == 2 and sum(r.test_net_pnl for r in results) == D("-3")
    with pytest.raises(ValueError, match="OOS_OVERLAP"):
        walk_forward(rows, (one, one))

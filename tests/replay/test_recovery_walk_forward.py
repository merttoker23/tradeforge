"""Research can demonstrate performance without authorizing an exchange environment."""

from datetime import timedelta
from decimal import Decimal

from tests.decision_fixtures import NOW, change
from tests.edge_fixtures import analyze, trades
from tradeforge.config.policy import RuntimeMode
from tradeforge.domain.decision import ProofState
from tradeforge.research.walk_forward import Fold, PolicyOutcome, walk_forward


def test_two_hundred_completed_research_labels_are_still_not_testnet_proof():
    started = NOW - timedelta(hours=4)
    rows = tuple(
        PolicyOutcome(
            policy_version="predeclared-v1",
            started_at=started + timedelta(seconds=i * 10),
            ended_at=started + timedelta(seconds=i * 10 + 1),
            available_at=started + timedelta(seconds=i * 10 + 2),
            net_pnl=Decimal("1"),
        )
        for i in range(200)
    )
    fold = Fold(
        train_start=started,
        train_end=started + timedelta(seconds=995),
        test_start=started + timedelta(seconds=1000),
        test_end=started + timedelta(seconds=2000),
        embargo_seconds=5,
    )
    (result,) = walk_forward(rows, (fold,))
    assert result.train_count == result.test_count == 100
    assert result.test_net_pnl == Decimal("100")
    assert result.status == "UNPROVEN" and result.live_eligible is False

    # Independently generated synthetic closed outcomes retain their replay label.
    outcomes = trades() + trades(180, 220, start=NOW - timedelta(hours=1), split="OOS", offset=2000)
    original = analyze()
    replay_bucket = change(original.bucket, environment="replay")
    evidence = analyze(
        tuple(change(r, bucket=replay_bucket) for r in outcomes),
        bucket=replay_bucket,
        mode=RuntimeMode.TESTNET,
    )
    assert evidence.train.effective_samples == 1000
    assert evidence.oos.effective_samples == 400
    assert "EVIDENCE_ENVIRONMENT_MISMATCH" in evidence.reason_codes
    assert evidence.proof_state is ProofState.UNPROVEN
    assert evidence.live_approval is False


def test_delayed_training_result_is_purged_even_when_trade_closed_before_cutoff():
    started = NOW - timedelta(hours=1)
    fold = Fold(
        train_start=started,
        train_end=started + timedelta(seconds=10),
        test_start=started + timedelta(seconds=15),
        test_end=started + timedelta(seconds=30),
        embargo_seconds=5,
    )

    def outcome(policy, begin, end, known, pnl):
        return PolicyOutcome(
            policy_version=policy,
            started_at=started + timedelta(seconds=begin),
            ended_at=started + timedelta(seconds=end),
            available_at=started + timedelta(seconds=known),
            net_pnl=Decimal(pnl),
        )

    results = walk_forward(
        (
            outcome("a", 1, 2, 3, "1"),
            outcome("b", 2, 3, 4, "0"),
            outcome("b", 4, 5, 11, "1000"),
            outcome("a", 16, 20, 21, "-1"),
            outcome("a", 22, 25, 31, "999"),
            outcome("b", 16, 20, 21, "1000"),
        ),
        (fold,),
    )
    (result,) = results
    assert result.selected_policy == "a"
    assert result.purged_count == 1
    assert result.test_count == 1
    assert result.test_net_pnl == Decimal("-1")
    assert result.status == "NEGATIVE" and result.live_eligible is False

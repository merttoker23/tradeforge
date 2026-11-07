import pytest

from tests.decision_fixtures import NOW, advance, change, discovery, validated, validating
from tradeforge.domain.decision import DecisionState as S


def approve(evidence):
    from tradeforge.decision.policy import DecisionPolicy
    from tradeforge.decision.state_machine import Journal

    journal = Journal()
    row = validating(journal, discovery())
    ev = change(
        evidence,
        proposal=change(evidence.proposal, decision_policy_version=DecisionPolicy().digest),
    )
    return journal.apply(advance(row, S.APPROVED, evidence=ev), now=NOW)


def test_bare_risk_digest_is_insufficient_without_portfolio_evidence():
    evidence = validated(discovery())
    assert approve(change(evidence, risk=None)).state is S.NO_TRADE


def test_rejected_or_mismatched_risk_evidence_cannot_be_promoted():
    from tests.risk_fixtures import state

    evidence = validated(discovery())
    assert evidence.risk is not None
    forged = change(evidence.risk, state=state(as_of=NOW, kill_switch=True))
    ev = change(evidence, risk=forged)
    assert approve(ev).state is S.NO_TRADE


@pytest.mark.parametrize("mismatch", ["symbol", "policy", "cost", "quantity", "stale"])
def test_lifecycle_binds_portfolio_evidence_to_candidate_cost_and_policy(mismatch):
    from datetime import timedelta
    from decimal import Decimal as D

    from tests.risk_fixtures import account, factor, state
    from tradeforge.risk.governor import RiskGovernor
    from tradeforge.risk.limits import RiskLimits

    ev = validated(discovery())
    assert approve(ev).state is S.APPROVED
    risk = ev.risk
    if mismatch == "policy":
        ev = change(ev, risk_version="a" * 64)
    else:
        req = risk.request
        at = NOW
        if mismatch == "symbol":
            req = change(req, instrument=change(req.instrument, symbol="ETHUSDT"))
        elif mismatch == "cost":
            req = change(req, cost_per_unit=D("0"))
        elif mismatch == "quantity":
            req = change(req, requested_qty=D("1"))
        elif mismatch == "stale":
            at -= timedelta(seconds=6)
            req = change(req, observed_at=at)
        risk = RiskGovernor(RiskLimits()).assess(
            req,
            account(
                equity=D("100000"),
                as_of=at,
                trading_day=at.date(),
                factors=(factor(req.instrument.symbol, as_of=at),),
            ),
            state(as_of=at),
            at=at,
            db_writable=True,
        )
        ev = change(ev, risk=risk)
    assert approve(ev).state is S.NO_TRADE


def test_risk_spread_cannot_understate_the_candidates_observed_spread():
    from decimal import Decimal as D

    from tradeforge.risk.governor import RiskGovernor
    from tradeforge.risk.limits import RiskLimits

    ev = validated(discovery())  # Candidate spread is 1 bp.
    req = change(ev.risk.request, spread_bps=D("0.1"))
    risk = RiskGovernor(RiskLimits(max_spread_bps=D("0.5"))).assess(
        req,
        ev.risk.account,
        ev.risk.state,
        at=NOW,
        db_writable=True,
    )
    assert risk.allowed  # Internally consistent but contradicts the candidate.
    ev = change(
        ev,
        risk=risk,
        risk_version=risk.policy.digest,
        proposal=change(ev.proposal, risk_policy_version=risk.policy.digest),
    )
    assert approve(ev).state is S.NO_TRADE

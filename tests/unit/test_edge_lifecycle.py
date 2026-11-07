from datetime import timedelta
from decimal import Decimal as D

import pytest

from tests.decision_fixtures import NOW, advance, change, discovery, validated, validating
from tests.edge_fixtures import bucket, inputs
from tradeforge.decision.policy import DecisionPolicy
from tradeforge.decision.state_machine import Journal
from tradeforge.domain.decision import DecisionState as S


def evidence(d):
    ev = validated(d)
    return change(ev, proposal=change(ev.proposal, decision_policy_version=DecisionPolicy().digest))


@pytest.mark.parametrize(
    "kind",
    ["absent", "wrong_symbol", "wrong_version", "stale", "nonpositive", "forged_ev", "wrong_fee"],
)
def test_lifecycle_cannot_approve_unknown_or_mismatched_edge(kind):
    d = discovery()
    j = Journal()
    r = validating(j, d)
    ev = evidence(d)
    if kind == "absent":
        ev = change(ev, edge=None)
    elif kind == "wrong_symbol":
        ev = change(ev, edge=change(ev.edge, bucket=bucket(symbol="ETHUSDT")))
    elif kind == "wrong_version":
        ev = change(ev, cost_version="9" * 64)
    elif kind == "stale":
        ev = change(ev, edge=change(ev.edge, as_of=NOW - timedelta(minutes=1)))
    elif kind == "nonpositive":
        ev = change(
            ev,
            edge=change(ev.edge, cost_input=inputs(taker_fee_bps=D("100"))),
        )
    elif kind == "forged_ev":
        ev = change(ev, proposal=change(ev.proposal, net_ev_low_R=D("99")))
    elif kind == "wrong_fee":
        ev = change(ev, proposal=change(ev.proposal, estimated_fees=D("0")))
    result = j.apply(advance(r, S.APPROVED, evidence=ev), now=NOW)
    assert result.state is S.NO_TRADE
    assert not result.trading_allowed


def test_bucket_regime_label_is_distinct_from_snapshot_version():
    d = discovery()
    j = Journal()
    r = validating(j, d)
    ev = evidence(d)
    # A regime bucket groups many immutable regime snapshot versions under one label.
    ev = change(ev, edge=change(ev.edge, bucket=change(ev.edge.bucket, regime="RANGE")))
    assert j.apply(advance(r, S.APPROVED, evidence=ev), now=NOW).state is S.APPROVED


def test_costs_are_normalized_to_the_candidate_stop_risk():
    d = discovery()
    j = Journal()
    r = validating(j, d)
    ev = evidence(d)
    cost = change(ev.edge.cost_input, initial_risk=D("1000000"))
    edge = change(ev.edge, cost_input=cost)
    p = change(ev.proposal, net_ev_low_R=edge.net_ev_low_R)
    ev = change(ev, edge=edge, proposal=p, cost_version=cost.digest)
    assert j.apply(advance(r, S.APPROVED, evidence=ev), now=NOW).state is S.NO_TRADE

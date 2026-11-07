"""Conservative overlap and deterministic selection across candidate orderings."""

from datetime import timedelta
from itertools import permutations

import pytest

from tests.decision_fixtures import NOW, advance, discovery, register
from tradeforge.domain.decision import AlphaFamily, Side
from tradeforge.domain.decision import DecisionState as S


@pytest.mark.parametrize(
    "other,reason",
    [
        ({}, "SYMBOL_OVERLAP"),
        ({"symbol": "ETHUSDT"}, "CLUSTER_OVERLAP"),
        ({"side": Side.SHORT, "family": AlphaFamily.REVERSAL}, "CONFLICTING_ALPHA"),
    ],
)
def test_active_candidates_hold_symbol_and_cluster(other, reason):
    from tradeforge.decision.state_machine import Journal

    j = Journal()
    j.apply(register(), now=NOW)
    r = j.apply(register(discovery("two", **other)), now=NOW)
    assert r.state is S.NO_TRADE and reason in r.reason_codes


def test_cooldown_survives_restart_and_ends_at_exact_boundary():
    from tradeforge.decision.policy import DecisionPolicy
    from tradeforge.decision.state_machine import Journal

    j = Journal(policy=DecisionPolicy(cooldown_seconds=30))
    r = j.apply(register(), now=NOW)
    j.apply(advance(r, S.CANCELLED), now=NOW)
    j = Journal.replay(j.records)
    r = j.apply(
        register(discovery("two", at=NOW + timedelta(seconds=29)), at=NOW + timedelta(seconds=29)),
        now=NOW + timedelta(seconds=29),
    )
    assert r.state is S.NO_TRADE and "COOLDOWN" in r.reason_codes
    at = NOW + timedelta(seconds=30)
    r = j.apply(register(discovery("three", at=at), at=at), now=at)
    assert r.state is S.DISCOVERED


def test_batch_order_does_not_choose_a_winner_from_opposing_alphas():
    from tradeforge.decision.policy import DecisionPolicy
    from tradeforge.decision.selector import select_candidates

    long = discovery()
    short = discovery("two", side=Side.SHORT, family=AlphaFamily.REVERSAL)
    third = discovery("three", symbol="SOLUSDT", cluster="independent")
    outputs = [
        select_candidates(tuple(items), at=NOW, policy=DecisionPolicy())
        for items in permutations((long, short, third))
    ]
    assert all(result == outputs[0] for result in outputs)
    reasons = {x.discovery.decision_id: x.reason_codes for x in outputs[0]}
    assert reasons[long.decision_id] == reasons[short.decision_id] == ("CONFLICTING_ALPHA",)
    assert reasons[third.decision_id] == ()


def test_duplicate_setup_and_future_command_do_not_mutate_history():
    from tradeforge.decision.state_machine import Journal

    j = Journal()
    j.apply(register(), now=NOW)
    with pytest.raises(ValueError, match="DUPLICATE_CANDIDATE"):
        j.apply(register(event="retry"), now=NOW)
    with pytest.raises(ValueError, match="FUTURE"):
        j.apply(register(discovery("future"), at=NOW + timedelta(seconds=1)), now=NOW)
    assert len(j.records) == 1


def test_cooldown_starts_when_terminal_event_is_actually_processed():
    from tradeforge.decision.state_machine import Journal

    j = Journal()
    r = j.apply(register(), now=NOW)
    j.apply(advance(r, S.CANCELLED), now=NOW + timedelta(seconds=5))
    j = Journal.replay(j.records)
    at = NOW + timedelta(seconds=30)
    r = j.apply(register(discovery("after", at=at), at=at), now=at)
    assert r.state is S.NO_TRADE and "COOLDOWN" in r.reason_codes

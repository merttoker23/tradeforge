"""Lifecycle boundaries, stale inputs, CAS and replay must fail closed."""

from datetime import timedelta

import pytest

from tests.decision_fixtures import NOW, advance, change, discovery, register, validated, validating
from tradeforge.domain.common import DataQuality
from tradeforge.domain.decision import DecisionState as S
from tradeforge.domain.market import StreamState


def test_only_sequential_analyst_transitions_and_terminal_approval():
    from tradeforge.decision.state_machine import Journal

    j = Journal()
    d = discovery()
    r = j.apply(register(d), now=NOW)
    for target in (S.ARMED, S.APPROVED, S.QUEUED):
        with pytest.raises(ValueError, match="INVALID_TRANSITION"):
            j.apply(advance(r, target), now=NOW)
    for target in (S.WATCHING, S.ARMED, S.VALIDATING):
        r = j.apply(advance(r, target), now=NOW)
    from tradeforge.decision.policy import DecisionPolicy

    ev = validated(d)
    ev = change(ev, proposal=change(ev.proposal, decision_policy_version=DecisionPolicy().digest))
    r = j.apply(advance(r, S.APPROVED, evidence=ev), now=NOW)
    assert r.state is S.APPROVED and r.version == 4
    assert r.trading_allowed is False
    for target in (S.APPROVED, S.QUEUED, S.CANCELLED):
        with pytest.raises(ValueError, match="TERMINAL"):
            j.apply(advance(r, target), now=NOW)
    assert len(j.records) == 5


def test_cas_out_of_order_duplicate_and_expiry_are_not_reapproved():
    from tradeforge.decision.state_machine import Journal

    j = Journal()
    r = j.apply(register(), now=NOW)
    cmd = advance(r, S.WATCHING, at=NOW + timedelta(seconds=1))
    r = j.apply(cmd, now=cmd.at)
    for attempt, error in (
        (cmd, "DUPLICATE_EVENT"),
        (change(cmd, event_id=register(event="other").event_id), "CAS"),
        (advance(r, S.ARMED, at=NOW), "OUT_OF_ORDER"),
    ):
        with pytest.raises(ValueError, match=error):
            j.apply(attempt, now=cmd.at)
    r = j.apply(
        advance(r, S.ARMED, at=NOW + timedelta(seconds=60)), now=NOW + timedelta(seconds=60)
    )
    assert r.state is S.EXPIRED and r.reason_codes == ("EXPIRED",)
    with pytest.raises(ValueError, match="TERMINAL"):
        j.apply(advance(r, S.APPROVED), now=NOW)


@pytest.mark.parametrize("target", [S.CANCELLED, S.NO_TRADE, S.REJECTED, S.EXPIRED])
def test_safe_exits_and_premature_expiry(target):
    from tradeforge.decision.state_machine import Journal

    j = Journal()
    r = j.apply(register(), now=NOW)
    if target is S.EXPIRED:
        with pytest.raises(ValueError, match="PREMATURE_EXPIRY"):
            j.apply(advance(r, target), now=NOW)
    else:
        assert j.apply(advance(r, target), now=NOW).state is target


@pytest.mark.parametrize(
    "patch,reason",
    [
        ({"triggered": False}, "NO_TRIGGER"),
        ({"data_quality": DataQuality.STALE}, "STALE"),
        ({"book_state": StreamState.GAP}, "ORDER_BOOK_GAP"),
        ({"cost_version": None}, "COST_UNKNOWN"),
        ({"risk_version": None}, "RISK_EVIDENCE_MISSING"),
        ({"proposal": None}, "INSUFFICIENT_EVIDENCE"),
    ],
)
def test_approval_gates_are_explicit_no_trade_reasons(patch, reason):
    from tradeforge.decision.state_machine import Journal

    j = Journal()
    d = discovery()
    r = validating(j, d)
    result = j.apply(advance(r, S.APPROVED, evidence=change(validated(d), **patch)), now=NOW)
    assert result.state is S.NO_TRADE and reason in result.reason_codes
    assert result.trading_allowed is False


def test_unknown_evidence_stays_unproven_and_cannot_authorize():
    from tradeforge.decision.state_machine import Journal

    j = Journal()
    r = validating(j)
    result = j.apply(advance(r, S.APPROVED), now=NOW)
    assert result.state is S.NO_TRADE
    assert "RISK_EVIDENCE_MISSING" in result.reason_codes
    assert "COST_UNKNOWN" in result.reason_codes


def test_source_policy_and_proposal_identity_are_pinned():
    from tradeforge.decision.state_machine import Journal

    d = discovery()
    for patch in ({"source_data_version": "0" * 64}, {"available_at": NOW + timedelta(seconds=1)}):
        j = Journal()
        if "source_data_version" in patch:
            with pytest.raises(ValueError, match="SOURCE_VERSION"):
                register(change(d, **patch))
        else:
            with pytest.raises(ValueError, match="FUTURE"):
                j.apply(register(change(d, **patch)), now=NOW)
    j = Journal()
    r = validating(j, d)
    ev = validated(d)
    result = j.apply(advance(r, S.APPROVED, evidence=ev), now=NOW)
    assert result.state is S.NO_TRADE and "PROVENANCE_MISMATCH" in result.reason_codes


def test_stale_future_and_unavailable_validation_cannot_leak():
    from tradeforge.decision.state_machine import Journal

    for patch, at in [
        ({"available_at": NOW + timedelta(seconds=1)}, NOW),
        ({}, NOW + timedelta(seconds=11)),
    ]:
        j = Journal()
        r = validating(j)
        result = j.apply(
            advance(r, S.APPROVED, at=at, evidence=change(validated(discovery()), **patch)), now=at
        )
        assert result.state is S.NO_TRADE and "STALE" in result.reason_codes


def test_replay_reproduces_records_and_rejects_tamper_reorder_policy_change():
    from tradeforge.decision.policy import DecisionPolicy
    from tradeforge.decision.state_machine import Journal

    j = Journal()
    validating(j)
    before = j.records
    rebuilt = Journal.replay(before)
    assert rebuilt.records == before
    assert rebuilt.head_hash == j.head_hash
    with pytest.raises(ValueError):
        Journal.replay(tuple(reversed(before)))
    from tradeforge.decision.reasons import DecisionReason

    corrupted = (*before[:-1], change(before[-1], reason_codes=(DecisionReason.NO_TRIGGER,)))
    with pytest.raises(ValueError, match="REPLAY"):
        Journal.replay(corrupted)
    with pytest.raises(ValueError, match="POLICY"):
        Journal.replay(before, policy=DecisionPolicy(cooldown_seconds=1))
    cmd = advance(before[-1], S.CANCELLED, at=NOW + timedelta(seconds=1))
    assert j.apply(cmd, now=cmd.at) == rebuilt.apply(cmd, now=cmd.at)
    assert before == j.records[:-1]


def test_delayed_processing_and_relabelled_source_time_cannot_refresh_evidence():
    from tradeforge.decision.state_machine import Journal

    d = discovery()
    later = NOW + timedelta(seconds=11)
    j = Journal()
    with pytest.raises(ValueError, match="STALE_COMMAND"):
        j.apply(register(d), now=later)
    refreshed = change(d, as_of=later, available_at=later)
    r = j.apply(register(refreshed, at=later), now=later)
    assert r.state is S.NO_TRADE and "STALE" in r.reason_codes


def test_candidate_expiring_between_event_and_processing_is_never_approved():
    from tradeforge.decision.state_machine import Journal

    j = Journal()
    r = validating(j)
    with pytest.raises(ValueError, match="EXPIRED_AT_PROCESSING"):
        j.apply(
            advance(r, S.APPROVED, at=NOW + timedelta(seconds=59)), now=NOW + timedelta(seconds=60)
        )


def test_changed_source_version_cannot_approve_matching_prices():
    from tradeforge.decision.policy import DecisionPolicy
    from tradeforge.decision.state_machine import Journal

    j = Journal()
    d = discovery()
    r = validating(j, d)
    ev = validated(d)
    ev = change(
        ev,
        source_data_version="0" * 64,
        proposal=change(ev.proposal, decision_policy_version=DecisionPolicy().digest),
    )
    r = j.apply(advance(r, S.APPROVED, evidence=ev), now=NOW)
    assert r.state is S.NO_TRADE and "PROVENANCE_MISMATCH" in r.reason_codes


def test_processing_clock_controls_freshness_and_is_replayed():
    from tradeforge.decision.policy import DecisionPolicy
    from tradeforge.decision.state_machine import Journal

    j = Journal()
    d = discovery()
    r = validating(j, d)
    ev = validated(d)
    ev = change(ev, proposal=change(ev.proposal, decision_policy_version=DecisionPolicy().digest))
    r = j.apply(
        advance(r, S.APPROVED, at=NOW + timedelta(seconds=8), evidence=ev),
        now=NOW + timedelta(seconds=16),
    )
    assert r.state is S.NO_TRADE and "STALE" in r.reason_codes
    assert Journal.replay(j.records).records == j.records


@pytest.mark.parametrize("target", [S.APPROVED, S.CANCELLED])
def test_future_nested_proposal_cannot_poison_durable_replay(target):
    from tradeforge.decision.state_machine import Journal

    j = Journal()
    d = discovery()
    r = validating(j, d)
    ev = validated(d, at=NOW + timedelta(seconds=1))
    with pytest.raises(ValueError, match="FUTURE_PROPOSAL"):
        j.apply(advance(r, target, evidence=ev), now=NOW)

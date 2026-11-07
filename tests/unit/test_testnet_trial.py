"""Explicit demo sampling is not statistical validation or live authority."""

from decimal import Decimal

import pytest

from tests.decision_fixtures import NOW, change, discovery, validated
from tradeforge.config.policy import RuntimeMode
from tradeforge.decision.edge import analyze_edge
from tradeforge.decision.policy import DecisionPolicy, approval_reasons
from tradeforge.domain.common import ReasonCode
from tradeforge.domain.decision import ProofState


def trial_evidence():
    from tradeforge.decision.trial import TestnetTrialPolicy

    d = discovery()
    original = validated(d)
    edge = analyze_edge(
        (),
        bucket=change(original.edge.bucket, environment="testnet"),
        cost=original.edge.cost_input,
        at=NOW,
        feature_hash=d.candidate.source_feature_hash,
        regime_version=d.candidate.regime_version,
        mode=RuntimeMode.TESTNET,
    )
    policy = TestnetTrialPolicy(enabled=True)
    risk = change(
        original.risk,
        request=change(original.risk.request, environment="testnet"),
        account=change(original.risk.account, environment="testnet"),
    )
    # Keep the independently reproducible assessment, while sizing within trial ceilings.
    from tradeforge.risk.governor import RiskGovernor

    request = change(risk.request, requested_qty=Decimal("0.1"))
    risk = RiskGovernor(risk.policy).assess(
        request, risk.account, risk.state, at=NOW, db_writable=True
    )
    cost = change(
        edge.cost_input, notional=risk.sized.notional, initial_risk=risk.sized.qty * Decimal("10")
    )
    edge = change(edge, cost_input=cost)
    proposal = change(
        original.proposal,
        net_ev_low_R=None,
        proof_state=ProofState.UNPROVEN,
        calibrated=False,
        execution_authority="TESTNET_TRIAL",
        reason_codes=(ReasonCode.TESTNET_TRIAL_AUTHORIZED,),
        confidence=None,
        estimated_fees=edge.costs.fees_R * cost.initial_risk,
        funding_cost=cost.stress.funding_bps * cost.notional / 10000,
        decision_policy_version=DecisionPolicy().digest,
    )
    return d, change(
        original,
        proposal=proposal,
        edge=edge,
        risk=risk,
        risk_version=risk.policy.digest,
        cost_version=cost.digest,
        trial=policy,
    )


def test_empty_history_can_only_receive_explicit_bounded_testnet_authority():
    d, evidence = trial_evidence()
    assert evidence.edge.proof_state is ProofState.UNPROVEN
    assert evidence.edge.net_ev_low_R is None
    assert evidence.proposal.confidence is None
    assert not approval_reasons(d, evidence, NOW, DecisionPolicy())
    assert approval_reasons(d, change(evidence, trial=None), NOW, DecisionPolicy())
    assert approval_reasons(
        d, change(evidence, trial=change(evidence.trial, enabled=False)), NOW, DecisionPolicy()
    )
    for mode in (RuntimeMode.LIVE, RuntimeMode.PAPER):
        assert approval_reasons(
            d, change(evidence, edge=change(evidence.edge, mode=mode)), NOW, DecisionPolicy()
        )


def test_trial_preserves_trigger_cost_book_and_risk_gates():
    from tradeforge.domain.market import StreamState

    d, evidence = trial_evidence()
    for patch in (
        {"triggered": False},
        {"book_state": StreamState.GAP},
        {"cost_version": None},
        {"risk": None},
    ):
        assert approval_reasons(d, change(evidence, **patch), NOW, DecisionPolicy())


def test_trial_caps_cannot_be_configured_above_hard_limits():
    from pydantic import ValidationError

    from tradeforge.decision.trial import TestnetTrialPolicy

    assert not TestnetTrialPolicy().enabled
    for patch in (
        {"max_entries": 21},
        {"max_notional": Decimal("101")},
        {"max_initial_risk": Decimal("1.1")},
    ):
        with pytest.raises(ValidationError):
            TestnetTrialPolicy(**patch)


class ForecastConnection:
    def __init__(self, forecasts=(), results=(), outcomes=(), terminal=()):
        self.forecasts, self.results, self.outcomes = forecasts, results, outcomes
        self.terminal = terminal

    def scalars(self, query, *params):
        sql = str(query)
        rows = (
            self.terminal
            if "FROM execution_intents" in sql
            else (
                self.forecasts
                if "runtime.forecast" in sql
                else (self.results if "runtime.closed_result" in sql else self.outcomes)
            )
        )

        class Rows:
            def all(self):
                return rows

        return Rows()


def test_trial_budget_is_durable_global_and_never_resets_by_policy(monkeypatch):
    from uuid import uuid4

    from tradeforge.persistence.db import EntryHalt
    from tradeforge.runtime.trading import reserve_forecast

    _, evidence = trial_evidence()
    events = []
    monkeypatch.setattr(
        "tradeforge.runtime.trading.audit", lambda c, kind, body: events.append(body)
    )
    reserve_forecast(ForecastConnection(), evidence, uuid4(), NOW)
    assert events[0]["split"] == "TRAIN" and events[0]["forecast_net_R"] is None
    previous = {**events[0], "intent_id": str(uuid4())}
    with pytest.raises(EntryHalt, match="SETTLEMENT_REQUIRED"):
        reserve_forecast(ForecastConnection([previous]), evidence, uuid4(), NOW)
    forecasts = [{**previous, "intent_id": str(uuid4())} for _ in range(20)]
    with pytest.raises(EntryHalt, match="BUDGET_EXHAUSTED"):
        reserve_forecast(ForecastConnection(forecasts), evidence, uuid4(), NOW)
    with pytest.raises(EntryHalt, match="LOSS_LIMIT"):
        reserve_forecast(
            ForecastConnection([previous], [{"intent_id": previous["intent_id"], "pnl": "-5"}]),
            evidence,
            uuid4(),
            NOW,
        )


def test_oos_is_preassigned_only_after_settled_training_and_embargo(monkeypatch):
    from datetime import timedelta
    from uuid import uuid4

    from tradeforge.persistence.db import EntryHalt
    from tradeforge.runtime.trading import reserve_forecast

    _, evidence = trial_evidence()
    evidence = change(evidence, trial=change(evidence.trial, train_entries=1))
    events = []
    monkeypatch.setattr(
        "tradeforge.runtime.trading.audit", lambda c, kind, body: events.append(body)
    )
    reserve_forecast(ForecastConnection(), evidence, uuid4(), NOW)
    previous = events[0]
    results = [{"intent_id": previous["intent_id"], "pnl": "1"}]
    outcomes = [{"trade_id": previous["intent_id"], "observed_at": NOW.isoformat()}]
    with pytest.raises(EntryHalt, match="EMBARGO"):
        reserve_forecast(ForecastConnection([previous], results, outcomes), evidence, uuid4(), NOW)
    reserve_forecast(
        ForecastConnection([previous], results, outcomes),
        evidence,
        uuid4(),
        NOW + timedelta(seconds=60),
    )
    assert events[-1]["split"] == "OOS"
    assert events[-1]["forecast_at"] == (NOW + timedelta(seconds=60)).isoformat()


def test_unmeasured_execution_friction_cannot_double_debit_realized_pnl():
    from pydantic import ValidationError

    from tradeforge.decision.cost_model import RealizedCost

    with pytest.raises(ValidationError, match="ALREADY_IN_GROSS"):
        RealizedCost(
            maker_fees=Decimal(0),
            taker_fees=Decimal(0),
            spread=Decimal(1),
            slippage=Decimal(0),
            funding=Decimal(0),
            latency=Decimal(0),
            pnl_basis="executed_price",
        )


def test_proven_terminal_zero_fill_consumes_attempt_without_blocking_or_fabricating_outcome(
    monkeypatch,
):
    from uuid import uuid4

    from tradeforge.runtime.trading import reserve_forecast

    _, evidence = trial_evidence()
    evidence = change(evidence, trial=change(evidence.trial, train_entries=1))
    events = []
    monkeypatch.setattr(
        "tradeforge.runtime.trading.audit", lambda c, kind, body: events.append(body)
    )
    reserve_forecast(ForecastConnection(), evidence, uuid4(), NOW)
    previous = events[0]
    connection = ForecastConnection([previous], terminal=[previous["intent_id"]])
    reserve_forecast(connection, evidence, uuid4(), NOW)
    assert len(events) == 2 and events[-1]["split"] == "TRAIN"
    assert not connection.results and not connection.outcomes


def test_flat_testnet_cleanup_checks_every_order_before_canceling(monkeypatch):
    from datetime import UTC, datetime
    from types import SimpleNamespace

    import pytest

    from tradeforge.execution import account_sync
    from tradeforge.persistence.db import EntryHalt

    at = datetime.now(UTC)
    deletes = []
    client = SimpleNamespace(
        environment="testnet",
        read_only=False,
        clock=lambda: at,
        request=lambda *args: deletes.append(args),
    )
    safe = {
        "algoId": 12,
        "orderType": "TAKE_PROFIT_MARKET",
        "reduceOnly": True,
        "positionSide": "BOTH",
        "algoStatus": "NEW",
    }
    unsafe = {**safe, "algoId": 13, "reduceOnly": False}
    view = account_sync.AccountView(at, (), (), (safe, unsafe))
    monkeypatch.setattr(account_sync, "read_account", lambda *args: view)
    with pytest.raises(EntryHalt, match="ORPHAN_CLEANUP_UNSAFE"):
        account_sync.cleanup_flat_testnet(client)
    assert deletes == []
    view = account_sync.AccountView(at, (), (), (safe,))
    final = account_sync.AccountView(at, (), (), ())
    reads = iter((view, view, final))
    monkeypatch.setattr(account_sync, "read_account", lambda *args: next(reads))
    assert account_sync.cleanup_flat_testnet(client) == 1
    assert deletes == [("DELETE", "/fapi/v1/algoOrder", {"algoId": "12"})]

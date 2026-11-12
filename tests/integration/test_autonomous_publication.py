"""A proven offline proposal crosses the actual publication/outbox/guardian chain."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import text


def test_symbol_wakeup_can_read_the_persisted_candidate_journal(migrated):
    from types import SimpleNamespace

    from tradeforge.runtime.demo_decisions import DemoDecisions

    decisions = DemoDecisions(migrated, SimpleNamespace(clock=lambda: datetime.now(UTC)))
    decisions.tick({}, symbols=frozenset({"BTCUSDT"}))
    assert decisions.status()["autonomous_decisions"]["last_reason"] == "NO_ARMED_CANDIDATES"


@pytest.mark.parametrize("trial", [False, True])
def test_approved_demo_publication_commits_once_and_dispatches_once(migrated, trial, monkeypatch):
    from tests.decision_fixtures import NOW, advance, discovery, register, validated
    from tests.edge_fixtures import inputs, trades
    from tests.execution_fixtures import FakeBroker
    from tests.risk_fixtures import account, change, factor, request
    from tests.universe_fixtures import payload, symbol
    from tradeforge.config.policy import RuntimeMode
    from tradeforge.decision.edge import analyze_edge
    from tradeforge.decision.policy import DecisionPolicy
    from tradeforge.decision.provenance import ValidationEvidence
    from tradeforge.decision.storage import DecisionService
    from tradeforge.decision.trial import TestnetTrialPolicy
    from tradeforge.domain.common import DataQuality, ReasonCode
    from tradeforge.domain.decision import DecisionState, ProofState
    from tradeforge.domain.market import StreamState
    from tradeforge.execution.broker import ExecutionContext
    from tradeforge.execution.order_manager import OrderManager
    from tradeforge.execution.outbox_worker import OutboxWorker
    from tradeforge.market.exchange_info import parse_exchange_info
    from tradeforge.risk.storage import RiskService
    from tradeforge.runtime.trading import publish_approved

    at = datetime.now(UTC)

    class FixedClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return at

    monkeypatch.setattr("tradeforge.runtime.trading.datetime", FixedClock)
    delta = at - NOW
    original = discovery()
    template = validated(original)
    bucket = change(template.edge.bucket, environment="testnet")
    outcomes = trades() + trades(180, 220, start=NOW - timedelta(hours=1), split="OOS", offset=2000)
    outcomes = tuple(
        change(
            row,
            bucket=bucket,
            **{
                field: getattr(row, field) + delta
                for field in ("opened_at", "closed_at", "observed_at", "forecast_at")
            },
        )
        for row in outcomes
    )
    cost = inputs(
        notional=Decimal("10") if trial else Decimal("1000"),
        initial_risk=Decimal("1") if trial else Decimal("100"),
        observed_at=at,
        expires_at=at + timedelta(seconds=60),
    )
    edge = analyze_edge(
        () if trial else outcomes,
        bucket=bucket,
        cost=cost,
        at=at,
        feature_hash="a" * 64,
        regime_version="r1",
        mode=RuntimeMode.TESTNET,
    )
    assert (edge.proof_state is ProofState.UNPROVEN) if trial else not edge.reason_codes
    d = discovery(at=at)
    meta = parse_exchange_info(payload(symbol()), at).instruments[0]
    risk = RiskService(migrated, clock=lambda: at)
    risk.set_kill_switch(False, reason="OFFLINE_EXPLICIT_ENABLE")
    req = request(
        symbol="BTCUSDT",
        observed_at=at,
        expires_at=d.candidate.expires_at,
        stop_price=Decimal("90"),
        requested_qty=Decimal("0.1") if trial else Decimal("10"),
        cost_per_unit=edge.costs.stress_R * Decimal("10"),
        environment="testnet",
    )
    req = change(req, filters=meta.filters)
    acc = account(
        environment="testnet",
        as_of=at,
        trading_day=at.date(),
        factors=(factor("BTCUSDT", as_of=at),),
        equity=Decimal("100000"),
        available_balance=Decimal("100000"),
        day_start_equity=Decimal("100000"),
    )
    quant = risk.assess(req, acc)
    assert quant.allowed and quant.sized.notional == cost.notional
    proposal = change(
        template.proposal,
        source_at=at,
        observed_at=at,
        as_of=at,
        expires_at=d.candidate.expires_at,
        decision_policy_version=DecisionPolicy().digest,
        net_ev_low_R=edge.net_ev_low_R,
        proof_state=edge.proof_state,
        calibrated=not trial,
        confidence=None,
        quality=None,
        execution_authority="TESTNET_TRIAL" if trial else "STATISTICAL_EDGE",
        reason_codes=(
            ReasonCode.TESTNET_TRIAL_AUTHORIZED if trial else ReasonCode.CANDIDATE_VALIDATED,
        ),
        estimated_fees=edge.costs.fees_R * cost.initial_risk,
        funding_cost=cost.stress.funding_bps * cost.notional / 10000,
    )
    evidence = ValidationEvidence(
        source_data_version=d.source_data_version,
        as_of=at,
        available_at=at,
        triggered=True,
        data_quality=DataQuality.OK,
        book_state=StreamState.READY,
        cost_version=cost.digest,
        risk_version=quant.policy.digest,
        proposal=proposal,
        edge=edge,
        risk=quant,
        trial=TestnetTrialPolicy(enabled=True) if trial else None,
    )
    service = DecisionService(migrated)
    record = service.submit(register(d, at=at), now=at)
    for state in (DecisionState.WATCHING, DecisionState.ARMED, DecisionState.VALIDATING):
        record = service.submit(advance(record, state, at=at), now=at)
    identifier = publish_approved(migrated, record, evidence)
    assert publish_approved(migrated, record, evidence) == identifier
    with migrated.transaction() as c:
        assert c.scalar(text("SELECT count(*) FROM execution_intents")) == 1
        assert c.scalar(text("SELECT count(*) FROM outbox")) == 1
        forecast = c.scalar(
            text("SELECT payload FROM audit_log WHERE event_type='runtime.forecast.v1'")
        )
        assert forecast["split"] == "TRAIN"
        if trial:
            assert forecast["forecast_net_R"] is None
            proposal_body = c.scalar(text("SELECT snapshot::jsonb FROM decisions"))
            assert proposal_body["proof_state"] == "UNPROVEN"
            assert proposal_body["calibrated"] is False
            assert proposal_body["confidence"] is None
        body = c.scalar(text("SELECT snapshot FROM execution_intents"))
    from tradeforge.domain.execution import ExecutionIntent
    from tradeforge.market.bars import canonical

    intent = ExecutionIntent.model_validate_json(canonical(body))
    context = ExecutionContext(
        environment="testnet",
        as_of=acc.as_of,
        metadata=meta,
        account=acc,
        mark_price=Decimal("100"),
        bid=Decimal("99.99"),
        ask=Decimal("100.01"),
        leverage=Decimal("1"),
        max_notional=Decimal("10000"),
        one_way=True,
        isolated=True,
        clock_ready=True,
        stream_ready=True,
    )
    broker = FakeBroker(intent)
    broker.environment = "testnet"
    manager = OrderManager(migrated, broker, lambda _: context)
    worker = OutboxWorker(migrated, manager)
    assert worker.run_once()
    assert not worker.run_once()
    assert broker.submissions == [intent.client_order_id]

"""Hand-defined analyst candidates and evidence; no exchange or invented alpha calibration."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from tests.market_fixtures import instrument
from tradeforge.alpha.base import AlphaCandidate
from tradeforge.domain.common import DataQuality, ReasonCode
from tradeforge.domain.decision import AlphaFamily, Decision, DecisionState, ProofState, Side
from tradeforge.domain.market import StreamState

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def change(model, **patch):
    return type(model).model_validate(
        {**model.model_dump(), **patch}, context={"now": NOW + timedelta(days=2)}
    )


def discovery(
    name="one",
    symbol="BTCUSDT",
    side=Side.LONG,
    family=AlphaFamily.MOMENTUM,
    cluster="market",
    at=NOW,
):
    from tradeforge.decision.provenance import Discovery
    from tradeforge.intelligence.models import digest

    candidate = AlphaCandidate(
        setup_id=digest(name),
        instrument=instrument(symbol),
        alpha_family=family,
        side=side,
        state=DecisionState.ARMED,
        observed_at=at,
        expires_at=at + timedelta(seconds=60),
        trigger=Decimal("100"),
        stop_proposal=Decimal("90") if side is Side.LONG else Decimal("110"),
        take_profit_proposal=Decimal("120") if side is Side.LONG else Decimal("80"),
        invalidations=("price_break",),
        source_feature_hash="a" * 64,
        input_hash="b" * 64,
        policy_hash="c" * 64,
        regime_version="r1",
        universe_version="d" * 64,
        reason_codes=("SETUP",),
        ordinal_quality=0.8,
        evidence_groups=(("price", 0.8),),
        spread_bps=Decimal("1"),
    )
    return Discovery(
        candidate=candidate,
        cluster=cluster,
        source_data_version=candidate.input_hash,
        as_of=at,
        available_at=at,
    )


def register(d=None, *, at=NOW, event="discover"):
    from tradeforge.decision.state_machine import Command

    d = d or discovery()
    return Command(
        event_id=uuid5(NAMESPACE_URL, event + d.candidate.setup_id),
        at=at,
        decision_id=d.decision_id,
        target=DecisionState.DISCOVERED,
        discovery=d,
    )


def advance(record, target, *, at=NOW, evidence=None, event=None):
    from tradeforge.decision.state_machine import Command

    return Command(
        event_id=uuid5(NAMESPACE_URL, event or f"{record.decision_id}/{record.version}/{target}"),
        decision_id=record.decision_id,
        at=at,
        target=target,
        expected_previous_state=record.state,
        expected_previous_version=record.version,
        evidence=evidence,
    )


def validated(d, *, at=NOW):
    from tests.edge_fixtures import candidate_analysis
    from tradeforge.decision.provenance import ValidationEvidence

    c = d.candidate
    edge = candidate_analysis(
        c.instrument.symbol, c.alpha_family, c.side, c.regime_version, c.source_feature_hash, at
    )
    cost = edge.cost_input
    from tests.risk_fixtures import account, factor, request, state
    from tradeforge.risk.governor import RiskGovernor

    risk_request = request(
        symbol=c.instrument.symbol,
        side=c.side,
        entry_price=c.trigger,
        stop_price=c.stop_proposal,
        requested_qty=cost.notional / c.trigger,
        cost_per_unit=edge.costs.stress_R * abs(c.trigger - c.stop_proposal),
        observed_at=at,
        expires_at=c.expires_at,
    )
    risk = RiskGovernor().assess(
        risk_request,
        account(
            as_of=at,
            trading_day=at.date(),
            factors=(factor(c.instrument.symbol, as_of=at),),
            equity=Decimal("100000"),
            available_balance=Decimal("100000"),
            day_start_equity=Decimal("100000"),
        ),
        state(as_of=at),
        at=at,
        db_writable=True,
    )
    proposal = Decision.model_validate(
        {
            **c.instrument.model_dump(),
            "schema_version": 1,
            "event_id": uuid5(NAMESPACE_URL, "proposal"),
            "source_at": at,
            "observed_at": at,
            "as_of": at,
            "decision_id": d.decision_id,
            "regime_version": c.regime_version,
            "universe_version": c.universe_version,
            "feature_hash": c.source_feature_hash,
            "alpha_family": c.alpha_family,
            "side": c.side,
            "state": DecisionState.APPROVED,
            "expires_at": c.expires_at,
            "trigger": c.trigger,
            "stop_proposal": c.stop_proposal,
            "take_profit_proposal": c.take_profit_proposal,
            "invalidations": c.invalidations,
            "spread_bps": c.spread_bps,
            "slippage_bps": cost.stress.slippage_bps,
            "estimated_fees": edge.costs.fees_R * cost.initial_risk,
            "funding_cost": cost.stress.funding_bps * cost.notional / 10_000,
            "net_ev_low_R": edge.net_ev_low_R,
            "proof_state": ProofState.VALIDATED,
            "calibrated": True,
            "confidence": None,
            "quality": None,
            "reason_codes": (ReasonCode.CANDIDATE_VALIDATED,),
            "risk_policy_version": risk.policy.digest,
            "decision_policy_version": "placeholder",
        },
        context={"now": at},
    )
    return ValidationEvidence(
        source_data_version="b" * 64,
        as_of=at,
        available_at=at,
        triggered=True,
        data_quality=DataQuality.OK,
        book_state=StreamState.READY,
        cost_version=cost.digest,
        risk_version=risk.policy.digest,
        proposal=proposal,
        edge=edge,
        risk=risk,
    )


def validating(journal, d=None):
    d = d or discovery()
    r = journal.apply(register(d), now=NOW)
    for state in (DecisionState.WATCHING, DecisionState.ARMED, DecisionState.VALIDATING):
        r = journal.apply(advance(r, state), now=NOW)
    return r

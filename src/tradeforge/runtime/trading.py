"""Demo-only decision publication and explicit initial activation.

The outbox, immutable authorization and decision commit together. Network effects
remain exclusively in OrderManager after this transaction has committed.
"""

from datetime import UTC, datetime
from decimal import ROUND_FLOOR, Decimal
from uuid import UUID, uuid5

from sqlalchemy import text
from sqlalchemy.engine import Connection

from tradeforge.decision.policy import approval_reasons
from tradeforge.decision.provenance import ValidationEvidence
from tradeforge.decision.state_machine import Command, Record
from tradeforge.decision.storage import DecisionService
from tradeforge.domain.execution import ExecutionIntent, PositionMode, StopPlan
from tradeforge.execution.storage import audit
from tradeforge.persistence.db import Database, EntryHalt
from tradeforge.persistence.repositories import add_instrument, add_risk_snapshot, save_decision
from tradeforge.risk.storage import LOCK_ID, _load, _save


def reserve_forecast(
    c: Connection, evidence: ValidationEvidence, identifier: UUID, at: datetime
) -> None:
    """Called under portfolio/publication locks; budget never resets on restart/version."""

    edge, trial = evidence.edge, evidence.trial
    assert edge is not None
    history = c.scalars(
        text("SELECT payload FROM audit_log WHERE event_type='runtime.forecast.v1'")
    ).all()
    # A terminal zero-fill attempt consumes the finite authorization budget but
    # produces neither PnL nor a calibration sample. An unreserved expiration
    # needs the worker's atomic expiration marker AND acknowledged outbox.
    zero_fills = {
        str(value)
        for value in c.scalars(
            text(
                "SELECT i.intent_id FROM execution_intents i "
                "JOIN outbox b USING(intent_id) LEFT JOIN orders o USING(intent_id) "
                "WHERE b.delivered_at IS NOT NULL AND ("
                "(o.status IN ('REJECTED','CANCELED','EXPIRED') AND o.filled_qty=0) OR "
                "(o.intent_id IS NULL AND i.expires_at<=:at AND EXISTS ("
                "SELECT 1 FROM audit_log a WHERE a.event_type='execution.intent_expired.v1' "
                "AND a.payload->>'intent_id'=i.intent_id::text)))"
            ),
            {"at": at},
        ).all()
    }
    matching = [p for p in history if p["bucket"] == edge.bucket.model_dump(mode="json")]
    if trial is not None:
        trials = [p for p in history if p.get("trial_policy") is not None]
        if not trial.enabled or len(trials) >= trial.max_entries:
            raise EntryHalt("TESTNET_TRIAL_BUDGET_EXHAUSTED")
        closed = c.scalars(
            text("SELECT payload FROM audit_log WHERE event_type='runtime.closed_result.v1'")
        ).all()
        results = {p["intent_id"]: p for p in closed}
        if any(p["intent_id"] not in results and p["intent_id"] not in zero_fills for p in trials):
            raise EntryHalt("TESTNET_TRIAL_SETTLEMENT_REQUIRED")
        if (
            sum(
                (
                    Decimal(results[p["intent_id"]]["pnl"])
                    for p in trials
                    if p["intent_id"] in results
                ),
                Decimal(0),
            )
            <= -trial.max_loss
        ):
            raise EntryHalt("TESTNET_TRIAL_LOSS_LIMIT")
    train_entries = trial.train_entries if trial is not None else 100
    filled_forecasts = [p for p in matching if p["intent_id"] not in zero_fills]
    split = "TRAIN" if len(filled_forecasts) < train_entries else "OOS"
    # Once OOS starts it never turns back into TRAIN, including policy changes.
    if any(p["split"] == "OOS" for p in matching):
        split = "OOS"
    if split == "OOS":
        trains = [p for p in matching if p["split"] == "TRAIN"]
        outcomes = c.scalars(
            text("SELECT payload->'body' FROM audit_log WHERE event_type='edge.outcome.v1'")
        ).all()
        by_id = {p["trade_id"]: p for p in outcomes}
        embargo = trial.embargo_seconds if trial is not None else edge.policy.embargo_seconds
        if any(
            p["intent_id"] not in by_id
            or (at - datetime.fromisoformat(by_id[p["intent_id"]]["observed_at"])).total_seconds()
            < embargo
            for p in trains
            if p["intent_id"] not in zero_fills
        ):
            raise EntryHalt("OOS_EMBARGO_OR_UNSETTLED_TRAIN")
    audit(
        c,
        "runtime.forecast.v1",
        {
            "intent_id": str(identifier),
            "bucket": edge.bucket.model_dump(mode="json"),
            "analysis_version": edge.digest,
            "forecast_at": at.isoformat(),
            "split": split,
            "forecast_net_R": None if edge.net_ev_low_R is None else str(edge.net_ev_low_R),
            "forecast_p_win": evidence.proposal.confidence
            if evidence.proposal is not None
            else None,
            "trial_policy": None if trial is None else trial.model_dump(mode="json"),
        },
    )


def initial_demo_activation(db: Database, *, first_start: bool, ready: bool) -> bool:
    if not first_start or not ready:
        return False
    with db.transaction() as c:
        c.execute(text("SELECT pg_advisory_xact_lock(:id)"), {"id": LOCK_ID})
        at = datetime.now(UTC)
        state = _load(c, at=at)
        # A manual command/risk event, even during startup, wins over first activation.
        if c.scalar(
            text(
                "SELECT 1 FROM audit_log WHERE event_type IN "
                "('risk.switch.v1','dashboard.command.v1') LIMIT 1"
            )
        ):
            return False
        _save(c, state.evolve(at=at, kill_switch=False))
        audit(c, "runtime.demo.activated.v1", {"live": False, "as_of": at.isoformat()})
    return True


def publish_approved(db: Database, record: Record, evidence: ValidationEvidence) -> UUID:
    from tradeforge.domain.decision import DecisionState

    at = datetime.now(UTC)
    quant, proposal = evidence.risk, evidence.proposal
    if (
        quant is None
        or proposal is None
        or not quant.allowed
        or quant.sized is None
        or quant.request.environment != "testnet"
        or quant.account.environment != "testnet"
        or evidence.edge is None
        or evidence.edge.mode.value != "testnet"
        or approval_reasons(record.discovery, evidence, at, record.policy)
    ):
        raise EntryHalt("AUTONOMOUS_APPROVAL_REQUIRED")
    # Leave headroom for the guardian's worst-price risk recheck and lot rounding.
    step = quant.request.filters.step_size
    assert proposal.stop_proposal is not None
    qty = (quant.sized.qty * Decimal("0.9") / step).to_integral_value(rounding=ROUND_FLOOR) * step
    if qty < quant.request.filters.min_qty:
        raise EntryHalt("ROUNDING_OR_FILTER_LIMIT")
    identifier = uuid5(proposal.decision_id, "demo-entry-v1")
    risk_id = uuid5(identifier, "risk")
    intent = ExecutionIntent(
        **{
            name: getattr(proposal, name)
            for name in (
                "symbol",
                "exchange",
                "market_type",
                "quote",
                "margin_asset",
                "contract_type",
            )
        },
        schema_version=1,
        event_id=identifier,
        source_at=at,
        observed_at=at,
        as_of=at,
        intent_id=identifier,
        client_order_id="tf_" + identifier.hex,
        decision_id=proposal.decision_id,
        side=proposal.side,
        qty=qty,
        price=None,
        position_mode=PositionMode.ONE_WAY,
        reduce_only=False,
        stop_plan=StopPlan(stop_price=proposal.stop_proposal, qty=qty),
        max_slippage_bps=Decimal("2"),
        expires_at=proposal.expires_at,
        created_at=at,
        policy_version="risk" + str(quant.policy.version),
        risk_snapshot_id=risk_id,
    )
    service = DecisionService(db)
    with db.transaction() as c:
        c.execute(text("SELECT pg_advisory_xact_lock(:id)"), {"id": LOCK_ID})
        c.execute(text("SELECT pg_advisory_xact_lock(74202010)"))
        old = c.scalar(
            text("SELECT intent_id FROM execution_intents WHERE intent_id=:id"), {"id": identifier}
        )
        if old is not None:
            return identifier
        state = _load(c, at=datetime.now(UTC))
        if state.kill_switch or state.version != quant.state.version:
            raise EntryHalt("RISK_CHANGED_BEFORE_PUBLICATION")
        proof = c.scalar(
            text(
                "SELECT payload FROM audit_log WHERE event_type='risk.assessment.v1' "
                "AND payload->>'id'=:id LIMIT 1"
            ),
            {"id": quant.digest},
        )
        if proof is None or proof.get("body") != quant.model_dump(mode="json"):
            raise EntryHalt("DURABLE_RISK_PROOF_REQUIRED")
        journal = service._load(c)
        current = next(
            (r for r in reversed(journal.records) if r.decision_id == record.decision_id), None
        )
        if current != record:
            raise EntryHalt("DECISION_CHANGED_BEFORE_PUBLICATION")
        reserve_forecast(c, evidence, identifier, at)
        approved = journal.apply(
            Command(
                event_id=uuid5(identifier, "approved"),
                decision_id=record.decision_id,
                at=at,
                target=DecisionState.APPROVED,
                expected_previous_state=record.state,
                expected_previous_version=record.version,
                evidence=evidence,
            ),
            now=datetime.now(UTC),
        )
        if approved.state is not DecisionState.APPROVED:
            raise EntryHalt("AUTONOMOUS_APPROVAL_REQUIRED")
        service._append(c, approved)
        add_instrument(c, proposal)
        decision_hash = save_decision(c, proposal)
        add_risk_snapshot(c, risk_id, quant.model_dump(mode="json"), quant.at)
        from tradeforge.market.bars import canonical
        from tradeforge.persistence.repositories import instrument_id

        c.execute(
            text(
                "INSERT INTO execution_intents (intent_id,client_order_id,decision_id,"
                "instrument_id,risk_snapshot_id,side,qty,price,created_at,expires_at,snapshot) "
                "VALUES (:id,:client,:decision,:instrument,:risk,:side,:qty,NULL,:at,:expiry,"
                "CAST(:body AS jsonb))"
            ),
            {
                "id": identifier,
                "client": intent.client_order_id,
                "decision": proposal.decision_id,
                "instrument": instrument_id(intent),
                "risk": risk_id,
                "side": intent.side.value,
                "qty": qty,
                "at": at,
                "expiry": intent.expires_at,
                "body": canonical(intent.model_dump(mode="json")),
            },
        )
        audit(
            c,
            "execution.authorization.v1",
            {
                "intent_id": str(identifier),
                "quant": quant.model_dump(mode="json"),
                "purpose": proposal.execution_authority,
            },
        )
        c.execute(
            text(
                "INSERT INTO outbox (message_id,intent_id,topic,payload) "
                "VALUES (:id,:intent,'execution.intent',CAST(:body AS jsonb))"
            ),
            {
                "id": uuid5(identifier, "outbox"),
                "intent": identifier,
                "body": canonical(
                    {
                        "intent_id": str(identifier),
                        "client_order_id": intent.client_order_id,
                        "decision_hash": decision_hash,
                    }
                ),
            },
        )
    return identifier

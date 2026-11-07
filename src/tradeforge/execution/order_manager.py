"""Durable reservation BEFORE side effects; an existing order is NEVER placed again.

A reservation without a query result is deliberately unresolved, including a crash
before the HTTP call. Safety takes precedence over automatic execution progress.
"""

import json
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from uuid import UUID

from sqlalchemy import text

from tradeforge.domain.common import utc
from tradeforge.domain.execution import ExecutionIntent, OrderState, OrderStatus, PositionMode
from tradeforge.domain.instrument import ContractStatus
from tradeforge.execution.broker import AckUnknown, Broker, ExchangeRejected, ExecutionContext
from tradeforge.execution.storage import (
    audit,
    intent_from,
    persist_report,
    state_from,
    write_state,
)
from tradeforge.persistence.db import Database, EntryHalt
from tradeforge.risk.governor import RiskAssessment, RiskGovernor
from tradeforge.risk.models import EntryRequest
from tradeforge.risk.storage import LOCK_ID, RiskService, _load


def authorize(db: Database, intent: ExecutionIntent, quant: RiskAssessment) -> None:
    """Bind an immutable, previously persisted quant risk assessment to an intent."""
    if not quant.allowed or quant.stage != "quant":
        raise EntryHalt("QUANT_NOT_ELIGIBLE")
    with db.transaction() as c:
        c.execute(text("SELECT pg_advisory_xact_lock(:id)"), {"id": LOCK_ID})
        proof = c.scalar(
            text(
                "SELECT payload FROM audit_log WHERE event_type='risk.assessment.v1' "
                "AND payload->>'id'=:id ORDER BY audit_id DESC LIMIT 1"
            ),
            {"id": quant.digest},
        )
        if proof is None or proof.get("body") != quant.model_dump(mode="json"):
            raise EntryHalt("DURABLE_RISK_PROOF_REQUIRED")
        if intent_from(c, intent.intent_id) != intent:
            raise EntryHalt("INTENT_MISMATCH")
        body = {"intent_id": str(intent.intent_id), "quant": quant.model_dump(mode="json")}
        old = c.scalar(
            text(
                "SELECT payload FROM audit_log WHERE event_type='execution.authorization.v1' "
                "AND payload->>'intent_id'=:id LIMIT 1"
            ),
            {"id": str(intent.intent_id)},
        )
        if old is not None and old != body:
            raise EntryHalt("AUTHORIZATION_CONFLICT")
        if old is None:
            audit(c, "execution.authorization.v1", body)


def stream_gap(db: Database) -> None:
    """No sequence numbers are promised by this stream. Any discontinuity latches risk."""
    RiskService(db).set_kill_switch(True, reason="PRIVATE_STREAM_GAP")


def execution_price(
    intent: ExecutionIntent,
    context: ExecutionContext,
    quant: RiskAssessment,
) -> Decimal:
    if intent.price is not None:
        return intent.price
    long = intent.side.value == "LONG"
    multiplier = 1 + (1 if long else -1) * intent.max_slippage_bps / 10000
    tick = context.metadata.filters.tick_size
    price = (quant.request.entry_price * multiplier / tick).to_integral_value(
        rounding=ROUND_FLOOR if long else ROUND_CEILING,
    ) * tick
    if price <= 0:
        raise EntryHalt("INVALID_EXECUTION_PRICE_BOUND")
    return price


def validate_context(
    intent: ExecutionIntent,
    context: ExecutionContext,
    quant: RiskAssessment,
    *,
    at: datetime,
) -> None:
    m, req, p = context.metadata, quant.request, quant.policy
    if (
        intent.reduce_only
        or not 0 <= (at - context.as_of).total_seconds() <= p.account_age_seconds
        or not 0 <= (at - m.observed_at).total_seconds() <= 60
        or not context.clock_ready
        or not context.stream_ready
        or not context.one_way
        or not context.isolated
        or m.contract_status is not ContractStatus.TRADING
        or m.key != intent.key
        or (m.delist_at is not None and m.delist_at <= at)
        or not intent.created_at <= at < intent.expires_at
        or intent.position_mode is not PositionMode.ONE_WAY
    ):
        raise EntryHalt("EXECUTION_CONTEXT_UNSAFE")
    if (
        req.instrument.key != intent.key
        or req.filters != m.filters
        or req.side != intent.side
        or req.expires_at < intent.expires_at
        or intent.qty > req.requested_qty
        or intent.stop_plan is None
        or req.stop_price != intent.stop_plan.stop_price
        or context.leverage != req.leverage
        or context.leverage > p.max_leverage
        or intent.policy_version != ("risk" + str(p.version))
    ):
        raise EntryHalt("INTENT_RISK_MISMATCH")
    price = execution_price(intent, context, quant)
    lot = m.filters if intent.price is not None else m.market_lot
    stop = intent.stop_plan.stop_price
    if (
        not lot.min_qty <= intent.qty <= lot.max_qty
        or intent.qty % lot.step_size
        or not m.filters.min_price <= price <= m.filters.max_price
        or (intent.price is not None and price % m.filters.tick_size)
        or stop % m.filters.tick_size
        or not m.filters.min_price <= stop <= m.filters.max_price
        or price * intent.qty < m.filters.min_notional
        or price * intent.qty > context.max_notional
        or (context.ask - context.bid) / (context.ask + context.bid) * 20000
        > min(req.spread_bps, p.max_spread_bps)
        or (intent.price is not None and intent.price != req.entry_price)
        or (
            intent.price is None
            and (context.ask > price if intent.side.value == "LONG" else context.bid < price)
        )
        or not (
            stop < min(price, context.mark_price)
            if intent.side.value == "LONG"
            else stop > max(price, context.mark_price)
        )
    ):
        raise EntryHalt("EXCHANGE_FILTER_OR_PRICE_LIMIT")
    if context.account.available_balance is None or (
        price * intent.qty / context.leverage + req.cost_per_unit * intent.qty
        > context.account.available_balance
    ):
        raise EntryHalt("MARGIN_INSUFFICIENT")


class OrderManager:
    def __init__(
        self,
        db: Database,
        broker: Broker,
        context: Callable[[ExecutionIntent], ExecutionContext],
        *,
        clock: Callable[[], datetime] | None = None,
        protection: Callable[[ExecutionIntent], None] | None = None,
    ) -> None:
        self.db, self.broker, self.context = db, broker, context
        self.clock = clock or (lambda: datetime.now(UTC))
        self.protection = protection

    def execute(self, intent_id: UUID) -> OrderState:
        if not self.db.probe().db_writable:
            raise EntryHalt("DB_UNAVAILABLE")
        with self.db.transaction() as c:
            intent = intent_from(c, intent_id)
            prior = state_from(c, intent, utc(self.clock()))
            closed = c.scalar(
                text(
                    "SELECT 1 FROM audit_log WHERE event_type='protection.flat.v1' "
                    "AND payload->>'intent_id'=:id LIMIT 1"
                ),
                {"id": str(intent_id)},
            )
        if closed and prior is not None:
            return prior
        if prior is not None and prior.status is OrderStatus.REJECTED:
            return prior
        frame = self.context(intent) if prior is None else None
        dispatch_intent = intent
        with self.db.transaction() as c:
            c.execute(text("SELECT pg_advisory_xact_lock(:id)"), {"id": LOCK_ID})
            at = utc(self.clock())
            prior = state_from(c, intent, at)
            fresh = prior is None
            if fresh:
                assert frame is not None
                evidence = c.scalar(
                    text(
                        "SELECT payload FROM audit_log "
                        "WHERE event_type='execution.authorization.v1' "
                        "AND payload->>'intent_id'=:id "
                        "ORDER BY audit_id LIMIT 1"
                    ),
                    {"id": str(intent_id)},
                )
                if evidence is None:
                    raise EntryHalt("EXECUTION_AUTHORIZATION_MISSING")
                quant = RiskAssessment.model_validate_json(json.dumps(evidence["quant"]))
                if getattr(self.broker, "environment", None) == "testnet" and (
                    frame.environment != "testnet"
                    or frame.account.environment != "testnet"
                    or quant.account.environment != "testnet"
                    or quant.request.environment != "testnet"
                ):
                    raise EntryHalt("EXCHANGE_ENVIRONMENT_MISMATCH")
                validate_context(intent, frame, quant, at=at)
                # Share the risk-control lock through the durable reservation COMMIT.
                try:
                    state = _load(c, at=at)
                except ValueError:
                    raise EntryHalt("RISK_STATE_TIME_INVALID") from None
                current = RiskGovernor().guardian_recheck(
                    quant, frame.account, state, at=at, db_writable=True
                )
                price = execution_price(intent, frame, quant)
                stressed_request = EntryRequest.model_validate(
                    {
                        **quant.request.model_dump(),
                        "entry_price": price,
                        "cost_per_unit": quant.request.cost_per_unit
                        * max(Decimal(1), price / quant.request.entry_price),
                    }
                )
                stressed = RiskGovernor().assess(
                    stressed_request,
                    frame.account,
                    state,
                    at=at,
                    db_writable=True,
                )
                if (
                    not current.allowed
                    or current.sized is None
                    or not stressed.allowed
                    or stressed.sized is None
                    or intent.qty > min(current.sized.qty, stressed.sized.qty)
                ):
                    raise EntryHalt("GUARDIAN_RISK_REJECTED")
                # A MARKET intent becomes marketable IOC at a persisted worst-price
                # bound. A gap can leave it unfilled, never above its risk budget.
                dispatch_intent = ExecutionIntent.model_validate(
                    {**intent.model_dump(), "price": price}
                )
                duplicate = c.scalar(
                    text(
                        "SELECT count(*) FROM orders o JOIN execution_intents i "
                        "USING(intent_id) WHERE i.decision_id=:id"
                    ),
                    {"id": intent.decision_id},
                )
                outstanding = c.scalar(
                    text(
                        "SELECT count(*) FROM orders o WHERE "
                        "(status NOT IN ('REJECTED','CANCELED','EXPIRED') OR filled_qty>0) "
                        "AND NOT EXISTS (SELECT 1 FROM audit_log a "
                        "WHERE a.event_type='protection.flat.v1' "
                        "AND a.payload->>'intent_id'=o.intent_id::text)"
                    )
                )
                # Only authoritative, terminal-to-flat reconciliation releases exposure.
                if duplicate or outstanding:
                    raise EntryHalt("DISPATCH_EXPOSURE_RESERVED")
                c.execute(
                    text(
                        "INSERT INTO orders(intent_id,client_order_id,qty,status) "
                        "VALUES (:id,:client,:qty,'ACK_UNKNOWN')"
                    ),
                    {"id": intent_id, "client": intent.client_order_id, "qty": intent.qty},
                )
                audit(
                    c,
                    "execution.reserved.v1",
                    {
                        "intent_id": str(intent_id),
                        "mode": "testnet",
                        "quant": quant.digest,
                        "guardian": current.model_dump(mode="json"),
                        "stressed": stressed.model_dump(mode="json"),
                        "execution_price": str(price),
                    },
                )
        # No database transaction encloses network I/O. No catch can roll back reservation.
        try:
            result = self.broker.place(dispatch_intent) if fresh else self.broker.query(intent)
        except ExchangeRejected as error:
            if not fresh:
                raise EntryHalt("EXCHANGE_QUERY_UNAVAILABLE") from None
            with self.db.transaction() as c:
                old = state_from(c, intent, utc(self.clock()))
                assert old is not None
                rejected = OrderState.model_validate(
                    {**old.model_dump(), "status": OrderStatus.REJECTED}
                )
                write_state(c, rejected, utc(self.clock()))
                reason = (
                    str(error)
                    if str(error)
                    in {
                        "MARGIN_INSUFFICIENT",
                        "RATE_LIMITED",
                        "ORDER_REJECTED",
                        "HEDGE_MODE_UNSUPPORTED",
                    }
                    else "ORDER_REJECTED"
                )
                audit(c, "execution.rejected.v1", {"intent_id": str(intent_id), "reason": reason})
            return rejected
        except AckUnknown:
            try:
                result = self.broker.query(intent)
            except (AckUnknown, ExchangeRejected):
                raise EntryHalt("ACK_UNRESOLVED") from None
        if result is None:
            raise EntryHalt("ACK_UNRESOLVED")
        with self.db.transaction() as c:
            saved = persist_report(c, intent, result, utc(self.clock()))
        if saved.filled_qty and not intent.reduce_only:
            if self.protection is not None:
                self.protection(intent)
            else:
                self._protect(intent)
        return saved

    def _protect(self, intent: ExecutionIntent) -> None:
        client_id = "sl_" + intent.intent_id.hex
        with self.db.transaction() as c:
            c.execute(text("SELECT pg_advisory_xact_lock(:id)"), {"id": LOCK_ID})
            reserved = c.scalar(
                text(
                    "SELECT count(*) FROM audit_log "
                    "WHERE event_type='execution.stop_reserved.v1' AND payload->>'intent_id'=:id"
                ),
                {"id": str(intent.intent_id)},
            )
            if not reserved:
                audit(
                    c,
                    "execution.stop_reserved.v1",
                    {
                        "intent_id": str(intent.intent_id),
                        "client_id": client_id,
                    },
                )
        try:
            ready = self.broker.protect(intent, client_id, query_only=bool(reserved))
        except (AckUnknown, ExchangeRejected):
            try:
                ready = self.broker.protect(intent, client_id, query_only=True)
            except (AckUnknown, ExchangeRejected):
                ready = False
        if not ready:
            stream_gap(self.db)
            # Cancellation is risk reducing and cannot authorize a second entry.
            try:
                self.broker.cancel(intent)
            except (AckUnknown, ExchangeRejected):
                pass
            raise EntryHalt("PROTECTION_UNAVAILABLE_ESCALATE")
        with self.db.transaction() as c:
            audit(
                c,
                "execution.protected.v1",
                {
                    "intent_id": str(intent.intent_id),
                    "client_id": client_id,
                    "close_all": True,
                },
            )

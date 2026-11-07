"""Order/fill evidence in the existing durable schema; no exchange calls here."""

import json
from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import Connection

from tradeforge.domain.execution import ExecutionIntent, OrderState, OrderStatus
from tradeforge.execution.broker import ExchangeReport
from tradeforge.market.bars import canonical
from tradeforge.persistence.db import EntryHalt

TERMINAL = {OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.REJECTED}


def audit(c: Connection, kind: str, body: object) -> None:
    c.execute(
        text("INSERT INTO audit_log(event_type,payload) VALUES (:kind,CAST(:body AS jsonb))"),
        {"kind": kind, "body": canonical(body)},
    )


def intent_from(c: Connection, intent_id: UUID) -> ExecutionIntent:
    row = c.scalar(
        text("SELECT snapshot FROM execution_intents WHERE intent_id=:id"), {"id": intent_id}
    )
    if row is None:
        raise EntryHalt("INTENT_MISSING")
    try:
        return ExecutionIntent.model_validate_json(json.dumps(row))
    except ValueError:
        raise EntryHalt("INTENT_CORRUPT") from None


def state_from(c: Connection, intent: ExecutionIntent, at: datetime) -> OrderState | None:
    row = c.execute(
        text("SELECT status,qty FROM orders WHERE intent_id=:id FOR UPDATE"),
        {"id": intent.intent_id},
    ).first()
    if row is None:
        return None
    body = c.scalar(
        text(
            "SELECT payload FROM audit_log WHERE event_type='execution.order.v1' "
            "AND payload->>'intent_id'=:id ORDER BY audit_id DESC LIMIT 1"
        ),
        {"id": str(intent.intent_id)},
    )
    if body is not None:
        state = OrderState.model_validate_json(json.dumps(body))
        if state.status.value != row.status or state.qty != row.qty:
            raise EntryHalt("ORDER_STATE_CORRUPT")
        return state
    if row.status not in {"NEW", "ACK_UNKNOWN"}:
        raise EntryHalt("ORDER_EVIDENCE_MISSING")
    return OrderState(
        **{
            k: getattr(intent, k)
            for k in ("symbol", "exchange", "market_type", "quote", "margin_asset", "contract_type")
        },
        schema_version=1,
        event_id=uuid4(),
        source_at=at,
        observed_at=at,
        as_of=at,
        intent_id=intent.intent_id,
        client_order_id=intent.client_order_id,
        exchange_order_id=None,
        status=OrderStatus(row.status),
        qty=intent.qty,
        filled_qty=Decimal(0),
        fill_ids=(),
        avg_executed_price=None,
        fee=Decimal(0),
        exchange_at=None,
        last_reconciliation=None,
    )


def persist_report(
    c: Connection,
    intent: ExecutionIntent,
    report: ExchangeReport,
    at: datetime,
) -> OrderState:
    old = state_from(c, intent, at)
    if old is None:
        raise EntryHalt("ORDER_RESERVATION_REQUIRED")
    if (
        report.client_order_id != intent.client_order_id
        or report.symbol != intent.symbol
        or report.qty != intent.qty
        or report.side != ("BUY" if intent.side.value == "LONG" else "SELL")
        or report.exchange_at > at
        or (old.exchange_order_id is not None and old.exchange_order_id != report.order_id)
    ):
        raise EntryHalt("EXCHANGE_ORDER_MISMATCH")
    if old.exchange_at is not None and report.exchange_at < old.exchange_at:
        return old
    status = OrderStatus.ACCEPTED if report.status == "NEW" else OrderStatus(report.status)
    if report.filled_qty < old.filled_qty:
        raise EntryHalt("EXCHANGE_FILL_REGRESSION")
    if old.status in TERMINAL and status != old.status:
        raise EntryHalt("TERMINAL_ORDER_REGRESSION")
    for fill in report.fills:
        params = {
            "id": fill.trade_id,
            "intent": intent.intent_id,
            "qty": fill.qty,
            "price": fill.price,
            "fee": fill.fee,
            "exchange": fill.exchange_at,
            "at": at,
        }
        existing = (
            c.execute(
                text("SELECT intent_id,qty,price,fee,exchange_at FROM fills WHERE fill_id=:id"),
                params,
            )
            .mappings()
            .one_or_none()
        )
        if existing is not None:
            if (
                existing["intent_id"] != intent.intent_id
                or existing["qty"] != fill.qty
                or existing["price"] != fill.price
                or existing["fee"] != fill.fee
                or existing["exchange_at"] != fill.exchange_at
            ):
                raise EntryHalt("FILL_ID_CONFLICT")
        else:
            c.execute(
                text(
                    "INSERT INTO fills(fill_id,intent_id,qty,price,fee,exchange_at,observed_at) "
                    "VALUES (:id,:intent,:qty,:price,:fee,:exchange,:at)"
                ),
                params,
            )
    fill_ids = tuple(sorted(x.trade_id for x in report.fills))
    if not set(old.fill_ids) <= set(fill_ids):
        raise EntryHalt("MISSING_PRIOR_FILL")
    fee = sum((x.fee for x in report.fills), Decimal(0))
    state = OrderState.model_validate(
        {
            **old.model_dump(),
            "event_id": uuid4(),
            "source_at": report.exchange_at,
            "observed_at": at,
            "as_of": at,
            "exchange_order_id": report.order_id,
            "status": status,
            "filled_qty": report.filled_qty,
            "fill_ids": fill_ids,
            "avg_executed_price": report.avg_price,
            "fee": fee,
            "exchange_at": report.exchange_at,
            "last_reconciliation": at,
        }
    )
    write_state(c, state, at)
    return state


def write_state(c: Connection, state: OrderState, at: datetime) -> None:
    c.execute(
        text(
            "UPDATE orders SET status=:status,exchange_order_id=:exchange,filled_qty=:fill,"
            "avg_price=:price,fee=:fee,version=version+1,updated_at=:at WHERE intent_id=:id"
        ),
        {
            "status": state.status.value,
            "exchange": state.exchange_order_id,
            "fill": state.filled_qty,
            "price": state.avg_executed_price,
            "fee": state.fee,
            "at": at,
            "id": state.intent_id,
        },
    )
    audit(c, "execution.order.v1", state.model_dump(mode="json"))

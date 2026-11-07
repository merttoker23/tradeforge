"""Cancel the entry before a durable, idempotent reduce-only emergency attempt."""

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import text

from tradeforge.domain.execution import ExecutionIntent
from tradeforge.execution.account_sync import amounts
from tradeforge.execution.binance_client import BinanceTestnetClient, number
from tradeforge.execution.broker import AckUnknown, ExchangeRejected
from tradeforge.execution.storage import audit, persist_report
from tradeforge.persistence.db import Database, EntryHalt


def emergency_close(
    db: Database,
    client: BinanceTestnetClient,
    intent: ExecutionIntent,
    at: datetime,
    *,
    clock: Callable[[], datetime] | None = None,
) -> bool:
    observed = clock or (lambda: datetime.now(UTC))
    key = "ec_" + intent.intent_id.hex
    with db.transaction() as c:
        reserved = c.scalar(
            text(
                "SELECT payload FROM audit_log "
                "WHERE event_type='protection.exit_reserved.v1' "
                "AND payload->>'client_id'=:key LIMIT 1"
            ),
            {"key": key},
        )
    if reserved is None:
        try:
            canceled = client.cancel(intent)
            if canceled.status not in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
                raise EntryHalt("EMERGENCY_ENTRY_CANCEL_UNKNOWN")
            with db.transaction() as c:
                persist_report(c, intent, canceled, observed())
        except (AckUnknown, ExchangeRejected, EntryHalt):
            # Do not close while a still-filling entry can reopen exposure.
            current = client.query(intent)
            if current is None or current.status not in {
                "FILLED",
                "CANCELED",
                "EXPIRED",
                "REJECTED",
            }:
                raise EntryHalt("EMERGENCY_ENTRY_CANCEL_UNKNOWN") from None
            with db.transaction() as c:
                persist_report(c, intent, current, observed())
            canceled = current
        risk = amounts(client.request("GET", "/fapi/v3/positionRisk"))
        amount = risk.get(intent.symbol, Decimal(0))
        if amount == 0:
            return True
        if (amount > 0) != (intent.side.value == "LONG") or abs(amount) != canceled.filled_qty:
            raise EntryHalt("EMERGENCY_POSITION_MISMATCH")
        side = "SELL" if amount > 0 else "BUY"
        reserved = {
            "client_id": key,
            "symbol": intent.symbol,
            "side": side,
            "qty": str(abs(amount)),
        }
        with db.transaction() as c:
            audit(c, "protection.exit_reserved.v1", reserved)
        try:
            client.request(
                "POST",
                "/fapi/v1/order",
                {
                    "symbol": intent.symbol,
                    "side": side,
                    "positionSide": "BOTH",
                    "type": "MARKET",
                    "quantity": str(abs(amount)),
                    "reduceOnly": "true",
                    "newClientOrderId": key,
                    "newOrderRespType": "RESULT",
                },
            )
        except (AckUnknown, ExchangeRejected):
            pass
    body = client.request(
        "GET",
        "/fapi/v1/order",
        {
            "symbol": intent.symbol,
            "origClientOrderId": key,
        },
    )
    if (
        not isinstance(body, dict)
        or body.get("clientOrderId") != key
        or body.get("symbol") != intent.symbol
        or body.get("side") != reserved["side"]
        or body.get("reduceOnly") is not True
        or body.get("positionSide") != "BOTH"
        or body.get("type") != "MARKET"
        or body.get("status")
        not in {"NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "EXPIRED", "REJECTED"}
        or number(body.get("origQty")) != Decimal(reserved["qty"])
        or not 0 <= number(body.get("executedQty")) <= number(body.get("origQty"))
        or (
            body.get("status") == "FILLED"
            and number(body.get("executedQty")) != number(body.get("origQty"))
        )
    ):
        raise EntryHalt("EMERGENCY_ACK_UNRESOLVED")
    with db.transaction() as c:
        audit(
            c,
            "protection.exit_observed.v1",
            {
                "client_id": key,
                "symbol": intent.symbol,
                "status": body.get("status"),
                "filled_qty": str(number(body.get("executedQty"))),
            },
        )
    # Even a FILLED ACK does not establish a flat position. REST decides.
    remaining = amounts(client.request("GET", "/fapi/v3/positionRisk")).get(
        intent.symbol, Decimal(0)
    )
    return remaining == 0

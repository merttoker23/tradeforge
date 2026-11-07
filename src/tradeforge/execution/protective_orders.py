"""Durable close-all SL/TP reservations; no cancel-before-replace exposure window."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import text

from tradeforge.domain.execution import ExecutionIntent
from tradeforge.execution.account_sync import ExchangePosition
from tradeforge.execution.binance_client import BinanceTestnetClient, number
from tradeforge.execution.broker import AckUnknown, ExchangeRejected
from tradeforge.execution.storage import audit
from tradeforge.persistence.db import Database, EntryHalt


def matches(body: object, position: ExchangePosition, price: Decimal, kind: str, key: str) -> bool:
    try:
        return bool(
            isinstance(body, dict)
            and body.get("clientAlgoId") == key
            and body.get("symbol") == position.symbol
            and body.get("algoStatus") == "NEW"
            and body.get("closePosition") is True
            and body.get("positionSide") == "BOTH"
            and body.get("workingType") == "MARK_PRICE"
            and body.get("side") == ("SELL" if position.amount > 0 else "BUY")
            and body.get("orderType") == kind
            and number(body.get("triggerPrice")) == price
            and body.get("algoId") is not None
        )
    except ValueError:
        return False


class ProtectiveOrders:
    def __init__(self, db: Database, client: BinanceTestnetClient) -> None:
        self.db, self.client = db, client

    def _reserve(
        self,
        position_id: UUID,
        position: ExchangePosition,
        intent: ExecutionIntent,
        key: str,
        price: Decimal,
        kind: str,
        at: datetime,
    ) -> None:
        with self.db.transaction() as c:
            c.execute(
                text(
                    "INSERT INTO protective_orders "
                    "(protective_id,position_id,client_order_id,qty,stop_price,status,as_of) "
                    "VALUES (:id,:position,:key,:qty,:price,'ACK_UNKNOWN',:at)"
                ),
                {
                    "id": uuid4(),
                    "position": position_id,
                    "key": key,
                    "qty": abs(position.amount),
                    "price": price,
                    "at": at,
                },
            )
            audit(
                c,
                "protection.reserved.v1",
                {
                    "client_id": key,
                    "kind": kind,
                    "intent_id": str(intent.intent_id),
                },
            )

    def _status(self, key: str, status: str, at: datetime) -> None:
        with self.db.transaction() as c:
            c.execute(
                text(
                    "UPDATE protective_orders SET status=:status,version=version+1,"
                    "as_of=:at WHERE client_order_id=:key"
                ),
                {"status": status, "at": at, "key": key},
            )

    def ensure(
        self,
        position_id: UUID,
        position: ExchangePosition,
        intent: ExecutionIntent,
        price: Decimal,
        kind: str,
        at: datetime,
    ) -> str:
        prefix = "sl_" if kind == "STOP_MARKET" else "tp_"
        initial_key = prefix + intent.intent_id.hex
        with self.db.transaction() as c:
            row = c.execute(
                text(
                    "SELECT client_order_id,status FROM protective_orders WHERE position_id=:id "
                    "AND client_order_id LIKE :prefix AND (client_order_id=:initial OR "
                    "client_order_id IN (SELECT payload->>'client_id' FROM audit_log "
                    "WHERE event_type='protection.reserved.v1' "
                    "AND payload->>'intent_id'=:intent)) "
                    "ORDER BY CASE WHEN status IN ('NEW','ACK_UNKNOWN') THEN 0 ELSE 1 END,"
                    "as_of DESC,version DESC LIMIT 1"
                ),
                {
                    "id": position_id,
                    "prefix": prefix + "%",
                    "initial": initial_key,
                    "intent": str(intent.intent_id),
                },
            ).first()
            prior_reserved = bool(
                c.scalar(
                    text(
                        "SELECT 1 FROM audit_log "
                        "WHERE event_type='execution.stop_reserved.v1' "
                        "AND payload->>'intent_id'=:intent AND payload->>'client_id'=:key LIMIT 1"
                    ),
                    {"intent": str(intent.intent_id), "key": initial_key},
                )
            )
        key = row[0] if row else initial_key
        body = self.client.request("GET", "/fapi/v1/algoOrder", {"clientAlgoId": key})
        if not matches(body, position, price, kind, key):
            if row is None and prior_reserved:
                self._reserve(position_id, position, intent, key, price, kind, at)
            terminal = body.get("algoStatus") if isinstance(body, dict) else None
            if body is None and row is not None and row.status == "REJECTED":
                terminal = "REJECTED"
            known = row is not None or prior_reserved
            if known and terminal not in {"CANCELED", "EXPIRED", "REJECTED"}:
                raise EntryHalt("PROTECTION_ACK_UNRESOLVED")
            # A terminal cancellation proves a replacement can be reserved. An
            # unknown reservation never authorizes another POST, even after restart.
            if known:
                assert isinstance(terminal, str)
                self._status(key, terminal, at)
                key = prefix + uuid4().hex
            below = (position.amount > 0) == (kind == "STOP_MARKET")
            if not (price < position.mark if below else price > position.mark):
                raise EntryHalt("PROTECTION_TRIGGER_CROSSED")
            self._reserve(position_id, position, intent, key, price, kind, at)
            params = {
                "clientAlgoId": key,
                "algoType": "CONDITIONAL",
                "symbol": position.symbol,
                "side": "SELL" if position.amount > 0 else "BUY",
                "positionSide": "BOTH",
                "type": kind,
                "triggerPrice": format(price, "f"),
                "closePosition": "true",
                "workingType": "MARK_PRICE",
            }
            try:
                self.client.request("POST", "/fapi/v1/algoOrder", params)
            except ExchangeRejected:
                # A definitive rejection has no possible accepted side effect.
                # Keep it distinct from a lost ACK so flat recovery can retire it.
                self._status(key, "REJECTED", at)
                with self.db.transaction() as c:
                    audit(c, "protection.rejected.v1", {"client_id": key})
            except AckUnknown:
                pass  # Acceptance may have preceded transport loss; query, never resend.
            body = self.client.request("GET", "/fapi/v1/algoOrder", {"clientAlgoId": key})
        if not matches(body, position, price, kind, key):
            raise EntryHalt("PROTECTION_UNAVAILABLE")
        assert isinstance(body, dict)
        with self.db.transaction() as c:
            # Adopt a valid protective order from PHASE_14 without resubmitting it.
            c.execute(
                text(
                    "INSERT INTO protective_orders "
                    "(protective_id,position_id,client_order_id,qty,stop_price,status,as_of,"
                    "exchange_order_id) "
                    "VALUES (:id,:position,:key,:qty,:price,'NEW',:at,:exchange) "
                    "ON CONFLICT (client_order_id) DO UPDATE SET qty=:qty,status='NEW',"
                    "exchange_order_id=:exchange,version=protective_orders.version+1,as_of=:at"
                ),
                {
                    "id": uuid4(),
                    "position": position_id,
                    "key": key,
                    "qty": abs(position.amount),
                    "price": price,
                    "at": at,
                    "exchange": str(body["algoId"]),
                },
            )
        return key

    def retire(self, active: set[UUID], at: datetime) -> None:
        with self.db.transaction() as c:
            saved = c.execute(
                text(
                    "SELECT position_id,client_order_id FROM protective_orders "
                    "WHERE status IN ('NEW','ACK_UNKNOWN')"
                )
            ).all()
        for position_id, key in saved:
            if position_id in active:
                continue
            body = self.client.request("GET", "/fapi/v1/algoOrder", {"clientAlgoId": key})
            if not isinstance(body, dict):
                raise EntryHalt("ORPHAN_PROTECTION_UNKNOWN")
            if body.get("algoStatus") == "NEW":
                self.client.request("DELETE", "/fapi/v1/algoOrder", {"clientAlgoId": key})
                body = self.client.request("GET", "/fapi/v1/algoOrder", {"clientAlgoId": key})
            if not isinstance(body, dict) or body.get("algoStatus") not in {
                "CANCELED",
                "EXPIRED",
                "REJECTED",
                "FINISHED",
                "TRIGGERED",
            }:
                raise EntryHalt("ORPHAN_PROTECTION_UNKNOWN")
            with self.db.transaction() as c:
                c.execute(
                    text(
                        "UPDATE protective_orders SET status=:status,version=version+1,"
                        "as_of=:at WHERE client_order_id=:key"
                    ),
                    {"status": body["algoStatus"], "at": at, "key": key},
                )

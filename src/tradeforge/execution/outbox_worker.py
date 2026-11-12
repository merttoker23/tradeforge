"""Durably execute before acknowledging an at-least-once intent delivery."""

from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy import text

from tradeforge.execution.order_manager import OrderManager
from tradeforge.execution.storage import audit
from tradeforge.persistence.db import Database, EntryHalt
from tradeforge.risk.storage import LOCK_ID


class OutboxWorker:
    def __init__(
        self,
        db: Database,
        manager: OrderManager,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.db, self.manager = db, manager
        self.clock = clock

    def reconcile_once(self) -> int:
        with self.db.transaction() as c:
            ids = (
                c.execute(
                    text(
                        "SELECT intent_id FROM orders o WHERE (status IN "
                        "('ACK_UNKNOWN','ACCEPTED','PARTIALLY_FILLED','UNKNOWN') OR filled_qty>0) "
                        "AND NOT EXISTS (SELECT 1 FROM audit_log a "
                        "WHERE a.event_type='protection.flat.v1' "
                        "AND a.payload->>'intent_id'=o.intent_id::text) "
                        "ORDER BY updated_at LIMIT 100"
                    )
                )
                .scalars()
                .all()
            )
        for identifier in ids:
            self.manager.execute(identifier)
        return len(ids)

    def drain(self, *, limit: int = 8) -> int:
        """Bounded dispatch of committed intents; never bypass manager guards."""
        if not 1 <= limit <= 100:
            raise ValueError('INVALID_OUTBOX_DRAIN_LIMIT')
        dispatched = 0
        for _ in range(limit):
            if not self.run_once():
                break
            dispatched += 1
        return dispatched

    def run_once(self) -> bool:
        with self.db.transaction() as c:
            c.execute(text("SELECT pg_advisory_xact_lock(:id)"), {"id": LOCK_ID})
            row = c.execute(
                text(
                    "SELECT message_id,intent_id,payload FROM outbox "
                    "WHERE delivered_at IS NULL ORDER BY created_at,message_id LIMIT 1"
                )
            )
            message = row.mappings().one_or_none()
            if message is None:
                return False
            client = c.scalar(
                text("SELECT client_order_id FROM execution_intents WHERE intent_id=:id"),
                {"id": message["intent_id"]},
            )
            if (
                message["payload"].get("intent_id") != str(message["intent_id"])
                or message["payload"].get("client_order_id") != client
            ):
                raise EntryHalt("OUTBOX_INTENT_MISMATCH")
            expired = c.scalar(
                text("SELECT expires_at<=:at FROM execution_intents WHERE intent_id=:id"),
                {"id": message["intent_id"], "at": self.clock()},
            )
            reserved = c.scalar(
                text("SELECT 1 FROM orders WHERE intent_id=:id"), {"id": message["intent_id"]}
            )
            if expired and not reserved:
                # No reservation means no exchange side effect was authorized. An
                # expired reserved order MUST still be looked up, never discarded.
                audit(c, "execution.intent_expired.v1", {"intent_id": str(message["intent_id"])})
                c.execute(
                    text(
                        "UPDATE outbox SET delivered_at=CURRENT_TIMESTAMP,version=version+1 "
                        "WHERE message_id=:id AND delivered_at IS NULL"
                    ),
                    {"id": message["message_id"]},
                )
                return True
        self.manager.execute(message["intent_id"])
        with self.db.transaction() as c:
            c.execute(
                text(
                    "UPDATE outbox SET delivered_at=CURRENT_TIMESTAMP,version=version+1 "
                    "WHERE message_id=:id AND delivered_at IS NULL"
                ),
                {"id": message["message_id"]},
            )
        return True

"""At-least-once transport and transaction-scoped consumer deduplication.

Publishers must enforce their own network timeout and return only after durable
acknowledgement. Never perform an exchange order as a consumer database effect.
"""

import json
from collections.abc import Callable
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection

from tradeforge.persistence import models as m
from tradeforge.persistence.db import Database


@dataclass(frozen=True)
class Message:
    message_id: UUID
    intent_id: UUID
    topic: str
    payload_json: str


def dispatch_one(db: Database, publish: Callable[[Message], None]) -> bool:
    """Lock one row, acknowledge only after publish; exceptions leave it pending.

    A crash after publish and before commit causes redelivery. Locks have bounded
    database timeouts; the transport callback must also be bounded by its adapter.
    """
    with db.transaction() as connection:
        row = (
            connection.execute(
                select(m.outbox)
                .where(
                    m.outbox.c.delivered_at.is_(None),
                )
                .order_by(m.outbox.c.created_at, m.outbox.c.message_id)
                .limit(1)
                # NO KEY UPDATE still excludes competing dispatchers, but permits
                # the consumer receipt's FK KEY SHARE before durable ACK returns.
                .with_for_update(skip_locked=True, key_share=True)
            )
            .mappings()
            .first()
        )
        if row is None:
            return False
        message = Message(
            row["message_id"],
            row["intent_id"],
            row["topic"],
            json.dumps(row["payload"], sort_keys=True, separators=(",", ":")),
        )
        publish(message)
        connection.execute(
            update(m.outbox)
            .where(
                m.outbox.c.message_id == message.message_id,
            )
            .values(delivered_at=func.now(), version=row["version"] + 1)
        )
        return True


def consume_once(
    connection: Connection,
    consumer: str,
    message: Message,
    effect: Callable[[Connection, Message], None],
) -> bool:
    """Receipt and database effect MUST share the caller's transaction."""
    if not consumer or len(consumer) > 256:
        raise ValueError("INVALID_CONSUMER")
    if not connection.in_transaction():
        raise ValueError("TRANSACTION_REQUIRED")
    # A caller may catch the effect's exception and still commit its outer work.
    # Keep this receipt and its effect atomic even in that case.
    with connection.begin_nested():
        receipt = connection.execute(
            insert(m.consumer_receipts)
            .values(
                consumer=consumer,
                message_id=message.message_id,
            )
            .on_conflict_do_nothing()
            .returning(m.consumer_receipts.c.message_id)
        ).first()
        if receipt is None:
            return False
        effect(connection, message)
    return True

"""Approved proposals are persisted atomically; this never sends exchange orders."""

import hashlib
import json
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from sqlalchemy import func, insert, select, update
from sqlalchemy.engine import Connection

from tradeforge.domain.decision import Decision, DecisionState
from tradeforge.domain.execution import ExecutionIntent, OrderStatus
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.persistence import models as m
from tradeforge.persistence.db import Database, EntryHalt


def instrument_id(key: InstrumentKey) -> UUID:
    return uuid5(NAMESPACE_URL, "tradeforge/" + "/".join(key.key))


def add_instrument(connection: Connection, key: InstrumentKey) -> UUID:
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    identifier = instrument_id(key)
    connection.execute(
        pg_insert(m.instruments)
        .values(
            instrument_id=identifier,
            exchange=key.exchange,
            market_type=key.market_type,
            symbol=key.symbol,
            contract_type=key.contract_type,
            observed_at=datetime.now(UTC),
        )
        .on_conflict_do_nothing(index_elements=[m.instruments.c.instrument_id])
    )
    return identifier


def add_risk_snapshot(
    connection: Connection,
    snapshot_id: UUID,
    payload: dict[str, object],
    as_of: datetime,
) -> None:
    from tradeforge.domain.common import utc

    connection.execute(
        insert(m.risk_snapshots).values(
            snapshot_id=snapshot_id,
            payload=payload,
            as_of=utc(as_of),
        )
    )


def save_decision(connection: Connection, decision: Decision) -> str:
    # Canonical JSON avoids hash changes due to dict ordering or serialization whitespace.
    snapshot = json.dumps(
        decision.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    digest = hashlib.sha256(snapshot.encode("utf-8")).hexdigest()
    connection.execute(
        insert(m.decisions).values(
            decision_id=decision.decision_id,
            instrument_id=instrument_id(decision),
            side=decision.side.value,
            state=decision.state.value,
            observed_at=decision.observed_at,
            expires_at=decision.expires_at,
            snapshot=snapshot,
            snapshot_hash=digest,
        )
    )
    connection.execute(
        insert(m.decision_reasons),
        [
            {"decision_id": decision.decision_id, "reason_code": code.value}
            for code in decision.reason_codes
        ],
    )
    connection.execute(
        insert(m.audit_log).values(
            event_type="decision_saved",
            payload={
                "decision_id": str(decision.decision_id),
                "snapshot_hash": digest,
            },
        )
    )
    return digest


def enqueue_intent(db: Database, intent: ExecutionIntent) -> UUID:
    """No retry: uncertain commit results require lookup by immutable identity."""
    if not db.probe().db_writable:
        raise EntryHalt("DB_UNAVAILABLE")
    with db.transaction() as connection:
        row = (
            connection.execute(
                select(m.decisions).where(
                    m.decisions.c.decision_id == intent.decision_id,
                )
            )
            .mappings()
            .one()
        )
        if (
            row["state"] != DecisionState.APPROVED.value
            or row["side"] != intent.side.value
            or row["instrument_id"] != instrument_id(intent)
            or row["expires_at"] < intent.expires_at
            or intent.expires_at <= datetime.now(UTC)
        ):
            raise ValueError("INTENT_DECISION_MISMATCH_OR_EXPIRED")
        connection.execute(
            insert(m.execution_intents).values(
                intent_id=intent.intent_id,
                client_order_id=intent.client_order_id,
                decision_id=intent.decision_id,
                instrument_id=instrument_id(intent),
                risk_snapshot_id=intent.risk_snapshot_id,
                side=intent.side.value,
                qty=intent.qty,
                price=intent.price,
                created_at=intent.created_at,
                expires_at=intent.expires_at,
                snapshot=intent.model_dump(mode="json"),
            )
        )
        message_id = uuid4()
        connection.execute(
            insert(m.outbox).values(
                message_id=message_id,
                intent_id=intent.intent_id,
                topic="execution.intent",
                payload={
                    "intent_id": str(intent.intent_id),
                    "client_order_id": intent.client_order_id,
                    "decision_hash": row["snapshot_hash"],
                },
            )
        )
        connection.execute(
            insert(m.audit_log).values(
                event_type="intent_queued",
                payload={
                    "intent_id": str(intent.intent_id),
                    "message_id": str(message_id),
                },
            )
        )
    return message_id


def advance_order(db: Database, intent_id: UUID, expected_version: int, status: str) -> bool:
    """Storage CAS only; exchange order lifecycle policy belongs to PHASE_14."""
    OrderStatus(status)
    if type(expected_version) is not int or expected_version < 0:
        raise ValueError("INVALID_EXPECTED_VERSION")
    with db.transaction() as connection:
        result = connection.execute(
            update(m.orders)
            .where(
                m.orders.c.intent_id == intent_id,
                m.orders.c.version == expected_version,
            )
            .values(status=status, version=expected_version + 1, updated_at=func.now())
        )
        return result.rowcount == 1

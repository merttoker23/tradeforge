"""Single durable analyst authority across workers; no order/outbox write path."""

from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import text
from sqlalchemy.engine import Connection

from tradeforge.decision.edge import EdgeStore
from tradeforge.decision.policy import DecisionPolicy, approval_reasons
from tradeforge.decision.provenance import Discovery
from tradeforge.decision.selector import select_candidates
from tradeforge.decision.state_machine import Command, Journal, Record
from tradeforge.domain.common import utc
from tradeforge.domain.decision import DecisionState
from tradeforge.intelligence.models import digest
from tradeforge.market.bars import canonical
from tradeforge.persistence.db import Database


class DecisionService:
    def __init__(self, db: Database, *, policy: DecisionPolicy | None = None) -> None:
        self.db = db
        self.policy = policy or DecisionPolicy()

    def _load(self, connection: Connection, as_of: datetime | None = None) -> Journal:
        rows = connection.execute(
            text(
                "SELECT * FROM decision_lifecycle "
                "WHERE (CAST(:at AS timestamptz) IS NULL OR at <= :at) "
                "ORDER BY sequence LIMIT :limit"
            ),
            {"at": as_of, "limit": self.policy.max_records + 1},
        ).mappings()
        records: list[Record] = []
        for row in rows:
            if len(records) >= self.policy.max_records:
                raise ValueError("JOURNAL_CAPACITY")
            record = Record.model_validate_json(row["payload"], context={"now": row["at"]})
            if (
                record.digest != row["record_hash"]
                or record.sequence != row["sequence"]
                or record.command.event_id != row["event_id"]
                or record.decision_id != row["decision_id"]
                or record.version != row["version"]
                or record.state != row["state"]
                or record.evaluated_at != row["at"]
                or record.previous_hash != row["previous_hash"]
            ):
                raise ValueError("CORRUPT_DECISION_JOURNAL")
            records.append(record)
        return Journal.replay(tuple(records), policy=self.policy)

    def load(self, *, as_of: datetime) -> Journal:
        utc(as_of)
        with self.db.transaction() as connection:
            return self._load(connection, as_of)

    @staticmethod
    def _append(connection: Connection, record: Record) -> None:
        evidence = record.command.evidence
        if evidence is not None and evidence.edge is not None:
            EdgeStore._save(connection, "edge.analysis.v1", evidence.edge.digest, evidence.edge)
        connection.execute(
            text("""
            INSERT INTO decision_lifecycle
                (sequence,event_id,decision_id,version,state,at,previous_hash,record_hash,payload)
            VALUES (:seq,:event,:id,:version,:state,:at,:previous,:hash,:payload)
        """),
            {
                "seq": record.sequence,
                "event": record.command.event_id,
                "id": record.decision_id,
                "version": record.version,
                "state": record.state.value,
                "at": record.evaluated_at,
                "previous": record.previous_hash,
                "hash": record.digest,
                "payload": canonical(record.model_dump(mode="json")),
            },
        )

    def submit(self, command: Command, *, now: datetime | None = None) -> Record:
        if now is not None:
            utc(now)
        with self.db.transaction() as connection:
            connection.execute(text("SELECT pg_advisory_xact_lock(74202010)"))
            journal = self._load(connection)
            record = journal.apply(command, now=now or datetime.now(UTC))
            self._append(connection, record)
            self._publication_check(record, now=now or datetime.now(UTC))
        # Never expose a result before the transaction commits successfully.
        return record

    def _publication_check(self, record: Record, *, now: datetime) -> None:
        if record.state not in {
            DecisionState.DISCOVERED,
            DecisionState.WATCHING,
            DecisionState.ARMED,
            DecisionState.VALIDATING,
            DecisionState.APPROVED,
        }:
            return
        if record.discovery.candidate.expires_at <= now:
            raise ValueError("EXPIRED_AT_PUBLICATION")
        if record.state is DecisionState.APPROVED and approval_reasons(
            record.discovery, record.command.evidence, now, self.policy
        ):
            raise ValueError("STALE_AT_PUBLICATION")

    def discover_batch(
        self, candidates: tuple[Discovery, ...], *, at: datetime, now: datetime | None = None
    ) -> tuple[Record, ...]:
        if now is not None:
            utc(now)
        ordered = select_candidates(candidates, at=at, policy=self.policy)
        peers = tuple(x.discovery for x in ordered)
        records = []
        with self.db.transaction() as connection:
            connection.execute(text("SELECT pg_advisory_xact_lock(74202010)"))
            journal = self._load(connection)
            for item in ordered:
                d = item.discovery
                command = Command(
                    event_id=uuid5(NAMESPACE_URL, digest((d.digest, at.isoformat(), "discovery"))),
                    decision_id=d.decision_id,
                    at=at,
                    target=DecisionState.DISCOVERED,
                    discovery=d,
                    selection_peers=peers,
                )
                record = journal.apply(command, now=now or datetime.now(UTC))
                self._append(connection, record)
                records.append(record)
            for record in records:
                self._publication_check(record, now=now or datetime.now(UTC))
        return tuple(records)

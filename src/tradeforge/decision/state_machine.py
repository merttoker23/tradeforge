"""Deterministic CAS reducer. Approval is terminal here and never grants order authority."""

from datetime import datetime, timedelta
from threading import RLock
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import BeforeValidator, Field, model_validator

from tradeforge.decision.policy import DecisionPolicy, approval_reasons
from tradeforge.decision.provenance import Discovery, ValidationEvidence
from tradeforge.decision.reasons import DecisionReason as R
from tradeforge.decision.selector import overlap, select_candidates
from tradeforge.domain.common import Count, Digest, StrictModel, Timestamp, exact_bool, utc
from tradeforge.domain.decision import DecisionState as S
from tradeforge.intelligence.models import digest

TERMINAL = frozenset({S.APPROVED, S.REJECTED, S.EXPIRED, S.NO_TRADE, S.CANCELLED})
NEXT = {
    S.DISCOVERED: S.WATCHING,
    S.WATCHING: S.ARMED,
    S.ARMED: S.VALIDATING,
    S.VALIDATING: S.APPROVED,
}
EXITS = frozenset({S.REJECTED, S.EXPIRED, S.NO_TRADE, S.CANCELLED})
GENESIS = "0" * 64


class Command(StrictModel):
    event_id: UUID
    decision_id: UUID
    at: Timestamp
    target: S
    expected_previous_state: S | None = None
    expected_previous_version: Count | None = None
    discovery: Discovery | None = None
    evidence: ValidationEvidence | None = None
    selection_peers: Annotated[tuple[Discovery, ...], Field(max_length=128)] = ()

    @model_validator(mode="after")
    def shape(self) -> Self:
        if (
            self.evidence is not None
            and self.evidence.proposal is not None
            and self.evidence.proposal.as_of > self.at
        ):
            raise ValueError("FUTURE_PROPOSAL")
        if self.discovery is not None:
            if (
                self.target is not S.DISCOVERED
                or self.expected_previous_state is not None
                or self.expected_previous_version is not None
                or self.evidence is not None
                or self.decision_id != self.discovery.decision_id
            ):
                raise ValueError("INVALID_DISCOVERY_COMMAND")
            if self.selection_peers and self.discovery not in self.selection_peers:
                raise ValueError("CANDIDATE_MISSING_FROM_BATCH")
        elif (
            self.expected_previous_state is None
            or self.expected_previous_version is None
            or self.selection_peers
        ):
            raise ValueError("CAS_REQUIRED")
        return self


class Record(StrictModel):
    sequence: Count
    command: Command
    evaluated_at: Timestamp
    discovery: Discovery
    policy: DecisionPolicy
    previous_hash: Digest
    state: S
    version: Count
    reason_codes: Annotated[tuple[R, ...], Field(min_length=1)]
    trading_allowed: Annotated[Literal[False], BeforeValidator(exact_bool)] = False

    @property
    def decision_id(self) -> UUID:
        return self.command.decision_id

    @property
    def digest(self) -> str:
        return digest(self.model_dump(mode="json"))


class Journal:
    """In-memory reducer only; production publication uses DecisionService's durable transaction."""

    def __init__(self, *, policy: DecisionPolicy | None = None) -> None:
        self.policy = policy or DecisionPolicy()
        self._records: tuple[Record, ...] = ()
        self._latest: dict[UUID, Record] = {}
        self._events: set[UUID] = set()
        self._lock = RLock()

    @property
    def records(self) -> tuple[Record, ...]:
        return self._records

    @property
    def head_hash(self) -> str:
        return self._records[-1].digest if self._records else GENESIS

    def _overlap(self, d: Discovery, at: datetime) -> tuple[R, ...]:
        for row in sorted(self._latest.values(), key=lambda r: str(r.decision_id)):
            if row.decision_id == d.decision_id:
                continue
            conflict = overlap(d, row.discovery)
            if conflict is None:
                continue
            # Rejected discoveries never acquired a reservation and cannot extend cooldown.
            if row.version == 0 and row.state in TERMINAL:
                continue
            expiry = row.discovery.candidate.expires_at
            end = (
                expiry if row.state not in TERMINAL or row.state is S.APPROVED else row.evaluated_at
            )
            if row.state not in TERMINAL and at < expiry:
                return (conflict,)
            if row.state is S.APPROVED and at < expiry:
                return (conflict,)
            if at < end + timedelta(seconds=self.policy.cooldown_seconds):
                return (R.COOLDOWN,)
        return ()

    def apply(self, command: Command, *, now: datetime) -> Record:
        with self._lock:
            return self._apply(command, now=now)

    def _apply(self, command: Command, *, now: datetime) -> Record:
        utc(now)
        if command.event_id in self._events:
            raise ValueError("DUPLICATE_EVENT")
        old = self._latest.get(command.decision_id)
        if old is not None and command.discovery is not None:
            raise ValueError("DUPLICATE_CANDIDATE")
        if old is not None and old.state in TERMINAL:
            raise ValueError("TERMINAL_STATE")
        if command.at > now:
            raise ValueError("FUTURE_COMMAND")
        if (now - command.at).total_seconds() > self.policy.evidence_age_seconds:
            raise ValueError("STALE_COMMAND")
        if self._records and (
            command.at < self._records[-1].command.at or now < self._records[-1].evaluated_at
        ):
            raise ValueError("OUT_OF_ORDER_EVENT")
        if len(self._records) >= self.policy.max_records:
            raise ValueError("JOURNAL_CAPACITY")
        if command.discovery is not None:
            d = command.discovery
            if command.at < d.candidate.expires_at <= now:
                raise ValueError("EXPIRED_AT_PROCESSING")
            if d.available_at > command.at:
                raise ValueError("FUTURE_DISCOVERY")
            if (
                d.candidate.expires_at - d.candidate.observed_at
            ).total_seconds() > self.policy.max_ttl_seconds:
                raise ValueError("TTL_EXCEEDS_POLICY")
            batch = select_candidates(command.selection_peers or (d,), at=now, policy=self.policy)
            reasons = next(x.reason_codes for x in batch if x.discovery == d)
            reasons = reasons or self._overlap(d, now)
            state = S.EXPIRED if R.EXPIRED in reasons else S.NO_TRADE if reasons else S.DISCOVERED
            reasons = reasons or (R.DISCOVERED,)
            version = 0
        else:
            if (
                old is None
                or command.expected_previous_state is not old.state
                or command.expected_previous_version != old.version
            ):
                raise ValueError("CAS_CONFLICT")
            d, version = old.discovery, old.version + 1
            if command.at < d.candidate.expires_at <= now:
                raise ValueError("EXPIRED_AT_PROCESSING")
            if command.target != NEXT.get(old.state) and command.target not in EXITS:
                raise ValueError("INVALID_TRANSITION")
            state, reasons = command.target, (R.STATE_ADVANCED,)
            if d.candidate.expires_at <= command.at:
                state, reasons = S.EXPIRED, (R.EXPIRED,)
            elif state is S.EXPIRED:
                raise ValueError("PREMATURE_EXPIRY")
            elif state is S.APPROVED:
                reasons = approval_reasons(d, command.evidence, now, self.policy)
                reasons = tuple(dict.fromkeys((*reasons, *self._overlap(d, now))))
                state = S.NO_TRADE if reasons else S.APPROVED
                trial = command.evidence is not None and command.evidence.trial is not None
                reasons = reasons or (
                    R.TESTNET_TRIAL_AUTHORIZED if trial else R.CANDIDATE_VALIDATED,
                )
            elif state in EXITS:
                reasons = (R(state.value),)
        record = Record(
            sequence=len(self._records),
            command=command,
            evaluated_at=now,
            discovery=d,
            policy=self.policy,
            previous_hash=self.head_hash,
            state=state,
            version=version,
            reason_codes=reasons,
        )
        self._records += (record,)
        self._latest[command.decision_id] = record
        self._events.add(command.event_id)
        return record

    @classmethod
    def replay(cls, records: tuple[Record, ...], *, policy: DecisionPolicy | None = None) -> Self:
        journal = cls(policy=policy or (records[0].policy if records else None))
        for record in records:
            if record.policy != journal.policy:
                raise ValueError("REPLAY_POLICY_MISMATCH")
            rebuilt = journal.apply(record.command, now=record.evaluated_at)
            if rebuilt != record:
                raise ValueError("REPLAY_RECORD_MISMATCH")
        return journal

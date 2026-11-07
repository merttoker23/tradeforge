"""Conservative net-edge gate. Analysis eligibility never grants live order authority."""

from datetime import datetime
from decimal import Decimal
from typing import Literal

from sqlalchemy import text
from sqlalchemy.engine import Connection

from tradeforge.config.policy import RuntimeMode
from tradeforge.decision.calibration import EdgePolicy, calibration_reasons, conservative_ev
from tradeforge.decision.cost_model import CostEstimate, CostInput, estimate_cost
from tradeforge.decision.stats import MAX_SAMPLES, Bucket, Summary, TradeOutcome, summarize
from tradeforge.domain.common import Digest, StrictModel, Text, Timestamp
from tradeforge.domain.decision import ProofState, Side
from tradeforge.intelligence.models import digest
from tradeforge.market.bars import canonical
from tradeforge.persistence.db import Database


class EdgeAnalysis(StrictModel):
    version: Literal[1] = 1
    as_of: Timestamp
    bucket: Bucket
    feature_hash: Digest
    regime_version: Text
    mode: RuntimeMode = RuntimeMode.PAPER
    cost_input: CostInput
    policy: EdgePolicy
    train: Summary
    oos: Summary

    @property
    def costs(self) -> CostEstimate:
        return estimate_cost(self.cost_input, at=self.as_of)

    @property
    def net_ev_low_R(self) -> Decimal | None:
        train = conservative_ev(self.train, self.costs.stress_R)
        oos = conservative_ev(self.oos, self.costs.stress_R)
        if train is None or oos is None:
            return None
        return min(train, oos)

    @property
    def calibration_label(self) -> Literal["calibrated", "uncalibrated"]:
        return (
            "uncalibrated"
            if calibration_reasons(
                self.train,
                self.oos,
                at=self.as_of,
                policy=self.policy,
                stress_cost_R=self.costs.stress_R,
            )
            else "calibrated"
        )

    @property
    def reason_codes(self) -> tuple[str, ...]:
        reasons = list(self.costs.reason_codes)
        reasons.extend(
            calibration_reasons(
                self.train,
                self.oos,
                at=self.as_of,
                policy=self.policy,
                stress_cost_R=self.costs.stress_R,
            )
        )
        edge = self.net_ev_low_R
        if edge is None:
            reasons.append("EDGE_UNKNOWN")
        elif edge <= self.policy.min_net_R:
            reasons.append("EDGE_BELOW_POLICY")
        if self.mode in {RuntimeMode.LIVE, RuntimeMode.DISABLED}:
            reasons.append("LIVE_LOCKED" if self.mode is RuntimeMode.LIVE else "DISABLED")
        if self.bucket.environment != self.mode.value:
            # Replay results cannot silently qualify a paper/testnet/live environment.
            reasons.append("EVIDENCE_ENVIRONMENT_MISMATCH")
        return tuple(dict.fromkeys(reasons))

    @property
    def proof_state(self) -> ProofState:
        return ProofState.UNPROVEN if self.reason_codes else ProofState.VALIDATED

    @property
    def side(self) -> Side:
        return Side.NO_TRADE if self.reason_codes else self.bucket.side

    @property
    def live_approval(self) -> Literal[False]:
        return False

    @property
    def confidence(self) -> None:
        return None

    @property
    def digest(self) -> str:
        return digest(self.model_dump(mode="json"))


def analyze_edge(
    rows: tuple[TradeOutcome, ...],
    *,
    bucket: Bucket,
    cost: CostInput,
    at: datetime,
    feature_hash: str,
    regime_version: str,
    policy: EdgePolicy | None = None,
    mode: RuntimeMode = RuntimeMode.PAPER,
) -> EdgeAnalysis:
    if len(rows) > MAX_SAMPLES or len({r.trade_id for r in rows}) != len(rows):
        raise ValueError("DUPLICATE_OR_EXCESS_OUTCOMES")
    policy = policy or EdgePolicy()
    rows = tuple(
        r for r in rows if 0 <= (at - r.observed_at).total_seconds() <= policy.max_age_seconds
    )
    train = summarize(tuple(r for r in rows if r.split == "TRAIN"), at=at, bucket=bucket)
    oos = summarize(tuple(r for r in rows if r.split == "OOS"), at=at, bucket=bucket)
    return EdgeAnalysis(
        as_of=at,
        bucket=bucket,
        feature_hash=feature_hash,
        regime_version=regime_version,
        mode=mode,
        cost_input=cost,
        policy=policy,
        train=train,
        oos=oos,
    )


class EdgeStore:
    """Reuse the append-only audit store; immutable identities and hash-checked replay.

    Outcome and analysis are separate events. A lifecycle transaction records its analysis
    in the same transaction, so a failed commit cannot publish an approved decision.
    """

    def __init__(self, db: Database) -> None:
        self.db = db

    @staticmethod
    def _save(
        c: Connection, kind: str, identifier: str, model: TradeOutcome | EdgeAnalysis
    ) -> None:
        c.execute(text("SELECT pg_advisory_xact_lock(74202011)"))
        body = model.model_dump(mode="json")
        payload = {"id": identifier, "hash": model.digest, "body": body}
        stored = c.scalar(
            text("SELECT payload FROM audit_log WHERE event_type=:kind AND payload->>'id'=:id"),
            {"kind": kind, "id": identifier},
        )
        if stored is not None:
            if stored != payload:
                raise ValueError("CONFLICTING_EDGE_EVIDENCE")
            return
        c.execute(
            text(
                "INSERT INTO audit_log(event_type,payload) VALUES (:kind,CAST(:payload AS jsonb))"
            ),
            {"kind": kind, "payload": canonical(payload)},
        )

    def save_outcome(self, row: TradeOutcome, *, connection: Connection | None = None) -> None:
        if connection is not None:
            self._save(connection, "edge.outcome.v1", str(row.trade_id), row)
            return
        with self.db.transaction() as c:
            self._save(c, "edge.outcome.v1", str(row.trade_id), row)

    def save_analysis(self, row: EdgeAnalysis) -> None:
        with self.db.transaction() as c:
            self._save(c, "edge.analysis.v1", row.digest, row)

    def load_outcomes(self, *, at: datetime) -> tuple[TradeOutcome, ...]:
        from tradeforge.domain.common import utc

        utc(at)
        with self.db.transaction() as c:
            payloads = tuple(
                c.scalars(
                    text(
                        "SELECT payload FROM audit_log WHERE "
                        "event_type='edge.outcome.v1' ORDER BY payload->>'id' LIMIT :limit"
                    ),
                    {"limit": MAX_SAMPLES + 1},
                )
            )
        if len(payloads) > MAX_SAMPLES:
            raise ValueError("SAMPLE_CAPACITY")
        results = []
        for p in payloads:
            row = TradeOutcome.model_validate_json(canonical(p["body"]))
            if p["hash"] != row.digest or p["id"] != str(row.trade_id):
                raise ValueError("CORRUPT_EDGE_EVIDENCE")
            if row.observed_at <= at:
                results.append(row)
        return tuple(results)

    def load_analysis(self, version: str, *, at: datetime) -> EdgeAnalysis | None:
        from tradeforge.domain.common import utc

        utc(at)
        with self.db.transaction() as c:
            p = c.scalar(
                text(
                    "SELECT payload FROM audit_log WHERE "
                    "event_type='edge.analysis.v1' AND payload->>'id'=:id"
                ),
                {"id": version},
            )
        if p is None:
            return None
        row = EdgeAnalysis.model_validate_json(canonical(p["body"]))
        if p["hash"] != row.digest or p["id"] != row.digest:
            raise ValueError("CORRUPT_EDGE_EVIDENCE")
        return row if row.as_of <= at else None

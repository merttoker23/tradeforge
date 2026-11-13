"""Fresh Demo facts and measured edge are required before any autonomous intent."""

from datetime import timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy import text

from tradeforge.alpha.base import MomentumInput
from tradeforge.alpha.reversal import ReversalInput
from tradeforge.config.policy import RuntimeMode
from tradeforge.decision.bootstrap import bootstrap_diagnostics
from tradeforge.decision.cost_model import CostInput, Friction
from tradeforge.decision.edge import EdgeStore, analyze_edge
from tradeforge.decision.policy import DecisionPolicy
from tradeforge.decision.provenance import ValidationEvidence
from tradeforge.decision.state_machine import Command, Record
from tradeforge.decision.stats import Bucket
from tradeforge.decision.storage import DecisionService
from tradeforge.decision.trial import TestnetTrialPolicy
from tradeforge.domain.common import DataQuality, ReasonCode
from tradeforge.domain.decision import Decision, DecisionState, ProofState, Side
from tradeforge.domain.market import MarketSnapshot, OrderBookSnapshot, StreamState
from tradeforge.execution.binance_client import BinanceTestnetClient, number
from tradeforge.execution.broker import AckUnknown, ExecutionContext
from tradeforge.execution.context import save_context, verify_exchange_context
from tradeforge.execution.storage import audit
from tradeforge.intelligence.models import digest
from tradeforge.market.bars import canonical
from tradeforge.market.exchange_info import milliseconds, parse_exchange_info
from tradeforge.persistence.db import Database, EntryHalt
from tradeforge.risk.models import AccountSnapshot, EntryRequest, RiskFactor
from tradeforge.risk.storage import RiskService
from tradeforge.runtime.trading import publish_approved


def demo_symbol_config(client: BinanceTestnetClient, symbol: str) -> dict[str, Any]:
    if getattr(client, "environment", None) != "testnet":
        raise EntryHalt("DEMO_ONLY")

    def read() -> dict[str, Any]:
        rows = client.request("GET", "/fapi/v1/symbolConfig", {"symbol": symbol})
        if (
            not isinstance(rows, list)
            or len(rows) != 1
            or not isinstance(rows[0], dict)
            or rows[0].get("symbol") != symbol
        ):
            raise EntryHalt("DEMO_CONFIG_UNVERIFIED")
        return rows[0]

    config = read()
    if config.get("marginType") != "ISOLATED" or config.get("leverage") != 1:
        account = client.request("GET", "/fapi/v3/account")
        if (
            not isinstance(account, dict)
            or not isinstance(account.get("positions"), list)
            or any(
                not isinstance(p, dict) or number(p.get("positionAmt")) != 0
                for p in account["positions"]
            )
        ):
            raise EntryHalt("FLAT_ACCOUNT_REQUIRED")
        if (
            client.request("GET", "/fapi/v1/openOrders") != []
            or client.request("GET", "/fapi/v1/openAlgoOrders") != []
        ):
            raise EntryHalt("EXISTING_ORDERS_REQUIRE_RECONCILIATION")
        try:
            if config.get("marginType") != "ISOLATED":
                client.request(
                    "POST", "/fapi/v1/marginType", {"symbol": symbol, "marginType": "ISOLATED"}
                )
            if config.get("leverage") != 1:
                client.request("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": "1"})
        except AckUnknown:
            pass  # Query assigned state; never infer successful configuration from an ACK.
        config = read()
    if (
        config.get("marginType") != "ISOLATED"
        or type(config.get("leverage")) is not int
        or config["leverage"] != 1
        or config.get("isAutoAddMargin") is not False
    ):
        raise EntryHalt("DEMO_CONFIG_UNVERIFIED")
    return config


def flat_account(client: BinanceTestnetClient, sample: MomentumInput) -> AccountSnapshot:
    """Bounded complete UTC-day income; transfers never masquerade as trading profit."""
    at = client.clock()
    body = client.request("GET", "/fapi/v3/account")
    if (
        not isinstance(body, dict)
        or not isinstance(body.get("positions"), list)
        or any(number(p["positionAmt"]) for p in body["positions"])
    ):
        raise EntryHalt("FLAT_ACCOUNT_REQUIRED")
    income = client.request(
        "GET",
        "/fapi/v1/income",
        {
            "startTime": str(
                int(at.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
            ),
            "endTime": str(int(at.timestamp() * 1000)),
            "limit": "1000",
        },
    )
    if not isinstance(income, list) or len(income) >= 1000:
        raise EntryHalt("INCOME_HISTORY_INCOMPLETE")
    daily, transfers = Decimal(0), Decimal(0)
    start = at.replace(hour=0, minute=0, second=0, microsecond=0)
    for row in income:
        if (
            not isinstance(row, dict)
            or row.get("asset") != "USDT"
            or not start <= milliseconds(row["time"]) <= at
        ):
            raise EntryHalt("INCOME_HISTORY_INVALID")
        value = number(row["income"])
        if row.get("incomeType") in {"REALIZED_PNL", "COMMISSION", "FUNDING_FEE"}:
            daily += value
        elif row.get("incomeType") in {"TRANSFER", "INTERNAL_TRANSFER"}:
            transfers += value
        else:
            raise EntryHalt("INCOME_TYPE_UNSUPPORTED")
    feature = next((f for f in sample.features.features if f.instrument == sample.instrument), None)
    if feature is None:
        raise EntryHalt("RISK_FACTOR_UNAVAILABLE")
    equity = number(body["totalMarginBalance"])
    unrealized = number(body["totalUnrealizedProfit"])
    # Factor timestamps describe the actual feature observation, never this REST read.
    factor = RiskFactor(
        symbol=sample.instrument.symbol,
        beta=Decimal(str(feature.beta)) if feature.beta is not None else None,
        realized_vol=Decimal(str(feature.volatility * (365 * 24 * 4) ** 0.5))
        if feature.volatility > 0
        else None,
        as_of=sample.features.frame.as_of,
    )
    return AccountSnapshot(
        environment="testnet",
        snapshot_id=uuid4(),
        as_of=at,
        trading_day=at.date(),
        equity=equity,
        available_balance=number(body["availableBalance"]),
        day_start_equity=equity - unrealized - daily - transfers,
        realized_pnl_today=daily,
        unrealized_pnl=unrealized,
        positions_complete=True,
        protection_ready=True,
        factors=(factor,),
    )


ADMISSION_REASONS = frozenset(
    {
        "TESTNET_TRIAL_BUDGET_EXHAUSTED",
        "TESTNET_TRIAL_SETTLEMENT_REQUIRED",
        "TESTNET_TRIAL_LOSS_LIMIT",
        "OOS_EMBARGO_OR_UNSETTLED_TRAIN",
    }
)


class DemoDecisions:
    def __init__(
        self,
        db: Database,
        client: BinanceTestnetClient,
        *,
        trial_policy: TestnetTrialPolicy | None = None,
    ) -> None:
        self.db, self.client = db, client
        self.trial_policy = trial_policy or TestnetTrialPolicy()
        self._reason_counts: dict[str, int] = {}
        self._last_reason = "NOT_EVALUATED"
        self._candidate_count = 0
        self._approvals = 0
        self._triggered = 0
        self._risk_allowed = 0
        self._last_decision_id: str | None = None
        self._statistical_evidence: dict[str, object] = {}

    def _note(self, reason: str) -> None:
        self._last_reason = reason
        self._reason_counts[reason] = min(2**31 - 1, self._reason_counts.get(reason, 0) + 1)

    def status(self) -> dict[str, Any]:
        return {
            "autonomous_decisions": {
                "candidate_count": self._candidate_count,
                "approvals": self._approvals,
                "triggered": self._triggered,
                "risk_allowed": self._risk_allowed,
                "last_decision_id": self._last_decision_id,
                "last_reason": self._last_reason,
                "reason_counts": dict(self._reason_counts),
                "trial_policy": self.trial_policy.model_dump(mode="json"),
                "statistical_evidence": dict(self._statistical_evidence),
            }
        }

    def _advance(
        self, record: Record, state: DecisionState, evidence: ValidationEvidence | None = None
    ) -> Record:
        at = self.client.clock()
        return DecisionService(self.db).submit(
            Command(
                event_id=uuid4(),
                decision_id=record.decision_id,
                at=at,
                target=state,
                expected_previous_state=record.state,
                expected_previous_version=record.version,
                evidence=evidence,
            ),
            now=at,
        )

    def tick(
        self,
        feed: dict[str, Any],
        *,
        symbols: frozenset[str] | None = None,
        pending: tuple[Record, ...] | None = None,
    ) -> None:
        try:
            self._tick(feed, symbols=symbols, pending=pending)
        except EntryHalt as error:
            reason = str(error)
            self._note(reason if reason in ADMISSION_REASONS else "DEMO_ADMISSION_HALTED")
            raise

    def pending(self, symbols: frozenset[str] | None) -> tuple[Record, ...]:
        if symbols == frozenset():
            return ()
        query = (
            "SELECT payload FROM (SELECT DISTINCT ON(decision_id) * "
            "FROM decision_lifecycle ORDER BY decision_id,version DESC) latest "
            "WHERE state IN ('ARMED','VALIDATING')"
        )
        parameters: dict[str, object] = {}
        if symbols is not None:
            query += (
                " AND CAST(payload AS jsonb) #>> '{discovery,candidate,instrument,symbol}'"
                " = ANY(:symbols)"
            )
            parameters["symbols"] = sorted(symbols)
        query += " ORDER BY at LIMIT 512"
        with self.db.transaction() as c:
            rows = c.execute(text(query), parameters).scalars().all()
        return tuple(Record.model_validate_json(payload) for payload in rows)

    def _tick(
        self,
        feed: dict[str, Any],
        *,
        symbols: frozenset[str] | None = None,
        pending: tuple[Record, ...] | None = None,
    ) -> None:
        at = self.client.clock()
        if symbols == frozenset():
            self._candidate_count = 0
            self._note("NO_CHANGED_SYMBOLS")
            return
        records = self.pending(symbols) if pending is None else pending
        self._candidate_count = len(records)
        if not records:
            self._note("NO_ARMED_CANDIDATES")
        for record in records:
            candidate = record.discovery.candidate
            if symbols is not None and candidate.instrument.symbol not in symbols:
                continue
            if candidate.expires_at <= at:
                self._advance(record, DecisionState.EXPIRED)
                self._note("CANDIDATE_EXPIRED")
                continue
            if feed.get("status", {}).get("data_ready") is not True:
                self._note("DATA_NOT_READY")
                continue
            market_row = next(
                (r for r in feed["markets"] if r["symbol"] == candidate.instrument.symbol), None
            )
            micro = next(
                (r for r in feed["deep"] if r["symbol"] == candidate.instrument.symbol), None
            )
            if market_row is None:
                self._note("MARKET_EVIDENCE_MISSING")
                continue
            if micro is None:
                self._note("MICRO_EVIDENCE_MISSING")
                continue
            market = MarketSnapshot.model_validate_json(canonical(market_row))
            book = OrderBookSnapshot.model_validate_json(canonical(micro["book"]))
            if feed.get("environment") == "testnet" and market.environment != "testnet":
                self._note("TESTNET_ENVIRONMENT_MISMATCH")
                continue
            if market.quality is not DataQuality.OK:
                self._note("DATA_QUALITY_BLOCKED")
                continue
            if book.stream_state is not StreamState.READY:
                self._note("BOOK_NOT_READY")
                continue
            if (
                not market.costs_fresh(at, 5)
                or not market.volume_fresh(at, 5)
                or not 0 <= (at - book.source_at).total_seconds() <= 5
                or not 0 <= (at - book.captured_at).total_seconds() <= 5
            ):
                self._note("MARKET_EVIDENCE_STALE")
                continue
            price = market.best_ask if candidate.side is Side.LONG else market.best_bid
            triggered = (
                price >= candidate.trigger
                if candidate.side is Side.LONG
                else price <= candidate.trigger
            )
            if not triggered:
                self._note("TRIGGER_NOT_CROSSED")
                continue
            if record.state is DecisionState.ARMED:
                record = self._advance(record, DecisionState.VALIDATING)
                self._triggered = min(2**31 - 1, self._triggered + 1)
            self._last_decision_id = str(record.decision_id)
            evidence = self.evidence(record, market, book)
            if evidence is None:
                # The existing lifecycle records explicit rejection reasons.
                self._advance(record, DecisionState.APPROVED)
            else:
                publish_approved(self.db, record, evidence)
                self._approvals = min(2**31 - 1, self._approvals + 1)
                self._note(
                    "TESTNET_TRIAL_PUBLISHED"
                    if evidence.trial is not None
                    else "APPROVED_PUBLISHED"
                )
            return  # At most one new entry proposal per reconciled cycle.

    def evidence(
        self, record: Record, market: MarketSnapshot, book: OrderBookSnapshot
    ) -> ValidationEvidence | None:
        if getattr(self.client, "environment", None) != "testnet":
            raise EntryHalt("DEMO_ONLY")
        candidate = record.discovery.candidate
        with self.db.transaction() as c:
            body = c.scalar(
                text(
                    "SELECT payload->'input' FROM audit_log "
                    "WHERE event_type='runtime.candidate.v1' "
                    "AND payload->>'decision_id'=:id LIMIT 1"
                ),
                {"id": str(record.decision_id)},
            )
        if not isinstance(body, dict):
            raise EntryHalt("CANDIDATE_INPUT_MISSING")
        sample = (
            ReversalInput.model_validate_json(canonical(body))
            if "previous_flow" in body
            else MomentumInput.model_validate_json(canonical(body))
        )
        if (
            sample.digest != candidate.input_hash
            or sample.features.digest != candidate.source_feature_hash
        ):
            raise EntryHalt("CANDIDATE_INPUT_MISMATCH")
        account = flat_account(self.client, sample)
        at = self.client.clock()
        metadata = parse_exchange_info(
            self.client.request("GET", "/fapi/v1/exchangeInfo", signed=False), at
        )
        meta = next(
            (m for m in metadata.instruments if m.symbol == candidate.instrument.symbol), None
        )
        if meta is None:
            raise EntryHalt("DEMO_INSTRUMENT_UNAVAILABLE")
        config = demo_symbol_config(self.client, meta.symbol)
        fees = self.client.request("GET", "/fapi/v1/commissionRate", {"symbol": meta.symbol})
        if not isinstance(fees, dict) or fees.get("symbol") != meta.symbol:
            raise EntryHalt("FEE_UNKNOWN")
        fee = max(Decimal(0), number(fees["takerCommissionRate"])) * 10000
        # Fixed conservative cost assumptions, independent of alpha scores.
        normal = Friction(
            spread_bps=candidate.spread_bps,
            slippage_bps=Decimal("4"),
            funding_bps=abs(market.funding) * 10000,
            latency_bps=Decimal("2"),
        )
        stress = Friction(
            spread_bps=max(candidate.spread_bps, Decimal("5")),
            slippage_bps=Decimal("10"),
            funding_bps=max(abs(market.funding) * 40000, Decimal("10")),
            latency_bps=Decimal("5"),
        )
        unit_cost = (fee * 2 + stress.total_bps) * candidate.trigger / 10000
        assert account.equity is not None
        request = EntryRequest(
            environment="testnet",
            instrument=candidate.instrument,
            filters=meta.filters,
            side=candidate.side,
            requested_qty=min(
                account.equity * Decimal("0.1") / candidate.trigger,
                self.trial_policy.max_notional / candidate.trigger,
                self.trial_policy.max_initial_risk
                / abs(candidate.trigger - candidate.stop_proposal),
            )
            if self.trial_policy.enabled
            else account.equity * Decimal("0.1") / candidate.trigger,
            entry_price=candidate.trigger,
            stop_price=candidate.stop_proposal,
            cost_per_unit=unit_cost,
            leverage=Decimal(config["leverage"]),
            observed_at=at,
            expires_at=candidate.expires_at,
            liquidity_scale=Decimal(1),
            spread_bps=candidate.spread_bps,
        )
        risk = RiskService(self.db).assess(request, account)
        if not risk.allowed or risk.sized is None:
            self._note("RISK_REJECTED")
            with self.db.transaction() as c:
                audit(
                    c,
                    "runtime.admission_rejected.v1",
                    {
                        "decision_id": str(record.decision_id),
                        "candidate_input_hash": candidate.input_hash,
                        "as_of": at.isoformat(),
                        "reason_codes": ["RISK_REJECTED", *risk.reason_codes],
                        "risk_assessment_id": risk.digest,
                    },
                )
            return None
        self._risk_allowed = min(2**31 - 1, self._risk_allowed + 1)
        cost = CostInput(
            notional=risk.sized.notional,
            initial_risk=risk.sized.qty * abs(candidate.trigger - candidate.stop_proposal),
            entry_role="taker",
            exit_role="taker",
            maker_fee_bps=None,
            taker_fee_bps=fee,
            normal=normal,
            stress=stress,
            observed_at=at,
            expires_at=at + timedelta(seconds=5),
            source_version=digest(market.model_dump(mode="json")),
        )
        bucket = Bucket(
            symbol=meta.symbol,
            regime=sample.features.state.label.value,
            alpha_family=candidate.alpha_family,
            side=candidate.side,
            environment="testnet",
        )
        edge = analyze_edge(
            EdgeStore(self.db).load_outcomes(at=at),
            bucket=bucket,
            cost=cost,
            at=at,
            feature_hash=candidate.source_feature_hash,
            regime_version=candidate.regime_version,
            mode=RuntimeMode.TESTNET,
        )
        EdgeStore(self.db).save_analysis(edge)
        self._statistical_evidence = bootstrap_diagnostics(edge, self.trial_policy)
        trial = edge.proof_state is not ProofState.VALIDATED and self.trial_policy.enabled
        if edge.proof_state is not ProofState.VALIDATED and not trial:
            self._note("INSUFFICIENT_STATISTICAL_EVIDENCE")
            with self.db.transaction() as c:
                audit(
                    c,
                    "runtime.admission_rejected.v1",
                    {
                        "decision_id": str(record.decision_id),
                        "candidate_input_hash": candidate.input_hash,
                        "as_of": at.isoformat(),
                        "reason_codes": [
                            "INSUFFICIENT_STATISTICAL_EVIDENCE",
                            *edge.reason_codes,
                        ],
                        "edge_analysis_version": edge.digest,
                    },
                )
            return None
        # Fresh exchange facts independently validate the handoff before publication.
        frame = ExecutionContext(
            environment="testnet",
            as_of=account.as_of,
            metadata=meta,
            account=account,
            mark_price=market.mark_price,
            bid=market.best_bid,
            ask=market.best_ask,
            leverage=request.leverage,
            max_notional=number(config["maxNotionalValue"]),
        )
        frame = verify_exchange_context(self.client, frame, stream_ready=True)
        save_context(self.db, frame)
        assert edge.costs.fees_R is not None
        proposal = Decision(
            **candidate.instrument.model_dump(),
            schema_version=1,
            event_id=uuid4(),
            source_at=at,
            observed_at=at,
            as_of=at,
            decision_id=record.decision_id,
            regime_version=candidate.regime_version,
            universe_version=candidate.universe_version,
            feature_hash=candidate.source_feature_hash,
            alpha_family=candidate.alpha_family,
            side=candidate.side,
            state=DecisionState.APPROVED,
            expires_at=candidate.expires_at,
            trigger=candidate.trigger,
            stop_proposal=candidate.stop_proposal,
            take_profit_proposal=candidate.take_profit_proposal,
            invalidations=candidate.invalidations,
            spread_bps=candidate.spread_bps,
            slippage_bps=stress.slippage_bps,
            estimated_fees=edge.costs.fees_R * cost.initial_risk,
            funding_cost=stress.funding_bps * cost.notional / 10000,
            net_ev_low_R=edge.net_ev_low_R,
            proof_state=edge.proof_state,
            calibrated=not trial,
            execution_authority="TESTNET_TRIAL" if trial else "STATISTICAL_EDGE",
            confidence=None,
            quality=None,
            reason_codes=(
                ReasonCode.TESTNET_TRIAL_AUTHORIZED if trial else ReasonCode.CANDIDATE_VALIDATED,
            ),
            risk_policy_version=risk.policy.digest,
            decision_policy_version=DecisionPolicy().digest,
        )
        return ValidationEvidence(
            source_data_version=candidate.input_hash,
            as_of=at,
            available_at=at,
            triggered=True,
            data_quality=market.quality,
            book_state=book.stream_state,
            cost_version=cost.digest,
            risk_version=risk.policy.digest,
            proposal=proposal,
            edge=edge,
            risk=risk,
            trial=self.trial_policy if trial else None,
        )

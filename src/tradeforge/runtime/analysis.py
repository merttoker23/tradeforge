"""Connect existing alpha engines to the durable analyst lifecycle.

Only an armed setup is published. Candidate evidence is retained once; high
frequency book/flow observations remain in the gateway's bounded memory.
"""

from collections import Counter, OrderedDict, deque
from collections.abc import Callable
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy import text

from tradeforge.alpha.base import AlphaResult, MomentumInput
from tradeforge.alpha.momentum import MomentumAlpha
from tradeforge.alpha.reversal import ReversalAlpha, ReversalInput
from tradeforge.decision.provenance import Discovery, candidate_decision_id
from tradeforge.decision.state_machine import Command
from tradeforge.decision.storage import DecisionService
from tradeforge.domain.decision import DecisionState, Side
from tradeforge.execution.storage import audit
from tradeforge.persistence.db import Database


class NoTradeReason(StrEnum):
    DATA_UNAVAILABLE = "DATA_UNAVAILABLE"
    INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"
    STALE_BARS = "STALE_BARS"
    STALE_REGIME = "STALE_REGIME"
    UNIVERSE_UNAVAILABLE = "UNIVERSE_UNAVAILABLE"
    REGIME_NOT_ALLOWED = "REGIME_NOT_ALLOWED"
    FEATURE_UNAVAILABLE = "FEATURE_UNAVAILABLE"
    STRONG_TREND_FADE = "STRONG_TREND_FADE"
    RELATIVE_STRENGTH_UNKNOWN = "RELATIVE_STRENGTH_UNKNOWN"
    TREND_MISALIGNED = "TREND_MISALIGNED"
    RELATIVE_STRENGTH_OPPOSED = "RELATIVE_STRENGTH_OPPOSED"
    MARKET_UNAVAILABLE = "MARKET_UNAVAILABLE"
    BOOK_UNAVAILABLE = "BOOK_UNAVAILABLE"
    SPREAD_GATE = "SPREAD_GATE"
    LIQUIDITY_GATE = "LIQUIDITY_GATE"
    FLOW_UNAVAILABLE = "FLOW_UNAVAILABLE"
    FLOW_OPPOSED = "FLOW_OPPOSED"
    MICROSTRUCTURE_DESYNC = "MICROSTRUCTURE_DESYNC"
    FLOW_WINDOW_GAP = "FLOW_WINDOW_GAP"
    FLOW_NOT_STABILIZED = "FLOW_NOT_STABILIZED"
    INSTRUMENT_CAPACITY_EXHAUSTED = "INSTRUMENT_CAPACITY_EXHAUSTED"
    EXPIRED = "EXPIRED"
    BREAKOUT_INVALIDATED = "BREAKOUT_INVALIDATED"
    VOLATILITY_NOT_EXPANDING = "VOLATILITY_NOT_EXPANDING"
    WEAK_PARTICIPATION = "WEAK_PARTICIPATION"
    TRIGGER_ALREADY_PASSED = "TRIGGER_ALREADY_PASSED"
    PULLBACK_TOO_DEEP = "PULLBACK_TOO_DEEP"
    STOP_ALREADY_BREACHED = "STOP_ALREADY_BREACHED"
    DUPLICATE_SETUP = "DUPLICATE_SETUP"
    SETUP_CAPACITY_EXHAUSTED = "SETUP_CAPACITY_EXHAUSTED"
    NO_SWEEP_RECLAIM = "NO_SWEEP_RECLAIM"
    STRETCH_GATE = "STRETCH_GATE"
    WEAK_WICK_REJECTION = "WEAK_WICK_REJECTION"
    RECLAIM_INVALIDATED = "RECLAIM_INVALIDATED"
    INVALID_TARGET_GEOMETRY = "INVALID_TARGET_GEOMETRY"
    SETUP_NOT_ARMED = "SETUP_NOT_ARMED"
    OTHER_NO_TRADE = "OTHER_NO_TRADE"
    RETURN_SHOCK = "RETURN_SHOCK"
    FUNDING_STRESS = "FUNDING_STRESS"


class AutonomousAnalysis:
    def __init__(
        self, db: Database, *, clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    ) -> None:
        self.db, self.clock = db, clock
        self.momentum, self.reversal = MomentumAlpha(), ReversalAlpha()
        # Monotonic process totals let the dashboard derive a real measurement
        # window instead of inventing a rate from a single sample.
        self.evaluation_total = 0
        self.analysis_cycle_total = 0
        self.last_analysis_at: str | None = None
        # Latest per-symbol outcome, retaining unaffected pairs when a wakeup
        # re-evaluates only the changed symbols. There is no analysis cycle:
        # every eligible pair is re-evaluated continuously as evidence arrives.
        self._summaries: OrderedDict[str, dict[str, object]] = OrderedDict()
        self._hourly: deque[tuple[datetime, Counter[str], int]] = deque(maxlen=61)
        self._process_reasons: Counter[str] = Counter()
        self._last_batch_symbol = ""
        self.state: dict[str, object] = {
            "candidate_count": 0,
            "reason_counts": {},
            "evaluated_count": 0,
            "analysis_symbols": 0,
            "analyzed_symbols": 0,
            "evaluation_total": 0,
            "analysis_cycle_total": 0,
            "last_analysis_at": None,
            "symbol_results": [],
            "symbol_summaries": [],
            "execution_ready": False,
        }

    def next_batch(
        self, ready: frozenset[str] | tuple[str, ...], *, limit: int = 24
    ) -> frozenset[str]:
        """Rotate fairly; retain ranked market-handoff order when available."""
        # Existing set callers keep a stable sorted order. Gateway tuples are
        # ranked by surveillance allocation and must not be re-sorted.
        ordered = sorted(ready) if isinstance(ready, frozenset) else list(dict.fromkeys(ready))
        start = (
            (ordered.index(self._last_batch_symbol) + 1) % len(ordered)
            if ordered and self._last_batch_symbol in ordered
            else 0
        )
        batch = (ordered[start:] + ordered[:start])[:limit]
        if batch:
            self._last_batch_symbol = batch[-1]
        return frozenset(batch)

    def evaluate(
        self,
        inputs: tuple[MomentumInput, ...],
        *,
        symbols: frozenset[str] | None = None,
    ) -> int:
        # Re-evaluation of one symbol may not mutate unrelated alpha state.
        if symbols is not None:
            inputs = tuple(sample for sample in inputs if sample.instrument.symbol in symbols)
        at = self.clock()
        momentum, reversal = deepcopy(self.momentum), deepcopy(self.reversal)
        discoveries = []
        counts: Counter[str] = Counter()
        evaluated = 0
        symbol_results = []
        symbol_summaries: dict[str, dict[str, object]] = {}
        evidence: dict[UUID, MomentumInput] = {}
        for sample in inputs:
            symbol = sample.instrument.symbol
            summary = symbol_summaries.setdefault(
                symbol,
                {
                    "symbol": symbol,
                    "analyzed_at": at.isoformat(),
                    "as_of": sample.as_of.isoformat(),
                    "decision": "NO_TRADE",
                    "strategy": None,
                    "strategies": ["MOMENTUM", "REVERSAL"]
                    if isinstance(sample, ReversalInput)
                    else ["MOMENTUM"],
                    "reasons": [],
                },
            )
            for side in (Side.LONG, Side.SHORT):
                results = [("MOMENTUM", momentum.evaluate(sample, side=side, now=at))]
                if isinstance(sample, ReversalInput):
                    results.append(("REVERSAL", reversal.evaluate(sample, side=side, now=at)))
                for strategy, result in results:
                    evaluated += 1
                    candidate = result.candidate
                    symbol_results.append(
                        {
                            "symbol": symbol,
                            "strategy": strategy,
                            "side": side.value,
                            "state": candidate.state.value if candidate else "NO_TRADE",
                            "reasons": list(result.reason_codes),
                            "as_of": sample.as_of.isoformat(),
                            "trigger": str(candidate.trigger) if candidate else None,
                            "stop": str(candidate.stop_proposal) if candidate else None,
                            "target": str(candidate.take_profit_proposal)
                            if candidate and candidate.take_profit_proposal is not None
                            else None,
                            "expires_at": candidate.expires_at.isoformat() if candidate else None,
                            "trace": evaluation_trace(sample, strategy, side, at, result),
                        }
                    )
                    if candidate is None or candidate.state is not DecisionState.ARMED:
                        for reason in (
                            result.reason_codes if candidate is None else ("SETUP_NOT_ARMED",)
                        ):
                            try:
                                code = NoTradeReason(reason).value
                            except ValueError:
                                code = NoTradeReason.OTHER_NO_TRADE.value
                            counts[code] += 1
                            reasons = summary["reasons"]
                            assert isinstance(reasons, list)
                            if code not in reasons:
                                reasons.append(code)
                        continue
                    if summary["decision"] == "NO_TRADE":
                        summary["decision"] = side.value
                        summary["strategy"] = strategy
                        summary["reasons"] = list(result.reason_codes)
                        summary["expires_at"] = candidate.expires_at.isoformat()
                    discovery = Discovery(
                        candidate=candidate,
                        cluster="portfolio",
                        as_of=sample.as_of,
                        available_at=at,
                        source_data_version=candidate.input_hash,
                    )
                    discoveries.append(discovery)
                    evidence[discovery.decision_id] = sample
        service = DecisionService(self.db)
        count = self._journal(service, discoveries, evidence, at) if discoveries else 0
        self.momentum, self.reversal = momentum, reversal
        if inputs:
            self.last_analysis_at = at.isoformat()
            self.analysis_cycle_total = min(2**31 - 1, self.analysis_cycle_total + 1)
        self.evaluation_total = min(2**31 - 1, self.evaluation_total + evaluated)
        for code, value in counts.items():
            self._process_reasons[code] = min(2**31 - 1, self._process_reasons[code] + value)
        minute = at.replace(second=0, microsecond=0)
        if not self._hourly or self._hourly[-1][0] != minute:
            self._hourly.append((minute, Counter(), 0))
        _, reasons, total = self._hourly[-1]
        reasons.update(counts)
        self._hourly[-1] = (minute, reasons, total + evaluated)
        while self._hourly and self._hourly[0][0] <= minute - timedelta(hours=1):
            self._hourly.popleft()
        hour_counts: Counter[str] = Counter()
        for _, reasons, _ in self._hourly:
            hour_counts.update(reasons)
        if symbols is None:
            self._summaries.clear()
        for symbol, row in symbol_summaries.items():
            self._summaries[symbol] = row
            self._summaries.move_to_end(symbol)
        while len(self._summaries) > 1024:
            self._summaries.popitem(last=False)
        self.state = {
            "candidate_count": count,
            "reason_counts": dict(counts),
            "process_reason_counts": dict(self._process_reasons),
            "last_hour": {
                "reason_counts": dict(hour_counts),
                "evaluated_count": sum(total for _, _, total in self._hourly),
                "as_of": at.isoformat(),
            },
            "evaluated_count": evaluated,
            "analysis_symbols": len(inputs),
            "analyzed_symbols": len(symbol_summaries),
            "evaluation_total": self.evaluation_total,
            "analysis_cycle_total": self.analysis_cycle_total,
            "last_analysis_at": self.last_analysis_at,
            "symbol_results": symbol_results[:64],
            "symbol_summaries": list(self._summaries.values()),
            "execution_ready": False,
        }
        return count

    def _journal(
        self,
        service: DecisionService,
        discoveries: list[Discovery],
        evidence: dict[UUID, MomentumInput],
        at: datetime,
    ) -> int:
        count = 0
        with self.db.transaction() as c:
            c.execute(text("SELECT pg_advisory_xact_lock(74202010)"))
            journal = service._load(c)
            known = {r.decision_id for r in journal.records}
            new = tuple(d for d in discoveries if d.decision_id not in known)
            for discovery in new:
                record = journal.apply(
                    Command(
                        event_id=uuid5(NAMESPACE_URL, discovery.digest),
                        decision_id=discovery.decision_id,
                        at=at,
                        target=DecisionState.DISCOVERED,
                        discovery=discovery,
                        selection_peers=new,
                    ),
                    now=at,
                )
                service._append(c, record)
                if record.state is not DecisionState.DISCOVERED:
                    continue
                for state in (DecisionState.WATCHING, DecisionState.ARMED):
                    record = journal.apply(
                        Command(
                            event_id=uuid5(discovery.decision_id, state.value),
                            decision_id=discovery.decision_id,
                            at=at,
                            target=state,
                            expected_previous_state=record.state,
                            expected_previous_version=record.version,
                        ),
                        now=at,
                    )
                    service._append(c, record)
                audit(
                    c,
                    "runtime.candidate.v1",
                    {
                        "decision_id": str(discovery.decision_id),
                        "input": evidence[discovery.decision_id].model_dump(mode="json"),
                    },
                )
                count += 1
        return count


def evaluation_trace(
    sample: MomentumInput, strategy: str, side: Side, at: datetime, result: AlphaResult
) -> dict[str, object]:
    candidate = result.candidate
    blockers = result.failed_gates or (result.reason_codes if candidate is None else ())
    market = sample.market

    def age(source: datetime) -> float:
        return round((at - source).total_seconds() * 1000, 3)

    return {
        "trace_id": str(uuid5(market.event_id, f"{strategy}/{side.value}/{at.isoformat()}")),
        "decision_id": str(candidate_decision_id(candidate))
        if candidate and candidate.state is DecisionState.ARMED
        else None,
        "market_event_at": market.source_at.isoformat(),
        "feature_source_at": sample.features.frame.source_at.isoformat(),
        "evaluation_at": at.isoformat(),
        "regime": sample.features.state.label.value,
        "bar_age_ms": {
            bars[-1].timeframe.value: age(bars[-1].close_at)
            for bars in (sample.setup_bars, sample.medium_bars, sample.higher_bars)
            if bars
        },
        "quote_age_ms": age(market.book_source_at or market.source_at),
        "book_age_ms": age(sample.book.source_at),
        "flow_age_ms": age(sample.flow.source_end),
        "stage": candidate.state.value if candidate else "ALPHA_REJECTED",
        "first_blocker": blockers[0] if blockers else None,
        "failed_gates": list(blockers),
    }

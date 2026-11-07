"""Connect existing alpha engines to the durable analyst lifecycle.

Only an armed setup is published. Candidate evidence is retained once; high
frequency book/flow observations remain in the gateway's bounded memory.
"""

from collections import Counter
from collections.abc import Callable
from copy import deepcopy
from datetime import UTC, datetime
from enum import StrEnum
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import text

from tradeforge.alpha.base import MomentumInput
from tradeforge.alpha.momentum import MomentumAlpha
from tradeforge.alpha.reversal import ReversalAlpha, ReversalInput
from tradeforge.decision.provenance import Discovery
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


class AutonomousAnalysis:
    def __init__(
        self, db: Database, *, clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    ) -> None:
        self.db, self.clock = db, clock
        self.momentum, self.reversal = MomentumAlpha(), ReversalAlpha()
        self.state: dict[str, object] = {
            "candidate_count": 0,
            "reason_counts": {},
            "evaluated_count": 0,
            "analysis_symbols": 0,
            "symbol_results": [],
            "execution_ready": False,
        }

    def evaluate(self, inputs: tuple[MomentumInput, ...]) -> int:
        at = self.clock()
        momentum, reversal = deepcopy(self.momentum), deepcopy(self.reversal)
        discoveries = []
        counts: Counter[str] = Counter()
        evaluated = 0
        symbol_results = []
        evidence = {}
        for sample in inputs:
            for side in (Side.LONG, Side.SHORT):
                results = [("MOMENTUM", momentum.evaluate(sample, side=side, now=at))]
                if isinstance(sample, ReversalInput):
                    results.append(("REVERSAL", reversal.evaluate(sample, side=side, now=at)))
                for strategy, result in results:
                    evaluated += 1
                    candidate = result.candidate
                    symbol_results.append(
                        {
                            "symbol": sample.instrument.symbol,
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
                        continue
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
        self.momentum, self.reversal = momentum, reversal
        self.state = {
            "candidate_count": count,
            "reason_counts": dict(counts),
            "evaluated_count": evaluated,
            "analysis_symbols": len(inputs),
            "symbol_results": symbol_results[:64],
            "execution_ready": False,
        }
        return count

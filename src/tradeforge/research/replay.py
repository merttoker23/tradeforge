"""Offline, ordered and point-in-time replay of normalized observations.

The policy sees an immutable prefix only. A callable is trusted code, not a
sandbox: closure/global data access is the policy author's responsibility.
Decision receipts hash every policy output, including no-trade outcomes.
"""

from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Self

from pydantic import Field, model_validator

from tradeforge.domain.common import Count, Digest, Money, StrictModel, Text, Timestamp
from tradeforge.domain.market import ClosedBar, UniverseSnapshot
from tradeforge.execution.paper_broker import PaperBroker, PaperOrder, PaperPosition, Trade
from tradeforge.intelligence.models import digest
from tradeforge.research.fill_simulator import ZERO, FillScenario, Funding, Quote
from tradeforge.research.metrics import (
    GroupScore,
    MarkedPosition,
    Mode,
    Scorecard,
    group_keys,
    grouped_scores,
    outcome_key,
    scorecard,
)

Observation = UniverseSnapshot | ClosedBar | Quote | Funding


class ReplayTape(StrictModel):
    dataset_version: Text
    events: Annotated[tuple[Observation, ...], Field(min_length=1, max_length=1_000_000)]

    @model_validator(mode="after")
    def ordered(self) -> Self:
        ids = set()
        previous: tuple[datetime, int] | None = None
        for e in self.events:
            if e.event_id in ids:
                raise ValueError("DUPLICATE_TAPE_EVENT")
            ids.add(e.event_id)
            if e.sequence is None:
                raise ValueError("RECORDED_SEQUENCE_REQUIRED")
            key = (e.as_of, e.sequence)
            if previous is not None and key <= previous:
                raise ValueError("TAPE_ORDER_VIOLATION")
            previous = key
            # Revalidate even model_construct inputs with this event's virtual clock.
            type(e).model_validate(e.model_dump(), context={"now": e.as_of})
        if not isinstance(self.events[0], UniverseSnapshot):
            raise ValueError("INITIAL_UNIVERSE_REQUIRED")
        return self

    @property
    def tape_hash(self) -> str:
        return digest(self.model_dump(mode="json"))


class ReplayView(StrictModel):
    at: Timestamp
    universe: UniverseSnapshot
    history: tuple[Observation, ...]
    positions: tuple[PaperPosition, ...]


Policy = Callable[[Observation, ReplayView], tuple[PaperOrder, ...]]


class ReplayResult(StrictModel):
    dataset_hash: Digest
    policy_version: Text
    scenario_hash: Digest
    decision_hashes: tuple[Digest, ...]
    trades: tuple[Trade, ...]
    open_positions: Count
    net_pnl: Money
    scorecard: Scorecard
    groups: tuple[GroupScore, ...]
    checkpoint: str


class RecordedAction(StrictModel):
    sequence: Count
    orders: tuple[PaperOrder, ...]


class ReplaySession(StrictModel):
    """Transport for already recorded policy outputs; not a profit-generating strategy."""

    tape: ReplayTape
    policy_version: Text
    scenario: FillScenario = FillScenario()
    actions: tuple[RecordedAction, ...] = ()

    @model_validator(mode="after")
    def linked(self) -> Self:
        sequences = [e.sequence for e in self.tape.events]
        action_sequences = [a.sequence for a in self.actions]
        if (
            len(sequences) != len(set(sequences))
            or len(action_sequences) != len(set(action_sequences))
            or not set(action_sequences) <= set(sequences)
        ):
            raise ValueError("ACTION_SEQUENCE_MISMATCH")
        return self

    def run(self, *, mode: Mode = "BACKTEST") -> ReplayResult:
        actions = {a.sequence: a.orders for a in self.actions}

        def policy(event: Observation, view: ReplayView) -> tuple[PaperOrder, ...]:
            assert event.sequence is not None
            return actions.get(event.sequence, ())

        return replay(
            self.tape,
            policy,
            policy_version=self.policy_version,
            scenario=self.scenario,
            mode=mode,
        )


def replay(
    tape: ReplayTape,
    policy: Policy,
    *,
    policy_version: str,
    scenario: FillScenario | None = None,
    mode: Mode = "BACKTEST",
    history_limit: int = 4096,
) -> ReplayResult:
    if type(history_limit) is not int or not 1 <= history_limit <= 65_536:
        raise ValueError("INVALID_HISTORY_LIMIT")
    tape = ReplayTape.model_validate(tape.model_dump())
    scenario = scenario or FillScenario()
    broker = PaperBroker(scenario)
    universe = tape.events[0]
    assert isinstance(universe, UniverseSnapshot)
    receipts: list[str] = []
    quotes: dict[tuple[str, ...], Quote] = {}
    equities: list[Decimal] = []
    scenario_hash = digest(scenario.model_dump(mode="json"))
    unrealized = ZERO
    marks: tuple[MarkedPosition, ...] = ()
    group_paths: dict[tuple[str, str], list[Decimal]] = {}
    for index, e in enumerate(tape.events):
        if isinstance(e, UniverseSnapshot):
            universe = e
            broker.reconcile(e.eligible, at=e.as_of)
        elif isinstance(e, Quote):
            quotes[e.key] = e
            broker.on_quote(e)
        elif isinstance(e, Funding):
            broker.on_funding(e)
        view = ReplayView(
            at=e.as_of,
            universe=universe,
            history=tape.events[max(0, index + 1 - history_limit) : index + 1],
            positions=broker.positions,
        )
        orders = policy(e, view)
        if not isinstance(orders, tuple):
            raise ValueError("IMMUTABLE_POLICY_OUTPUT_REQUIRED")
        for o in orders:
            if not isinstance(o, PaperOrder):
                raise ValueError("PAPER_ORDER_REQUIRED")
            broker.submit(o, at=e.as_of, eligible=universe.eligible)
        receipts.append(
            digest(
                {
                    "previous": receipts[-1] if receipts else "0" * 64,
                    "event": e.model_dump(mode="json"),
                    "policy": policy_version,
                    "scenario": scenario_hash,
                    "history_limit": history_limit,
                    "orders": [o.model_dump(mode="json") for o in orders],
                }
            )
        )
        unrealized = ZERO
        current_marks: list[MarkedPosition] = []
        for p in broker.positions:
            q = quotes[p.instrument.key]
            mark = q.best_bid if p.order.intent.side.value == "LONG" else q.best_ask
            direction = 1 if p.order.intent.side.value == "LONG" else -1
            pnl = (mark - p.entry_price) * p.qty * direction - p.entry_fees - p.funding
            unrealized += pnl
            current_marks.append(MarkedPosition(position=p, unrealized_pnl=pnl or ZERO))
        marks = tuple(current_marks)
        equities.append(sum((t.net_pnl for t in broker.trades), ZERO) + unrealized)
        totals: dict[tuple[str, str], Decimal] = {}
        for trade in broker.trades:
            for key in group_keys(trade):
                totals[key] = totals.get(key, ZERO) + trade.net_pnl
        for marked in marks:
            for key in group_keys(marked.position):
                totals[key] = totals.get(key, ZERO) + marked.unrealized_pnl
        for key, value in totals.items():
            group_paths.setdefault(key, []).append(value)
    card = scorecard(
        broker.trades,
        mode=mode,
        open_positions=len(broker.positions),
        unrealized_pnl=unrealized,
        open_fees=sum((p.entry_fees for p in broker.positions), ZERO),
        open_funding=sum((p.funding for p in broker.positions), ZERO),
        open_slippage=sum((p.entry_slippage for p in broker.positions), ZERO),
        equity=tuple(equities),
        incomplete=frozenset(outcome_key(p) for p in broker.positions),
    )
    return ReplayResult(
        dataset_hash=tape.tape_hash,
        policy_version=policy_version,
        scenario_hash=scenario_hash,
        decision_hashes=tuple(receipts),
        trades=broker.trades,
        open_positions=len(broker.positions),
        net_pnl=card.net_pnl,
        scorecard=card,
        groups=grouped_scores(
            broker.trades,
            mode=mode,
            marks=marks,
            equity_paths={key: tuple(path) for key, path in group_paths.items()},
        ),
        checkpoint=broker.checkpoint(),
    )


def main(argv: list[str] | None = None, *, mode: Mode = "BACKTEST") -> int:
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser(description="Offline normalized recording replay")
    parser.add_argument("--input", type=Path, required=True, help="Recorded ReplaySession JSON")
    args = parser.parse_args(argv)
    try:
        session = ReplaySession.model_validate_json(args.input.read_text(encoding="utf-8"))
        result = session.run(mode=mode)
    except (ValueError, OSError):
        # Validation inputs may contain arbitrary data; no raw payload in diagnostics.
        parser.error("INVALID_RECORDING_OR_REPLAY: input rejected")
    print(result.model_dump_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

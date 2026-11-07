"""Deterministic paper account. No environment, HTTP client, private key or live mode.

One position per complete instrument identity. Entries cannot pyramid. Stops and
manual exits cancel pending entry remainders; exit fills share displayed capacity.
Checkpoint integrity detects corruption, not malicious tampering (no signing key).
"""

import json
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, model_validator

from tradeforge.domain.common import (
    Digest,
    Money,
    NonNegative,
    Positive,
    StrictModel,
    Timestamp,
    utc,
)
from tradeforge.domain.decision import AlphaFamily, Side
from tradeforge.domain.execution import ExecutionIntent, PositionMode
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import RegimeLabel
from tradeforge.intelligence.models import digest
from tradeforge.research.fill_simulator import (
    ZERO,
    Fill,
    FillScenario,
    Funding,
    Quote,
    simulate_fill,
)


class PaperOrder(StrictModel):
    intent: ExecutionIntent
    take_profit: Positive
    alpha: AlphaFamily
    regime: RegimeLabel
    decision_hash: Digest
    maker: bool = False

    @model_validator(mode="after")
    def entry_shape(self) -> Self:
        i = self.intent
        if i.reduce_only or i.position_mode is not PositionMode.ONE_WAY or i.price is None:
            raise ValueError("PAPER_ENTRY_REQUIRED")
        if not (self.take_profit > i.price if i.side is Side.LONG else self.take_profit < i.price):
            raise ValueError("INVALID_TAKE_PROFIT")
        return self


class Pending(StrictModel):
    order: PaperOrder
    remaining: NonNegative
    active: bool = True


class PaperPosition(StrictModel):
    order: PaperOrder
    qty: Positive
    entry_price: Positive
    opened_at: Timestamp
    entry_fees: NonNegative
    entry_slippage: NonNegative
    funding: Money = ZERO
    exit_reason: Literal["STOP", "TARGET", "MANUAL", "DELISTED"] | None = None
    exit_requested_at: Timestamp | None = None

    @property
    def protected_qty(self) -> Decimal:
        return self.qty

    @property
    def instrument(self) -> InstrumentKey:
        i = self.order.intent
        return InstrumentKey(**{name: getattr(i, name) for name in InstrumentKey.model_fields})


class Trade(StrictModel):
    intent_id: UUID
    instrument: InstrumentKey
    side: Side
    alpha: AlphaFamily
    regime: RegimeLabel
    decision_hash: Digest
    opened_at: Timestamp
    closed_at: Timestamp
    qty: Positive
    gross_pnl: Money
    fees: NonNegative
    funding: Money
    slippage_cost: NonNegative
    exit_reason: Literal["STOP", "TARGET", "MANUAL", "DELISTED"]

    @property
    def net_pnl(self) -> Decimal:
        # Slippage is already embedded in execution prices; never subtract twice.
        return self.gross_pnl - self.fees - self.funding


class PaperFill(Fill):
    fill_id: Digest
    intent_id: UUID
    instrument: InstrumentKey
    side: Side
    role: Literal["ENTRY", "EXIT"]
    source_at: Timestamp


def identified_fill(
    fill: Fill, order: PaperOrder, quote: Quote, role: Literal["ENTRY", "EXIT"], side: Side
) -> PaperFill:
    i = order.intent
    instrument = InstrumentKey(**{name: getattr(i, name) for name in InstrumentKey.model_fields})
    return PaperFill(
        **fill.model_dump(),
        intent_id=i.intent_id,
        instrument=instrument,
        side=side,
        role=role,
        source_at=quote.source_at,
        fill_id=digest((str(i.intent_id), str(quote.event_id), role)),
    )


class BrokerState(StrictModel):
    version: Literal[1] = 1
    scenario: FillScenario
    pending: tuple[Pending, ...] = ()
    positions: tuple[PaperPosition, ...] = ()
    fills: tuple[PaperFill, ...] = ()
    trades: tuple[Trade, ...] = ()
    last_at: Timestamp | None = None
    last_sequence: Annotated[int, Field(ge=0)] | None = None
    quotes: tuple[Quote, ...] = ()

    @model_validator(mode="after")
    def reconcile_inventory(self) -> Self:
        intents = {p.order.intent.intent_id: p for p in self.pending}
        if len(intents) != len(self.pending) or len(
            {p.order.intent.client_order_id for p in self.pending}
        ) != len(self.pending):
            raise ValueError("INVENTORY_DUPLICATE_INTENT")
        if len({p.instrument.key for p in self.positions}) != len(self.positions):
            raise ValueError("INVENTORY_DUPLICATE_POSITION")
        if len({f.fill_id for f in self.fills}) != len(self.fills):
            raise ValueError("INVENTORY_DUPLICATE_FILL")
        inventory_items: tuple[PaperPosition | Trade, ...] = (*self.positions, *self.trades)
        for item in inventory_items:
            intent_id = (
                item.order.intent.intent_id if isinstance(item, PaperPosition) else item.intent_id
            )
            if intent_id not in intents:
                raise ValueError("INVENTORY_UNKNOWN_INTENT")
        for intent_id, pending in intents.items():
            entries = sum(
                (f.qty for f in self.fills if f.intent_id == intent_id and f.role == "ENTRY"), ZERO
            )
            exits = sum(
                (f.qty for f in self.fills if f.intent_id == intent_id and f.role == "EXIT"), ZERO
            )
            held = sum(
                (p.qty for p in self.positions if p.order.intent.intent_id == intent_id), ZERO
            )
            closed = sum((t.qty for t in self.trades if t.intent_id == intent_id), ZERO)
            if (
                pending.remaining > pending.order.intent.qty
                or entries != pending.order.intent.qty - pending.remaining
                or entries != exits + held
                or exits != closed
            ):
                raise ValueError("INVENTORY_QUANTITY_MISMATCH")
        if any(f.intent_id not in intents for f in self.fills):
            raise ValueError("INVENTORY_UNKNOWN_FILL")
        return self


def revised[T: StrictModel](model: T, **patch: object) -> T:
    return type(model).model_validate({**model.model_dump(), **patch})


class PaperBroker:
    def __init__(self, scenario: FillScenario) -> None:
        self._state = BrokerState(scenario=scenario)

    @property
    def positions(self) -> tuple[PaperPosition, ...]:
        return self._state.positions

    @property
    def fills(self) -> tuple[PaperFill, ...]:
        return self._state.fills

    @property
    def trades(self) -> tuple[Trade, ...]:
        return self._state.trades

    def submit(
        self, order: PaperOrder, *, at: datetime, eligible: tuple[InstrumentKey, ...]
    ) -> None:
        utc(at)
        i = order.intent
        if i.as_of > at or i.created_at > at:
            raise ValueError("FUTURE_ORDER")
        if at >= i.expires_at:
            raise ValueError("EXPIRED_ORDER")
        if self._state.last_at is not None and at < self._state.last_at:
            raise ValueError("ORDER_TIME_REVERSED")
        if i.key not in {k.key for k in eligible}:
            raise ValueError("OUTSIDE_POINT_IN_TIME_UNIVERSE")
        for pending in self._state.pending:
            previous = pending.order.intent
            if i.intent_id == previous.intent_id or i.client_order_id == previous.client_order_id:
                if order != pending.order:
                    raise ValueError("ORDER_ID_CONFLICT")
                return
            if previous.key == i.key and pending.active:
                raise ValueError("PAPER_POSITION_CONFLICT")
        if any(p.order.intent.key == i.key for p in self.positions):
            raise ValueError("PAPER_POSITION_CONFLICT")
        # Submission time is authoritative; old intents cannot bypass latency.
        if at != i.created_at:
            raise ValueError("SUBMISSION_TIME_MISMATCH")
        if order.maker:
            q = next((q for q in self._state.quotes if q.key == i.key), None)
            if q is None or (at - q.source_at).total_seconds() * 1000 > (
                self._state.scenario.max_quote_age_ms
            ):
                raise ValueError("MAKER_SUBMISSION_QUOTE_REQUIRED")
            assert i.price is not None
            if i.price >= q.best_ask if i.side is Side.LONG else i.price <= q.best_bid:
                raise ValueError("MARKETABLE_MAKER_ORDER")
        self._state = revised(
            self._state, pending=(*self._state.pending, Pending(order=order, remaining=i.qty))
        )

    def close(
        self,
        instrument: InstrumentKey,
        *,
        at: datetime,
        reason: Literal["MANUAL", "DELISTED"] = "MANUAL",
    ) -> None:
        utc(at)
        if self._state.last_at is not None and at < self._state.last_at:
            raise ValueError("ORDER_TIME_REVERSED")
        self._state = revised(
            self._state,
            pending=tuple(
                revised(p, active=False) if p.order.intent.key == instrument.key else p
                for p in self._state.pending
            ),
            positions=tuple(
                revised(p, exit_reason=reason, exit_requested_at=at)
                if p.instrument == instrument and p.exit_reason is None
                else p
                for p in self.positions
            ),
            last_at=at,
            last_sequence=self._state.last_sequence if at == self._state.last_at else None,
        )

    def reconcile(self, eligible: tuple[InstrumentKey, ...], *, at: datetime) -> None:
        keys = {k.key for k in eligible}
        for p in self.positions:
            if p.instrument.key not in keys:
                self.close(p.instrument, at=at, reason="DELISTED")
        self._state = revised(
            self._state,
            pending=tuple(
                revised(p, active=False) if p.order.intent.key not in keys else p
                for p in self._state.pending
            ),
        )

    def on_quote(self, quote: Quote) -> None:
        s = self._state
        if s.last_at is not None and (
            quote.as_of < s.last_at
            or (quote.as_of == s.last_at and (quote.sequence or 0) <= (s.last_sequence or 0))
        ):
            raise ValueError("QUOTE_ORDER_VIOLATION")
        previous_quote = next((q for q in s.quotes if q.key == quote.key), None)
        if previous_quote is not None and quote.source_at < previous_quote.source_at:
            raise ValueError("QUOTE_SOURCE_ORDER_VIOLATION")
        # Local calculations commit atomically only after the whole event succeeds.
        pending = list(s.pending)
        positions = list(s.positions)
        fills, trades = list(s.fills), list(s.trades)
        capacity = {Side.LONG: quote.ask_qty, Side.SHORT: quote.bid_qty}
        for p in tuple(positions):
            if p.order.intent.key != quote.key:
                continue
            if quote.source_at < p.opened_at:
                continue
            i = p.order.intent
            assert i.stop_plan is not None
            long = i.side is Side.LONG
            mark = quote.best_bid if long else quote.best_ask
            stop = mark <= i.stop_plan.stop_price if long else mark >= i.stop_plan.stop_price
            target = mark >= p.order.take_profit if long else mark <= p.order.take_profit
            reason = p.exit_reason or ("STOP" if stop else "TARGET" if target else None)
            if reason is None:
                continue
            pending = [
                revised(x, active=False) if x.order.intent.key == quote.key else x for x in pending
            ]
            exit_side = Side.SHORT if long else Side.LONG
            if p.exit_requested_at is not None and quote.source_at < (
                p.exit_requested_at + timedelta(milliseconds=s.scenario.latency_ms)
            ):
                continue
            fill = simulate_fill(exit_side, p.qty, quote, s.scenario, available=capacity[exit_side])
            positions.remove(p)
            if fill is None:
                positions.append(revised(p, exit_reason=reason))
                continue
            capacity[exit_side] -= fill.qty
            ratio = fill.qty / p.qty
            entry_fees = p.entry_fees * ratio
            entry_slip = p.entry_slippage * ratio
            funding = p.funding * ratio
            gross = (fill.price - p.entry_price) * fill.qty * (1 if long else -1)
            trades.append(
                Trade(
                    intent_id=i.intent_id,
                    instrument=p.instrument,
                    side=i.side,
                    alpha=p.order.alpha,
                    regime=p.order.regime,
                    decision_hash=p.order.decision_hash,
                    opened_at=p.opened_at,
                    closed_at=quote.as_of,
                    qty=fill.qty,
                    gross_pnl=gross or ZERO,
                    fees=entry_fees + fill.fee,
                    funding=funding,
                    slippage_cost=entry_slip + fill.slippage_cost,
                    exit_reason=reason,
                )
            )
            fills.append(identified_fill(fill, p.order, quote, "EXIT", exit_side))
            if fill.qty < p.qty:
                positions.append(
                    revised(
                        p,
                        qty=p.qty - fill.qty,
                        entry_fees=p.entry_fees - entry_fees,
                        entry_slippage=p.entry_slippage - entry_slip,
                        funding=p.funding - funding,
                        exit_reason=reason,
                    )
                )
        for index, item in enumerate(pending):
            i = item.order.intent
            if i.key != quote.key or not item.active:
                continue
            if quote.as_of >= i.expires_at:
                pending[index] = revised(item, active=False)
                continue
            ready = i.created_at + timedelta(milliseconds=s.scenario.latency_ms)
            if quote.source_at <= i.created_at or quote.source_at < ready:
                continue
            assert i.stop_plan is not None and i.price is not None
            # Never initiate a position after its invalidation/exit has already occurred.
            if (
                quote.best_bid <= i.stop_plan.stop_price or quote.best_ask >= item.order.take_profit
                if i.side is Side.LONG
                else quote.best_ask >= i.stop_plan.stop_price
                or quote.best_bid <= item.order.take_profit
            ):
                pending[index] = revised(item, active=False)
                continue
            fill = simulate_fill(
                i.side,
                item.remaining,
                quote,
                s.scenario,
                available=capacity[i.side],
                maker=item.order.maker,
                limit=i.price if item.order.maker else None,
                reference=i.price,
                max_slippage_bps=i.max_slippage_bps,
            )
            if fill is None:
                continue
            capacity[i.side] -= fill.qty
            fills.append(identified_fill(fill, item.order, quote, "ENTRY", i.side))
            previous = next((p for p in positions if p.order.intent.key == i.key), None)
            if previous is None:
                positions.append(
                    PaperPosition(
                        order=item.order,
                        qty=fill.qty,
                        entry_price=fill.price,
                        opened_at=quote.as_of,
                        entry_fees=fill.fee,
                        entry_slippage=fill.slippage_cost,
                    )
                )
            else:
                positions.remove(previous)
                total = previous.qty + fill.qty
                positions.append(
                    revised(
                        previous,
                        qty=total,
                        entry_price=(previous.entry_price * previous.qty + fill.price * fill.qty)
                        / total,
                        entry_fees=previous.entry_fees + fill.fee,
                        entry_slippage=previous.entry_slippage + fill.slippage_cost,
                    )
                )
            remaining = item.remaining - fill.qty
            pending[index] = revised(item, remaining=remaining, active=remaining > 0)
        self._state = BrokerState(
            scenario=s.scenario,
            pending=tuple(pending),
            positions=tuple(positions),
            fills=tuple(fills),
            trades=tuple(trades),
            last_at=quote.as_of,
            last_sequence=quote.sequence,
            quotes=tuple(q for q in s.quotes if q.key != quote.key) + (quote,),
        )

    def on_funding(self, event: Funding) -> None:
        if event.source_at != event.as_of:
            # Late settlement cannot be allocated to inventory that may have changed.
            raise ValueError("LATE_FUNDING_REQUIRES_INVENTORY_HISTORY")
        if self._state.last_at is not None and (
            event.as_of < self._state.last_at
            or (
                event.as_of == self._state.last_at
                and (event.sequence or 0) <= (self._state.last_sequence or 0)
            )
        ):
            raise ValueError("FUNDING_ORDER_VIOLATION")
        positions = tuple(
            revised(
                p,
                funding=(
                    p.funding
                    + p.qty
                    * event.mark_price
                    * event.rate
                    * (1 if p.order.intent.side is Side.LONG else -1)
                )
                or ZERO,
            )
            if p.order.intent.key == event.key and p.opened_at < event.source_at
            else p
            for p in self.positions
        )
        self._state = revised(
            self._state, positions=positions, last_at=event.as_of, last_sequence=event.sequence
        )

    def checkpoint(self) -> str:
        state = self._state.model_dump(mode="json")
        return json.dumps(
            {"state": state, "digest": digest(state)}, sort_keys=True, separators=(",", ":")
        )

    @classmethod
    def restore(cls, checkpoint: str) -> Self:
        payload = json.loads(checkpoint)
        if set(payload) != {"state", "digest"} or digest(payload["state"]) != payload["digest"]:
            raise ValueError("CHECKPOINT_INTEGRITY_FAILURE")
        state = BrokerState.model_validate_json(json.dumps(payload["state"]))
        instance = cls(state.scenario)
        instance._state = state
        return instance


def main(argv: list[str] | None = None) -> int:
    import argparse
    import sys
    from pathlib import Path

    args = sys.argv[1:] if argv is None else argv
    if args and args[0] == "--check-state":
        parser = argparse.ArgumentParser(description="Validate and reconcile local paper state")
        parser.add_argument("--check-state", type=Path, required=True)
        parsed = parser.parse_args(args)
        try:
            broker = PaperBroker.restore(parsed.check_state.read_text(encoding="utf-8"))
        except (ValueError, OSError, KeyError, TypeError):
            parser.error("INVALID_PAPER_CHECKPOINT")
        print(
            json.dumps(
                {
                    "mode": "PAPER",
                    "live": False,
                    "open_positions": len(broker.positions),
                    "fills": len(broker.fills),
                    "trades": len(broker.trades),
                }
            )
        )
        return 0
    # Importing via a separate entry module avoids runpy's duplicate module identities.
    from tradeforge.research.replay import main as replay_main

    return replay_main(args, mode="PAPER")


if __name__ == "__main__":
    raise SystemExit(main())

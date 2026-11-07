"""Conservative quote-liquidity fills; no invented queue priority or OHLC path."""

from decimal import Decimal
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from tradeforge.domain.common import Money, NonNegative, Positive, StrictModel, Text, Timestamp
from tradeforge.domain.decision import Side
from tradeforge.domain.instrument import InstrumentEvent

BPS = Decimal(10_000)
ZERO = Decimal(0)
Rate = Annotated[NonNegative, Field(lt=1)]


class FeeSchedule(StrictModel):
    version: Text
    observed_at: Timestamp
    effective_at: Timestamp
    maker_rate: Rate
    taker_rate: Rate


class Quote(InstrumentEvent):
    best_bid: Positive
    best_ask: Positive
    bid_qty: NonNegative
    ask_qty: NonNegative
    fees: FeeSchedule

    @model_validator(mode="after")
    def coherent(self) -> Self:
        if self.best_bid >= self.best_ask:
            raise ValueError("CROSSED_QUOTE")
        if max(self.fees.observed_at, self.fees.effective_at) > self.as_of:
            raise ValueError("FUTURE_FEE_SCHEDULE")
        return self


class FillScenario(StrictModel):
    version: Text = "conservative-v1"
    latency_ms: Annotated[int, Field(ge=0, le=3_600_000)] = 100
    slippage_bps: Annotated[NonNegative, Field(lt=10_000)] = Decimal("5")
    max_quote_age_ms: Annotated[int, Field(ge=0, le=3_600_000)] = 5_000


class Funding(InstrumentEvent):
    """A recorded settlement, charged only to inventory held at source_at."""

    rate: Annotated[Money, Field(gt=-1, lt=1)]
    mark_price: Positive


class Fill(StrictModel):
    at: Timestamp
    qty: Positive
    price: Positive
    fee: NonNegative
    slippage_cost: NonNegative
    fee_version: Text
    liquidity: Literal["MAKER", "TAKER"]


def simulate_fill(
    side: Side,
    qty: Decimal,
    quote: Quote,
    scenario: FillScenario,
    *,
    available: Decimal,
    maker: bool = False,
    limit: Decimal | None = None,
    reference: Decimal | None = None,
    max_slippage_bps: Decimal | None = None,
) -> Fill | None:
    if side is Side.NO_TRADE or qty <= 0 or available < 0:
        raise ValueError("INVALID_FILL_REQUEST")
    if (quote.as_of - quote.source_at).total_seconds() * 1000 > scenario.max_quote_age_ms:
        return None
    amount = min(qty, available)
    if amount <= 0:
        return None
    buy = side is Side.LONG
    touch = quote.best_ask if buy else quote.best_bid
    if maker:
        if limit is None:
            raise ValueError("MAKER_LIMIT_REQUIRED")
        # Touching does not establish that our place in the queue traded.
        if not (touch < limit if buy else touch > limit):
            return None
        price, slip = limit, ZERO
    else:
        slip = touch * scenario.slippage_bps / BPS
        price = touch + slip if buy else touch - slip
        if limit is not None and (price > limit if buy else price < limit):
            return None
    if reference is not None and max_slippage_bps is not None:
        adverse = (price - reference) if buy else (reference - price)
        if adverse / reference * BPS > max_slippage_bps:
            return None
    rate = quote.fees.maker_rate if maker else quote.fees.taker_rate
    return Fill(
        at=quote.as_of,
        qty=amount,
        price=price,
        fee=amount * price * rate,
        slippage_cost=amount * slip,
        fee_version=quote.fees.version,
        liquidity="MAKER" if maker else "TAKER",
    )

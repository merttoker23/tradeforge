"""Deterministic canonical-market fixtures; no wall clock or exchange access."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import ClosedBar, Timeframe

START = datetime(2026, 10, 15, tzinfo=UTC)


def instrument(symbol="BTCUSDT"):
    return InstrumentKey(
        symbol=symbol,
        exchange="BINANCE",
        market_type="USD_M",
        quote="USDT",
        margin_asset="USDT",
        contract_type="PERPETUAL",
    )


def bar(index=0, timeframe=Timeframe.M15, symbol="BTCUSDT", delay=0, close="101"):
    opened = START + timedelta(seconds=index * timeframe.seconds)
    closed = opened + timedelta(seconds=timeframe.seconds) - timedelta(milliseconds=1)
    observed = closed + timedelta(seconds=1 + delay)
    return ClosedBar.model_validate(
        dict(
            **instrument(symbol).model_dump(),
            schema_version=1,
            event_id=uuid5(NAMESPACE_URL, f"{symbol}/{timeframe}/{index}/{delay}/{close}"),
            source_at=closed,
            observed_at=observed,
            as_of=observed,
            timeframe=timeframe,
            open_at=opened,
            close_at=closed,
            open=Decimal("100"),
            high=Decimal("110"),
            low=Decimal("90"),
            close=Decimal(close),
            volume=Decimal("2"),
            source="fixture",
            complete=True,
        ),
        context={"now": observed},
    )

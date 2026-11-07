"""Strict Python fixtures: no exchange network, credentials or wall-clock dependency."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest

NOW = datetime(2026, 10, 15, 12, tzinfo=UTC)


@pytest.fixture
def event() -> dict[str, object]:
    return {
        "schema_version": 1,
        "event_id": UUID("00000000-0000-4000-8000-000000000001"),
        "source_at": NOW - timedelta(seconds=1),
        "observed_at": NOW,
        "as_of": NOW,
    }


@pytest.fixture
def identity() -> dict[str, object]:
    return {
        "symbol": "BTCUSDT",
        "exchange": "BINANCE",
        "market_type": "USD_M",
        "quote": "USDT",
        "margin_asset": "USDT",
        "contract_type": "PERPETUAL",
    }


@pytest.fixture
def bar(event: dict[str, object], identity: dict[str, object]) -> dict[str, object]:
    from tradeforge.domain.market import Timeframe

    return {
        **event,
        **identity,
        "source_at": NOW,
        "timeframe": Timeframe.M1,
        "open_at": NOW - timedelta(minutes=1),
        "close_at": NOW - timedelta(milliseconds=1),
        "open": Decimal("100"),
        "high": Decimal("110"),
        "low": Decimal("90"),
        "close": Decimal("105"),
        "volume": Decimal("12"),
        "source": "recorded",
        "complete": True,
    }

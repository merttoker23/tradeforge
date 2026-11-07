"""All chart intervals use validated native closed bars without changing quant resampling."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import Timeframe

KEY = InstrumentKey(
    symbol="BTCUSDT",
    exchange="BINANCE",
    market_type="USD_M",
    quote="USDT",
    margin_asset="USDT",
    contract_type="PERPETUAL",
)


@pytest.mark.parametrize(
    "timeframe", [Timeframe.M1, Timeframe.M5, Timeframe.M15, Timeframe.H1, Timeframe.H4]
)
def test_native_chart_history_has_no_open_candles_or_fabricated_bars(timeframe):
    from tradeforge.market.chart_history import fetch_chart

    end = datetime(2026, 10, 23, tzinfo=UTC)
    start = end - timedelta(seconds=300 * timeframe.seconds)

    class Client:
        async def fetch(self, path, params):
            assert path == "/fapi/v1/klines" and params["interval"] == timeframe.value
            rows = []
            for i in range(300):
                at = int((start + timedelta(seconds=i * timeframe.seconds)).timestamp() * 1000)
                rows.append(
                    [
                        at,
                        "100",
                        "110",
                        "90",
                        "105",
                        "2",
                        at + timeframe.seconds * 1000 - 1,
                        "0",
                        1,
                        "0",
                        "0",
                        "0",
                    ]
                )
            return rows, end

    view = asyncio.run(fetch_chart(Client(), KEY, timeframe, end))
    assert len(view.bars) == 300
    assert view.bars[-1].close == Decimal("105")
    assert view.bars[-1].close_at < end


def test_gap_in_chart_response_is_rejected():
    from tradeforge.market.chart_history import fetch_chart

    class Client:
        async def fetch(self, *args):
            return [], datetime(2026, 10, 23, tzinfo=UTC)

    with pytest.raises(ValueError):
        asyncio.run(fetch_chart(Client(), KEY, Timeframe.M1, datetime(2026, 10, 23, tzinfo=UTC)))

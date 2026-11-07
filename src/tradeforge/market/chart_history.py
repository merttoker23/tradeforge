"""Native public chart bars, separate from the quant engine's derived timeframes."""

from datetime import datetime, timedelta

from sqlalchemy import text

from tradeforge.domain.common import DataQuality, utc
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import Timeframe
from tradeforge.market.bars import canonical, make_bar
from tradeforge.market.exchange_info import milliseconds
from tradeforge.market.market_state import BarView
from tradeforge.market.recovery import PublicMarketClient
from tradeforge.market.streams import decimal_field
from tradeforge.persistence.db import Database
from tradeforge.persistence.repositories import instrument_id


async def fetch_chart(
    client: PublicMarketClient, key: InstrumentKey, timeframe: Timeframe, end: datetime
) -> BarView:
    utc(end)
    if (
        timeframe not in {Timeframe.M1, Timeframe.M5, Timeframe.M15, Timeframe.H1, Timeframe.H4}
        or end.timestamp() % timeframe.seconds
    ):
        raise ValueError("CHART_QUERY_INVALID")
    start = end - timedelta(seconds=300 * timeframe.seconds)
    raw, observed = await client.fetch(
        "/fapi/v1/klines",
        {
            "symbol": key.symbol,
            "interval": timeframe.value,
            "startTime": str(int(start.timestamp() * 1000)),
            "endTime": str(int(end.timestamp() * 1000) - 1),
            "limit": "300",
        },
    )
    if not isinstance(raw, list) or not 1 <= len(raw) <= 300:
        raise ValueError("CHART_HISTORY_MISSING")
    bars = []
    previous = None
    for row in raw:
        if not isinstance(row, list) or len(row) != 12:
            raise ValueError("CHART_HISTORY_INVALID")
        opened, closed = milliseconds(row[0]), milliseconds(row[6])
        if (
            opened < start
            or closed >= end
            or closed > observed
            or (previous is not None and opened != previous + timedelta(seconds=timeframe.seconds))
        ):
            raise ValueError("CHART_HISTORY_INVALID")
        prices = {
            name: decimal_field({name: row[index]}, name)
            for name, index in (("open", 1), ("high", 2), ("low", 3), ("close", 4), ("volume", 5))
        }
        bars.append(
            make_bar(key, timeframe, opened, closed, prices, closed, observed, "binance:rest")
        )
        previous = opened
    return BarView(key, timeframe, observed, tuple(bars), DataQuality.OK)


def save_chart(db: Database, view: BarView) -> None:
    if not view.bars or len(view.bars) > 300:
        raise ValueError("CHART_HISTORY_INVALID")
    with db.transaction() as c:
        c.execute(
            text("""
          INSERT INTO terminal_chart_cache(instrument_id,timeframe,as_of,payload)
          VALUES(:id,:tf,:at,CAST(:payload AS jsonb))
          ON CONFLICT(instrument_id,timeframe) DO UPDATE SET
            as_of=EXCLUDED.as_of,payload=EXCLUDED.payload
          WHERE terminal_chart_cache.as_of<EXCLUDED.as_of
        """),
            {
                "id": instrument_id(view.instrument),
                "tf": view.timeframe.value,
                "at": view.as_of,
                "payload": canonical({"bars": [bar.model_dump(mode="json") for bar in view.bars]}),
            },
        )

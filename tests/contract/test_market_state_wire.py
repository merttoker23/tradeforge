import json
from dataclasses import replace
from datetime import timedelta

import pytest

from tests.market_fixtures import START, instrument
from tests.unit.test_market_state_depth_flow import event
from tradeforge.domain.market import Timeframe
from tradeforge.market.bars import closed_kline
from tradeforge.market.streams import StreamSpec


def kline(complete):
    source = START + timedelta(minutes=1)
    row = {
        "k": {
            "s": "BTCUSDT",
            "i": "1m",
            "x": complete,
            "t": int(START.timestamp() * 1000),
            "T": int(source.timestamp() * 1000) - 1,
            "o": "100",
            "h": "105",
            "l": "90",
            "c": "101",
            "v": "2",
        }
    }
    return replace(
        event(),
        stream=StreamSpec("btcusdt@kline_1m"),
        event_time=source,
        received_at=source,
        payload_json=json.dumps(row),
    )


def test_unclosed_never_enters_finalized_history():
    assert closed_kline(kline(False), instrument()) is None
    result = closed_kline(kline(True), instrument())
    assert result.complete and result.timeframe is Timeframe.M1


@pytest.mark.parametrize(
    "field,value", [("h", "99"), ("l", "102"), ("v", "NaN"), ("x", 1), ("t", True), ("i", "15m")]
)
def test_invalid_kline_fails_closed(field, value):
    original = kline(True)
    row = json.loads(original.payload_json)
    row["k"][field] = value
    with pytest.raises(ValueError):
        closed_kline(replace(original, payload_json=json.dumps(row)), instrument())

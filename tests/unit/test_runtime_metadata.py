"""Public production filter observed during PHASE_16A; never ignore restrictions."""

from datetime import UTC, datetime

import pytest

from tests.universe_fixtures import payload, symbol
from tradeforge.market.exchange_info import parse_exchange_info


def test_unrestricted_position_control_is_preserved_in_metadata():
    row = symbol()
    row["filters"].append({"filterType": "POSITION_RISK_CONTROL", "positionControlSide": "NONE"})
    result = parse_exchange_info(payload(row), datetime.now(UTC))
    assert len(result.instruments) == 1
    assert "POSITION_RISK_CONTROL" in result.instruments[0].constraints_json


@pytest.mark.parametrize("value", ["LONG", "SHORT", "BOTH", "INVALID", None])
def test_restricted_or_unknown_position_control_fails_closed(value):
    row = symbol()
    row["filters"].append({"filterType": "POSITION_RISK_CONTROL", "positionControlSide": value})
    result = parse_exchange_info(payload(row), datetime.now(UTC))
    assert result.instruments == ()
    assert result.excluded[0].reason == "UNSUPPORTED_POSITION_CONTROL"


def test_unsupported_native_symbol_does_not_disable_supported_discovery():
    result = parse_exchange_info(payload(symbol(), symbol("TEST_NAMEUSDT")), datetime.now(UTC))
    assert [row.symbol for row in result.instruments] == ["BTCUSDT"]
    assert result.excluded[0].reason == "UNSUPPORTED_SYMBOL_FORMAT"

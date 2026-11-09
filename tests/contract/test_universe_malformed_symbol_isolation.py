"""A broken Testnet symbol must not disable valid market discovery."""

from copy import deepcopy

import pytest

from tests.universe_fixtures import NOW, payload, symbol
from tradeforge.market.exchange_info import parse_exchange_info


def _zero_order_limit(name: str) -> dict:
    row = symbol(name)
    next(f for f in row["filters"] if f["filterType"] == "MAX_NUM_ORDERS")["limit"] = 0
    return row


def _unusable_numeric_limit(name: str) -> dict:
    row = symbol(name)
    next(f for f in row["filters"] if f["filterType"] == "MIN_NOTIONAL")["notional"] = "0"
    return row


def test_testnet_bad_symbols_are_excluded_without_admitting_invalid_filters():
    data = payload(
        symbol("BTCUSDT"),
        _zero_order_limit("XMRUSDT"),
        _unusable_numeric_limit("TUSDT"),
        symbol("ETHUSDT"),
    )
    result = parse_exchange_info(data, NOW)
    assert result.total == 4
    assert [item.symbol for item in result.instruments] == ["BTCUSDT", "ETHUSDT"]
    assert [(item.symbol, item.reason) for item in result.excluded] == [
        ("TUSDT", "UNSUPPORTED_FILTER_VALUE"),
        ("XMRUSDT", "INVALID_ORDER_LIMIT"),
    ]
    assert result.weight_limit == 2400


def test_six_unusable_order_caps_and_one_invalid_numeric_constraint():
    bad = [
        _zero_order_limit(name)
        for name in ("XMRUSDT", "ETHFIUSDT", "PORT3USDT", "1000000BOBUSDT", "SKATEUSDT", "HOMEUSDT")
    ]
    data = payload(symbol("BTCUSDT"), *bad, _unusable_numeric_limit("TUSDT"))
    result = parse_exchange_info(data, NOW)
    assert [item.symbol for item in result.instruments] == ["BTCUSDT"]
    assert len(result.excluded) == 7
    assert all(item.symbol != "BTCUSDT" for item in result.excluded)


@pytest.mark.parametrize(
    "bad",
    [
        _zero_order_limit("XMRUSDT"),
        _unusable_numeric_limit("TUSDT"),
    ],
)
def test_only_unusable_symbols_cannot_produce_healthy_empty_universe(bad):
    with pytest.raises(ValueError, match="UNUSABLE_UNIVERSE_FILTERS"):
        parse_exchange_info(payload(bad), NOW)


def test_unknown_filter_still_halts_full_universe_refresh():
    bad = deepcopy(symbol("XMRUSDT"))
    bad["filters"].append({"filterType": "UNSUPPORTED_NEW_ORDER_RESTRICTION"})
    with pytest.raises(ValueError, match="UNSUPPORTED_OR_DUPLICATE_FILTER"):
        parse_exchange_info(payload(symbol("BTCUSDT"), bad), NOW)


def test_duplicate_symbols_remain_global_rejection():
    with pytest.raises(ValueError, match="DUPLICATE_SYMBOL"):
        parse_exchange_info(payload(symbol("BTCUSDT"), symbol("BTCUSDT")), NOW)

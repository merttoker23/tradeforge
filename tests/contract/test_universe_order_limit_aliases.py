"""Testnet/Production USD-M order-limit metadata compatibility."""

from copy import deepcopy

import pytest

from tests.universe_fixtures import NOW, payload
from tradeforge.market.exchange_info import parse_exchange_info


@pytest.mark.parametrize(
    ("filter_type", "alias"),
    [
        ("MAX_NUM_ORDERS", "maxNumOrders"),
        ("MAX_NUM_ALGO_ORDERS", "maxNumAlgoOrders"),
    ],
)
def test_documented_typed_order_limit_alias_is_accepted(filter_type, alias):
    response = payload()
    for item in response["symbols"][0]["filters"]:
        if item["filterType"] == filter_type:
            item[alias] = item.pop("limit")
    assert len(parse_exchange_info(response, NOW).instruments) == 1


def test_original_limit_fields_are_still_supported():
    assert len(parse_exchange_info(payload(), NOW).instruments) == 1


@pytest.mark.parametrize(
    ("filter_type", "alias"),
    [
        ("MAX_NUM_ORDERS", "maxNumOrders"),
        ("MAX_NUM_ALGO_ORDERS", "maxNumAlgoOrders"),
    ],
)
@pytest.mark.parametrize("invalid", [0, -1, True, "200", None])
def test_invalid_typed_order_limits_remain_rejected(filter_type, alias, invalid):
    response = payload()
    for item in response["symbols"][0]["filters"]:
        if item["filterType"] == filter_type:
            item.pop("limit")
            item[alias] = invalid
    with pytest.raises(ValueError, match="UNUSABLE_UNIVERSE_FILTERS"):
        parse_exchange_info(response, NOW)


def test_mismatched_legacy_and_typed_limit_is_rejected():
    response = deepcopy(payload())
    for item in response["symbols"][0]["filters"]:
        if item["filterType"] == "MAX_NUM_ORDERS":
            item["maxNumOrders"] = item["limit"] + 1
    with pytest.raises(ValueError, match="UNUSABLE_UNIVERSE_FILTERS"):
        parse_exchange_info(response, NOW)


def test_matching_legacy_and_typed_limit_is_accepted():
    response = payload()
    for item in response["symbols"][0]["filters"]:
        if item["filterType"] == "MAX_NUM_ORDERS":
            item["maxNumOrders"] = item["limit"]
    assert len(parse_exchange_info(response, NOW).instruments) == 1

from copy import deepcopy
from datetime import timedelta
from decimal import Decimal

import pytest

from tests.universe_fixtures import NOW, payload, symbol
from tradeforge.market.exchange_info import parse_exchange_info


def test_all_eligible_sorted_without_whitelist_and_exact_filters():
    data = payload(*(symbol(f"TOKEN{i}USDT") for i in range(75)))
    parsed = parse_exchange_info(data, NOW)
    assert len(parsed.instruments) == 75
    assert [i.symbol for i in parsed.instruments] == sorted(s["symbol"] for s in data["symbols"])
    first = parsed.instruments[0]
    assert first.filters.tick_size == Decimal("0.010")
    assert first.filters.step_size == Decimal("0.001")
    assert first.market_lot.step_size == Decimal("0.002")
    assert first.market_lot.max_qty == Decimal("500")
    assert first.source_at == first.observed_at == NOW
    assert parsed.total == 75 and not parsed.excluded


@pytest.mark.parametrize(
    "change",
    [
        {"status": "SETTLING"},
        {"status": "FUTURE_UNKNOWN_STATUS"},
        {"contractType": "CURRENT_QUARTER"},
        {"quoteAsset": "USDC"},
        {"marginAsset": "BTC"},
        {"marketType": "COIN_M"},
        {"symbol": "BTCUSD_PERP"},
        {"onboardDate": int((NOW + timedelta(days=1)).timestamp() * 1000)},
        {"deliveryDate": int(NOW.timestamp() * 1000)},
    ],
)
def test_ineligible_contract_excluded_without_affecting_other_symbols(change):
    parsed = parse_exchange_info(payload(symbol("OTHERUSDT"), symbol(**change)), NOW)
    assert [i.symbol for i in parsed.instruments] == ["OTHERUSDT"]
    assert parsed.total == 2 and len(parsed.excluded) == 1


@pytest.mark.parametrize(
    "field",
    [
        "status",
        "contractType",
        "quoteAsset",
        "marginAsset",
        "onboardDate",
        "deliveryDate",
        "orderTypes",
        "filters",
    ],
)
def test_missing_safety_metadata_invalidates_whole_refresh(field):
    data = payload()
    del data["symbols"][0][field]
    with pytest.raises(ValueError):
        parse_exchange_info(data, NOW)


@pytest.mark.parametrize(
    "value",
    [None, [], {}, {"symbols": []}, {"symbols": [None]}, {"code": -1}, payload(symbol(), symbol())],
)
def test_empty_wrong_and_duplicate_responses_rejected(value):
    with pytest.raises(ValueError):
        parse_exchange_info(value, NOW)


@pytest.mark.parametrize(
    "index,field,value",
    [
        (0, "tickSize", "NaN"),
        (0, "tickSize", "0"),
        (0, "tickSize", 0.01),
        (0, "minPrice", "-1"),
        (0, "maxPrice", "0.001"),
        (1, "stepSize", "Infinity"),
        (1, "maxQty", "0.0001"),
        (2, "minQty", None),
        (2, "stepSize", "-0"),
        (3, "notional", "0"),
        (4, "multiplierUp", "0.5"),
        (5, "limit", True),
    ],
)
def test_invalid_constraints_fail_closed(index, field, value):
    data = payload()
    data["symbols"][0]["filters"][index][field] = value
    with pytest.raises(ValueError):
        parse_exchange_info(data, NOW)


@pytest.mark.parametrize(
    "mutation", ["missing_market", "duplicate", "unknown", "no_market_order", "exchange_filter"]
)
def test_incomplete_or_unknown_constraints_rejected(mutation):
    data = payload()
    row = data["symbols"][0]
    if mutation == "missing_market":
        row["filters"].pop(2)
    elif mutation == "duplicate":
        row["filters"].append(deepcopy(row["filters"][0]))
    elif mutation == "unknown":
        row["filters"].append({"filterType": "NEW_RESTRICTION"})
    elif mutation == "no_market_order":
        row["orderTypes"] = ["LIMIT"]
    else:
        data["exchangeFilters"] = [{"filterType": "NEW_EXCHANGE_RULE"}]
    with pytest.raises(ValueError):
        parse_exchange_info(data, NOW)


def test_wire_order_does_not_change_canonical_metadata():
    data = payload(symbol("ETHUSDT"), symbol())
    other = deepcopy(data)
    other["symbols"].reverse()
    for row in other["symbols"]:
        row["filters"].reverse()
    assert parse_exchange_info(data, NOW) == parse_exchange_info(other, NOW)

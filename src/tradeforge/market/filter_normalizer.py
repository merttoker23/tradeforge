"""Conservative exact exchange filters; precision fields are never tick/lot sizes."""

import json
from decimal import Decimal, InvalidOperation
from typing import Self

from pydantic import model_validator

from tradeforge.domain.common import Positive, StrictModel
from tradeforge.domain.instrument import InstrumentFilters


class QuantityLimits(StrictModel):
    min_qty: Positive
    max_qty: Positive
    step_size: Positive

    @model_validator(mode="after")
    def coherent(self) -> Self:
        if self.min_qty > self.max_qty or self.step_size > self.max_qty:
            raise ValueError("UNUSABLE_QUANTITY_LIMITS")
        return self


def decimal_field(row: dict[str, object], key: str) -> Decimal:
    value = row.get(key)
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError("DECIMAL_STRING_REQUIRED")
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise ValueError("INVALID_FILTER_DECIMAL") from None
    if not number.is_finite() or number <= 0 or abs(number.adjusted()) > 24:
        raise ValueError("UNSUPPORTED_FILTER_VALUE")
    return number


def normalize_filters(raw: object) -> tuple[InstrumentFilters, QuantityLimits, str]:
    if not isinstance(raw, list) or not raw:
        raise ValueError("MISSING_FILTERS")
    filters: dict[str, dict[str, object]] = {}
    known = {
        "PRICE_FILTER",
        "LOT_SIZE",
        "MARKET_LOT_SIZE",
        "MIN_NOTIONAL",
        "PERCENT_PRICE",
        "MAX_NUM_ORDERS",
        "MAX_NUM_ALGO_ORDERS",
        "POSITION_RISK_CONTROL",
    }
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("INVALID_FILTER")
        name = item.get("filterType")
        if not isinstance(name, str) or name not in known or name in filters:
            raise ValueError("UNSUPPORTED_OR_DUPLICATE_FILTER")
        filters[name] = item
        if name == "POSITION_RISK_CONTROL" and (
            set(item) != {"filterType", "positionControlSide"}
            or item.get("positionControlSide") != "NONE"
        ):
            raise ValueError("UNSUPPORTED_POSITION_CONTROL")
        if name in {"MAX_NUM_ORDERS", "MAX_NUM_ALGO_ORDERS"}:
            if type(item.get("limit")) is not int or item["limit"] <= 0:
                raise ValueError("INVALID_ORDER_LIMIT")
        if name == "PERCENT_PRICE":
            if (
                not decimal_field(item, "multiplierDown")
                <= 1
                <= decimal_field(item, "multiplierUp")
            ):
                raise ValueError("INVALID_PERCENT_PRICE")
    if not {"PRICE_FILTER", "LOT_SIZE", "MARKET_LOT_SIZE", "MIN_NOTIONAL"} <= filters.keys():
        raise ValueError("MISSING_REQUIRED_FILTER")
    price, lot, market, notional = (
        filters[k] for k in ("PRICE_FILTER", "LOT_SIZE", "MARKET_LOT_SIZE", "MIN_NOTIONAL")
    )
    normal = InstrumentFilters(
        tick_size=decimal_field(price, "tickSize"),
        min_price=decimal_field(price, "minPrice"),
        max_price=decimal_field(price, "maxPrice"),
        step_size=decimal_field(lot, "stepSize"),
        min_qty=decimal_field(lot, "minQty"),
        max_qty=decimal_field(lot, "maxQty"),
        min_notional=decimal_field(notional, "notional"),
    )
    market_lot = QuantityLimits(
        min_qty=decimal_field(market, "minQty"),
        max_qty=decimal_field(market, "maxQty"),
        step_size=decimal_field(market, "stepSize"),
    )
    canonical = json.dumps(
        [filters[k] for k in sorted(filters)],
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return normal, market_lot, canonical

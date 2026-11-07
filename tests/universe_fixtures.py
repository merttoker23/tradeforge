"""Synthetic exchangeInfo wire fixtures shaped by official Binance documentation."""

from copy import deepcopy
from datetime import UTC, datetime

NOW = datetime(2026, 10, 15, 12, tzinfo=UTC)


def symbol(name="BTCUSDT", **changes):
    return {
        "symbol": name,
        "pair": name,
        "contractType": "PERPETUAL",
        "status": "TRADING",
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "onboardDate": 1598252400000,
        "deliveryDate": 4133404800000,
        "pricePrecision": 1,
        "quantityPrecision": 0,
        "orderTypes": ["LIMIT", "MARKET", "STOP_MARKET"],
        "filters": [
            {
                "filterType": "PRICE_FILTER",
                "minPrice": "0.010",
                "maxPrice": "1000000",
                "tickSize": "0.010",
            },
            {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "1000", "stepSize": "0.001"},
            {
                "filterType": "MARKET_LOT_SIZE",
                "minQty": "0.002",
                "maxQty": "500",
                "stepSize": "0.002",
            },
            {"filterType": "MIN_NOTIONAL", "notional": "5"},
            {
                "filterType": "PERCENT_PRICE",
                "multiplierUp": "1.15",
                "multiplierDown": "0.85",
                "multiplierDecimal": "4",
            },
            {"filterType": "MAX_NUM_ORDERS", "limit": 200},
            {"filterType": "MAX_NUM_ALGO_ORDERS", "limit": 10},
        ],
        **changes,
    }


def payload(*symbols):
    return deepcopy(
        {
            "timezone": "UTC",
            "serverTime": 1565613908500,  # Documented as ignored, never observation time.
            "exchangeFilters": [],
            "rateLimits": [
                {
                    "rateLimitType": "REQUEST_WEIGHT",
                    "interval": "MINUTE",
                    "intervalNum": 1,
                    "limit": 2400,
                }
            ],
            "symbols": list(symbols or [symbol()]),
        }
    )

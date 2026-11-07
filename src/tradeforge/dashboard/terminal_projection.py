"""Never forward arbitrary nested JSON from an account audit row to the browser."""

import re
from decimal import Decimal
from typing import Any

DECIMALS = {
    "daily_pnl",
    "total_pnl",
    "total_risk",
    "unrealized_pnl",
    "qty",
    "entry_price",
    "mark_price",
    "price",
    "stop_price",
    "commission",
    "realized_pnl",
}
FIELDS = {
    "symbol",
    "side",
    "qty",
    "entry_price",
    "mark_price",
    "unrealized_pnl",
    "kind",
    "status",
    "price",
    "stop_price",
    "commission",
    "commission_asset",
    "realized_pnl",
    "as_of",
}


def sanitize_account(raw: object) -> dict[str, Any] | None:
    if not isinstance(raw, dict) or raw.get("environment") != "BINANCE_FUTURES_DEMO":
        return None
    result: dict[str, Any] = {"environment": "BINANCE_FUTURES_DEMO"}
    for key in ("daily_pnl", "total_pnl", "total_risk", "unrealized_pnl"):
        value = raw.get(key)
        result[key] = value if valid_decimal(value) else None
    result["reason_codes"] = [
        x
        for x in (raw.get("reason_codes") or [])[:32]
        if isinstance(x, str) and re.fullmatch(r"[A-Z][A-Z0-9_]{0,95}", x)
    ]
    for name in ("positions", "orders", "stops", "history"):
        result[name] = []
        rows = raw.get(name)
        for row in (rows if isinstance(rows, list) else [])[:100]:
            if not isinstance(row, dict):
                continue
            safe = {}
            for key in FIELDS & row.keys():
                value = row[key]
                if key in DECIMALS:
                    safe[key] = value if valid_decimal(value) else None
                elif (
                    isinstance(value, str)
                    and len(value) <= 64
                    and re.fullmatch(r"[A-Za-z0-9_.:+-]+", value)
                ):
                    safe[key] = value
            result[name].append(safe)
    return result


def valid_decimal(value: object) -> bool:
    try:
        return isinstance(value, str) and len(value) <= 64 and Decimal(value).is_finite()
    except ArithmeticError:
        return False

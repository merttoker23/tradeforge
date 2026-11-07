"""Bounded allowlisted Demo read projection. No public prices or derived order authority."""

import re
from decimal import Decimal
from typing import Any

from tradeforge.execution.binance_client import BinanceTestnetClient, number
from tradeforge.market.exchange_info import milliseconds


def symbol(row: dict[str, Any]) -> str:
    value = row.get("symbol")
    if not isinstance(value, str) or re.fullmatch(r"[A-Z0-9_]{2,40}", value) is None:
        raise ValueError("DEMO_RESPONSE_INVALID")
    return value


def terminal_account(
    client: BinanceTestnetClient,
    account: dict[str, Any],
    positions: list[Any],
    orders: list[Any],
    algos: list[Any],
) -> dict[str, Any]:
    at = client.clock()
    result: dict[str, Any] = {
        "environment": "BINANCE_FUTURES_DEMO",
        "as_of": at.isoformat(),
        "positions": [],
        "orders": [],
        "stops": [],
        "history": [],
        "daily_pnl": None,
        "total_pnl": None,
        "total_risk": None,
        "unrealized_pnl": None,
        "history_complete": False,
        "reason_codes": ["LIFETIME_PNL_UNAVAILABLE", "RISK_PROOF_UNAVAILABLE"],
    }
    try:
        if "totalUnrealizedProfit" in account:
            result["unrealized_pnl"] = str(number(account["totalUnrealizedProfit"]))
        active = [row for row in positions if number(row["positionAmt"]) != 0]
        if len(active) > 100 or len(orders) + len(algos) > 100:
            result["reason_codes"].append("ACCOUNT_ROWS_TRUNCATED")
        for row in active[:100]:
            qty = number(row["positionAmt"])
            result["positions"].append(
                {
                    "symbol": symbol(row),
                    "side": "LONG" if qty > 0 else "SHORT",
                    "qty": str(abs(qty)),
                    "entry_price": str(number(row["entryPrice"])),
                    "mark_price": str(number(row["markPrice"])),
                    "unrealized_pnl": str(number(row["unRealizedProfit"])),
                }
            )
        for row in (orders + algos)[:100]:
            kind = row.get("type", row.get("orderType"))
            if kind not in {
                "LIMIT",
                "MARKET",
                "STOP",
                "STOP_MARKET",
                "TAKE_PROFIT",
                "TAKE_PROFIT_MARKET",
                "TRAILING_STOP_MARKET",
            }:
                kind = "UNKNOWN"
            status = row.get("status", row.get("algoStatus"))
            if status not in {
                "NEW",
                "PARTIALLY_FILLED",
                "FILLED",
                "CANCELED",
                "REJECTED",
                "EXPIRED",
                "FINISHED",
                "TRIGGERED",
            }:
                status = "UNKNOWN"
            item = {
                "symbol": symbol(row),
                "kind": kind,
                "status": status,
                "side": row.get("side") if row.get("side") in {"BUY", "SELL"} else "UNKNOWN",
                "qty": str(number(row.get("origQty", row.get("quantity", "0")))),
                "price": str(number(row.get("price", "0"))),
                "stop_price": str(number(row.get("stopPrice", row.get("triggerPrice", "0")))),
            }
            result[
                "stops"
                if kind
                in {
                    "STOP",
                    "STOP_MARKET",
                    "TAKE_PROFIT",
                    "TAKE_PROFIT_MARKET",
                    "TRAILING_STOP_MARKET",
                }
                else "orders"
            ].append(item)
    except (ValueError, KeyError, TypeError, ArithmeticError):
        result.update(positions=[], orders=[], stops=[], unrealized_pnl=None)
        result["reason_codes"].append("DEMO_ACCOUNT_DETAILS_INVALID")
    try:
        start = at.replace(hour=0, minute=0, second=0, microsecond=0)
        income = client.request(
            "GET",
            "/fapi/v1/income",
            {
                "startTime": str(int(start.timestamp() * 1000)),
                "endTime": str(int(at.timestamp() * 1000)),
                "limit": "1000",
            },
        )
        if not isinstance(income, list) or len(income) >= 1000:
            raise ValueError
        daily = Decimal(0)
        for row in income:
            if not isinstance(row, dict) or not start <= milliseconds(row["time"]) <= at:
                raise ValueError
            if row.get("incomeType") in {"REALIZED_PNL", "COMMISSION", "FUNDING_FEE"}:
                if row.get("asset") != "USDT":
                    raise ValueError
                daily += number(row["income"])
        result["daily_pnl"] = str(daily)
        # Only a bounded, explicitly scoped execution history is presented.
        symbols = sorted(
            {symbol(row) for row in active[:100]}
            | {symbol(row) for row in income if row.get("symbol")}
        )
        for member in symbols[:5]:
            trades = client.request(
                "GET", "/fapi/v1/userTrades", {"symbol": member, "limit": "100"}
            )
            if not isinstance(trades, list) or len(trades) > 100:
                raise ValueError
            for row in trades:
                if (
                    not isinstance(row, dict)
                    or symbol(row) != member
                    or row.get("side") not in {"BUY", "SELL"}
                ):
                    raise ValueError
                stamp = milliseconds(row["time"])
                if stamp > at:
                    raise ValueError
                asset = row.get("commissionAsset")
                if not isinstance(asset, str) or re.fullmatch(r"[A-Z0-9]{2,20}", asset) is None:
                    raise ValueError
                result["history"].append(
                    {
                        "symbol": member,
                        "side": row["side"],
                        "price": str(number(row["price"])),
                        "qty": str(number(row["qty"])),
                        "commission": str(number(row["commission"])),
                        "commission_asset": asset,
                        "realized_pnl": str(number(row["realizedPnl"])),
                        "as_of": stamp.isoformat(),
                    }
                )
        result["history"] = sorted(result["history"], key=lambda x: x["as_of"], reverse=True)[:100]
        result["reason_codes"].append("HISTORY_RECENT_SYMBOLS_ONLY")
    except Exception:
        result["daily_pnl"] = None
        result["reason_codes"].append("INCOME_HISTORY_INCOMPLETE")
    return result

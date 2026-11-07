"""GET-only Futures Demo diagnostics. Account flags are not API-key permission proof."""

from datetime import timedelta
from typing import Any

from tradeforge.execution.binance_client import BinanceTestnetClient, number
from tradeforge.execution.broker import AckUnknown, ExchangeRejected
from tradeforge.execution.diagnostics_transport import diagnostic_client
from tradeforge.execution.transport_diagnostics import safe_exchange_reason
from tradeforge.market.exchange_info import milliseconds


def preflight(client: BinanceTestnetClient, *, terminal: bool = False) -> dict[str, Any]:
    with diagnostic_client(client) as probe:
        result = _preflight(probe, terminal=terminal)
        result["account_requests"] = list(probe.account_requests)
        if result["failed_stage"] and probe.account_requests:
            failed = result["account_requests"][-1]
            if failed["path"] == result["failed_stage"] and failed["reason"] == "OK":
                failed["reason"] = result["reason_codes"][-1]
        return result


def _preflight(client: BinanceTestnetClient, *, terminal: bool = False) -> dict[str, Any]:
    at = client.clock()
    result: dict[str, Any] = {
        "environment": "BINANCE_FUTURES_DEMO",
        "as_of": at.isoformat(),
        "expires_at": (at + timedelta(seconds=30)).isoformat(),
        "connection": "UNKNOWN",
        "account": "UNKNOWN",
        "balance": "UNKNOWN",
        "positions": "UNKNOWN",
        "clock": "UNKNOWN",
        "key_permissions": "UNKNOWN",
        "account_can_trade": None,
        "equity": None,
        "available_balance": None,
        "position_count": None,
        "open_order_count": None,
        "ready": False,
        "live": False,
        "orders_enabled": False,
        "reason_codes": [],
        "failed_stage": None,
    }
    reasons = result["reason_codes"]

    def request(path: str, *, signed: bool = True) -> object:
        result["failed_stage"] = path
        return client.request("GET", path, signed=signed)

    try:
        if request("/fapi/v1/ping", signed=False) != {}:
            raise ValueError
        result["connection"] = "OK"
        for attempt in range(3):
            result["clock_attempts"] = attempt + 1
            sent = client.clock()
            server = request("/fapi/v1/time", signed=False)
            received = client.clock()
            if not isinstance(server, dict):
                raise ValueError
            midpoint = sent + (received - sent) / 2
            skew = (milliseconds(server["serverTime"]) - midpoint).total_seconds()
            result["clock_skew_ms"] = round(skew * 1000, 3)
            elapsed = (received - sent).total_seconds()
            result["clock_request_ms"] = round(elapsed * 1000, 3)
            if elapsed < 0 or abs(skew) > 1:
                reasons.append("CLOCK_UNSAFE")
                return result
            if elapsed <= 1:
                break
            # A slow public measurement is inconclusive. Re-sample only this GET;
            # private requests, transport failures and rate limits are never retried.
        else:
            reasons.append("CLOCK_PROBE_SLOW")
            return result
        result["clock"] = "OK"
        account = request("/fapi/v3/account")
        if not isinstance(account, dict):
            raise ValueError
        equity, available = (
            number(account["totalMarginBalance"]),
            number(account["availableBalance"]),
        )
        if equity < 0 or available < 0:
            raise ValueError
        result.update(account="OK", equity=str(equity), available_balance=str(available))
        balances = request("/fapi/v3/balance")
        if not isinstance(balances, list) or len(balances) > 10000:
            raise ValueError
        usdt = [row for row in balances if isinstance(row, dict) and row.get("asset") == "USDT"]
        if len(usdt) != 1:
            reasons.append("USDT_BALANCE_MISSING")
        elif number(usdt[0]["availableBalance"]) < 0:
            raise ValueError
        else:
            result["balance"] = "OK"
        positions = request("/fapi/v3/positionRisk")
        if not isinstance(positions, list) or len(positions) > 10000:
            raise ValueError
        count = 0
        for row in positions:
            if not isinstance(row, dict) or not isinstance(row.get("symbol"), str):
                raise ValueError
            count += number(row.get("positionAmt")) != 0
        result.update(positions="OK", position_count=count)
        config = request("/fapi/v1/accountConfig")
        if not isinstance(config, dict):
            raise ValueError
        result["account_can_trade"] = config.get("canTrade") is True
        if config.get("canTrade") is not True:
            reasons.append("ACCOUNT_TRADING_DISABLED")
        mode = request("/fapi/v1/positionSide/dual")
        if not isinstance(mode, dict):
            raise ValueError
        if mode.get("dualSidePosition") is not False or config.get("dualSidePosition") is not False:
            reasons.append("ONE_WAY_REQUIRED")
        if config.get("multiAssetsMargin") is not False:
            reasons.append("SINGLE_ASSET_REQUIRED")
        orders = request("/fapi/v1/openOrders")
        if not isinstance(orders, list) or len(orders) > 10000:
            raise ValueError
        algos = request("/fapi/v1/openAlgoOrders")
        if not isinstance(algos, list) or len(algos) > 10000:
            raise ValueError
        result["open_order_count"] = len(orders) + len(algos)
        if count or orders or algos:
            reasons.append("EXISTING_EXPOSURE_REQUIRES_RECONCILIATION")
        # Futures accountConfig exposes account capabilities, not the key's
        # scopes/IP restrictions. Do not call a mainnet /sapi permission endpoint.
        result["ready"] = not reasons
        reasons.extend(["KEY_PERMISSIONS_UNVERIFIED", "EXPLICIT_TESTNET_EXECUTION_REQUIRED"])
        result["failed_stage"] = None
        if terminal:
            from tradeforge.execution.terminal_account import terminal_account

            result["terminal"] = terminal_account(client, account, positions, orders, algos)
    except (AckUnknown, ExchangeRejected) as error:
        reasons.append(safe_exchange_reason(error))
    except (ValueError, KeyError, TypeError, ArithmeticError):
        reasons.append("DEMO_RESPONSE_INVALID")
    return result

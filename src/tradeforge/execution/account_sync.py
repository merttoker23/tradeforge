"""Bounded REST snapshots. Exchange facts are authoritative, never inferred from ACKs."""

import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from tradeforge.execution.binance_client import BinanceTestnetClient, number
from tradeforge.persistence.db import EntryHalt


@dataclass(frozen=True)
class ExchangePosition:
    symbol: str
    amount: Decimal
    entry: Decimal
    mark: Decimal
    margin: Decimal

    @property
    def side(self) -> str:
        return "LONG" if self.amount > 0 else "SHORT"


@dataclass(frozen=True)
class AccountView:
    as_of: datetime
    positions: tuple[ExchangePosition, ...]
    orders: tuple[dict[str, object], ...]
    algos: tuple[dict[str, object], ...]


def rows(body: object) -> tuple[dict[str, object], ...]:
    if (
        not isinstance(body, list)
        or len(body) > 10000
        or any(not isinstance(x, dict) for x in body)
    ):
        raise EntryHalt("ACCOUNT_RESPONSE_INVALID")
    return tuple(body)


def amounts(body: object) -> dict[str, Decimal]:
    result: dict[str, Decimal] = {}
    for row in rows(body):
        symbol = row.get("symbol")
        if (
            not isinstance(symbol, str)
            or re.fullmatch(r"[A-Z0-9]{1,24}USDT", symbol) is None
            or row.get("positionSide") != "BOTH"
            or symbol in result
        ):
            raise EntryHalt("ACCOUNT_POSITION_UNSUPPORTED")
        result[symbol] = number(row.get("positionAmt"))
    return {k: v for k, v in result.items() if v}


def read_account(client: BinanceTestnetClient, at: datetime) -> AccountView:
    try:
        account = client.request("GET", "/fapi/v3/account")
        risk = rows(client.request("GET", "/fapi/v3/positionRisk"))
        orders = rows(client.request("GET", "/fapi/v1/openOrders"))
        algos = rows(client.request("GET", "/fapi/v1/openAlgoOrders"))
        if not isinstance(account, dict) or amounts(account["positions"]) != amounts(list(risk)):
            raise ValueError
        # Persist only validated, bounded facts; never the raw REST response or credentials.
        number(account["totalMarginBalance"])
        number(account["availableBalance"])
        positions = []
        for row in risk:
            amount = number(row.get("positionAmt"))
            if not amount:
                continue
            symbol = row["symbol"]
            assert isinstance(symbol, str)
            entry, mark = number(row.get("entryPrice")), number(row.get("markPrice"))
            margin = number(row.get("initialMargin"))
            if min(entry, mark) <= 0 or margin < 0 or row.get("marginAsset") != "USDT":
                raise ValueError
            positions.append(ExchangePosition(symbol, amount, entry, mark, margin))
        return AccountView(at, tuple(positions), orders, algos)
    except (KeyError, TypeError, ValueError):
        raise EntryHalt("ACCOUNT_RESPONSE_INVALID") from None


def cleanup_flat_testnet(client: BinanceTestnetClient) -> int:
    """Explicit operator cleanup; never remove protection from an open position."""
    if client.environment != "testnet" or client.read_only:
        raise EntryHalt("TESTNET_EXECUTION_REQUIRED")
    view = read_account(client, client.clock())
    if view.positions or view.orders:
        raise EntryHalt("FLAT_ACCOUNT_REQUIRED")
    if len(view.algos) > 16 or any(
        row.get("orderType") not in {"STOP_MARKET", "TAKE_PROFIT_MARKET"}
        or row.get("positionSide") != "BOTH"
        or row.get("algoStatus") != "NEW"
        or not (row.get("reduceOnly") is True or row.get("closePosition") is True)
        or type(row.get("algoId")) is not int
        or int(str(row["algoId"])) <= 0
        for row in view.algos
    ):
        raise EntryHalt("ORPHAN_CLEANUP_UNSAFE")
    for row in view.algos:
        current = read_account(client, client.clock())
        if current.positions or current.orders:
            raise EntryHalt("FLAT_ACCOUNT_REQUIRED")
        client.request("DELETE", "/fapi/v1/algoOrder", {"algoId": str(row["algoId"])})
    final = read_account(client, client.clock())
    if final.positions or final.orders or final.algos:
        raise EntryHalt("ACCOUNT_RECONCILIATION_REQUIRED")
    return len(view.algos)

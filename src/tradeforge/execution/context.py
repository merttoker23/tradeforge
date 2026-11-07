"""Explicit guardian context snapshots, independently compared with testnet REST.

Quant/risk evidence supplies factors and daily loss history. Exchange calls verify
account facts, metadata, price, margin and leverage; they never relax risk limits.
"""

import json
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import text

from tradeforge.domain.execution import ExecutionIntent
from tradeforge.execution.binance_client import BinanceTestnetClient, number
from tradeforge.execution.broker import ExecutionContext
from tradeforge.market.bars import canonical
from tradeforge.market.exchange_info import milliseconds, parse_exchange_info
from tradeforge.persistence.db import Database, EntryHalt


def save_context(db: Database, context: ExecutionContext) -> None:
    with db.transaction() as c:
        c.execute(
            text(
                "INSERT INTO account_snapshots(snapshot_id,as_of,payload) "
                "VALUES (:id,:at,CAST(:body AS jsonb))"
            ),
            {
                "id": uuid4(),
                "at": context.as_of,
                "body": canonical(
                    {"kind": "execution.context.v1", "body": context.model_dump(mode="json")}
                ),
            },
        )


def load_context(db: Database, intent: ExecutionIntent) -> ExecutionContext:
    with db.transaction() as c:
        body = c.scalar(
            text(
                "SELECT payload FROM account_snapshots "
                "WHERE payload->>'kind'='execution.context.v1' "
                "AND payload->'body'->'metadata'->>'symbol'=:symbol ORDER BY as_of DESC LIMIT 1"
            ),
            {"symbol": intent.symbol},
        )
    if body is None:
        raise EntryHalt("CONTEXT_MISSING")
    try:
        return ExecutionContext.model_validate_json(json.dumps(body["body"]))
    except (KeyError, TypeError, ValueError):
        raise EntryHalt("CONTEXT_CORRUPT") from None


def verify_exchange_context(
    client: BinanceTestnetClient,
    frame: ExecutionContext,
    *,
    stream_ready: bool,
) -> ExecutionContext:
    """Reject any discrepancy. This phase permits only an otherwise flat account.

    PHASE_15 owns authoritative portfolio/protection reconciliation. Existing
    positions or external open orders cannot be approved merely by a DB flag.
    """
    if frame.environment != "testnet" or frame.account.environment != "testnet":
        raise EntryHalt("EXCHANGE_ENVIRONMENT_MISMATCH")
    sent = datetime.now(UTC)
    try:
        server = client.request("GET", "/fapi/v1/time", signed=False)
        received = datetime.now(UTC)
        if (
            not isinstance(server, dict)
            or (received - sent).total_seconds() > 1
            or abs((milliseconds(server["serverTime"]) - received).total_seconds()) > 1
        ):
            raise ValueError
        account = client.request("GET", "/fapi/v3/account")
        mode = client.request("GET", "/fapi/v1/positionSide/dual")
        config = client.request("GET", "/fapi/v1/symbolConfig", {"symbol": frame.metadata.symbol})
        open_orders = client.request("GET", "/fapi/v1/openOrders")
        configuration = client.request("GET", "/fapi/v1/accountConfig")
        algo_orders = client.request("GET", "/fapi/v1/openAlgoOrders")
        if (
            not isinstance(account, dict)
            or not isinstance(mode, dict)
            or not isinstance(config, list)
            or len(config) != 1
            or open_orders != []
            or not isinstance(configuration, dict)
            or configuration.get("canTrade") is not True
            or configuration.get("multiAssetsMargin") is not False
            or configuration.get("dualSidePosition") is not False
            or algo_orders != []
            or mode.get("dualSidePosition") is not False
        ):
            raise ValueError
        # Account v3 positions include only non-zero positions. Do not invent protection.
        positions = account["positions"]
        if not isinstance(positions, list) or any(
            not isinstance(x, dict) or number(x.get("positionAmt")) != 0 for x in positions
        ):
            raise ValueError
        if (
            frame.account.positions
            or frame.account.pending
            or number(account["totalMarginBalance"]) != frame.account.equity
            or number(account["availableBalance"]) != frame.account.available_balance
            or number(account["totalUnrealizedProfit"]) != frame.account.unrealized_pnl
        ):
            raise ValueError
        conf = config[0]
        if (
            not isinstance(conf, dict)
            or conf.get("symbol") != frame.metadata.symbol
            or conf.get("marginType") != "ISOLATED"
            or conf.get("isAutoAddMargin") is not False
            or type(conf.get("leverage")) is not int
            or conf["leverage"] != frame.leverage
            or number(conf.get("maxNotionalValue")) < frame.max_notional
        ):
            raise ValueError
        observed = datetime.now(UTC)
        meta = parse_exchange_info(
            client.request("GET", "/fapi/v1/exchangeInfo", signed=False),
            observed,
        )
        current = next(x for x in meta.instruments if x.symbol == frame.metadata.symbol)
        quote = client.request(
            "GET", "/fapi/v1/ticker/bookTicker", {"symbol": current.symbol}, signed=False
        )
        mark = client.request(
            "GET", "/fapi/v1/premiumIndex", {"symbol": current.symbol}, signed=False
        )
        if (
            not isinstance(quote, dict)
            or not isinstance(mark, dict)
            or quote.get("symbol") != current.symbol
            or mark.get("symbol") != current.symbol
            or abs((observed - milliseconds(quote["time"])).total_seconds()) > 2
            or abs((observed - milliseconds(mark["time"])).total_seconds()) > 2
        ):
            raise ValueError
        return ExecutionContext.model_validate(
            {
                **frame.model_dump(),
                "metadata": current,
                "mark_price": number(mark["markPrice"]),
                "bid": number(quote["bidPrice"]),
                "ask": number(quote["askPrice"]),
                "one_way": True,
                "isolated": True,
                "clock_ready": True,
                "stream_ready": stream_ready,
            }
        )
    except (KeyError, TypeError, ValueError, StopIteration):
        raise EntryHalt("EXCHANGE_CONTEXT_UNVERIFIED") from None

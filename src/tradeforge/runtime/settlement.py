"""Loss controls consume complete, exchange-confirmed close cashflows, once.

Missing or truncated fills/income halt admission. They never become zero PnL or
invented calibration samples. Gross execution PnL is not a mid-price benchmark.
"""

from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid5

from sqlalchemy import text

from tradeforge.decision.cost_model import RealizedCost
from tradeforge.decision.edge import EdgeStore
from tradeforge.decision.stats import Bucket, SettlementProvenance, TradeOutcome
from tradeforge.execution.binance_client import BinanceTestnetClient, number
from tradeforge.execution.storage import audit
from tradeforge.intelligence.models import digest
from tradeforge.market.bars import canonical
from tradeforge.market.exchange_info import milliseconds
from tradeforge.persistence.db import Database, EntryHalt
from tradeforge.risk.storage import RiskService


def owned_exit_orders(
    db: Database, client: BinanceTestnetClient, identifier: UUID, symbol: str
) -> set[str]:
    """Only local reservations plus exchange order identity establish attribution."""
    with db.transaction() as c:
        keys = c.scalars(
            text(
                "SELECT payload->>'client_id' FROM audit_log "
                "WHERE event_type IN ('execution.stop_reserved.v1','protection.reserved.v1') "
                "AND payload->>'intent_id'=:id"
            ),
            {"id": str(identifier)},
        ).all()
        emergency = c.scalar(
            text(
                "SELECT 1 FROM audit_log WHERE event_type='protection.exit_reserved.v1' "
                "AND payload->>'client_id'=:key"
            ),
            {"key": "ec_" + identifier.hex},
        )
    result: set[str] = set()
    for key in set(keys):
        body = client.request("GET", "/fapi/v1/algoOrder", {"clientAlgoId": key})
        if (
            isinstance(body, dict)
            and body.get("clientAlgoId") == key
            and body.get("symbol") == symbol
            and body.get("closePosition") is True
            and body.get("actualOrderId") not in (None, "", "0", 0)
            and body.get("algoStatus") in {"FINISHED", "TRIGGERED"}
        ):
            result.add(str(body["actualOrderId"]))
    if emergency:
        key = "ec_" + identifier.hex
        body = client.request("GET", "/fapi/v1/order", {"symbol": symbol, "origClientOrderId": key})
        if (
            isinstance(body, dict)
            and body.get("clientOrderId") == key
            and body.get("symbol") == symbol
            and body.get("reduceOnly") is True
            and body.get("positionSide") == "BOTH"
            and body.get("orderId") is not None
        ):
            result.add(str(body["orderId"]))
    return result


def settle_closed_results(db: Database, client: BinanceTestnetClient) -> None:
    if getattr(client, "environment", None) != "testnet":
        raise EntryHalt("DEMO_ONLY")
    with db.transaction() as c:
        rows = (
            c.execute(
                text(
                    "SELECT e.intent_id,e.created_at,e.snapshot->>'symbol' AS symbol,"
                    "e.side,e.snapshot,o.exchange_order_id,o.filled_qty,"
                    "(SELECT a.payload FROM audit_log a WHERE a.event_type='runtime.forecast.v1' "
                    "AND a.payload->>'intent_id'=e.intent_id::text LIMIT 1) AS forecast,"
                    "(SELECT (a.payload->>'as_of')::timestamptz FROM audit_log a "
                    "WHERE a.event_type='protection.flat.v1' "
                    "AND a.payload->>'intent_id'=e.intent_id::text LIMIT 1) AS flat_at "
                    "FROM execution_intents e "
                    "JOIN orders o USING(intent_id) WHERE o.filled_qty>0 "
                    "AND EXISTS (SELECT 1 FROM audit_log a WHERE a.event_type='protection.flat.v1' "
                    "AND a.payload->>'intent_id'=e.intent_id::text) "
                    "AND NOT EXISTS (SELECT 1 FROM audit_log a "
                    "WHERE a.event_type='runtime.closed_result.v1' "
                    "AND a.payload->>'intent_id'=e.intent_id::text) ORDER BY e.created_at LIMIT 100"
                )
            )
            .mappings()
            .all()
        )
    for row in rows:
        at = client.clock()
        if not row["created_at"] <= row["flat_at"] <= at:
            raise EntryHalt("CLOSE_TIME_INVALID")
        params = {
            "symbol": row["symbol"],
            "startTime": str(int(row["created_at"].timestamp() * 1000)),
            "endTime": str(int(row["flat_at"].timestamp() * 1000)),
            "limit": "1000",
        }
        trades = client.request("GET", "/fapi/v1/userTrades", params)
        if not isinstance(trades, list) or not 2 <= len(trades) < 1000:
            raise EntryHalt("CLOSE_FILLS_INCOMPLETE")
        entry_qty, exit_qty, gross, fees = (Decimal(0) for _ in range(4))
        entry_value, exit_value, maker_fees, taker_fees = (Decimal(0) for _ in range(4))
        ids = set()
        exit_orders: set[str] = set()
        opened_at: datetime | None = None
        exit_at: datetime | None = None
        for trade in trades:
            if (
                not isinstance(trade, dict)
                or trade.get("symbol") != row["symbol"]
                or trade.get("commissionAsset") != "USDT"
                or type(trade.get("id")) is not int
                or trade["id"] < 0
                or type(trade.get("orderId")) is not int
                or trade["orderId"] <= 0
                or trade.get("id") in ids
                or trade.get("side") not in {"BUY", "SELL"}
                or not row["created_at"] <= milliseconds(trade["time"]) <= row["flat_at"]
            ):
                raise EntryHalt("CLOSE_FILLS_INVALID")
            ids.add(trade["id"])
            qty = number(trade["qty"])
            price = number(trade["price"])
            if qty <= 0 or price <= 0 or number(trade["commission"]) < 0:
                raise EntryHalt("CLOSE_FILLS_INVALID")
            if str(trade["orderId"]) == row["exchange_order_id"]:
                if trade["side"] != ("BUY" if row["side"] == "LONG" else "SELL"):
                    raise EntryHalt("CLOSE_FILLS_INVALID")
                entry_qty += qty
                entry_value += qty * price
                opened_at = min(opened_at or at, milliseconds(trade["time"]))
                if number(trade["realizedPnl"]) != 0:
                    raise EntryHalt("CLOSE_FILLS_INVALID")
            else:
                if trade["side"] != ("SELL" if row["side"] == "LONG" else "BUY"):
                    raise EntryHalt("UNATTRIBUTED_TRADE")
                exit_qty += qty
                exit_value += qty * price
                exit_orders.add(str(trade["orderId"]))
                exit_at = max(exit_at or row["created_at"], milliseconds(trade["time"]))
            gross += number(trade["realizedPnl"])
            fees += number(trade["commission"])
            if type(trade.get("maker")) is not bool:
                raise EntryHalt("CLOSE_FEE_ROLE_UNKNOWN")
            if trade["maker"]:
                maker_fees += number(trade["commission"])
            else:
                taker_fees += number(trade["commission"])
        if (
            entry_qty != row["filled_qty"]
            or exit_qty != entry_qty
            or exit_at is None
            or opened_at is None
        ):
            raise EntryHalt("CLOSE_QUANTITY_MISMATCH")
        expected_gross = (exit_value - entry_value) * (1 if row["side"] == "LONG" else -1)
        if abs(expected_gross - gross) > Decimal("0.00000001") * len(trades):
            raise EntryHalt("CLOSE_PNL_MISMATCH")
        if not exit_orders <= owned_exit_orders(db, client, row["intent_id"], row["symbol"]):
            raise EntryHalt("UNATTRIBUTED_CLOSE_ORDER")
        income = client.request(
            "GET",
            "/fapi/v1/income",
            {
                "symbol": row["symbol"],
                "incomeType": "FUNDING_FEE",
                "startTime": str(int(opened_at.timestamp() * 1000)),
                "endTime": str(int(exit_at.timestamp() * 1000)),
                "limit": "1000",
            },
        )
        if not isinstance(income, list) or len(income) >= 1000:
            raise EntryHalt("CLOSE_INCOME_INCOMPLETE")
        funding = Decimal(0)
        funding_ids: set[str] = set()
        for item in income:
            if (
                not isinstance(item, dict)
                or item.get("asset") != "USDT"
                or item.get("symbol") != row["symbol"]
                or item.get("incomeType") != "FUNDING_FEE"
                or not opened_at <= milliseconds(item["time"]) <= exit_at
                or item.get("tranId") is None
                or str(item["tranId"]) in funding_ids
            ):
                raise EntryHalt("CLOSE_INCOME_INVALID")
            funding += number(item["income"])
            funding_ids.add(str(item["tranId"]))
        pnl = gross - fees + funding
        observed_at = client.clock()
        if observed_at < at:
            raise EntryHalt("CLOSE_CLOCK_REGRESSION")
        identifier: UUID = row["intent_id"]
        RiskService(db).record_result(uuid5(identifier, "closed-result"), pnl)
        with db.transaction() as c:
            c.execute(text("SELECT pg_advisory_xact_lock(74202011)"))
            if c.scalar(
                text(
                    "SELECT 1 FROM audit_log WHERE event_type='runtime.closed_result.v1' "
                    "AND payload->>'intent_id'=:id"
                ),
                {"id": str(identifier)},
            ):
                continue
            forecast = row["forecast"]
            if forecast is not None:
                stop = number(row["snapshot"]["stop_plan"]["stop_price"])
                entry_price, exit_price = entry_value / entry_qty, exit_value / exit_qty
                initial_risk = entry_qty * abs(entry_price - stop)
                if initial_risk <= 0:
                    raise EntryHalt("CLOSE_INITIAL_RISK_INVALID")
                outcome = TradeOutcome(
                    trade_id=identifier,
                    bucket=Bucket.model_validate_json(canonical(forecast["bucket"])),
                    split=forecast["split"],
                    opened_at=opened_at,
                    closed_at=exit_at,
                    observed_at=observed_at,
                    initial_risk=initial_risk,
                    forecast_at=datetime.fromisoformat(forecast["forecast_at"]),
                    gross_R=gross / initial_risk,
                    costs=RealizedCost(
                        maker_fees=maker_fees,
                        taker_fees=taker_fees,
                        spread=Decimal(0),
                        slippage=Decimal(0),
                        funding=-funding,
                        latency=Decimal(0),
                        pnl_basis="executed_price",
                    ),
                    forecast_net_R=(
                        None
                        if forecast["forecast_net_R"] is None
                        else number(forecast["forecast_net_R"])
                    ),
                    forecast_p_win=forecast["forecast_p_win"],
                    analysis_version=forecast["analysis_version"],
                    settlement=SettlementProvenance(
                        environment="testnet",
                        entry_order_id=row["exchange_order_id"],
                        exit_order_ids=tuple(sorted(exit_orders)),
                        trade_ids=tuple(str(i) for i in sorted(ids)),
                        funding_ids=tuple(sorted(funding_ids)),
                        qty=entry_qty,
                        entry_price=entry_price,
                        exit_price=exit_price,
                        stop_price=stop,
                        fills_hash=digest(trades),
                        income_hash=digest(income),
                    ),
                )
                EdgeStore(db).save_outcome(outcome, connection=c)
            audit(
                c,
                "runtime.closed_result.v1",
                {
                    "intent_id": str(identifier),
                    "pnl": str(pnl),
                    "gross": str(gross),
                    "fees": str(fees),
                    "funding": str(funding),
                    "trade_ids": sorted(ids),
                    "closed_at": exit_at.isoformat(),
                },
            )

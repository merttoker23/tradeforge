"""Offline exchange boundary and real risk/intent fixtures for PHASE_14."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal as D
from uuid import uuid4

from tests.risk_fixtures import account, change, factor, request
from tests.universe_fixtures import payload, symbol


def setup_execution(seeded, *, market=False, equity=D("10000"), environment="unspecified"):
    from tradeforge.domain.execution import StopPlan
    from tradeforge.execution.broker import ExecutionContext
    from tradeforge.execution.order_manager import authorize
    from tradeforge.market.exchange_info import parse_exchange_info
    from tradeforge.persistence.repositories import enqueue_intent
    from tradeforge.risk.storage import RiskService

    db, decision, original = seeded
    now = datetime.now(UTC)
    intent = change(
        original,
        qty=D("1"),
        price=None if market else D("100"),
        stop_plan=StopPlan(qty=D("1"), stop_price=D("95")),
    )
    metadata = parse_exchange_info(payload(symbol()), now).instruments[0]
    req = change(
        request(symbol=intent.symbol, observed_at=now, expires_at=intent.expires_at),
        filters=metadata.filters,
        environment=environment,
    )
    acc = account(
        environment=environment,
        as_of=now,
        trading_day=now.date(),
        factors=(factor(intent.symbol, as_of=now),),
        equity=equity,
        available_balance=equity,
        day_start_equity=equity,
    )
    risk = RiskService(db, clock=lambda: now)
    risk.set_kill_switch(False, reason="offline fixture operator")
    quant = risk.assess(req, acc)
    assert quant.allowed
    enqueue_intent(db, intent)
    authorize(db, intent, quant)
    context = ExecutionContext(
        environment=environment,
        as_of=now,
        metadata=metadata,
        account=acc,
        mark_price=D("100"),
        bid=D("99.99"),
        ask=D("100.01"),
        leverage=D("1"),
        max_notional=D("10000"),
        one_way=True,
        isolated=True,
        clock_ready=True,
        stream_ready=True,
    )
    return db, intent, context, now


def report(intent, status="NEW", qty="0", *, at=None, order_id="101"):
    from tradeforge.execution.broker import ExchangeFill, ExchangeReport

    at = at or intent.as_of
    fills = (
        ()
        if qty == "0"
        else (
            ExchangeFill(
                trade_id="BTCUSDT:7",
                qty=D(qty),
                price=D("100"),
                fee=D("0.01"),
                exchange_at=at - timedelta(milliseconds=1),
            ),
        )
    )
    return ExchangeReport(
        client_order_id=intent.client_order_id,
        symbol=intent.symbol,
        order_id=order_id,
        side="BUY",
        qty=intent.qty,
        filled_qty=D(qty),
        status=status,
        avg_price=None if qty == "0" else D("100"),
        exchange_at=at,
        fills=fills,
    )


class FakeBroker:
    """Timeout after acceptance preserves the exchange's accepted order for lookup."""

    def __init__(self, intent, *, order_id="101"):
        self.intent = intent
        self.order_id = order_id
        self.orders = {}
        self.submissions = []
        self.placed_prices = []
        self.queries = []
        self.stops = []
        self.outcome = "ok"
        self.stop_outcome = "ok"

    def place(self, intent):
        from tradeforge.execution.broker import AckUnknown, ExchangeRejected

        self.submissions.append(intent.client_order_id)
        self.placed_prices.append(intent.price)
        if self.outcome in {"MARGIN_INSUFFICIENT", "RATE_LIMITED", "ORDER_REJECTED"}:
            raise ExchangeRejected(self.outcome)
        value = report(intent, order_id=self.order_id)
        self.orders[intent.client_order_id] = value
        if self.outcome == "timeout":
            raise AckUnknown("ACK_UNKNOWN")
        if self.outcome == "crash":
            raise SystemExit("injected process death")
        return value

    def query(self, intent):
        self.queries.append(intent.client_order_id)
        return self.orders.get(intent.client_order_id)

    def protect(self, intent, client_id, *, query_only=False):
        from tradeforge.execution.broker import AckUnknown

        if self.stop_outcome == "timeout":
            raise AckUnknown("PROTECTION_UNKNOWN")
        if not query_only:
            self.stops.append(client_id)
        return client_id in self.stops

    def cancel(self, intent):
        return report(intent, "CANCELED", order_id=self.order_id)


def other_intent(intent, **patch):
    return change(intent, intent_id=uuid4(), client_order_id="tf_" + uuid4().hex, **patch)

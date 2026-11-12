"""OFFLINE synthetic pipeline proof; no live evidence, network, or exchange credentials.

Only Binance's wire transport and module wall clocks are replaced. Alpha, archived
provenance, evidence, risk, publication, reservation, fills, and the production
PositionLifecycle protection callback all run against disposable PostgreSQL.
"""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal as D
from urllib.parse import parse_qs

import pytest
from sqlalchemy import text

from tests.unit.test_momentum_alpha import change, evidence
from tests.universe_fixtures import payload, symbol
from tradeforge.decision.edge import EdgeAnalysis
from tradeforge.decision.state_machine import Record
from tradeforge.decision.trial import TestnetTrialPolicy as TrialPolicy
from tradeforge.domain.decision import DecisionState, Side
from tradeforge.execution.binance_client import BinanceTestnetClient, Credentials, Response
from tradeforge.execution.context import load_context, verify_exchange_context
from tradeforge.execution.order_manager import OrderManager
from tradeforge.execution.outbox_worker import OutboxWorker
from tradeforge.execution.position_lifecycle import PositionLifecycle
from tradeforge.execution.storage import intent_from
from tradeforge.persistence.db import EntryHalt, PersistenceError
from tradeforge.risk.governor import RiskAssessment
from tradeforge.risk.storage import RiskService
from tradeforge.runtime.analysis import AutonomousAnalysis
from tradeforge.runtime.demo_decisions import DemoDecisions

# Synthetic, UTC-aligned source generation needs a fixed instant later than the
# database wall clock: PostgreSQL intentionally enforces expiry independently.
# This is chosen once, before generating or archiving any input, never renewed.
OFFLINE_AT = (datetime.now(UTC) + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)


class OfflineClock:
    def __init__(self, at):
        self.at = at

    def __call__(self):
        return self.at

    def freeze_module_clocks(self, monkeypatch):
        clock = self

        class FrozenDatetimeMeta(type):
            def __instancecheck__(cls, value):
                return isinstance(value, datetime)

        class FrozenDatetime(datetime, metaclass=FrozenDatetimeMeta):
            @classmethod
            def now(cls, tz=None):
                return clock.at if tz is not None else clock.at.replace(tzinfo=None)

        # These modules use wall time instead of the existing injected client clock.
        # No business method or assessment result is substituted.
        for module in (
            "tradeforge.domain.common",
            "tradeforge.risk.storage",
            "tradeforge.runtime.trading",
            "tradeforge.execution.context",
        ):
            monkeypatch.setattr(f"{module}.datetime", FrozenDatetime)


class OfflineBinanceTransport:
    """Strict signed-wire fixture: unsupported requests fail, never reach HTTPS."""

    def __init__(self, clock):
        self.clock = clock
        self.calls = []
        self.bid, self.ask = D("103.49"), D("103.51")
        self.entry = None
        self.filled = D(0)
        self.algos = {}
        self.ambiguous = False
        self.lookup_missing = False
        self.reject_entry = False
        self.trade_rows = []
        self.exit_rows = []

    @property
    def milliseconds(self):
        return int(self.clock().timestamp() * 1000)

    @property
    def entry_posts(self):
        return [p for m, path, p in self.calls if (m, path) == ("POST", "/fapi/v1/order")]

    @property
    def stop_posts(self):
        return [
            p
            for m, path, p in self.calls
            if (m, path) == ("POST", "/fapi/v1/algoOrder") and p["type"] == "STOP_MARKET"
        ]

    def position(self):
        if not self.filled or self.exit_rows:
            return []
        return [
            {
                "symbol": "ETHUSDT",
                "positionSide": "BOTH",
                "positionAmt": str(self.filled),
                "entryPrice": str(self.ask),
                "markPrice": str(self.ask),
                "initialMargin": str(self.filled * self.ask),
                "marginAsset": "USDT",
            }
        ]

    def report(self):
        assert self.entry is not None
        qty = D(self.entry["quantity"])
        return {
            "symbol": "ETHUSDT",
            "orderId": 4201,
            "clientOrderId": self.entry["newClientOrderId"],
            "side": self.entry["side"],
            "origQty": str(qty),
            "executedQty": str(self.filled),
            "status": "FILLED"
            if self.filled == qty
            else ("PARTIALLY_FILLED" if self.filled else "NEW"),
            "avgPrice": str(self.ask if self.filled else 0),
            "updateTime": self.milliseconds,
        }

    def fill(self, quantity, trade_id):
        assert self.entry is not None
        self.filled += quantity
        self.trade_rows.append(
            {
                "symbol": "ETHUSDT",
                "orderId": 4201,
                "id": trade_id,
                "side": self.entry["side"],
                "qty": str(quantity),
                "price": str(self.ask),
                "realizedPnl": "0",
                "commission": "0.001",
                "commissionAsset": "USDT",
                "maker": False,
                "time": self.milliseconds,
            }
        )

    def hit_take_profit(self):
        target = next(
            algo for algo in self.algos.values() if algo["orderType"] == "TAKE_PROFIT_MARKET"
        )
        assert target["algoStatus"] == "NEW" and target["side"] == "SELL"
        assert target["closePosition"] and self.filled > 0
        target.update(algoStatus="FINISHED", actualOrderId="4202")
        price = D(target["triggerPrice"])
        gross = (price - self.ask) * self.filled
        assert gross > 0
        self.exit_rows.append({
            "symbol": "ETHUSDT", "orderId": 4202, "id": 703, "side": "SELL",
            "qty": str(self.filled), "price": str(price), "realizedPnl": str(gross),
            "commission": "0.001", "commissionAsset": "USDT", "maker": False,
            "time": self.milliseconds,
        })
        return gross

    def __call__(self, method, path, encoded, headers):
        params = {key: values[0] for key, values in parse_qs(encoded).items()}
        self.calls.append((method, path, params))
        if path not in {
            "/fapi/v1/time",
            "/fapi/v1/exchangeInfo",
            "/fapi/v1/ticker/bookTicker",
            "/fapi/v1/premiumIndex",
        }:
            assert headers["X-MBX-APIKEY"] == "OFFLINE_SYNTHETIC_KEY"
            assert len(params["signature"]) == 64
            assert params["timestamp"] == str(self.milliseconds)
        status = 200
        if (method, path) == ("GET", "/fapi/v1/time"):
            body = {"serverTime": self.milliseconds}
        elif (method, path) == ("GET", "/fapi/v3/account"):
            body = {
                "totalMarginBalance": "10000",
                "totalUnrealizedProfit": "0",
                "availableBalance": "10000",
                "positions": self.position(),
            }
        elif (method, path) == ("GET", "/fapi/v1/income"):
            body = []
        elif (method, path) == ("GET", "/fapi/v1/exchangeInfo"):
            body = payload(symbol("ETHUSDT"))
        elif (method, path) == ("GET", "/fapi/v1/symbolConfig"):
            body = [
                {
                    "symbol": "ETHUSDT",
                    "marginType": "ISOLATED",
                    "isAutoAddMargin": False,
                    "leverage": 1,
                    "maxNotionalValue": "10000",
                }
            ]
        elif (method, path) == ("GET", "/fapi/v1/commissionRate"):
            body = {
                "symbol": "ETHUSDT",
                "makerCommissionRate": "0.0002",
                "takerCommissionRate": "0.0005",
            }
        elif (method, path) == ("GET", "/fapi/v1/positionSide/dual"):
            body = {"dualSidePosition": False}
        elif (method, path) == ("GET", "/fapi/v1/accountConfig"):
            body = {"canTrade": True, "dualSidePosition": False, "multiAssetsMargin": False}
        elif (method, path) == ("GET", "/fapi/v1/openOrders"):
            body = []
        elif (method, path) == ("GET", "/fapi/v1/openAlgoOrders"):
            body = [algo for algo in self.algos.values() if algo["algoStatus"] == "NEW"]
        elif (method, path) == ("GET", "/fapi/v3/positionRisk"):
            body = self.position()
        elif (method, path) == ("GET", "/fapi/v1/ticker/bookTicker"):
            body = {
                "symbol": "ETHUSDT",
                "bidPrice": str(self.bid),
                "bidQty": "1000",
                "askPrice": str(self.ask),
                "askQty": "1000",
                "time": self.milliseconds,
            }
        elif (method, path) == ("GET", "/fapi/v1/premiumIndex"):
            body = {
                "symbol": "ETHUSDT",
                "markPrice": str(self.ask),
                "indexPrice": str(self.ask),
                "lastFundingRate": "0.0001",
                "nextFundingTime": self.milliseconds + 3600000,
                "time": self.milliseconds,
            }
        elif (method, path) == ("POST", "/fapi/v1/order"):
            assert self.entry is None, "An entry reservation must never be sent twice"
            assert params["symbol"] == "ETHUSDT" and params["positionSide"] == "BOTH"
            assert params["type"] == "LIMIT" and params["timeInForce"] == "IOC"
            assert params["reduceOnly"] == "false"
            self.entry = params
            if self.reject_entry:
                status, body = 400, {"code": -2019, "msg": "OFFLINE margin rejection"}
            elif self.ambiguous:
                raise TimeoutError("OFFLINE accepted request with lost ACK")
            else:
                body = self.report()
        elif (method, path) == ("GET", "/fapi/v1/order"):
            if self.lookup_missing:
                status, body = 400, {"code": -2013, "msg": "OFFLINE order not found"}
            else:
                assert self.entry is not None
                assert params["origClientOrderId"] == self.entry["newClientOrderId"]
                body = self.report()
        elif (method, path) == ("GET", "/fapi/v1/userTrades"):
            assert params["symbol"] == "ETHUSDT"
            if "orderId" in params:
                assert params["orderId"] == "4201"
                body = list(self.trade_rows)
            else:
                assert "startTime" in params and "endTime" in params
                body = self.trade_rows + self.exit_rows
        elif (method, path) == ("GET", "/fapi/v1/algoOrder"):
            body = self.algos.get(params["clientAlgoId"])
            if body is None:
                status, body = 400, {"code": -2013, "msg": "OFFLINE algo not found"}
        elif (method, path) == ("POST", "/fapi/v1/algoOrder"):
            key = params["clientAlgoId"]
            assert key not in self.algos, "Initial protection must not be sent twice"
            assert params["closePosition"] == "true" and "quantity" not in params
            assert params["positionSide"] == "BOTH" and params["workingType"] == "MARK_PRICE"
            assert params["type"] in {"STOP_MARKET", "TAKE_PROFIT_MARKET"}
            body = {
                "algoId": 9000 + len(self.algos),
                "clientAlgoId": key,
                "symbol": params["symbol"],
                "algoStatus": "NEW",
                "closePosition": True,
                "positionSide": "BOTH",
                "workingType": "MARK_PRICE",
                "side": params["side"],
                "orderType": params["type"],
                "triggerPrice": params["triggerPrice"],
            }
            self.algos[key] = body
        elif (method, path) == ("DELETE", "/fapi/v1/algoOrder"):
            body = self.algos[params["clientAlgoId"]]
            assert body["algoStatus"] == "NEW"
            body["algoStatus"] = "CANCELED"
        else:
            raise AssertionError(f"Unexpected OFFLINE wire request: {method} {path}")
        return Response(status, {}, json.dumps(body).encode())


class OfflinePipeline:
    def __init__(self, db, monkeypatch, *, trial_policy=None):
        self.db = db
        start = OFFLINE_AT - timedelta(hours=8)
        monkeypatch.setattr("tests.intelligence_fixtures.START", start)
        # The fixture's universe() binds its original START in a Python default.
        monkeypatch.setattr(
            "tests.intelligence_fixtures.universe.__defaults__",
            (("BTCUSDT", "ETHUSDT", "SOLUSDT"), start),
        )
        self.clock = OfflineClock(OFFLINE_AT)
        self.clock.freeze_module_clocks(monkeypatch)
        self.sample = evidence()
        assert self.sample.as_of == OFFLINE_AT
        self.transport = OfflineBinanceTransport(self.clock)
        self.client = BinanceTestnetClient(
            Credentials(api_key="OFFLINE_SYNTHETIC_KEY", api_secret="OFFLINE_SYNTHETIC_SECRET"),
            mode="testnet",
            opted_in=True,
            read_only=False,
            transport=self.transport,
            clock=self.clock,
        )
        self.policy = trial_policy or TrialPolicy(enabled=True)
        self.demo = DemoDecisions(db, self.client, trial_policy=self.policy)
        RiskService(db, clock=self.clock).set_kill_switch(False, reason="OFFLINE disposable test")
        self.record = self.arm(self.sample)

    def arm(self, sample):
        analysis = AutonomousAnalysis(self.db, clock=self.clock)
        assert analysis.evaluate((sample,)) == 1, analysis.state
        with self.db.transaction() as c:
            body = c.scalar(
                text(
                    "SELECT payload FROM decision_lifecycle WHERE state='ARMED' "
                    "ORDER BY at DESC, version DESC LIMIT 1"
                )
            )
        record = Record.model_validate_json(body)
        assert record.state is DecisionState.ARMED
        assert record.discovery.candidate.input_hash == sample.digest
        return record

    def next_setup(self, monkeypatch, *, side=Side.LONG, policy=None):
        # Generate new honest synthetic observations after the old lifecycle's
        # expiry/cooldown; the archived input for the prior setup is untouched.
        self.clock.at = (self.clock() + timedelta(hours=1)).replace(
            minute=0, second=0, microsecond=0
        )
        start = self.clock() - timedelta(hours=8)
        monkeypatch.setattr("tests.intelligence_fixtures.START", start)
        monkeypatch.setattr(
            "tests.intelligence_fixtures.universe.__defaults__",
            (("BTCUSDT", "ETHUSDT", "SOLUSDT"), start),
        )
        self.sample = evidence(side)
        self.record = self.arm(self.sample)
        self.demo = DemoDecisions(self.db, self.client, trial_policy=policy or self.policy)

    def latest(self):
        with self.db.transaction() as c:
            body = c.scalar(
                text(
                    "SELECT payload FROM decision_lifecycle WHERE decision_id=:id "
                    "ORDER BY version DESC LIMIT 1"
                ),
                {"id": self.record.decision_id},
            )
        return Record.model_validate_json(body)

    def feed(self, *, crossed=True):
        candidate = self.record.discovery.candidate
        side = candidate.side
        at = self.clock()
        if crossed:
            if side is Side.LONG:
                bid, ask = candidate.trigger - D("0.01"), candidate.trigger
            else:
                bid, ask = candidate.trigger, candidate.trigger + D("0.01")
        else:
            bid, ask = self.sample.market.best_bid, self.sample.market.best_ask
        self.transport.bid, self.transport.ask = bid, ask
        market = change(
            self.sample.market,
            source_at=at,
            observed_at=at,
            as_of=at,
            best_bid=bid,
            best_ask=ask,
            mark_price=(bid + ask) / 2,
            index_price=(bid + ask) / 2,
            environment="testnet",
            source="OFFLINE_SYNTHETIC",
        )
        book = change(
            self.sample.book,
            source_at=at,
            observed_at=at,
            as_of=at,
            captured_at=at,
            bids=((bid, D("1000")),),
            asks=((ask, D("1000")),),
        )
        return {
            "status": {"data_ready": True},
            "markets": [market.model_dump(mode="json")],
            "deep": [{"symbol": "ETHUSDT", "book": book.model_dump(mode="json")}],
        }

    def publish(self):
        before = self.count("execution_intents")
        self.demo.tick(self.feed(crossed=False))
        assert self.demo.status()["autonomous_decisions"]["last_reason"] == "TRIGGER_NOT_CROSSED"
        assert self.count("execution_intents") == before
        self.clock.at += timedelta(seconds=1)
        self.demo.tick(self.feed())
        assert self.latest().state is DecisionState.APPROVED
        with self.db.transaction() as c:
            identifier = c.scalar(
                text("SELECT intent_id FROM execution_intents WHERE decision_id=:id"),
                {"id": self.record.decision_id},
            )
            return intent_from(c, identifier)

    def workers(self):
        recovery = PositionLifecycle(self.db, self.client, clock=self.clock)
        manager = OrderManager(
            self.db,
            self.client,
            lambda intent: verify_exchange_context(
                self.client,
                load_context(self.db, intent),
                stream_ready=True,
            ),
            clock=self.clock,
            protection=lambda _: recovery.reconcile(),
        )
        return OutboxWorker(self.db, manager, clock=self.clock), recovery

    def count(self, table):
        assert table in {"execution_intents", "outbox", "orders", "fills", "decisions"}
        with self.db.transaction() as c:
            return c.scalar(text(f"SELECT count(*) FROM {table}"))

    def events(self, event_type):
        with self.db.transaction() as c:
            return c.scalars(
                text("SELECT payload FROM audit_log WHERE event_type=:kind ORDER BY audit_id"),
                {"kind": event_type},
            ).all()

    def assert_no_publication(self):
        assert self.count("execution_intents") == self.count("outbox") == self.count("orders") == 0
        assert self.events("runtime.forecast.v1") == []
        assert self.events("execution.authorization.v1") == []
        assert self.transport.entry_posts == []


def test_offline_real_alpha_trial_publishes_then_fills_and_protects_once(migrated, monkeypatch):
    pipeline = OfflinePipeline(migrated, monkeypatch)
    intent = pipeline.publish()
    assert pipeline.transport.entry_posts == []
    assert pipeline.count("execution_intents") == pipeline.count("outbox") == 1
    quant = RiskAssessment.model_validate_json(
        json.dumps(pipeline.events("risk.assessment.v1")[-1]["body"])
    )
    assert quant.allowed and quant.policy.factor_age_seconds == 60
    authorization = pipeline.events("execution.authorization.v1")
    assert len(authorization) == 1 and authorization[0]["purpose"] == "TESTNET_TRIAL"
    forecast = pipeline.events("runtime.forecast.v1")
    assert len(forecast) == 1 and forecast[0]["forecast_p_win"] is None
    assert forecast[0]["forecast_net_R"] is None
    with migrated.transaction() as c:
        decision = json.loads(c.scalar(text("SELECT snapshot FROM decisions")))
        assert not c.scalar(text("SELECT delivered_at IS NOT NULL FROM outbox"))
    assert decision["proof_state"] == "UNPROVEN" and decision["calibrated"] is False
    assert decision["confidence"] is None and decision["execution_authority"] == "TESTNET_TRIAL"
    worker, recovery = pipeline.workers()
    assert worker.run_once() and not worker.run_once()
    assert len(pipeline.transport.entry_posts) == 1
    pipeline.clock.at += timedelta(milliseconds=1)
    partial = intent.qty / 2
    pipeline.transport.fill(partial, 701)
    assert worker.reconcile_once() == 1
    assert recovery.ready, recovery.health()
    assert len(pipeline.transport.stop_posts) == 1
    pipeline.clock.at += timedelta(milliseconds=1)
    pipeline.transport.fill(intent.qty - partial, 702)
    restarted, recovered = pipeline.workers()
    assert restarted.reconcile_once() == 1
    assert recovered.ready, recovered.health()
    assert restarted.reconcile_once() == 1 and not restarted.run_once()
    assert len(pipeline.transport.entry_posts) == len(pipeline.transport.stop_posts) == 1
    stop = pipeline.transport.stop_posts[0]
    assert stop["triggerPrice"] == str(intent.stop_plan.stop_price)
    calls = pipeline.transport.calls
    stop_index = next(i for i, x in enumerate(calls) if x[0:2] == ("POST", "/fapi/v1/algoOrder"))
    assert calls[stop_index + 1][0:2] == ("GET", "/fapi/v1/algoOrder")
    with migrated.transaction() as c:
        assert c.scalar(text("SELECT delivered_at IS NOT NULL FROM outbox"))
        assert c.scalar(text("SELECT status FROM orders")) == "FILLED"
        assert c.scalar(text("SELECT count(DISTINCT fill_id) FROM fills")) == 2
        assert c.scalar(text("SELECT sum(qty) FROM fills")) == intent.qty
        assert c.scalar(text("SELECT min(exchange_at) FROM fills")) >= datetime.fromisoformat(
            forecast[0]["forecast_at"]
        )
        assert c.scalar(text("SELECT protected_qty=qty FROM positions"))
        assert c.scalar(text("SELECT count(*) FROM protective_orders WHERE status='NEW'")) == 2
    assert pipeline.events("edge.outcome.v1") == []

    from tradeforge.runtime.settlement import settle_closed_results

    pipeline.clock.at += timedelta(seconds=1)
    gross = pipeline.transport.hit_take_profit()
    recovered.reconcile()
    assert recovered.ready, recovered.health()
    settle_closed_results(migrated, pipeline.client)
    settle_closed_results(migrated, pipeline.client)
    with migrated.transaction() as c:
        assert c.scalar(text("SELECT qty=0 AND protected_qty=0 FROM positions"))
        protectors = dict(c.execute(text(
            "SELECT split_part(client_order_id,'_',1),status FROM protective_orders"
        )).all())
        assert protectors == {"sl": "CANCELED", "tp": "FINISHED"}
    results = pipeline.events("runtime.closed_result.v1")
    assert len(results) == 1 and D(results[0]["pnl"]) == gross - D("0.003")
    assert len(pipeline.events("edge.outcome.v1")) == 1
    assert len(pipeline.transport.entry_posts) == 1


def test_offline_trial_off_keeps_insufficient_history_without_publication(migrated, monkeypatch):
    pipeline = OfflinePipeline(migrated, monkeypatch, trial_policy=TrialPolicy(enabled=False))
    pipeline.clock.at += timedelta(seconds=1)
    pipeline.demo.tick(pipeline.feed())
    assert pipeline.latest().state is DecisionState.NO_TRADE
    assert pipeline.demo.status()["autonomous_decisions"]["last_reason"] == (
        "INSUFFICIENT_STATISTICAL_EVIDENCE"
    )
    edge = EdgeAnalysis.model_validate_json(
        json.dumps(pipeline.events("edge.analysis.v1")[-1]["body"])
    )
    assert edge.proof_state.value == "UNPROVEN"
    assert edge.train.count == edge.oos.count == 0
    pipeline.assert_no_publication()


def test_offline_candidate_expires_at_its_authentic_deadline(migrated, monkeypatch):
    pipeline = OfflinePipeline(migrated, monkeypatch)
    pipeline.clock.at = pipeline.record.discovery.candidate.expires_at
    pipeline.demo.tick({})
    assert pipeline.latest().state is DecisionState.EXPIRED
    assert pipeline.demo.status()["autonomous_decisions"]["last_reason"] == "CANDIDATE_EXPIRED"
    pipeline.assert_no_publication()


def test_offline_expired_unreserved_outbox_is_acknowledged_without_order(migrated, monkeypatch):
    pipeline = OfflinePipeline(migrated, monkeypatch)
    intent = pipeline.publish()
    pipeline.clock.at = intent.expires_at
    worker, _ = pipeline.workers()
    assert worker.run_once() and not worker.run_once()
    assert pipeline.count("orders") == pipeline.count("fills") == 0
    assert pipeline.transport.entry_posts == []
    assert pipeline.events("execution.intent_expired.v1") == [{"intent_id": str(intent.intent_id)}]
    assert len(pipeline.events("runtime.forecast.v1")) == 1
    assert pipeline.events("edge.outcome.v1") == pipeline.events("runtime.closed_result.v1") == []
    with migrated.transaction() as c:
        assert c.scalar(text("SELECT delivered_at IS NOT NULL FROM outbox"))


def test_offline_fresh_rest_cannot_refresh_archived_risk_factor(migrated, monkeypatch):
    pipeline = OfflinePipeline(migrated, monkeypatch)
    archived_at = pipeline.sample.features.frame.as_of
    pipeline.clock.at = archived_at + timedelta(seconds=61)
    assert pipeline.clock() < pipeline.record.discovery.candidate.expires_at
    pipeline.demo.tick(pipeline.feed())
    assessment = RiskAssessment.model_validate_json(
        json.dumps(pipeline.events("risk.assessment.v1")[-1]["body"])
    )
    assert not assessment.allowed and assessment.reason_codes == ("FACTOR_UNKNOWN",)
    assert assessment.account.as_of == pipeline.clock()
    assert assessment.account.factors[0].as_of == archived_at
    assert pipeline.latest().state is DecisionState.NO_TRADE
    assert pipeline.demo.status()["autonomous_decisions"]["last_reason"] == "RISK_REJECTED"
    pipeline.assert_no_publication()


def test_offline_archived_input_is_immutable_before_trial_authority(migrated, monkeypatch):
    pipeline = OfflinePipeline(migrated, monkeypatch)
    archived = pipeline.events("runtime.candidate.v1")
    with pytest.raises(PersistenceError, match="DB_CONSTRAINT_REJECTED"):
        with migrated.transaction() as c:
            c.execute(
                text(
                    "UPDATE audit_log SET payload=jsonb_set(payload,'{input,flow,count}','101') "
                    "WHERE event_type='runtime.candidate.v1'"
                )
            )
    assert pipeline.events("runtime.candidate.v1") == archived
    assert pipeline.events("risk.assessment.v1") == []
    pipeline.assert_no_publication()


def test_offline_ambiguous_ack_remains_reserved_and_never_resends_after_expiry(
    migrated, monkeypatch
):
    pipeline = OfflinePipeline(migrated, monkeypatch)
    intent = pipeline.publish()
    pipeline.transport.ambiguous = pipeline.transport.lookup_missing = True
    for expired in (False, False, True):
        if expired:
            pipeline.clock.at = intent.expires_at + timedelta(seconds=1)
        worker, _ = pipeline.workers()
        with pytest.raises(EntryHalt, match="ACK_UNRESOLVED"):
            worker.run_once()
        with migrated.transaction() as c:
            assert c.scalar(text("SELECT status FROM orders")) == "ACK_UNKNOWN"
            assert not c.scalar(text("SELECT delivered_at IS NOT NULL FROM outbox"))
    assert len(pipeline.transport.entry_posts) == 1
    assert pipeline.events("execution.intent_expired.v1") == []
    assert pipeline.events("edge.outcome.v1") == pipeline.events("runtime.closed_result.v1") == []
    assert pipeline.count("fills") == 0
    original_quote = pipeline.transport.bid, pipeline.transport.ask
    pipeline.next_setup(monkeypatch, side=Side.SHORT)
    with pytest.raises(EntryHalt, match="TESTNET_TRIAL_SETTLEMENT_REQUIRED"):
        pipeline.demo.tick(pipeline.feed())
    assert pipeline.count("execution_intents") == pipeline.count("outbox") == 1
    assert len(pipeline.events("runtime.forecast.v1")) == 1
    assert len(pipeline.events("execution.authorization.v1")) == 1
    assert pipeline.latest().state is DecisionState.VALIDATING
    pipeline.transport.bid, pipeline.transport.ask = original_quote
    # Actual late wire evidence can resolve the existing reservation and protect it.
    pipeline.transport.lookup_missing = False
    pipeline.transport.fill(intent.qty, 801)
    worker, recovery = pipeline.workers()
    assert worker.run_once()
    assert recovery.ready, recovery.health()
    assert worker.reconcile_once() == 1
    assert len(pipeline.transport.entry_posts) == len(pipeline.transport.stop_posts) == 1
    assert pipeline.count("fills") == 1


def test_offline_default_global_trial_budget_survives_expiry_restart_and_bucket_change(
    migrated,
    monkeypatch,
):
    pipeline = OfflinePipeline(migrated, monkeypatch)
    assert pipeline.policy.max_entries == 20
    first_policy = pipeline.policy.digest
    for index in range(20):
        if index:
            pipeline.next_setup(
                monkeypatch,
                side=Side.SHORT if index % 2 else Side.LONG,
            )
        intent = pipeline.publish()
        pipeline.clock.at = intent.expires_at
        worker, _ = pipeline.workers()
        assert worker.run_once() and not worker.run_once()
        assert pipeline.count("orders") == pipeline.count("fills") == 0
    # A changed trial-policy hash and a restarted decision worker do not reset
    # the default global budget. Every previous attempt has real zero-fill proof.
    changed_policy = TrialPolicy(enabled=True, train_entries=11)
    assert changed_policy.digest != first_policy and changed_policy.max_entries == 20
    pipeline.next_setup(monkeypatch, side=Side.SHORT, policy=changed_policy)
    with pytest.raises(EntryHalt, match="TESTNET_TRIAL_BUDGET_EXHAUSTED"):
        pipeline.demo.tick(pipeline.feed())
    assert pipeline.latest().state is DecisionState.VALIDATING
    assert pipeline.count("execution_intents") == pipeline.count("outbox") == 20
    assert pipeline.count("decisions") == 20
    assert len(pipeline.events("runtime.forecast.v1")) == 20
    assert len(pipeline.events("execution.authorization.v1")) == 20
    assert len(pipeline.events("execution.intent_expired.v1")) == 20
    assert len({p["bucket"]["side"] for p in pipeline.events("runtime.forecast.v1")}) == 2
    assert pipeline.events("edge.outcome.v1") == pipeline.events("runtime.closed_result.v1") == []
    assert pipeline.transport.entry_posts == []
    with migrated.transaction() as c:
        assert c.scalar(text("SELECT count(*) FROM outbox WHERE delivered_at IS NULL")) == 0
    assert pipeline.demo.status()["autonomous_decisions"]["last_reason"] == (
        "TESTNET_TRIAL_BUDGET_EXHAUSTED"
    )

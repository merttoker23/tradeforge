"""Failure injection against real disposable PostgreSQL; no exchange credentials."""

from datetime import timedelta
from decimal import Decimal as D

import pytest
from sqlalchemy import text

from tests.execution_fixtures import FakeBroker, report, setup_execution
from tests.integration.test_execution_orders import manager
from tests.risk_fixtures import change
from tradeforge.execution.broker import AckUnknown, ExchangeRejected


class RecoveryExchange(FakeBroker):
    def __init__(self, intent):
        super().__init__(intent)
        self.qty = D(0)
        self.algos = {}
        self.calls = []
        self.fail_stop = False
        self.stop_timeout = False
        self.close_timeout = False
        self.close_partial = False
        self.extra_orders = []
        self.exits = {}
        self.unavailable = False

    def cancel(self, intent):
        old = self.orders[intent.client_order_id]
        if old.status != "FILLED":
            old = change(old, status="CANCELED")
            self.orders[intent.client_order_id] = old
        return old

    def request(self, method, path, params=None, **kwargs):
        self.calls.append((method, path, params))
        params = params or {}
        if self.unavailable:
            raise AckUnknown("untrusted secret error")
        if path == "/fapi/v3/account":
            return {
                "totalMarginBalance": "10000",
                "availableBalance": "9990",
                "positions": [
                    {
                        "symbol": self.intent.symbol,
                        "positionSide": "BOTH",
                        "positionAmt": str(self.qty),
                    }
                ]
                if self.qty
                else [],
            }
        if path == "/fapi/v3/positionRisk":
            return (
                [
                    {
                        "symbol": self.intent.symbol,
                        "positionSide": "BOTH",
                        "positionAmt": str(self.qty),
                        "entryPrice": "100",
                        "markPrice": "100",
                        "initialMargin": "10",
                        "marginAsset": "USDT",
                    }
                ]
                if self.qty
                else []
            )
        if path == "/fapi/v1/openOrders":
            return self.extra_orders
        if path == "/fapi/v1/openAlgoOrders":
            return [x.copy() for x in self.algos.values() if x["algoStatus"] == "NEW"]
        if path == "/fapi/v1/algoOrder":
            key = params["clientAlgoId"]
            if method == "GET":
                return self.algos.get(key)
            if method == "DELETE":
                self.algos[key]["algoStatus"] = "CANCELED"
                return {"code": "200"}
            if self.fail_stop:
                raise ExchangeRejected("ORDER_REJECTED")
            self.algos[key] = {
                **params,
                "algoId": len(self.algos) + 1,
                "algoStatus": "NEW",
                "orderType": params["type"],
                "closePosition": True,
            }
            if self.stop_timeout:
                raise AckUnknown("ACK_UNKNOWN")
            return self.algos[key].copy()
        if path == "/fapi/v1/order":
            key = params.get("newClientOrderId", params.get("origClientOrderId"))
            if method == "GET":
                return self.exits.get(key)
            assert method == "POST" and params["reduceOnly"] == "true"
            assert params["side"] == ("SELL" if self.qty > 0 else "BUY")
            qty = D(params["quantity"])
            assert 0 < qty <= abs(self.qty)
            filled = qty / 2 if self.close_partial else qty
            self.qty -= filled if self.qty > 0 else -filled
            self.exits[key] = {
                "symbol": self.intent.symbol,
                "clientOrderId": key,
                "orderId": 999,
                "side": params["side"],
                "origQty": str(qty),
                "executedQty": str(filled),
                "reduceOnly": True,
                "positionSide": "BOTH",
                "type": "MARKET",
                "status": "PARTIALLY_FILLED" if self.close_partial else "FILLED",
            }
            if self.close_timeout:
                raise AckUnknown("ACK_UNKNOWN")
            return self.exits[key]
        raise AssertionError((method, path, params))


def prepared(seeded, qty="0.4"):
    db, intent, ctx, at = setup_execution(seeded)
    client = RecoveryExchange(intent)
    client.context = ctx
    manager(db, client, ctx, at).execute(intent.intent_id)
    client.orders[intent.client_order_id] = report(intent, "PARTIALLY_FILLED", qty, at=at)
    client.qty = D(qty)
    from tradeforge.execution.position_lifecycle import PositionLifecycle

    recovery = PositionLifecycle(db, client, clock=lambda: at)
    return db, intent, client, recovery, at


def test_restart_requires_exchange_reconciliation_and_never_rearms_operator_kill(seeded):
    from tradeforge.execution.position_lifecycle import PositionLifecycle
    from tradeforge.risk.storage import RiskService

    db, intent, client, recovery, at = prepared(seeded)
    assert not recovery.ready
    RiskService(db).set_kill_switch(True, reason="operator")
    recovery.reconcile()
    assert recovery.ready
    assert RiskService(db).state().kill_switch
    restarted = PositionLifecycle(db, client, clock=lambda: at)
    assert not restarted.ready
    restarted.reconcile()
    assert restarted.ready
    assert len([x for x in client.calls if x[0] == "POST" and x[1].endswith("algoOrder")]) == 2
    with db.transaction() as c:
        assert c.scalar(text("SELECT protected_qty FROM positions")) == D("0.4")


def test_partial_growth_uses_continuous_close_all_protection_and_persists_actual_qty(seeded):
    db, intent, client, recovery, at = prepared(seeded)
    recovery.reconcile()
    from tradeforge.execution.broker import ExchangeFill

    first = client.orders[intent.client_order_id].fills[0]
    client.orders[intent.client_order_id] = change(
        report(intent, "FILLED", "1", at=at),
        fills=(
            first,
            ExchangeFill(
                trade_id="BTCUSDT:8", qty=D("0.6"), price=D("100"), fee=D("0.01"), exchange_at=at
            ),
        ),
    )
    client.qty = D(1)
    recovery.reconcile()
    assert recovery.ready
    with db.transaction() as c:
        assert c.scalar(text("SELECT qty FROM positions")) == D(1)
        assert c.scalar(text("SELECT protected_qty FROM positions")) == D(1)
        assert c.scalar(text("SELECT min(qty) FROM protective_orders WHERE status='NEW'")) == D(1)
    assert not [x for x in client.calls if x[0] == "DELETE" and x[1].endswith("algoOrder")]
    for method, path, params in client.calls:
        if method == "POST" and path.endswith("algoOrder"):
            assert params["closePosition"] == "true"
            assert "quantity" not in params and "reduceOnly" not in params


def test_missing_stop_is_repaired_after_confirmed_cancel_and_never_resends_unknown(seeded):
    db, intent, client, recovery, at = prepared(seeded)
    recovery.reconcile()
    old = next(x for x in client.algos.values() if x["orderType"] == "STOP_MARKET")
    old["algoStatus"] = "CANCELED"
    recovery.reconcile()
    assert recovery.ready
    assert len([x for x in client.algos.values() if x["orderType"] == "STOP_MARKET"]) == 2
    assert all(x["side"] == "SELL" for x in client.algos.values())


def test_lost_protective_ack_is_queried_without_duplicate_and_survives_restart(seeded):
    db, intent, client, recovery, at = prepared(seeded)
    client.stop_timeout = True
    recovery.reconcile()
    assert recovery.ready and len(client.algos) == 2
    recovery.reconcile()
    assert recovery.ready and len(client.algos) == 2


@pytest.mark.parametrize("close_partial", [False, True])
def test_protection_rejected_cancels_entry_then_reduce_only_emergency_and_latches(
    seeded, close_partial
):
    from tradeforge.risk.storage import RiskService

    db, intent, client, recovery, at = prepared(seeded)
    client.fail_stop = True
    client.close_timeout = True
    client.close_partial = close_partial
    recovery.reconcile()
    assert not recovery.ready
    assert RiskService(db).state().kill_switch
    posts = [x for x in client.calls if x[0] == "POST" and x[1] == "/fapi/v1/order"]
    assert len(posts) == 1
    recovery.reconcile()
    assert len([x for x in client.calls if x[0] == "POST" and x[1] == "/fapi/v1/order"]) == 1
    if close_partial:
        assert recovery.health()["unprotected"][0]["qty"] == "0.2"
    else:
        assert client.qty == 0


def test_unknown_manual_position_and_order_are_authoritative_but_block_entries(seeded):
    from tradeforge.risk.storage import RiskService

    db, intent, ctx, at = setup_execution(seeded)
    client = RecoveryExchange(intent)
    client.qty = D("2")
    client.extra_orders = [{"clientOrderId": "manual", "symbol": intent.symbol}]
    from tradeforge.execution.position_lifecycle import PositionLifecycle

    recovery = PositionLifecycle(db, client, clock=lambda: at)
    recovery.reconcile()
    assert not recovery.ready and RiskService(db).state().kill_switch
    health = recovery.health()
    assert "UNKNOWN_EXCHANGE_ORDER" in health["reasons"]
    assert "POSITION_MISMATCH" in health["reasons"]
    assert health["unprotected"][0]["qty"] == "2"
    assert not client.exits
    with db.transaction() as c:
        assert c.scalar(text("SELECT qty FROM positions")) == D(2)


def test_unprotected_duration_survives_restart_and_rest_failure_is_fail_closed(seeded):
    from tradeforge.execution.position_lifecycle import PositionLifecycle

    db, intent, client, recovery, at = prepared(seeded)
    client.fail_stop = True
    client.close_partial = True
    recovery.reconcile()
    later = at + timedelta(seconds=10)
    restarted = PositionLifecycle(db, client, clock=lambda: later)
    client.unavailable = True
    restarted.reconcile()
    assert not restarted.ready
    health = restarted.health()
    assert health["unprotected"][0]["seconds"] >= 10
    assert "untrusted" not in str(health)


def test_wrong_side_protection_is_never_counted_as_ready(seeded):
    db, intent, client, recovery, at = prepared(seeded)
    recovery.reconcile()
    for algo in client.algos.values():
        algo["side"] = "BUY"
    client.fail_stop = True
    recovery.reconcile()
    assert not recovery.ready and client.qty == 0


def test_orphan_reservation_after_crash_does_not_blindly_place_stop(seeded):
    db, intent, client, recovery, at = prepared(seeded)
    recovery.reconcile()
    client.algos.clear()  # remote no longer proves any reserved stop
    recovery.reconcile()
    assert not recovery.ready
    assert len([x for x in client.calls if x[0] == "POST" and x[1].endswith("algoOrder")]) == 2
    assert client.qty == 0


def test_stop_cancel_between_queries_is_detected_before_ready(seeded):
    db, intent, client, recovery, at = prepared(seeded)
    original = client.request
    count = 0

    def race(method, path, params=None, **kwargs):
        nonlocal count
        if path == "/fapi/v1/openAlgoOrders":
            count += 1
            if count == 2:
                for algo in client.algos.values():
                    algo["algoStatus"] = "CANCELED"
        return original(method, path, params, **kwargs)

    client.request = race
    recovery.reconcile()
    assert not recovery.ready
    assert recovery.health()["unprotected"][0]["qty"] == "0.4"


def test_emergency_cancel_uses_observation_time_after_network_reply(seeded):
    db, intent, client, recovery, at = prepared(seeded)
    client.fail_stop = True
    now = at
    recovery.clock = lambda: now
    original = client.cancel

    def cancel(value):
        nonlocal now
        old = original(value)
        now = at + timedelta(milliseconds=2)
        result = change(old, exchange_at=at + timedelta(milliseconds=1))
        client.orders[intent.client_order_id] = result
        return result

    client.cancel = cancel
    recovery.reconcile()
    assert client.qty == 0
    assert len(client.exits) == 1


def test_confirmed_flat_releases_old_fill_reservation_without_deleting_fills(seeded):
    import json
    from datetime import UTC, datetime
    from uuid import uuid4

    from tests.execution_fixtures import other_intent
    from tradeforge.execution.order_manager import authorize
    from tradeforge.persistence.repositories import enqueue_intent, save_decision
    from tradeforge.risk.governor import RiskAssessment
    from tradeforge.risk.storage import RiskService

    db, intent, client, recovery, at = prepared(seeded)
    client.fail_stop = True
    recovery.reconcile()
    assert client.qty == 0
    recovery.reconcile()
    RiskService(db).set_kill_switch(False, reason="offline operator rearm after flat")
    decision = change(seeded[1], decision_id=uuid4())
    later = other_intent(intent, decision_id=decision.decision_id)
    with db.transaction() as c:
        save_decision(c, decision)
    enqueue_intent(db, later)
    with db.transaction() as c:
        evidence = c.scalar(
            text(
                "SELECT payload FROM audit_log "
                "WHERE event_type='execution.authorization.v1' LIMIT 1"
            )
        )
    authorize(db, later, RiskAssessment.model_validate_json(json.dumps(evidence["quant"])))
    fresh = datetime.now(UTC)
    frame = change(client.context, as_of=fresh)
    broker = FakeBroker(later, order_id="102")
    result = manager(db, broker, frame, fresh).execute(later.intent_id)
    assert result.status.value == "ACCEPTED"
    with db.transaction() as c:
        assert c.scalar(text("SELECT count(*) FROM fills")) == 1


def test_definitive_stop_rejection_has_terminal_evidence(seeded):
    db, intent, client, recovery, at = prepared(seeded)
    client.fail_stop = True
    recovery.reconcile()
    with db.transaction() as c:
        assert c.scalar(text("SELECT status FROM protective_orders")) == "REJECTED"
        assert (
            c.scalar(
                text("SELECT count(*) FROM audit_log WHERE event_type='protection.rejected.v1'")
            )
            == 1
        )
    assert not recovery.ready


def test_new_trade_does_not_reuse_finished_protection_of_previous_trade(seeded):
    from tests.execution_fixtures import other_intent
    from tradeforge.execution.account_sync import ExchangePosition

    db, intent, client, recovery, at = prepared(seeded)
    recovery.reconcile()
    with db.transaction() as c:
        identifier = c.scalar(text("SELECT position_id FROM positions"))
        c.execute(text("UPDATE protective_orders SET status='FINISHED',version=version+1"))
    for algo in client.algos.values():
        algo["algoStatus"] = "FINISHED"
    next_intent = other_intent(intent)
    position = ExchangePosition(intent.symbol, D("0.4"), D("100"), D("100"), D("10"))
    key = recovery.protection.ensure(identifier, position, next_intent, D("95"), "STOP_MARKET", at)
    assert key == "sl_" + next_intent.intent_id.hex
    assert key != "sl_" + intent.intent_id.hex
    assert client.algos[key]["algoStatus"] == "NEW"


def test_short_stop_and_target_close_the_position_with_buy_side(seeded):
    from tradeforge.domain.decision import Side
    from tradeforge.domain.execution import StopPlan
    from tradeforge.execution.account_sync import ExchangePosition

    db, intent, client, recovery, at = prepared(seeded)
    short = change(intent, side=Side.SHORT, stop_plan=StopPlan(qty=intent.qty, stop_price=D("105")))
    position = ExchangePosition(intent.symbol, D("-0.4"), D("100"), D("100"), D("10"))
    identifier = recovery._position(position, at)
    stop = recovery.protection.ensure(identifier, position, short, D("105"), "STOP_MARKET", at)
    target = recovery.protection.ensure(
        identifier, position, short, D("95"), "TAKE_PROFIT_MARKET", at
    )
    assert stop != target
    assert all(client.algos[key]["side"] == "BUY" for key in (stop, target))
    assert all(client.algos[key]["closePosition"] is True for key in (stop, target))
    assert all("quantity" not in client.algos[key] for key in (stop, target))


def test_terminal_zero_fill_order_not_queried_after_exchange_retention_expires(seeded):
    from tradeforge.execution.position_lifecycle import PositionLifecycle

    db, intent, ctx, at = setup_execution(seeded)
    client = RecoveryExchange(intent)
    manager(db, client, ctx, at).execute(intent.intent_id)
    client.orders[intent.client_order_id] = report(intent, "CANCELED", at=at)
    recovery = PositionLifecycle(db, client, clock=lambda: at)
    recovery.reconcile()
    assert recovery.ready
    client.orders.clear()
    client.queries.clear()
    recovery.reconcile()
    assert recovery.ready
    assert client.queries == []


def test_prior_durable_stop_reservation_is_queried_never_blindly_resubmitted(seeded):
    from tradeforge.execution.storage import audit

    db, intent, client, recovery, at = prepared(seeded)
    with db.transaction() as c:
        audit(
            c,
            "execution.stop_reserved.v1",
            {"intent_id": str(intent.intent_id), "client_id": "sl_" + intent.intent_id.hex},
        )
    recovery.reconcile()
    assert not recovery.ready
    assert not [x for x in client.calls if x[0] == "POST" and x[1] == "/fapi/v1/algoOrder"]


def test_canceled_stop_replacement_is_reused_on_subsequent_reconciliation(seeded):
    db, intent, client, recovery, at = prepared(seeded)
    recovery.reconcile()
    old = next(x for x in client.algos.values() if x["orderType"] == "STOP_MARKET")
    old["algoStatus"] = "CANCELED"
    recovery.reconcile()
    assert recovery.ready
    for _ in range(3):
        recovery.reconcile()
        assert recovery.ready
    assert len([x for x in client.algos.values() if x["orderType"] == "STOP_MARKET"]) == 2


def test_take_profit_cancel_in_final_read_blocks_entries_while_stop_still_protects(seeded):
    db, intent, client, recovery, at = prepared(seeded)
    original = client.request
    count = 0

    def race(method, path, params=None, **kwargs):
        nonlocal count
        if path == "/fapi/v1/openAlgoOrders":
            count += 1
            if count == 2:
                for algo in client.algos.values():
                    if algo["orderType"] == "TAKE_PROFIT_MARKET":
                        algo["algoStatus"] = "CANCELED"
        return original(method, path, params, **kwargs)

    client.request = race
    recovery.reconcile()
    assert not recovery.ready
    assert "TAKE_PROFIT_MISSING" in recovery.health()["reasons"]
    assert recovery.health()["unprotected"] == []


def test_manual_increase_cannot_authorize_emergency_close_of_external_exposure(seeded):
    db, intent, client, recovery, at = prepared(seeded)
    client.qty = D("0.8")  # only 0.4 belongs to this entry
    client.fail_stop = True
    recovery.reconcile()
    assert not recovery.ready
    assert "POSITION_MISMATCH" in recovery.health()["reasons"]
    assert not client.exits


def test_changing_mark_does_not_invalidate_a_correct_close_all_stop(seeded):
    db, intent, client, recovery, at = prepared(seeded)
    original = client.request
    counter = 0

    def changing(method, path, params=None, **kwargs):
        nonlocal counter
        result = original(method, path, params, **kwargs)
        if path == "/fapi/v3/positionRisk":
            counter += 1
            result[0]["markPrice"] = str(100 + counter)
        return result

    client.request = changing
    recovery.reconcile()
    assert recovery.ready


@pytest.mark.parametrize("mutation", ["hedge", "duplicate", "nan", "account_disagrees"])
def test_invalid_exchange_account_never_becomes_ready_or_sends_protection(seeded, mutation):
    db, intent, client, recovery, at = prepared(seeded)
    original = client.request

    def malformed(method, path, params=None, **kwargs):
        result = original(method, path, params, **kwargs)
        if path == "/fapi/v3/positionRisk":
            if mutation == "hedge":
                result[0]["positionSide"] = "LONG"
            elif mutation == "duplicate":
                result.append(result[0].copy())
            elif mutation == "nan":
                result[0]["markPrice"] = "NaN"
            elif mutation == "account_disagrees":
                result[0]["positionAmt"] = "0.5"
        return result

    client.request = malformed
    recovery.reconcile()
    assert not recovery.ready and not client.algos


def test_private_rest_boundary_conformance_uses_real_testnet_client_offline(seeded):
    import json
    from urllib.parse import parse_qs

    from tradeforge.execution.binance_client import BinanceTestnetClient, Credentials, Response
    from tradeforge.execution.position_lifecycle import PositionLifecycle

    db, intent, venue, _, at = prepared(seeded)
    calls = []

    def wire(method, path, encoded, headers):
        params = {k: v[0] for k, v in parse_qs(encoded).items()}
        calls.append((method, path, params))
        assert headers["X-MBX-APIKEY"] == "offline-key"
        assert len(params["signature"]) == 64
        if (
            path == "/fapi/v1/order"
            and method == "GET"
            and params["origClientOrderId"] == intent.client_order_id
        ):
            report_value = venue.orders[intent.client_order_id]
            body = {
                "symbol": intent.symbol,
                "clientOrderId": intent.client_order_id,
                "orderId": 101,
                "side": "BUY",
                "origQty": str(intent.qty),
                "executedQty": str(report_value.filled_qty),
                "status": report_value.status,
                "avgPrice": "100",
                "updateTime": int(at.timestamp() * 1000),
            }
        elif path == "/fapi/v1/userTrades":
            body = [
                {
                    "symbol": intent.symbol,
                    "orderId": 101,
                    "id": 7,
                    "qty": "0.4",
                    "price": "100",
                    "commission": "0.01",
                    "commissionAsset": "USDT",
                    "time": int((at - timedelta(milliseconds=1)).timestamp() * 1000),
                }
            ]
        else:
            body = venue.request(method, path, params)
        if body is None:
            return Response(400, {}, b'{"code":-2013}')
        return Response(200, {}, json.dumps(body).encode())

    client = BinanceTestnetClient(
        Credentials(api_key="offline-key", api_secret="offline-secret"),
        mode="testnet",
        opted_in=True,
        read_only=False,
        transport=wire,
        clock=lambda: at,
    )
    recovery = PositionLifecycle(db, client, clock=lambda: at)
    recovery.reconcile()
    assert recovery.ready, recovery.health()
    placed = [params for method, path, params in calls if method == "POST"]
    assert {p["type"] for p in placed} == {"STOP_MARKET", "TAKE_PROFIT_MARKET"}
    assert all(p["closePosition"] == "true" and p["side"] == "SELL" for p in placed)
    assert all("quantity" not in p and "reduceOnly" not in p for p in placed)


@pytest.mark.parametrize("race", ["manual_increase", "entry_not_canceled"])
def test_emergency_rechecks_canceled_entry_and_owned_qty_before_close(seeded, race):
    db, intent, client, recovery, at = prepared(seeded)
    client.fail_stop = True
    original = client.cancel

    def cancel(value):
        if race == "entry_not_canceled":
            return client.orders[intent.client_order_id]
        result = original(value)
        client.qty = D("0.8")
        return result

    client.cancel = cancel
    recovery.reconcile()
    assert not recovery.ready
    assert not client.exits


def test_unknown_regular_order_in_final_snapshot_blocks_readiness(seeded):
    db, intent, client, recovery, at = prepared(seeded)
    original = client.request
    count = 0

    def race(method, path, params=None, **kwargs):
        nonlocal count
        if path == "/fapi/v1/openOrders":
            count += 1
            if count == 2:
                client.extra_orders = [{"clientOrderId": "external", "symbol": intent.symbol}]
        return original(method, path, params, **kwargs)

    client.request = race
    recovery.reconcile()
    assert not recovery.ready
    assert "UNKNOWN_EXCHANGE_ORDER" in recovery.health()["reasons"]


def test_failure_after_authoritative_position_read_preserves_unprotected_health(
    seeded, monkeypatch
):
    from tradeforge.persistence.db import PersistenceError

    db, intent, client, recovery, at = prepared(seeded)

    def fail(*args):
        raise PersistenceError("DB_CONSTRAINT_REJECTED")

    monkeypatch.setattr(recovery.protection, "ensure", fail)
    recovery.reconcile()
    assert not recovery.ready
    assert recovery.health()["unprotected"][0]["qty"] == "0.4"

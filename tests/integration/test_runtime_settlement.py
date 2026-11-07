"""Exchange-confirmed closure updates loss controls exactly once before more entries."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import text

from tests.execution_fixtures import setup_execution


@pytest.mark.parametrize("failure", [None, "unowned", "truncated", "fees", "mainnet"])
def test_closed_loss_enters_cooldown_once_across_restart(seeded, failure):
    from tradeforge.execution.storage import audit
    from tradeforge.risk.storage import RiskService
    from tradeforge.runtime.settlement import settle_closed_results

    db, intent, _, at = setup_execution(seeded)
    with db.transaction() as c:
        c.execute(
            text(
                "INSERT INTO orders(intent_id,client_order_id,exchange_order_id,qty,"
                "filled_qty,avg_price,status) VALUES (:id,:client,'101',1,1,100,'FILLED')"
            ),
            {"id": intent.intent_id, "client": intent.client_order_id},
        )
        audit(
            c,
            "protection.flat.v1",
            {
                "intent_id": str(intent.intent_id),
                "symbol": intent.symbol,
                "as_of": (at + timedelta(seconds=2)).isoformat(),
            },
        )
        audit(
            c,
            "execution.stop_reserved.v1",
            {
                "intent_id": str(intent.intent_id),
                "client_id": "sl_" + intent.intent_id.hex,
            },
        )
        audit(
            c,
            "runtime.forecast.v1",
            {
                "intent_id": str(intent.intent_id),
                "analysis_version": "a" * 64,
                "bucket": {
                    "symbol": intent.symbol,
                    "regime": "r1",
                    "alpha_family": "MOMENTUM",
                    "side": "LONG",
                    "environment": "testnet",
                },
                "forecast_at": intent.created_at.isoformat(),
                "split": "TRAIN",
                "forecast_net_R": None,
                "forecast_p_win": None,
            },
        )

    class Venue:
        environment = "mainnet" if failure == "mainnet" else "testnet"

        def clock(self):
            return at + timedelta(seconds=2)

        def request(self, method, path, params):
            if path == "/fapi/v1/algoOrder":
                if failure == "unowned":
                    return None
                return {
                    "clientAlgoId": params["clientAlgoId"],
                    "symbol": intent.symbol,
                    "closePosition": True,
                    "actualOrderId": "102",
                    "algoStatus": "FINISHED",
                }
            if path == "/fapi/v1/income":
                return []
            trades = [
                {
                    "symbol": intent.symbol,
                    "id": 1,
                    "orderId": 101,
                    "side": "BUY",
                    "qty": "1",
                    "price": "100",
                    "realizedPnl": "0",
                    "commission": "0.1",
                    "commissionAsset": "USDT",
                    "maker": False,
                    "time": int(at.timestamp() * 1000),
                },
                {
                    "symbol": intent.symbol,
                    "id": 2,
                    "orderId": 102,
                    "side": "SELL",
                    "qty": "1",
                    "price": "95",
                    "realizedPnl": "-5",
                    "commission": "0.1",
                    "commissionAsset": "USDT",
                    "maker": False,
                    "time": int((at + timedelta(seconds=1)).timestamp() * 1000),
                },
            ]
            if failure == "truncated":
                return trades[:1]
            if failure == "fees":
                trades[0].pop("maker")
            return trades

    if failure is not None:
        from tradeforge.persistence.db import EntryHalt

        with pytest.raises(EntryHalt):
            settle_closed_results(db, Venue())
        with db.transaction() as c:
            assert (
                c.scalar(text("SELECT count(*) FROM audit_log WHERE event_type='edge.outcome.v1'"))
                == 0
            )
            assert (
                c.scalar(text("SELECT count(*) FROM audit_log WHERE event_type='risk.result.v1'"))
                == 0
            )
        return
    settle_closed_results(db, Venue())
    settle_closed_results(db, Venue())
    state = RiskService(db).state()
    assert state.consecutive_losses == 1 and state.cooldown_until is not None
    with db.transaction() as c:
        assert (
            c.scalar(text("SELECT count(*) FROM audit_log WHERE event_type='risk.result.v1'")) == 1
        )
    from tradeforge.decision.edge import EdgeStore

    outcomes = EdgeStore(db).load_outcomes(at=at + timedelta(seconds=3))
    assert len(outcomes) == 1
    assert outcomes[0].initial_risk == Decimal("5")
    assert outcomes[0].gross_R == Decimal("-1")
    assert outcomes[0].net_R == Decimal("-1.04")
    assert outcomes[0].costs.total == Decimal("0.2")
    assert (
        outcomes[0].net_R
        == outcomes[0].gross_R - outcomes[0].costs.total / outcomes[0].initial_risk
    )
    assert outcomes[0].forecast_net_R is None
    assert outcomes[0].settlement.trade_ids == ("1", "2")
    assert outcomes[0].costs.pnl_basis == "executed_price"


def test_expired_unreserved_outbox_does_not_block_the_next_message(seeded):
    from tests.execution_fixtures import other_intent
    from tests.risk_fixtures import change
    from tradeforge.execution.outbox_worker import OutboxWorker
    from tradeforge.persistence.repositories import enqueue_intent

    db, _, original = seeded
    at = datetime.now(UTC)
    first = change(original, expires_at=at + timedelta(seconds=1))
    enqueue_intent(db, first)
    second = other_intent(original)
    enqueue_intent(db, second)
    sent = []

    class Manager:
        def execute(self, identifier):
            sent.append(identifier)

    worker = OutboxWorker(db, Manager(), clock=lambda: at + timedelta(seconds=2))
    assert worker.run_once()
    assert worker.run_once()
    assert sent == [second.intent_id]


@pytest.mark.parametrize(
    "status,allowed",
    [
        ("REJECTED", True),
        ("CANCELED", True),
        ("EXPIRED", True),
        ("ACK_UNKNOWN", False),
        ("NEW", False),
        ("PARTIALLY_FILLED", False),
        ("UNRESERVED_EXPIRED", True),
        ("UNRESERVED_NO_MARKER", False),
        ("UNDELIVERED_TERMINAL", False),
    ],
)
def test_trial_zero_fill_requires_definitive_terminal_exchange_or_unsent_expiry(
    seeded, status, allowed
):
    from uuid import uuid4

    from tests.decision_fixtures import change
    from tests.unit.test_testnet_trial import trial_evidence
    from tradeforge.execution.storage import audit
    from tradeforge.persistence.db import EntryHalt
    from tradeforge.persistence.repositories import enqueue_intent
    from tradeforge.runtime.trading import reserve_forecast

    db, _, intent = seeded
    _, evidence = trial_evidence()
    evidence = change(evidence, trial=change(evidence.trial, train_entries=1))
    at = (
        intent.expires_at + timedelta(seconds=1)
        if status.startswith("UNRESERVED")
        else datetime.now(UTC)
    )
    enqueue_intent(db, intent)
    with db.transaction() as c:
        audit(
            c,
            "runtime.forecast.v1",
            {
                "intent_id": str(intent.intent_id),
                "bucket": evidence.edge.bucket.model_dump(mode="json"),
                "trial_policy": evidence.trial.model_dump(mode="json"),
                "split": "TRAIN",
            },
        )
        if not status.startswith("UNRESERVED"):
            c.execute(
                text(
                    "INSERT INTO orders(intent_id,client_order_id,qty,filled_qty,status) "
                    "VALUES (:id,:client,:qty,:filled,:status)"
                ),
                {
                    "id": intent.intent_id,
                    "client": intent.client_order_id,
                    "qty": intent.qty,
                    "filled": Decimal("0.1") if status == "PARTIALLY_FILLED" else Decimal(0),
                    "status": "CANCELED" if status == "UNDELIVERED_TERMINAL" else status,
                },
            )
        else:
            if status == "UNRESERVED_EXPIRED":
                audit(c, "execution.intent_expired.v1", {"intent_id": str(intent.intent_id)})
        if status != "UNDELIVERED_TERMINAL":
            c.execute(
                text(
                    "UPDATE outbox SET delivered_at=CURRENT_TIMESTAMP,version=version+1 "
                    "WHERE intent_id=:id"
                ),
                {"id": intent.intent_id},
            )
    if allowed:
        with db.transaction() as c:
            reserve_forecast(c, evidence, uuid4(), at)
    else:
        with pytest.raises(EntryHalt, match="SETTLEMENT_REQUIRED"), db.transaction() as c:
            reserve_forecast(c, evidence, uuid4(), at)
    with db.transaction() as c:
        assert (
            c.scalar(text("SELECT count(*) FROM audit_log WHERE event_type='edge.outcome.v1'")) == 0
        )
        assert (
            c.scalar(text("SELECT count(*) FROM audit_log WHERE event_type='risk.result.v1'")) == 0
        )
        assert c.scalar(
            text("SELECT count(*) FROM audit_log WHERE event_type='runtime.forecast.v1'")
        ) == (2 if allowed else 1)
        if allowed:
            assert c.scalars(
                text(
                    "SELECT payload->>'split' FROM audit_log WHERE event_type='runtime.forecast.v1'"
                )
            ).all() == ["TRAIN", "TRAIN"]

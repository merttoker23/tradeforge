"""Crash, concurrency and redelivery guarantees against disposable PostgreSQL."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from decimal import Decimal as D

import pytest
from sqlalchemy import text

from tests.execution_fixtures import FakeBroker, other_intent, report, setup_execution
from tests.risk_fixtures import change


def manager(db, broker, context, at):
    from tradeforge.execution.order_manager import OrderManager

    return OrderManager(db, broker, lambda intent: context, clock=lambda: at)


def test_timeout_after_acceptance_queries_without_resend_on_duplicate_or_restart(seeded):
    db, intent, context, at = setup_execution(seeded)
    broker = FakeBroker(intent)
    broker.outcome = "timeout"
    assert manager(db, broker, context, at).execute(intent.intent_id).status.value == "ACCEPTED"
    assert manager(db, broker, context, at).execute(intent.intent_id).status.value == "ACCEPTED"
    assert broker.submissions == [intent.client_order_id]
    assert len(broker.queries) >= 1
    with db.transaction() as c:
        assert c.scalar(text("SELECT count(*) FROM orders")) == 1


def test_crash_after_accepted_request_and_concurrent_workers_never_resubmit(seeded):
    db, intent, context, at = setup_execution(seeded)
    broker = FakeBroker(intent)
    broker.outcome = "crash"
    with pytest.raises(SystemExit):
        manager(db, broker, context, at).execute(intent.intent_id)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(lambda _: manager(db, broker, context, at).execute(intent.intent_id), range(2))
        )
    assert all(x.status.value == "ACCEPTED" for x in results)
    assert broker.submissions == [intent.client_order_id]


def test_reserved_but_never_sent_and_missing_lookup_stays_unknown(seeded):
    from tradeforge.persistence.db import EntryHalt

    db, intent, context, at = setup_execution(seeded)
    broker = FakeBroker(intent)
    with db.transaction() as c:
        c.execute(
            text(
                "INSERT INTO orders(intent_id,client_order_id,qty,status) "
                "VALUES (:id,:client,:qty,'ACK_UNKNOWN')"
            ),
            {"id": intent.intent_id, "client": intent.client_order_id, "qty": intent.qty},
        )
    for _ in range(2):
        with pytest.raises(EntryHalt, match="ACK_UNRESOLVED"):
            manager(db, broker, context, at).execute(intent.intent_id)
    assert broker.submissions == []


@pytest.mark.parametrize("reason", ["MARGIN_INSUFFICIENT", "RATE_LIMITED", "ORDER_REJECTED"])
def test_definitive_exchange_rejects_are_durable_and_never_retried(seeded, reason):
    db, intent, context, at = setup_execution(seeded)
    broker = FakeBroker(intent)
    broker.outcome = reason
    m = manager(db, broker, context, at)
    assert m.execute(intent.intent_id).status.value == "REJECTED"
    assert manager(db, broker, context, at).execute(intent.intent_id).status.value == "REJECTED"
    assert broker.submissions == [intent.client_order_id]
    with db.transaction() as c:
        assert (
            c.scalar(
                text("SELECT count(*) FROM audit_log WHERE payload->>'reason'=:r"), {"r": reason}
            )
            >= 1
        )


@pytest.mark.parametrize(
    "patch",
    [
        {"stream_ready": False},
        {"clock_ready": False},
        {"one_way": False},
        {"isolated": False},
        {"leverage": D("11")},
        {"max_notional": D("50")},
    ],
)
def test_guardian_rejects_unsafe_exchange_context_without_sending(seeded, patch):
    from tradeforge.persistence.db import EntryHalt

    db, intent, context, at = setup_execution(seeded)
    broker = FakeBroker(intent)
    with pytest.raises(EntryHalt):
        manager(db, broker, change(context, **patch), at).execute(intent.intent_id)
    assert broker.submissions == []


def test_kill_switch_freshness_and_missing_risk_evidence_block_new_orders(seeded):
    from tradeforge.persistence.db import EntryHalt
    from tradeforge.risk.storage import RiskService

    db, intent, context, at = setup_execution(seeded)
    broker = FakeBroker(intent)
    with pytest.raises(EntryHalt):
        manager(db, broker, context, at + timedelta(seconds=6)).execute(intent.intent_id)
    RiskService(db, clock=lambda: at).set_kill_switch(True, reason="operator")
    with pytest.raises(EntryHalt):
        manager(db, broker, context, at).execute(intent.intent_id)
    assert broker.submissions == []


def test_double_intent_for_same_decision_cannot_submit_twice(seeded):
    from tradeforge.execution.order_manager import authorize
    from tradeforge.persistence.db import EntryHalt
    from tradeforge.persistence.repositories import enqueue_intent

    db, intent, context, at = setup_execution(seeded)
    broker = FakeBroker(intent)
    manager(db, broker, context, at).execute(intent.intent_id)
    second = other_intent(intent)
    enqueue_intent(db, second)
    with db.transaction() as c:
        body = c.scalar(
            text(
                "SELECT payload FROM audit_log "
                "WHERE event_type='execution.authorization.v1' LIMIT 1"
            )
        )
    import json

    from tradeforge.risk.governor import RiskAssessment

    authorize(db, second, RiskAssessment.model_validate_json(json.dumps(body["quant"])))
    with pytest.raises(EntryHalt):
        manager(db, broker, context, at).execute(second.intent_id)
    assert broker.submissions == [intent.client_order_id]


def test_partial_fill_unique_trade_ids_exchange_time_and_terminal_monotonicity(seeded):
    db, intent, context, at = setup_execution(seeded)
    broker = FakeBroker(intent)
    m = manager(db, broker, context, at)
    m.execute(intent.intent_id)
    broker.orders[intent.client_order_id] = report(intent, "PARTIALLY_FILLED", "0.4", at=at)
    for _ in range(2):
        state = m.execute(intent.intent_id)
        assert state.filled_qty == D("0.4")
        assert state.status.value == "PARTIALLY_FILLED"
    assert len(broker.stops) == 1
    with db.transaction() as c:
        assert c.scalar(text("SELECT count(*) FROM fills")) == 1
        assert c.scalar(text("SELECT exchange_at FROM fills")) == at - timedelta(milliseconds=1)
    broker.orders[intent.client_order_id] = report(intent, at=at - timedelta(seconds=1))
    assert m.execute(intent.intent_id).filled_qty == D("0.4")


def test_duplicate_outbox_delivery_is_acknowledged_only_after_durable_order(seeded):
    from tradeforge.execution.outbox_worker import OutboxWorker

    db, intent, context, at = setup_execution(seeded)
    broker = FakeBroker(intent)
    worker = OutboxWorker(db, manager(db, broker, context, at))
    assert worker.run_once()
    assert not worker.run_once()
    with db.transaction() as c:
        assert c.scalar(text("SELECT delivered_at IS NOT NULL FROM outbox"))
    assert broker.submissions == [intent.client_order_id]


def test_stream_gap_survives_restart_and_still_allows_order_queries(seeded):
    from tradeforge.execution.order_manager import stream_gap
    from tradeforge.persistence.db import EntryHalt

    db, intent, context, at = setup_execution(seeded)
    broker = FakeBroker(intent)
    stream_gap(db)
    with pytest.raises(EntryHalt):
        manager(db, broker, context, at).execute(intent.intent_id)
    assert broker.submissions == []


def test_database_write_failure_before_reservation_prevents_any_network_effect(seeded):
    from tradeforge.persistence.db import PersistenceError

    db, intent, context, at = setup_execution(seeded)
    broker = FakeBroker(intent)
    with db.transaction() as c:
        c.execute(
            text(
                "CREATE FUNCTION reject_reservation() RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN IF NEW.event_type='execution.reserved.v1' THEN "
                "RAISE EXCEPTION 'injected'; END IF; RETURN NEW; END $$"
            )
        )
        c.execute(
            text(
                "CREATE TRIGGER reject_reservation BEFORE INSERT ON audit_log "
                "FOR EACH ROW EXECUTE FUNCTION reject_reservation()"
            )
        )
    with pytest.raises(PersistenceError):
        manager(db, broker, context, at).execute(intent.intent_id)
    assert broker.submissions == []
    with db.transaction() as c:
        assert c.scalar(text("SELECT count(*) FROM orders")) == 0


def test_protection_failure_latches_halt_and_never_resubmits_partial_entry(seeded):
    from tradeforge.persistence.db import EntryHalt

    db, intent, context, at = setup_execution(seeded)
    broker = FakeBroker(intent)
    m = manager(db, broker, context, at)
    m.execute(intent.intent_id)
    broker.orders[intent.client_order_id] = report(intent, "PARTIALLY_FILLED", "0.4", at=at)
    broker.stop_outcome = "timeout"
    with pytest.raises(EntryHalt, match="PROTECTION_UNAVAILABLE"):
        m.execute(intent.intent_id)
    assert broker.submissions == [intent.client_order_id]
    with db.transaction() as c:
        assert c.scalar(text("SELECT kill_switch FROM risk_state")) is True


def test_growth_to_full_fill_keeps_unique_fills_and_does_not_duplicate_close_all_stop(seeded):
    from tradeforge.execution.broker import ExchangeFill

    db, intent, context, at = setup_execution(seeded)
    broker = FakeBroker(intent)
    m = manager(db, broker, context, at)
    m.execute(intent.intent_id)
    partial = report(intent, "PARTIALLY_FILLED", "0.4", at=at)
    broker.orders[intent.client_order_id] = partial
    m.execute(intent.intent_id)
    full = change(
        partial,
        status="FILLED",
        filled_qty=D("1"),
        fills=(
            *partial.fills,
            ExchangeFill(
                trade_id="BTCUSDT:8", qty=D("0.6"), price=D("100"), fee=D("0.02"), exchange_at=at
            ),
        ),
    )
    broker.orders[intent.client_order_id] = full
    state = m.execute(intent.intent_id)
    assert state.status.value == "FILLED" and state.fee == D("0.03")
    assert len(broker.stops) == 1
    with db.transaction() as c:
        assert c.scalar(text("SELECT sum(qty) FROM fills")) == D("1")


def test_alpha_authorization_cannot_be_reused_as_reduce_only_command(seeded):
    # An alpha intent cannot obtain the risk-reducing path merely by setting a bool.
    import json

    from tradeforge.execution.order_manager import validate_context
    from tradeforge.persistence.db import EntryHalt
    from tradeforge.risk.governor import RiskAssessment

    db, intent, context, at = setup_execution(seeded)
    with db.transaction() as c:
        body = c.scalar(
            text(
                "SELECT payload FROM audit_log "
                "WHERE event_type='execution.authorization.v1' LIMIT 1"
            )
        )
    quant = RiskAssessment.model_validate_json(json.dumps(body["quant"]))
    with pytest.raises(EntryHalt):
        validate_context(change(intent, reduce_only=True), context, quant, at=at)


def test_market_order_risk_is_sized_at_worst_executable_price_before_send(seeded):
    from tradeforge.persistence.db import EntryHalt

    db, intent, context, at = setup_execution(seeded, market=True, equity=D("2400"))
    broker = FakeBroker(intent)
    with pytest.raises(EntryHalt):
        manager(db, broker, context, at).execute(intent.intent_id)
    assert broker.submissions == []


def test_market_request_uses_bounded_ioc_price_instead_of_unbounded_fill(seeded):
    db, intent, context, at = setup_execution(seeded, market=True)
    broker = FakeBroker(intent)
    manager(db, broker, context, at).execute(intent.intent_id)
    assert broker.placed_prices == [D("100.02")]


def test_terminal_fill_after_outbox_ack_is_selected_for_protection_after_restart(
    seeded, monkeypatch
):
    from tradeforge.execution.outbox_worker import OutboxWorker

    db, intent, context, at = setup_execution(seeded)
    broker = FakeBroker(intent)
    m = manager(db, broker, context, at)
    worker = OutboxWorker(db, m)
    assert worker.run_once()
    broker.orders[intent.client_order_id] = report(intent, "FILLED", "1", at=at)

    def crash(_):
        raise SystemExit("crash after durable full fill, before protection")

    monkeypatch.setattr(m, "_protect", crash)
    with pytest.raises(SystemExit):
        m.execute(intent.intent_id)
    assert broker.stops == []
    restored = OutboxWorker(db, manager(db, broker, context, at))
    assert restored.reconcile_once() == 1
    assert len(broker.stops) == 1
    assert broker.submissions == [intent.client_order_id]

"""Diagnostic-only demo data must remain separate, expire, and never grant orders."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from tradeforge.dashboard.read_model import ReadModel, refresh_freshness
from tradeforge.market.bars import canonical


@pytest.mark.parametrize("environment", ["testnet", "mainnet", "unspecified"])
def test_demo_dispatch_requires_matching_environment_on_durable_risk_evidence(seeded, environment):
    from tests.execution_fixtures import FakeBroker, setup_execution
    from tradeforge.execution.order_manager import OrderManager
    from tradeforge.persistence.db import EntryHalt

    db, intent, context, at = setup_execution(seeded, environment=environment)
    broker = FakeBroker(intent)
    broker.environment = "testnet"
    manager = OrderManager(db, broker, lambda _: context, clock=lambda: at)
    if environment == "testnet":
        assert manager.execute(intent.intent_id).client_order_id == intent.client_order_id
        assert broker.submissions == [intent.client_order_id]
    else:
        with pytest.raises(EntryHalt, match="EXCHANGE_ENVIRONMENT_MISMATCH"):
            manager.execute(intent.intent_id)
        assert broker.submissions == []
        with db.transaction() as c:
            assert c.scalar(text("SELECT count(*) FROM orders")) == 0


def test_demo_missing_is_explained_and_authenticated_snapshot_expires(migrated):
    now = datetime.now(UTC) - timedelta(seconds=1)
    result = ReadModel(migrated, clock=lambda: now).snapshot()
    assert result["demo_readiness"]["reason_codes"] == [
        "DEMO_PREFLIGHT_NOT_RUN",
        "EXECUTION_NOT_RUNNING",
    ]
    state = dict(
        environment="BINANCE_FUTURES_DEMO",
        as_of=now.isoformat(),
        expires_at=(now + timedelta(seconds=30)).isoformat(),
        connection="OK",
        account="OK",
        balance="OK",
        positions="OK",
        clock="OK",
        account_can_trade=True,
        key_permissions="UNKNOWN",
        equity="5000",
        available_balance="4900",
        position_count=0,
        open_order_count=0,
        ready=True,
        live=False,
        orders_enabled=False,
        guardian="NOT_RUNNING",
        reason_codes=["KEY_PERMISSIONS_UNVERIFIED", "GUARDIAN_NOT_RUNNING"],
    )
    with migrated.transaction() as c:
        c.execute(
            text(
                "INSERT INTO audit_log(event_type,observed_at,payload) "
                "VALUES ('demo.preflight.v1',:at,CAST(:body AS jsonb))"
            ),
            {"at": now, "body": canonical(state)},
        )
    result = ReadModel(migrated, clock=lambda: now).snapshot()
    assert result["sections"]["demo"]["status"] == "OK"
    assert result["sections"]["account"]["status"] == "UNKNOWN"
    assert result["demo_readiness"]["orders_enabled"] is False
    assert result["trading_authority"]["status"] == "NOT_READY"
    aged = refresh_freshness(result, now + timedelta(seconds=31))
    assert aged["demo_readiness"]["status"] == "STALE"
    assert aged["demo_readiness"]["orders_enabled"] is False

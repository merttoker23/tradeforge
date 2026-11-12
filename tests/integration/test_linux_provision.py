"""Actual PostgreSQL roles enforce the Linux observer service boundaries."""

from sqlalchemy import text

from tradeforge.dashboard.read_model import ReadModel
from tradeforge.persistence.db import Database
from tradeforge.runtime.container import grant_runtime_access


def test_linux_runtime_roles_cannot_acquire_order_or_peer_write_authority(migrated):
    roles = ("tf_market", "tf_quant", "tf_dashboard")
    with migrated.transaction() as connection:
        for role in roles:
            connection.execute(text(f"CREATE ROLE {role} NOLOGIN"))
        grant_runtime_access(connection, migrated.sql_engine.url.database)
    try:
        for role in roles:
            with migrated.transaction() as connection:
                connection.execute(text(f"SET LOCAL ROLE {role}"))
                assert connection.scalar(text("SELECT count(*) FROM instruments")) == 0
                for table in ("orders", "execution_intents", "outbox", "positions", "commands"):
                    allowed = role == "tf_quant" or (role == "tf_dashboard" and table == "commands")
                    assert (
                        bool(
                            connection.scalar(
                                text(
                                    f"SELECT has_table_privilege(current_user,'{table}',"
                                    "'INSERT,UPDATE,DELETE')"
                                )
                            )
                        )
                        is allowed
                    )
                if role == "tf_dashboard":
                    assert connection.scalar(
                        text("SELECT has_table_privilege(current_user,'audit_log','INSERT')")
                    )
                if role == "tf_quant":
                    connection.execute(
                        text(
                            "INSERT INTO audit_log(event_type,payload) "
                            "VALUES ('demo.preflight.v1','{}')"
                        )
                    )
                    assert not connection.scalar(
                        text("SELECT has_table_privilege(current_user,'runtime_market','UPDATE')")
                    )
        # The real read model can query its complete projection with SELECT only.
        observer = Database(migrated.sql_engine.url)
        from sqlalchemy import event

        @event.listens_for(observer.sql_engine, "connect")
        def observer_role(connection, record):
            with connection.cursor() as cursor:
                cursor.execute("SET ROLE tf_dashboard")
            connection.commit()

        try:
            result = ReadModel(observer).snapshot()
            assert result["trading_authority"]["orders_enabled"] is False
        finally:
            observer.close()
    finally:
        with migrated.transaction() as connection:
            for role in roles:
                connection.execute(text(f"DROP OWNED BY {role}"))
                connection.execute(text(f"DROP ROLE {role}"))


def test_failed_monitor_poll_replaces_previous_successful_readiness(migrated, tmp_path):
    from datetime import UTC, datetime, timedelta

    from tools.demo_runtime import publish

    config = tmp_path / "monitor.url"
    config.write_text(migrated.sql_engine.url.render_as_string(hide_password=False))
    now = datetime.now(UTC)
    state = {
        "environment": "BINANCE_FUTURES_DEMO",
        "connection": "OK",
        "account": "OK",
        "balance": "OK",
        "positions": "OK",
        "clock": "OK",
        "as_of": now.isoformat(),
        "expires_at": (now + timedelta(seconds=30)).isoformat(),
        "reason_codes": [],
    }
    publish(config, state)
    publish(
        config, {**state, "connection": "UNKNOWN", "reason_codes": ["EXCHANGE_TRANSPORT_UNKNOWN"]}
    )
    result = ReadModel(migrated).snapshot()
    assert "EXCHANGE_TRANSPORT_UNKNOWN" in result["demo_readiness"]["reason_codes"]
    assert result["sections"]["demo"]["status"] != "OK"

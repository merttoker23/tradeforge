"""Real disposable PostgreSQL, never a production/VPS connection or SQLite stand-in."""

import os
import shutil
import socket
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def postgres_service(tmp_path_factory):
    supplied = os.environ.get("TF_TEST_DATABASE_URL")
    if supplied:
        url = make_url(supplied)
        if url.host not in {"127.0.0.1", "localhost", "::1"}:
            pytest.fail("Test database must be local and disposable")
        yield {"url": supplied}
        return
    configured = os.environ.get("TF_TEST_PG_BIN")
    located = shutil.which("initdb")
    bin_dir = (
        Path(configured)
        if configured
        else (Path(located).parent if located else ROOT / ".cache/phase02/pgsql/bin")
    )
    suffix = ".exe" if os.name == "nt" else ""
    initdb = bin_dir / f"initdb{suffix}"
    pg_ctl = bin_dir / f"pg_ctl{suffix}"
    if not initdb.is_file():
        pytest.fail("Real PostgreSQL required: set TF_TEST_DATABASE_URL or _TEST_PG_BIN")
    work = tmp_path_factory.mktemp("postgres")
    data = work / "cluster"
    subprocess.run(
        [
            str(initdb),
            "-D",
            str(data),
            "-U",
            "postgres",
            "-A",
            "trust",
            "--no-locale",
            "-E",
            "UTF8",
        ],
        check=True,
        capture_output=True,
        timeout=60,
    )
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    start = [
        str(pg_ctl),
        "-D",
        str(data),
        "-l",
        str(work / "server.log"),
        "-o",
        f"-h 127.0.0.1 -p {port}",
        "-w",
        "start",
    ]
    subprocess.run(
        start, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60
    )
    try:
        yield {
            "url": f"postgresql+psycopg://postgres@127.0.0.1:{port}/postgres",
            "pg_ctl": str(pg_ctl),
            "data": str(data),
            "start": start,
        }
    finally:
        subprocess.run(
            [str(pg_ctl), "-D", str(data), "-m", "immediate", "-w", "stop"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=60,
        )


@pytest.fixture
def database_url(postgres_service):
    """CREATE/DROP only a randomly named database owned by this test."""
    name = "tf_test_" + uuid4().hex
    url = make_url(postgres_service["url"])
    admin = create_engine(url, isolation_level="AUTOCOMMIT", pool_pre_ping=True)
    with admin.connect() as connection:
        connection.execute(text(f'CREATE DATABASE "{name}"'))
    try:
        yield url.set(database=name).render_as_string(hide_password=False)
    finally:
        with admin.connect() as connection:
            connection.execute(text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()


@pytest.fixture
def migrated(database_url):
    from tradeforge.persistence.db import Database
    from tradeforge.persistence.transactions import migrate

    db = Database(database_url)
    migrate(db, "head")
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def proposal():
    from tradeforge.domain.decision import Decision
    from tradeforge.domain.execution import ExecutionIntent

    now = datetime.now(UTC) - timedelta(seconds=1)
    event = {
        "schema_version": 1,
        "event_id": str(uuid4()),
        "source_at": now.isoformat(),
        "observed_at": now.isoformat(),
        "as_of": now.isoformat(),
    }
    identity = {
        "symbol": "BTCUSDT",
        "exchange": "BINANCE",
        "market_type": "USD_M",
        "quote": "USDT",
        "margin_asset": "USDT",
        "contract_type": "PERPETUAL",
    }
    import json

    decision = Decision.model_validate_json(
        json.dumps(
            {
                **event,
                **identity,
                "decision_id": str(uuid4()),
                "regime_version": "r1",
                "universe_version": "u1",
                "feature_hash": "a" * 64,
                "alpha_family": "MOMENTUM",
                "side": "LONG",
                "state": "APPROVED",
                "expires_at": (now + timedelta(hours=1)).isoformat(),
                "trigger": "100",
                "stop_proposal": "90",
                "take_profit_proposal": "120",
                "invalidations": ["price_break"],
                "spread_bps": "1",
                "slippage_bps": "2",
                "estimated_fees": "0.05",
                "funding_cost": "0",
                "net_ev_low_R": "0.5",
                "proof_state": "VALIDATED",
                "calibrated": True,
                "confidence": 0.6,
                "quality": 0.8,
                "reason_codes": ["CANDIDATE_VALIDATED"],
                "risk_policy_version": "risk1",
                "decision_policy_version": "decision1",
            }
        )
    )
    risk_id = uuid4()
    intent = ExecutionIntent.model_validate_json(
        json.dumps(
            {
                **event,
                **identity,
                "event_id": str(uuid4()),
                "intent_id": str(uuid4()),
                "client_order_id": "tf_" + uuid4().hex,
                "decision_id": str(decision.decision_id),
                "side": "LONG",
                "qty": "1.000000000000000123",
                "price": "100.000000000000000001",
                "position_mode": "ONE_WAY",
                "reduce_only": False,
                "stop_plan": {"stop_price": "90", "qty": "1.000000000000000123"},
                "max_slippage_bps": "2",
                "expires_at": decision.expires_at.isoformat(),
                "created_at": now.isoformat(),
                "policy_version": "risk1",
                "risk_snapshot_id": str(risk_id),
            }
        )
    )
    return decision, intent


@pytest.fixture
def seeded(migrated, proposal):
    from tradeforge.persistence.repositories import add_instrument, add_risk_snapshot, save_decision

    decision, intent = proposal
    with migrated.transaction() as connection:
        add_instrument(connection, decision)
        add_risk_snapshot(connection, intent.risk_snapshot_id, {"policy": "risk1"}, decision.as_of)
        save_decision(connection, decision)
    return migrated, decision, intent

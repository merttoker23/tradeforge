"""Real PostgreSQL persistence, cross-worker CAS, rollback and analyst-only approval."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from sqlalchemy import text

from tests.decision_fixtures import NOW, advance, change, discovery, register, validated
from tradeforge.domain.decision import AlphaFamily, Side
from tradeforge.domain.decision import DecisionState as S
from tradeforge.persistence.db import PersistenceError


def test_restart_replay_and_approval_never_create_orders(migrated):
    from tradeforge.decision.storage import DecisionService

    service = DecisionService(migrated)
    d = discovery()
    r = service.submit(register(d), now=NOW)
    for target in (S.WATCHING, S.ARMED, S.VALIDATING):
        r = service.submit(advance(r, target), now=NOW)
    before = service.load(as_of=NOW)
    service = DecisionService(migrated)
    assert service.load(as_of=NOW).records == before.records
    ev = validated(d)
    ev = change(ev, proposal=change(ev.proposal, decision_policy_version=before.policy.digest))
    r = service.submit(advance(r, S.APPROVED, evidence=ev), now=NOW)
    assert r.state is S.APPROVED and not r.trading_allowed
    assert service.load(as_of=NOW).records[-1] == r
    with migrated.transaction() as conn:
        for table in ("decisions", "execution_intents", "outbox", "orders"):
            assert conn.scalar(text(f"SELECT count(*) FROM {table}")) == 0


@pytest.mark.parametrize(
    "operation",
    [
        "UPDATE decision_lifecycle SET state='APPROVED'",
        "DELETE FROM decision_lifecycle",
        "TRUNCATE decision_lifecycle",
    ],
)
def test_database_rejects_history_mutation(migrated, operation):
    from tradeforge.decision.storage import DecisionService

    service = DecisionService(migrated)
    service.submit(register(), now=NOW)
    with pytest.raises(PersistenceError):
        with migrated.transaction() as conn:
            conn.execute(text(operation))
    assert len(service.load(as_of=NOW).records) == 1


def test_simultaneous_cas_from_two_services_has_one_successor(migrated):
    from tradeforge.decision.storage import DecisionService

    r = DecisionService(migrated).submit(register(), now=NOW)
    commands = [advance(r, S.WATCHING, event=f"race-{i}") for i in range(2)]

    def submit(cmd):
        try:
            return DecisionService(migrated).submit(cmd, now=NOW).state
        except ValueError as error:
            return str(error)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(submit, commands))
    assert results.count(S.WATCHING) == 1 and results.count("CAS_CONFLICT") == 1
    assert len(DecisionService(migrated).load(as_of=NOW).records) == 2


def test_simultaneous_cluster_discovery_has_one_reservation(migrated):
    from tradeforge.decision.storage import DecisionService

    commands = [register(discovery("one")), register(discovery("two", symbol="ETHUSDT"))]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(lambda cmd: DecisionService(migrated).submit(cmd, now=NOW), commands)
        )
    assert sorted(r.state.value for r in results) == ["DISCOVERED", "NO_TRADE"]
    assert "CLUSTER_OVERLAP" in next(r for r in results if r.state is S.NO_TRADE).reason_codes


def test_failed_append_rolls_back_without_publishing_result(migrated):
    from tradeforge.decision.storage import DecisionService

    service = DecisionService(migrated)
    r = service.submit(register(), now=NOW)
    with migrated.transaction() as conn:
        conn.execute(
            text(
                "CREATE TRIGGER reject_probe BEFORE INSERT ON decision_lifecycle "
                "FOR EACH ROW EXECUTE FUNCTION append_only()"
            )
        )
    with pytest.raises(PersistenceError):
        service.submit(advance(r, S.WATCHING), now=NOW)
    assert service.load(as_of=NOW).records == (r,)
    assert not migrated.readiness.can_open_entries


def test_batch_conflict_is_durable_and_input_order_independent(migrated):
    from tradeforge.decision.storage import DecisionService

    a = discovery()
    b = discovery("two", side=Side.SHORT, family=AlphaFamily.REVERSAL)
    rows = DecisionService(migrated).discover_batch((b, a), at=NOW, now=NOW)
    assert len(rows) == 2 and all(r.state is S.NO_TRADE for r in rows)
    assert all(r.reason_codes == ("CONFLICTING_ALPHA",) for r in rows)
    assert DecisionService(migrated).load(as_of=NOW).records == rows


def test_reboot_at_expiry_and_historical_asof(migrated):
    from tradeforge.decision.storage import DecisionService

    service = DecisionService(migrated)
    r = service.submit(register(), now=NOW)
    at = NOW + timedelta(seconds=60)
    r = DecisionService(migrated).submit(advance(r, S.WATCHING, at=at), now=at)
    assert r.state is S.EXPIRED
    assert len(service.load(as_of=NOW).records) == 1
    assert len(service.load(as_of=at).records) == 2


def test_migration_can_roll_back_only_empty_lifecycle(migrated):
    from tradeforge.decision.storage import DecisionService
    from tradeforge.persistence.transactions import migrate

    # Preserve durable audit records instead of quietly deleting them on downgrade.
    DecisionService(migrated).submit(register(), now=NOW)
    with pytest.raises(PersistenceError):
        migrate(migrated, "base")
    assert len(DecisionService(migrated).load(as_of=NOW).records) == 1


def test_clock_is_sampled_after_waiting_for_database_lock(migrated):
    from datetime import UTC, datetime
    from time import sleep

    from tradeforge.decision.storage import DecisionService

    at = datetime.now(UTC)
    d = discovery(at=at)
    d = change(d, candidate=change(d.candidate, expires_at=at + timedelta(seconds=1)))
    cmd = register(d, at=at)
    with ThreadPoolExecutor(max_workers=1) as pool:
        with migrated.sql_engine.begin() as conn:
            conn.execute(text("SELECT pg_advisory_xact_lock(74202010)"))
            future = pool.submit(DecisionService(migrated).submit, cmd)
            sleep(1.1)
        with pytest.raises(ValueError, match="EXPIRED_AT_PROCESSING"):
            future.result(timeout=10)
    assert not DecisionService(migrated).load(as_of=datetime.now(UTC)).records

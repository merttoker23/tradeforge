"""Production deployment must fail closed before touching persistent state."""

import hashlib
import importlib
from pathlib import Path

import pytest


def policy():
    return importlib.import_module("deploy.automation.policy")


def source(revision="0007", parent="0006", sql="ALTER TABLE orders ADD COLUMN note text"):
    return (
        "from alembic import op\n"
        f"revision = {revision!r}\ndown_revision = {parent!r}\n"
        "branch_labels = None\ndepends_on = None\n"
        f"def upgrade() -> None:\n    op.execute({sql!r})\n"
        "def downgrade() -> None:\n    pass\n"
    )


def test_policy_is_available():
    assert policy().REPOSITORY == "merttoker23/tradeforge"


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE orders",
        "TRUNCATE orders",
        "DELETE FROM orders",
        "UPDATE orders SET note='x'",
        "ALTER TABLE orders DROP COLUMN note",
        "ALTER TABLE orders ALTER COLUMN note TYPE integer",
        "ALTER TABLE orders ADD COLUMN note text DEFAULT dangerous()",
        "ALTER TABLE orders ADD COLUMN note text NOT NULL",
        "CREATE UNIQUE INDEX x ON orders(note)",
        "CREATE INDEX x ON orders(dangerous(note))",
        "CREATE TABLE x AS SELECT * FROM orders",
        "CREATE TABLE x (id text DEFAULT dangerous())",
        "CREATE FUNCTION x() RETURNS void LANGUAGE sql AS ''",
        "ALTER TABLE orders ADD COLUMN note text; DROP TABLE fills",
        "COPY orders TO PROGRAM 'x'",
        "ALTER TABLE public.orders ADD COLUMN note text",
        "CREATE TABLE schema_migrations (id text)",
        "ALTER TABLE schema_migrations ADD COLUMN note text",
        "SELECT dangerous()",
    ],
)
def test_unsafe_sql_requires_manual_review(sql):
    with pytest.raises(ValueError, match="MANUAL_REVIEW"):
        policy().safe_sql(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "ALTER TABLE orders ADD COLUMN note text",
        "ALTER TABLE orders ADD COLUMN note jsonb NULL",
        "CREATE TABLE display_cache (id uuid NOT NULL, payload jsonb)",
        "CREATE INDEX orders_note ON orders (note)",
    ],
)
def test_additive_literal_sql_is_allowed(sql):
    assert policy().safe_sql(sql)


@pytest.mark.parametrize(
    "extra",
    [
        "import os\nos.system('x')\n",
        "revision = dangerous()\n",
        "@dangerous()\ndef extra():\n    pass\n",
        "from other import op\n",
        "def upgrade(x=dangerous()):\n    pass\n",
    ],
)
def test_migration_module_cannot_run_arbitrary_python(extra):
    with pytest.raises(ValueError, match="MANUAL_REVIEW"):
        policy().parse_revision(source() + extra)


def test_valid_revision_metadata_and_immutable_history():
    s = source()
    assert policy().parse_revision(s)[:2] == ("0007", "0006")
    old = {"0006.py": "trusted existing historical migration"}
    hashes = {k: hashlib.sha256(v.encode()).hexdigest() for k, v in old.items()}
    assert policy().migration_plan({**old, "0007.py": s}, hashes, "0006")[0] == "0007"
    with pytest.raises(ValueError, match="HISTORY_CHANGED"):
        policy().migration_plan({"0006.py": "changed", "0007.py": s}, hashes, "0006")


def test_exact_reviewed_binance_revision_is_approved_without_weakening_generic_sql():
    root = Path(__file__).resolve().parents[2]
    migration = (root / "migrations/versions/0007_binance_environment.py").read_text()
    hashes = policy().REVIEWED_MIGRATIONS
    assert hashlib.sha256(migration.encode()).hexdigest() == hashes["0007_binance_environment.py"]
    head, pending = policy().migration_plan({"0007_binance_environment.py": migration}, {}, "0006")
    assert head == "0007" and len(pending) == 1
    assert pending[0][:2] == ("0007", "0006")
    assert any(s.startswith("CREATE EXTENSION IF NOT EXISTS pgcrypto") for s in pending[0][2])
    # A modified migration or arbitrary extension remains forbidden.
    with pytest.raises(ValueError, match="MANUAL_REVIEW_APPROVED_MIGRATION_CHANGED"):
        policy().migration_plan(
            {"0007_binance_environment.py": migration.replace("pgcrypto", "untrusted")},
            {},
            "0006",
        )
    with pytest.raises(ValueError, match="MANUAL_REVIEW_UNSAFE_SQL"):
        policy().parse_revision(migration)


def test_binance_role_grants_are_limited_to_reviewed_revision():
    from deploy.automation.runtime import BINANCE_0007_GRANTS

    assert "REVOKE ALL ON public.binance_credentials FROM tf_market" in BINANCE_0007_GRANTS
    assert "GRANT SELECT ON public.binance_credentials TO tf_dashboard, tf_guardian" in (
        BINANCE_0007_GRANTS
    )
    assert "GRANT INSERT, UPDATE ON public.binance_credentials TO tf_dashboard" in (
        BINANCE_0007_GRANTS
    )
    assert "GRANT" in BINANCE_0007_GRANTS and "tf_quant" in BINANCE_0007_GRANTS


def test_revised_deployment_applies_reviewed_permissions_atomically(monkeypatch):
    from unittest.mock import Mock

    from deploy.automation import runtime

    ops = runtime.Operations.__new__(runtime.Operations)
    ops.head = "0007"
    ops.pending = [("0007", "0006", ["CREATE EXTENSION IF NOT EXISTS pgcrypto"])]
    ops.sql = "-- Alembic upgrade;\nUPDATE schema_migrations SET version_num='0007';\n"
    ops.sources = {"0007_binance_environment.py": "reviewed"}
    ops.state = {"head": "0006"}
    ops.save = Mock()
    sql = Mock()
    monkeypatch.setattr(runtime, "psql", sql)
    monkeypatch.setattr(runtime, "revision", lambda: "0007")

    ops.migrate()
    submitted = sql.call_args.args[0]
    assert submitted.index("UPDATE schema_migrations") < submitted.index("REVOKE ALL")
    assert "GRANT SELECT ON public.binance_runtime" in submitted
    assert ops.state["head"] == "0007"
    assert ops.save.call_count == 2


def test_branch_or_missing_revision_is_rejected():
    with pytest.raises(ValueError, match="MANUAL_REVIEW"):
        policy().migration_plan({"x.py": source(parent="0005")}, {}, "0006")
    with pytest.raises(ValueError, match="MANUAL_REVIEW"):
        policy().migration_plan({"x.py": source(), "y.py": source("0008")}, {}, "0006")


def claims():
    return {
        "repository": "merttoker23/tradeforge",
        "repository_id": "1305289470",
        "repository_owner_id": "61198883",
        "ref": "refs/heads/main",
        "event_name": "push",
        "workflow_ref": "merttoker23/tradeforge/.github/workflows/ci.yml@refs/heads/main",
        "workflow_sha": "a" * 40,
        "sha": "a" * 40,
        "runner_environment": "github-hosted",
        "run_id": "123",
        "run_attempt": "1",
        "check_run_id": "456",
        "jti": "unique",
    }


@pytest.mark.parametrize(
    "key,value",
    [
        ("repository_id", "123"),
        ("repository_owner_id", "123"),
        ("ref", "refs/heads/other"),
        ("event_name", "pull_request"),
        ("workflow_sha", "b" * 40),
        ("workflow_ref", "merttoker23/tradeforge/.github/workflows/other.yml@refs/heads/main"),
        ("runner_environment", "self-hosted"),
        ("check_run_id", "../../x"),
    ],
)
def test_wrong_oidc_identity_is_rejected(key, value):
    c = claims()
    c[key] = value
    with pytest.raises(ValueError):
        policy().validate_claims(c, "a" * 40)


def test_expected_identity_and_digest():
    policy().validate_claims(claims(), "a" * 40)
    assert policy().image_ref("sha256:" + "a" * 64).endswith("@sha256:" + "a" * 64)
    with pytest.raises(ValueError):
        policy().image_ref("latest; echo bad")


def test_schema_compatibility_requires_all_previous_contracts():
    before = {"orders.id": "uuid not null", "function:x": "body"}
    assert policy().compatible(before, {**before, "orders.note": "text"})
    assert not policy().compatible(before, {"orders.id": "text", "function:x": "body"})
    assert not policy().compatible(before, {"orders.id": "uuid not null"})


def test_cleanup_never_selects_foreign_or_retained_images():
    def own(n):
        return {
            "Id": n,
            "RepoTags": [f"ghcr.io/merttoker23/tradeforge:{n}"],
            "Config": {"Labels": {"io.tradeforge.managed": "true"}},
        }

    foreign = {"Id": "foreign", "RepoTags": ["birdunyataki-app:old"], "Config": {}}
    shared = own("shared")
    shared["RepoTags"].append("other-app:keep")
    assert policy().removable_images(
        [own("active"), own("previous"), own("old"), foreign, shared],
        {"active", "previous"},
        {"foreign"},
    ) == ["old"]


def test_controller_has_no_database_backup_or_global_prune_commands():
    root = Path(__file__).resolve().parents[2]
    for path in (root / "deploy/automation").glob("*.py"):
        body = path.read_text()
        assert "pg_dump" not in body
        assert '["docker", "system", "prune"' not in body
        assert '["docker", "volume", "prune"' not in body

"""Regression tests for the repository security/legacy gate itself."""

import importlib
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "path",
    ["core/a.py", "engine/a.py", "services/a.py", "scripts/a.py", "Dockerfile", "requirements.txt"],
)
def test_audit_rejects_old_tracked_roots(path: str, tmp_path: Path) -> None:
    audit = importlib.import_module("tools.check_repository")
    file = tmp_path / path
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text("", encoding="utf-8")
    assert audit.inspect_files(tmp_path, [path])


@pytest.mark.parametrize(
    "content",
    [
        "from engine.foo import x",
        "import redis",
        "NATS_URL = 'x'",
        "from core import x",
        "RAVEN_NOT_READY = True",
    ],
)
def test_audit_rejects_forbidden_runtime_content(content: str, tmp_path: Path) -> None:
    audit = importlib.import_module("tools.check_repository")
    file = tmp_path / "src/tradeforge/bad.py"
    file.parent.mkdir(parents=True)
    file.write_text(content, encoding="utf-8")
    assert audit.inspect_files(tmp_path, ["src/tradeforge/bad.py"])


def test_audit_allows_quoted_negative_test_fixture(tmp_path: Path) -> None:
    audit = importlib.import_module("tools.check_repository")
    for path in ["tests/test_new.py"]:
        file = tmp_path / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text("# engine.foo legacy specification", encoding="utf-8")
    assert audit.inspect_files(tmp_path, ["tests/test_new.py"]) == []


@pytest.mark.parametrize("path", ["tests/test_helper.py", "tools/helper.py", "helper.py"])
@pytest.mark.parametrize(
    "content",
    [
        "import redis",
        "from core import x",
        "import engine.foo",
        "import os\nos.getenv('REDIS_URL')",
        "RAVEN_NOT_READY = True",
    ],
)
def test_audit_checks_executable_legacy_in_every_python_location(
    path: str, content: str, tmp_path: Path
) -> None:
    audit = importlib.import_module("tools.check_repository")
    file = tmp_path / path
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(content, encoding="utf-8")
    assert audit.inspect_files(tmp_path, [path])


def test_audit_allows_quoted_negative_fixture_but_not_executable_import(tmp_path: Path) -> None:
    audit = importlib.import_module("tools.check_repository")
    file = tmp_path / "test_example.py"
    file.write_text('fixture = "import redis"', encoding="utf-8")
    assert audit.inspect_files(tmp_path, ["test_example.py"]) == []
    file.write_text("import redis", encoding="utf-8")
    assert audit.inspect_files(tmp_path, ["test_example.py"])


def test_audit_distinguishes_module_name_data_from_uppercase_config(tmp_path: Path) -> None:
    audit = importlib.import_module("tools.check_repository")
    file = tmp_path / "scanner.py"
    file.write_text("blocked_modules = {'influxdb_client'}", encoding="utf-8")
    assert audit.inspect_files(tmp_path, ["scanner.py"]) == []
    file.write_text("import os\nos.getenv('INFLUXDB_URL')", encoding="utf-8")
    assert audit.inspect_files(tmp_path, ["scanner.py"])


@pytest.mark.parametrize(
    "path,content",
    [(".env", ""), ("secret.pem", ""), ("src/key.txt", "-----BEGIN " + "PRIVATE KEY-----")],
)
def test_audit_rejects_secrets(path: str, content: str, tmp_path: Path) -> None:
    audit = importlib.import_module("tools.check_repository")
    file = tmp_path / path
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(content, encoding="utf-8")
    assert audit.inspect_files(tmp_path, [path])


@pytest.mark.parametrize(
    "name",
    [
        "docs/spec/plan.md",
        "docs/spec/phase_index.json",
        "docs/superpowers/plans/plan.md",
        "deploy/postgres/README.md",
    ],
)
def test_audit_rejects_tracking_local_plans(name: str, tmp_path: Path) -> None:
    audit = importlib.import_module("tools.check_repository")
    file = tmp_path / name
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text("local-only instructions", encoding="utf-8")
    assert audit.inspect_files(tmp_path, [name])


def test_audit_keeps_root_readme_versioned(tmp_path: Path) -> None:
    audit = importlib.import_module("tools.check_repository")
    (tmp_path / "README.md").write_text("Tradeforge 2.0", encoding="utf-8")
    assert audit.inspect_files(tmp_path, ["README.md"]) == []


def test_audit_allows_root_agents_only(tmp_path: Path) -> None:
    audit = importlib.import_module("tools.check_repository")
    (tmp_path / "AGENTS.md").write_text("User rules", encoding="utf-8")
    assert audit.inspect_files(tmp_path, ["AGENTS.md"]) == []
    (tmp_path / "apps").mkdir()
    (tmp_path / "apps/AGENTS.md").write_text("Local", encoding="utf-8")
    assert audit.inspect_files(tmp_path, ["apps/AGENTS.md"])


def test_postgres_has_internal_network_and_no_published_host_ports() -> None:
    compose = yaml.safe_load((ROOT / "deploy/docker-compose.yml").read_text(encoding="utf-8"))
    assert {
        "postgres",
        "market_gateway",
        "quant_core",
        "dashboard",
        "caddy",
        "migrate",
    } == set(compose["services"])
    postgres = compose["services"]["postgres"]
    assert not postgres.get("ports")
    assert postgres["networks"] == ["state"]
    assert compose["networks"]["state"]["internal"] is True
    assert postgres["environment"]["POSTGRES_PASSWORD_FILE"] == "/run/secrets/postgres_password"
    assert "POSTGRES_PASSWORD" not in postgres["environment"]
    assert postgres["healthcheck"]["test"][0] == "CMD-SHELL"

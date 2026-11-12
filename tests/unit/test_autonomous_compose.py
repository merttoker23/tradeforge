"""VPS topology has a single authenticated HTTPS ingress and independent workers."""

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("x-app", "/tmp:size=64m,mode=1777"),
        ("migrate", "/tmp:size=64m,mode=1777"),
        ("market_gateway", "/tmp:size=64m,mode=1777"),
        ("quant_core", "/tmp:size=64m,mode=1777"),
        ("dashboard", "/tmp:size=64m,mode=1777"),
        ("caddy", "/tmp:size=32m,mode=1777"),
    ],
)
def test_compose_tmpfs_options_parse_as_one_absolute_mount(name, expected):
    config = yaml.safe_load((ROOT / "deploy/docker-compose.yml").read_text())
    service = config["x-app"] if name == "x-app" else config["services"][name]
    assert service["tmpfs"] == [expected]


def test_compose_owns_runtime_and_publishes_only_https_proxy():
    config = yaml.safe_load((ROOT / "deploy/docker-compose.yml").read_text())
    topology = config["services"]
    assert {
        "postgres",
        "migrate",
        "market_gateway",
        "quant_core",
        "dashboard",
        "caddy",
    } == set(topology)
    for name, service in topology.items():
        if name != "caddy":
            assert not service.get("ports")
        if name not in {"migrate"}:
            assert service["restart"] == "unless-stopped"
            assert service["healthcheck"]["test"]
    for name in ("market_gateway", "quant_core"):
        assert "dashboard" not in topology[name].get("depends_on", {})
    assert "binance_demo_key" not in topology["quant_core"].get("secrets", [])
    assert "postgres_password" not in topology["dashboard"].get("secrets", [])
    assert config["networks"]["state"]["internal"] is True
    assert topology["postgres"]["volumes"] == ["postgres_state:/var/lib/postgresql/data"]
    assert "execution_guardian" not in topology
    assert "guardian_password" not in config["secrets"]
    assert topology["quant_core"]["environment"]["TF_AUTO_ORDERS"] == "${TF_AUTO_ORDERS:-false}"
    assert "egress" in topology["quant_core"]["networks"]
    assert "postgres_state:/postgres-volume:ro" in topology["quant_core"]["volumes"]


def test_container_build_copies_migrations_and_excludes_all_local_state():
    assert (ROOT / "deploy/Dockerfile").is_file()
    assert "!migrations/" in (ROOT / ".dockerignore").read_text()
    assert not list((ROOT / "deploy/linux").glob("*.service"))

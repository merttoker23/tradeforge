"""Local process provisioning never needs exchange keys or changes live mode."""

import json

import pytest


def test_dashboard_cli_check_stays_offline(monkeypatch, capsys):
    from apps.dashboard.runtime import main

    monkeypatch.setattr("sys.argv", ["dashboard", "--check"])
    assert main() == 0
    assert json.loads(capsys.readouterr().out)["orders_enabled"] is False


def test_dashboard_server_without_credentials_is_locked():
    from apps.dashboard.runtime import build_app

    assert build_app() is not None


def test_dashboard_rejects_nonlocal_database_and_redacts_config(tmp_path):
    from apps.dashboard.runtime import build_app

    config = tmp_path / "connection"
    config.write_text("postgresql+psycopg://user:PASSWORD_SENTINEL@remote.invalid/db")
    with pytest.raises(ValueError, match="DASHBOARD_CONFIG_INVALID") as error:
        build_app(database_file=config)
    assert "PASSWORD_SENTINEL" not in str(error.value)


def test_provision_token_is_private_unique_and_digest_only(tmp_path):
    from apps.dashboard.runtime import provision
    from tradeforge.dashboard.auth import Authenticator

    config, credential = tmp_path / "auth", tmp_path / "token"
    provision(config, credential)
    token = credential.read_text().strip()
    assert len(token) >= 43 and token not in config.read_text()
    assert Authenticator.from_json(config.read_text()).authenticate("Bearer " + token, "control")
    with pytest.raises(ValueError):
        provision(config, credential)

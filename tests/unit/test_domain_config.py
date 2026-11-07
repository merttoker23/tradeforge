"""Mode policies apply equally to defaults, environment, updates and entrypoints."""

import ast
import logging
from pathlib import Path

import pytest
from pydantic import ValidationError

from tradeforge.config.policy import RuntimeMode
from tradeforge.config.settings import ConfigError, Settings, load_settings, update_settings


def test_default_settings_are_paper_and_frozen() -> None:
    settings = load_settings({})
    assert settings.mode is RuntimeMode.PAPER
    assert settings.live is False
    with pytest.raises(ValidationError):
        settings.live = True
    with pytest.raises(ValidationError):
        Settings(mode="paper")


@pytest.mark.parametrize("mode", ["disabled", "paper", "testnet"])
def test_non_live_modes_are_explicit_labels_only(mode: str) -> None:
    settings = load_settings({"TRADEFORGE_MODE": mode, "TRADEFORGE_LIVE": "false"})
    assert settings.mode.value == mode
    assert settings.live is False


@pytest.mark.parametrize("mode", ["live", "LIVE", "PAPER", "", "true", " paper", "unknown"])
def test_invalid_or_live_mode_refused(mode: str) -> None:
    with pytest.raises(ConfigError, match="RUNTIME_CONFIG_INVALID"):
        load_settings({"TRADEFORGE_MODE": mode})


@pytest.mark.parametrize("value", ["true", "True", "1", "0", "", "FALSE", " false"])
def test_live_flag_never_enables_itself(value: str) -> None:
    with pytest.raises(ConfigError):
        load_settings({"TRADEFORGE_LIVE": value})


def test_direct_live_and_json_cannot_bypass_policy() -> None:
    for values in [
        {"mode": RuntimeMode.LIVE},
        {"live": True},
        {"live": 0},
        {"mode": RuntimeMode.PAPER, "live": "false"},
    ]:
        with pytest.raises(ValidationError):
            Settings.model_validate(values)
    with pytest.raises(ValidationError):
        Settings.model_validate_json('{"mode":"live","live":false}')
    with pytest.raises(ValueError):
        Settings().model_copy(update={"mode": RuntimeMode.LIVE})


def test_unknown_prefixed_keys_fail_and_legacy_keys_are_not_read() -> None:
    with pytest.raises(ConfigError):
        load_settings({"TRADEFORGE_LVIE": "true"})
    assert (
        load_settings({"MODE": "live", "LIVE": "true", "BINANCE_API_KEY": "sentinel"}) == Settings()
    )


def test_no_automatic_env_file_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / ".env").write_text("TRADEFORGE_MODE=live\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TRADEFORGE_MODE", raising=False)
    monkeypatch.delenv("TRADEFORGE_LIVE", raising=False)
    assert load_settings().mode is RuntimeMode.PAPER


def test_config_replacement_logs_names_only_and_failure_keeps_original(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="tradeforge.config")
    current = Settings()
    replacement = update_settings(current, {"mode": RuntimeMode.DISABLED})
    assert replacement.mode is RuntimeMode.DISABLED
    assert current.mode is RuntimeMode.PAPER
    assert "mode" in caplog.text
    assert "disabled" not in caplog.text
    for changes in [
        {"mode": "TEST_SECRET_SENTINEL"},
        {"api_key": "TEST_SECRET_SENTINEL"},
        {"mode": RuntimeMode.LIVE},
        {"live": True},
    ]:
        with pytest.raises(ConfigError) as error:
            update_settings(current, changes)
        assert "TEST_SECRET_SENTINEL" not in str(error.value) + repr(error.value)
    for environment in [
        {"TRADEFORGE_MODE": "TEST_SECRET_SENTINEL"},
        {"TRADEFORGE_API_KEY": "TEST_SECRET_SENTINEL"},
    ]:
        with pytest.raises(ConfigError) as error:
            load_settings(environment)
        assert "TEST_SECRET_SENTINEL" not in str(error.value)
    assert "TEST_SECRET_SENTINEL" not in caplog.text
    assert current == Settings()


def test_domain_and_config_cannot_import_network_or_execution_clients() -> None:
    root = Path(__file__).resolve().parents[2] / "src/tradeforge"
    forbidden = {"httpx", "aiohttp", "requests", "socket", "subprocess", "binance", "ccxt"}
    for directory in ["domain", "config"]:
        for file in (root / directory).glob("*.py"):
            tree = ast.parse(file.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    assert not any(alias.name.split(".")[0] in forbidden for alias in node.names)
                elif isinstance(node, ast.ImportFrom):
                    assert (node.module or "").split(".")[0] not in forbidden

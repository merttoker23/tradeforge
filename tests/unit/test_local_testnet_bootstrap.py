import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from apps.market_gateway import runtime as gateway
from apps.quant_core import runtime as quant
from tradeforge.config.policy import RuntimeMode
from tradeforge.runtime import auto_execution, binance_environment


def test_gateway_cli_accepts_local_testnet(monkeypatch):
    observed = []
    db = SimpleNamespace(close=lambda: None)

    async def serve(database, port, **kwargs):
        observed.append(
            (database, port, kwargs["loopback"], kwargs["full_depth"], kwargs["depth_batch_size"])
        )

    monkeypatch.setenv("TRADEFORGE_MODE", "testnet")
    monkeypatch.setattr(gateway, "local_database", lambda _: db)
    monkeypatch.setattr(gateway, "serve", serve)
    monkeypatch.setattr("sys.argv", ["gateway", "--stream", "--database-url-file", "db"])
    assert gateway.main() == 0
    assert observed == [(db, 8101, True, True, 24)]


def test_testnet_gateway_fences_environment_before_opening_sockets(monkeypatch):
    monkeypatch.setenv("TRADEFORGE_MODE", "testnet")
    monkeypatch.setattr(
        binance_environment, "select_environment", lambda _: SimpleNamespace(name="live")
    )
    with pytest.raises(ValueError, match="TESTNET_ENVIRONMENT_MISMATCH"):
        asyncio.run(gateway.serve(None, 8101))


@pytest.mark.parametrize(
    ("mode", "env", "token", "flag", "reason"),
    [
        (RuntimeMode.TESTNET, "testnet", None, "true", "INTERNAL_TOKEN_MISSING"),
        (RuntimeMode.PAPER, "testnet", "x" * 48, "true", "TESTNET_MODE_REQUIRED"),
        (RuntimeMode.TESTNET, "live", "x" * 48, "true", "TESTNET_ENVIRONMENT_MISMATCH"),
        (RuntimeMode.TESTNET, "testnet", "x" * 48, "false", "TF_AUTO_ORDERS_DISABLED"),
        (RuntimeMode.TESTNET, "testnet", "x" * 48, "true", "TESTNET_CREDENTIALS_MISSING"),
    ],
)
def test_execution_preflight_reports_blocker(monkeypatch, mode, env, token, flag, reason):
    monkeypatch.setenv("TF_AUTO_ORDERS", flag)
    monkeypatch.setattr(quant, "load_credentials", lambda *_: None)
    assert quant.execution_preflight(None, mode, env, token) == (reason, None, False)


def test_testnet_execution_without_trial_reaches_db_preflight():
    class PreflightReached(Exception):
        pass

    def transaction():
        raise PreflightReached

    with pytest.raises(PreflightReached):
        asyncio.run(
            auto_execution.serve(
                SimpleNamespace(transaction=transaction),
                SimpleNamespace(environment="testnet"),
                internal_token="x" * 48,
                trial_enabled=False,
            )
        )


def test_quant_cli_defaults_to_loopback_with_token(monkeypatch):
    observed = []

    async def serve(*args, **kwargs):
        observed.append(kwargs["gateway_base"])

    monkeypatch.setattr(quant, "serve", serve)
    monkeypatch.setattr(quant, "local_database", lambda _: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(
        quant, "local_token_file", lambda path: "x" * 48 if path == Path("token") else None
    )
    monkeypatch.setattr(
        "sys.argv", ["quant", "--database-file", "db", "--gateway-token-file", "token"]
    )
    assert quant.main() == 0
    assert observed == ["http://127.0.0.1:8101"]

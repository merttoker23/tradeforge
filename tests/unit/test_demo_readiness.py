"""No network, credentials or orders: demo capability and diagnostic contracts."""

import hashlib
import hmac
import json
import os
import subprocess
from datetime import UTC, datetime

import pytest

from tradeforge.execution.binance_client import BinanceTestnetClient, Credentials, Response

NOW = datetime(2026, 10, 23, tzinfo=UTC)


def demo_client(*, patch=None, read_only=True):
    calls = []
    bodies = {
        "/fapi/v1/ping": {},
        "/fapi/v1/time": {"serverTime": int(NOW.timestamp() * 1000)},
        "/fapi/v3/account": {
            "totalMarginBalance": "5000",
            "availableBalance": "4900",
            "totalUnrealizedProfit": "0",
            "positions": [],
        },
        "/fapi/v3/balance": [{"asset": "USDT", "balance": "5000", "availableBalance": "4900"}],
        "/fapi/v3/positionRisk": [],
        "/fapi/v1/accountConfig": {
            "canTrade": True,
            "canWithdraw": False,
            "dualSidePosition": False,
            "multiAssetsMargin": False,
        },
        "/fapi/v1/positionSide/dual": {"dualSidePosition": False},
        "/fapi/v1/openOrders": [],
        "/fapi/v1/openAlgoOrders": [],
    }
    bodies.update(patch or {})

    def transport(method, path, params, headers):
        calls.append((method, path, params, headers))
        return Response(200, {}, json.dumps(bodies[path]).encode())

    client = BinanceTestnetClient(
        Credentials(api_key="offline-demo-key", api_secret="offline-demo-secret"),
        mode="testnet",
        opted_in=True,
        read_only=read_only,
        transport=transport,
        clock=lambda: NOW,
    )
    return client, calls


@pytest.mark.parametrize(
    "method,path",
    [
        ("POST", "/fapi/v1/order"),
        ("POST", "/fapi/v1/algoOrder"),
        ("DELETE", "/fapi/v1/order"),
        ("PUT", "/fapi/v1/listenKey"),
    ],
)
def test_observation_capability_blocks_every_mutation_before_transport(method, path):
    client, calls = demo_client()
    with pytest.raises(ValueError, match="DEMO_READ_ONLY"):
        client.request(method, path)
    assert calls == []


def test_preflight_get_only_signed_reads_and_no_invented_permission_proof():
    from tradeforge.execution.preflight import preflight

    client, calls = demo_client()
    result = preflight(client)
    assert result["connection"] == result["account"] == "OK"
    assert result["environment"] == "BINANCE_FUTURES_DEMO"
    assert result["equity"] == "5000" and result["position_count"] == 0
    assert result["orders_enabled"] is False and result["live"] is False
    assert result["key_permissions"] == "UNKNOWN"
    assert result["account_can_trade"] is True
    assert {call[0] for call in calls} == {"GET"}
    for _, path, params, headers in calls:
        if path not in {"/fapi/v1/ping", "/fapi/v1/time"}:
            unsigned, signature = params.rsplit("&signature=", 1)
            assert (
                hmac.new(b"offline-demo-secret", unsigned.encode(), hashlib.sha256).hexdigest()
                == signature
            )
            assert headers["X-MBX-APIKEY"] == "offline-demo-key"
    assert "offline-demo" not in json.dumps(result)


@pytest.mark.parametrize(
    "patch,reason",
    [
        ({"/fapi/v1/time": {"serverTime": int(NOW.timestamp() * 1000) + 2000}}, "CLOCK_UNSAFE"),
        ({"/fapi/v3/positionRisk": {}}, "DEMO_RESPONSE_INVALID"),
        ({"/fapi/v1/accountConfig": {"canTrade": False}}, "ACCOUNT_TRADING_DISABLED"),
        ({"/fapi/v3/balance": []}, "USDT_BALANCE_MISSING"),
    ],
)
def test_preflight_failures_never_report_ready(patch, reason):
    from tradeforge.execution.preflight import preflight

    client, _ = demo_client(patch=patch)
    result = preflight(client)
    assert reason in result["reason_codes"]
    assert result["ready"] is False and result["orders_enabled"] is False


@pytest.mark.skipif(os.name != "posix", reason="Linux private credential files")
def test_only_ignored_untracked_separate_private_files_can_supply_keys(tmp_path):
    from tradeforge.runtime.demo import load_demo_credentials

    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    (tmp_path / ".gitignore").write_text("/private/\n")
    private = tmp_path / "private"
    private.mkdir()
    for name, value in [
        ("binance-demo.key", "demo-key-value"),
        ("binance-demo.secret", "demo-secret-value"),
    ]:
        target = private / name
        target.write_text(value)
        target.chmod(0o600)
    credentials = load_demo_credentials(private, root=tmp_path)
    assert credentials.api_key.get_secret_value() == "demo-key-value"
    (tmp_path / ".gitignore").write_text("")
    with pytest.raises(ValueError, match="DEMO_SECRET_FILE_UNSAFE"):
        load_demo_credentials(private, root=tmp_path)


@pytest.mark.skipif(os.name != "posix", reason="Linux private credential files")
def test_dashboard_token_reuse_rejected_even_in_ignored_directory(tmp_path):
    from tradeforge.runtime.demo import load_demo_credentials

    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    (tmp_path / ".gitignore").write_text("/private/\n")
    private = tmp_path / "private"
    private.mkdir()
    for name in ("binance-demo.key", "binance-demo.secret", "dashboard.token"):
        target = private / name
        target.write_text("same-secret-value")
        target.chmod(0o600)
    with pytest.raises(ValueError, match="DEMO_SECRET_FILE_UNSAFE"):
        load_demo_credentials(private, root=tmp_path)


def test_guardian_rejects_public_or_unlabelled_context_before_any_demo_read():
    from tests.integration.test_execution_context import frame
    from tradeforge.execution.context import verify_exchange_context
    from tradeforge.persistence.db import EntryHalt

    client, calls = demo_client()
    with pytest.raises(EntryHalt, match="EXCHANGE_ENVIRONMENT_MISMATCH"):
        verify_exchange_context(client, frame(), stream_ready=True)
    assert calls == []


@pytest.mark.skipif(os.name != "posix", reason="Linux private credential files")
def test_custom_observer_bearer_token_cannot_be_used_as_demo_key(tmp_path):
    from tradeforge.runtime.demo import load_demo_credentials

    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    (tmp_path / ".gitignore").write_text("/private/\n/observer/\n")
    private = tmp_path / "private"
    observer = tmp_path / "observer"
    private.mkdir()
    observer.mkdir()
    for path, value in [
        (private / "binance-demo.key", "custom-bearer-value"),
        (private / "binance-demo.secret", "different-secret-value"),
        (observer / "dashboard.token", "custom-bearer-value"),
    ]:
        path.write_text(value)
        path.chmod(0o600)
    with pytest.raises(ValueError, match="DEMO_SECRET_FILE_UNSAFE"):
        load_demo_credentials(private, root=tmp_path, observer_home=observer)


def test_account_transfer_flag_is_not_mistaken_for_key_withdrawal_permission():
    from tradeforge.execution.preflight import preflight

    client, _ = demo_client(
        patch={
            "/fapi/v1/accountConfig": {
                "canTrade": True,
                "canWithdraw": True,
                "dualSidePosition": False,
                "multiAssetsMargin": False,
            }
        }
    )
    result = preflight(client)
    assert result["ready"] is True
    assert result["key_permissions"] == "UNKNOWN"
    assert result["orders_enabled"] is False
    assert "WITHDRAWAL_STATUS_UNVERIFIED" not in result["reason_codes"]

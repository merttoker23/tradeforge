"""Endpoint isolation and the production order boundary never use exchange network I/O."""

import pytest

from tradeforge.execution.binance_client import (
    BinanceLiveReadOnlyClient,
    Credentials,
    send_https,
)
from tradeforge.execution.broker import ExchangeRejected
from tradeforge.market.clock_sync import ClockSync
from tradeforge.market.exchange_info import ExchangeInfoClient, https_get
from tradeforge.market.market_gateway import MarketGateway
from tradeforge.market.recovery import PublicMarketClient
from tradeforge.market.socket_manager import AiohttpConnector
from tradeforge.runtime.binance_environment import ENDPOINTS, _passphrase


def test_endpoint_mapping_fixed_no_fallback():
    assert ENDPOINTS["testnet"] == (
        "demo-fapi.binance.com",
        "https://demo-fapi.binance.com",
        "https://demo-fstream.binance.com",
    )
    assert ENDPOINTS["live"][0] == "fapi.binance.com"
    assert PublicMarketClient(environment="testnet").base_url == "https://demo-fapi.binance.com"
    assert PublicMarketClient(environment="live").base_url == "https://fapi.binance.com"
    assert AiohttpConnector(base_url="https://demo-fstream.binance.com").base_url.endswith(
        "demo-fstream.binance.com"
    )
    with pytest.raises(ValueError, match="BINANCE_ENVIRONMENT_INVALID"):
        PublicMarketClient(environment="mainnet")
    with pytest.raises(ValueError, match="UNSAFE_MARKET_ENDPOINT"):
        AiohttpConnector(base_url="https://demo-fstream.binance.com.evil.example")


def test_public_metadata_respects_selected_host():
    client = ExchangeInfoClient(environment="testnet")
    from functools import partial

    assert isinstance(client.transport, partial)
    assert client.transport.keywords == {"host": "demo-fapi.binance.com"}
    with pytest.raises(ValueError, match="UNSAFE_MARKET_ENDPOINT"):
        https_get({}, 1, host="evil.example")
    with pytest.raises(ValueError, match="UNSAFE_EXCHANGE_HOST"):
        send_https("GET", "/fapi/v1/ping", "", {}, host="evil.example")


def test_gateway_environment_and_live_read_only_order_gate():
    assert MarketGateway(ClockSync(), environment="testnet").environment == "testnet"
    with pytest.raises(ValueError, match="BINANCE_ENVIRONMENT_INVALID"):
        MarketGateway(ClockSync(), environment="unknown")
    client = BinanceLiveReadOnlyClient(
        Credentials(api_key="example_key", api_secret="example_secret")
    )
    assert client.environment == "live"
    assert client.read_only is True
    with pytest.raises(ValueError, match="DEMO_READ_ONLY"):
        client.request("POST", "/fapi/v1/order", {})
    with pytest.raises(ExchangeRejected, match="LIVE_ORDERS_LOCKED"):
        client.place(None)
    with pytest.raises(ExchangeRejected, match="LIVE_ORDERS_LOCKED"):
        client.cancel(None)


def test_passphrase_domain_separated_and_short_master_rejected():
    assert len(_passphrase("a" * 32)) == 64
    assert _passphrase("a" * 32) != "a" * 32
    with pytest.raises(ValueError, match="BINANCE_ENCRYPTION_SECRET_INVALID"):
        _passphrase("short")


def test_pinned_database_transactions_hold_revision_lock_and_fail_closed():
    from contextlib import contextmanager
    from unittest.mock import MagicMock

    from tradeforge.persistence.db import Database, EntryHalt

    db = Database.__new__(Database)
    connection = MagicMock()
    connection.scalar.return_value = 8
    mock_engine = MagicMock()

    @contextmanager
    def begin():
        yield connection

    mock_engine.begin = begin
    db.sql_engine = mock_engine
    db._pinned_environment_revision = None
    db.pin_environment(8)
    with db.transaction() as acquired:
        assert acquired is connection
    assert "FOR SHARE" in str(connection.scalar.call_args.args[0])
    with pytest.raises(ValueError, match="BINANCE_ENVIRONMENT_PIN_INVALID"):
        db.pin_environment(9)
    connection.scalar.return_value = 9
    with pytest.raises(EntryHalt, match="BINANCE_ENVIRONMENT_RESTART_REQUIRED"):
        with db.transaction():
            pytest.fail("mismatched worker must not reach stored market data")


def test_environment_switch_rejects_unpartitioned_history():
    from contextlib import contextmanager
    from unittest.mock import MagicMock

    from tradeforge.runtime.binance_environment import switch_environment

    c = MagicMock()
    c.execute.return_value.scalar_one.return_value = "testnet"

    def scalar(statement):
        sql = str(statement)
        if "FROM instruments" in sql:
            return True
        return False

    c.scalar.side_effect = scalar

    @contextmanager
    def transaction():
        yield c

    db = MagicMock()
    db.transaction = transaction
    with pytest.raises(ValueError, match="BINANCE_UNPARTITIONED_HISTORY_BLOCKS_SWITCH"):
        switch_environment(db, "live")
    assert not any("UPDATE binance_runtime" in str(x.args[0]) for x in c.execute.call_args_list)

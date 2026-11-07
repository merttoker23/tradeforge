"""Explicit Demo operation configures flat symbols conservatively before entry."""

import pytest


def test_demo_symbol_isolated_and_one_times_leverage_are_verified_after_assignment():
    from tradeforge.runtime.demo_decisions import demo_symbol_config

    class Venue:
        environment = "testnet"

        def __init__(self):
            self.mode, self.leverage, self.writes = "CROSSED", 20, []

        def request(self, method, path, params=None):
            if method == "POST":
                self.writes.append((path, params))
                self.mode, self.leverage = "ISOLATED", 1
                return {}
            if path == "/fapi/v3/account":
                return {"positions": []}
            if path in {"/fapi/v1/openOrders", "/fapi/v1/openAlgoOrders"}:
                return []
            return [
                {
                    "symbol": "BTCUSDT",
                    "marginType": self.mode,
                    "leverage": self.leverage,
                    "isAutoAddMargin": False,
                    "maxNotionalValue": "10000",
                }
            ]

    venue = Venue()
    assert demo_symbol_config(venue, "BTCUSDT")["leverage"] == 1
    assert {p for p, _ in venue.writes} == {"/fapi/v1/marginType", "/fapi/v1/leverage"}
    assert all("order" not in p.lower() for p, _ in venue.writes)
    assert demo_symbol_config(venue, "BTCUSDT")["marginType"] == "ISOLATED"
    assert len(venue.writes) == 2


def test_demo_configuration_never_changes_an_exposed_account():
    from tradeforge.persistence.db import EntryHalt
    from tradeforge.runtime.demo_decisions import demo_symbol_config

    class Venue:
        environment = "testnet"

        def request(self, method, path, params=None):
            assert method == "GET"
            if path == "/fapi/v3/account":
                return {"positions": [{"positionAmt": "1"}]}
            return [{"symbol": "BTCUSDT", "marginType": "CROSSED", "leverage": 20}]

    with pytest.raises(EntryHalt, match="FLAT_ACCOUNT_REQUIRED"):
        demo_symbol_config(Venue(), "BTCUSDT")


def test_demo_configuration_rejects_any_mainnet_broker_before_io():
    from tradeforge.persistence.db import EntryHalt
    from tradeforge.runtime.demo_decisions import demo_symbol_config

    with pytest.raises(EntryHalt, match="DEMO_ONLY"):
        demo_symbol_config(object(), "BTCUSDT")


def test_trial_runtime_switch_requires_explicit_testnet_order_opt_in():
    from tradeforge.config.policy import RuntimeMode, require_demo_trial

    assert require_demo_trial(RuntimeMode.PAPER, orders="false", trial="false") is False
    assert require_demo_trial(RuntimeMode.TESTNET, orders="true", trial="true") is True
    for mode, orders, trial in (
        (RuntimeMode.PAPER, "false", "true"),
        (RuntimeMode.LIVE, "true", "true"),
        (RuntimeMode.TESTNET, "false", "true"),
        (RuntimeMode.TESTNET, "true", "TRUE"),
    ):
        with pytest.raises(ValueError):
            require_demo_trial(mode, orders=orders, trial=trial)

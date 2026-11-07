"""REST repairs sparse WS input without renewing WS coverage or stale prices."""

from datetime import UTC, datetime
from types import SimpleNamespace

from tradeforge.market.runtime import GatewayPersistence


def test_rest_snapshot_supplies_cost_inputs_with_explicit_mainnet_provenance():
    at = datetime(2026, 10, 23, tzinfo=UTC)
    gateway = SimpleNamespace(
        eligible={"BTCUSDT"},
        clock=lambda: at,
        monotonic=lambda: 1,
        clock_sync=SimpleNamespace(ready=lambda *args: True),
    )
    persistence = GatewayPersistence(gateway, None)
    stamp = int(at.timestamp() * 1000)
    quotes = [{"symbol": "BTCUSDT", "bidPrice": "100", "askPrice": "101", "time": stamp}]
    marks = [
        {
            "symbol": "BTCUSDT",
            "markPrice": "100.5",
            "indexPrice": "100.4",
            "lastFundingRate": "0.0001",
            "time": stamp,
        }
    ]
    tickers = [{"symbol": "BTCUSDT", "volume": "123", "closeTime": stamp}]
    samples = persistence.rest_markets(quotes, marks, tickers, at)
    assert len(samples) == 1
    assert samples[0].environment == "mainnet" and samples[0].source == "binance:rest"
    assert samples[0].volume == 123 and str(samples[0].funding) == "0.0001"
    quotes[0]["time"] = stamp - 16000
    assert persistence.rest_markets(quotes, marks, tickers, at) == ()
    quotes[0]["time"] = stamp
    assert persistence.rest_markets(quotes, [], tickers, at) == ()
    marks[0]["time"] = stamp + 3600000
    assert persistence.rest_markets(quotes, marks, tickers, at) == ()


def test_broad_cost_recovery_does_not_wait_for_a_watchlist():
    import asyncio
    from datetime import timedelta

    at = datetime(2026, 10, 23, tzinfo=UTC)
    stamp = int(at.timestamp() * 1000)
    observation = SimpleNamespace(
        observed_at=at, expires_at=at + timedelta(hours=1), version_hash="v1"
    )
    gateway = SimpleNamespace(
        eligible={"BTCUSDT"},
        clock=lambda: at,
        monotonic=lambda: 1,
        clock_sync=SimpleNamespace(ready=lambda *a: True),
        view=SimpleNamespace(ready=True, observation=observation),
        plan=SimpleNamespace(groups=()),
        _epoch=1,
        fresh=lambda *a: True,
        stale_after=15,
    )
    persistence = GatewayPersistence(gateway, None)
    calls = []

    class Client:
        async def fetch(self, path, params):
            calls.append(path)
            rows = {
                "/fapi/v1/premiumIndex": [
                    {
                        "symbol": "BTCUSDT",
                        "markPrice": "100.5",
                        "indexPrice": "100.4",
                        "lastFundingRate": "0.0001",
                        "time": stamp,
                    }
                ],
                "/fapi/v1/ticker/24hr": [
                    {"symbol": "BTCUSDT", "volume": "123", "closeTime": stamp}
                ],
                "/fapi/v1/ticker/bookTicker": [
                    {"symbol": "BTCUSDT", "bidPrice": "100", "askPrice": "101", "time": stamp}
                ],
            }
            return rows[path], at

    assert asyncio.run(persistence.quote_once(Client())) is True
    assert len(persistence.rest_samples) == 1
    assert "/fapi/v1/ticker/bookTicker" in calls

"""Demo websocket URL routing keeps the live feed isolated and unchanged."""

import asyncio
from contextlib import asynccontextmanager

import pytest

from tradeforge.market.socket_manager import AiohttpConnector


@pytest.mark.parametrize("route", ["public", "market"])
def test_testnet_uses_demo_category_route(route):
    connector = AiohttpConnector(base_url="https://demo-fstream.binance.com")
    assert connector.stream_url(route) == f"https://demo-fstream.binance.com/{route}/stream"


@pytest.mark.parametrize("route", ["public", "market"])
def test_production_retains_category_routes(route):
    connector = AiohttpConnector(base_url="https://fstream.binance.com")
    assert connector.stream_url(route) == f"https://fstream.binance.com/{route}/stream"


def test_no_cross_environment_or_private_stream():
    connector = AiohttpConnector(base_url="https://demo-fstream.binance.com")
    with pytest.raises(ValueError, match="PUBLIC_MARKET_ROUTE_REQUIRED"):
        connector.stream_url("private")
    with pytest.raises(ValueError, match="UNSAFE_MARKET_ENDPOINT"):
        AiohttpConnector(base_url="https://demo-fstream.binance.com.evil.example")


def test_testnet_open_passes_correct_endpoint_without_network():
    async def scenario():
        connector = AiohttpConnector(base_url="https://demo-fstream.binance.com")
        recorded = []

        class Socket:
            async def receive(self):
                raise AssertionError("Unexpected network operation")

        class Session:
            @asynccontextmanager
            async def ws_connect(self, url, **kwargs):
                recorded.append((url, kwargs))
                yield Socket()

        connector.session = Session()
        for route in ("market", "public"):
            async with connector.open(route):
                pass
        return recorded

    recorded = asyncio.run(scenario())
    assert [url for url, _ in recorded] == [
        "https://demo-fstream.binance.com/market/stream",
        "https://demo-fstream.binance.com/public/stream",
    ]

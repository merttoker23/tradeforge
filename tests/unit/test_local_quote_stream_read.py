"""Regressions: large Gateway payloads and JSON-mode Decimal parsing."""

import asyncio
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from tradeforge.dashboard import local_terminal as module


class ChunkedBody:
    def __init__(self, body: bytes, *, chunk: int = 4096):
        self.body = body
        self.chunk = chunk
        self.iterated = False

    async def iter_chunked(self, n):
        self.iterated = True
        for start in range(0, len(self.body), min(n, self.chunk)):
            yield self.body[start : start + min(n, self.chunk)]


class Response:
    status = 200

    def __init__(self, payload, chunk=4096):
        self.content = ChunkedBody(json.dumps(payload).encode(), chunk=chunk)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None


class Session:
    def __init__(self, response):
        self.response = response

    def get(self, *_args, **_kwargs):
        return self.response


def test_270kb_gateway_response_is_reassembled_before_json_decode(monkeypatch):
    at = datetime.now(UTC)
    payload = {
        "as_of": at.isoformat(),
        "clock_safe": True,
        "eligible": ["BTCUSDT", "ETHUSDT"],
        "quotes": [{"symbol": "BTCUSDT", "source_at": at.isoformat(), "mark_price": "99999.12"}],
        "padding": "A" * 270_000,
    }
    response = Response(payload, chunk=512)
    feed = module.LocalQuoteFeed()
    feed._session = Session(response)
    calls = []

    def parse(cls, body):
        calls.append(body)
        record = json.loads(body)
        return SimpleNamespace(symbol=record["symbol"], source_at=at)

    monkeypatch.setattr(
        module.MarketQuote, "model_validate_json", classmethod(parse), raising=False
    )
    asyncio.run(feed._sample())
    assert response.content.iterated
    assert feed.state()["transport"] == "OK"
    assert feed.state()["eligible"] == {"BTCUSDT", "ETHUSDT"}
    assert len(feed.quotes()) == 1
    assert json.loads(calls[0])["mark_price"] == "99999.12"


def test_partial_individual_quote_does_not_invalidate_entire_feed(monkeypatch):
    at = datetime.now(UTC)
    payload = {
        "as_of": at.isoformat(),
        "clock_safe": True,
        "eligible": ["BTCUSDT", "ETHUSDT"],
        "quotes": [
            {"symbol": "ETHUSDT", "source_at": at.isoformat()},
            {"symbol": "BTCUSDT", "source_at": at.isoformat()},
        ],
    }
    feed = module.LocalQuoteFeed()
    feed._session = Session(Response(payload, chunk=10))

    def parse(cls, body):
        row = json.loads(body)
        if row["symbol"] == "ETHUSDT":
            raise ValueError("INVALID_QUOTE")
        return SimpleNamespace(symbol=row["symbol"], source_at=at)

    monkeypatch.setattr(
        module.MarketQuote, "model_validate_json", classmethod(parse), raising=False
    )
    asyncio.run(feed._sample())
    assert feed.state()["transport"] == "OK"
    assert [q.symbol for q in feed.quotes()] == ["BTCUSDT"]


def test_quote_feed_size_limit_is_enforced(monkeypatch):
    at = datetime.now(UTC)
    payload = {
        "as_of": at.isoformat(),
        "clock_safe": True,
        "eligible": ["BTCUSDT"],
        "quotes": [],
        "padding": "A" * 2048,
    }
    feed = module.LocalQuoteFeed()
    feed._session = Session(Response(payload, chunk=125))
    monkeypatch.setattr(module, "_MAX_RESPONSE", 1024)
    with pytest.raises(ValueError, match="GATEWAY_QUOTES_OVERSIZED"):
        asyncio.run(feed._sample())
    assert feed.state()["transport"] == "UNAVAILABLE"

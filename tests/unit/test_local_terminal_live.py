"""Regression tests for standalone terminal quote and chart read paths."""

import asyncio
import json
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from tradeforge.dashboard import local_terminal as module


class _Response:
    status = 200

    def __init__(self, payload):
        self.content = SimpleNamespace(iter_chunked=self._chunks)
        self.payload = payload

    async def _chunks(self, size):
        data = json.dumps(self.payload).encode()
        for offset in range(0, len(data), min(1024, size)):
            yield data[offset : offset + min(1024, size)]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


def test_quote_feed_is_fail_closed_without_fresh_gateway():
    feed = module.LocalQuoteFeed()
    assert feed.state()["transport"] == "UNAVAILABLE"
    assert feed.quotes() == ()
    feed._clock_safe = True
    feed._last_received = time.monotonic() - 10
    assert feed.state()["transport"] == "UNAVAILABLE"


def test_valid_gateway_quotes_reach_dashboard_without_renewing_source(monkeypatch):
    observed = datetime.now(UTC)
    payload = {
        "as_of": observed.isoformat(),
        "clock_safe": True,
        "eligible": ["BTCUSDT", "ETHUSDT"],
        "quotes": [{"symbol": "BTCUSDT", "source_at": observed.isoformat()}],
    }
    feed = module.LocalQuoteFeed()
    feed._session = SimpleNamespace(get=lambda *_args, **_kwargs: _Response(payload))
    monkeypatch.setattr(
        module.MarketQuote,
        "model_validate_json",
        classmethod(
            lambda cls, item: SimpleNamespace(
                symbol=json.loads(item)["symbol"],
                source_at=datetime.fromisoformat(json.loads(item)["source_at"]),
            )
        ),
        raising=False,
    )
    asyncio.run(feed._sample())
    assert feed.state()["transport"] == "OK"
    assert feed.state()["eligible"] == {"BTCUSDT", "ETHUSDT"}
    assert len(feed.quotes()) == 1
    assert feed.quotes()[0].source_at == observed


def test_gateway_clock_failure_never_marks_quotes_fresh():
    observed = datetime.now(UTC)
    feed = module.LocalQuoteFeed()
    feed._session = SimpleNamespace(
        get=lambda *_a, **_kw: _Response(
            {
                "as_of": observed.isoformat(),
                "clock_safe": False,
                "eligible": ["BTCUSDT"],
                "quotes": [],
            }
        )
    )
    with pytest.raises(ValueError, match="GATEWAY_CLOCK_UNSAFE"):
        asyncio.run(feed._sample())
    assert feed.state()["transport"] == "UNAVAILABLE"


class _DbConnection:
    def __init__(self, cache, latest, history):
        self.cache, self.latest, self.history = cache, latest, history
        self.history_reads = 0

    def scalar(self, sql, _params):
        query = str(sql)
        if "terminal_chart_cache" in query:
            return self.cache
        if "ORDER BY b.open_at DESC" in query:
            return self.latest
        raise AssertionError(query)

    def execute(self, sql, _params):
        query = str(sql)
        if "FROM regime_snapshots" in query:
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: None))
        if "DISTINCT ON (b.open_at)" in query:
            self.history_reads += 1
            return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: self.history))
        raise AssertionError(query)


@contextmanager
def _transaction(connection):
    yield connection


def _terminal(cache, latest, history, monkeypatch):
    model = object.__new__(module.TerminalReadModel)
    model.clock = lambda: datetime.now(UTC)
    connection = _DbConnection(cache, latest, history)
    model.transaction = lambda: _transaction(connection)
    monkeypatch.setattr(
        module,
        "_render_bars",
        lambda rows, symbol, tf, now: [dict(row) for row in rows],
    )
    return model, connection


def test_newer_canonical_candle_replaces_old_cached_chart(monkeypatch):
    model, db = _terminal(
        [{"time": 100}],
        {"time": 200},
        [{"time": 190}, {"time": 200}],
        monkeypatch,
    )
    chart = model.terminal_chart("BTCUSDT", "15m")
    assert [b["time"] for b in chart["bars"]] == [190, 200]
    assert db.history_reads == 1


def test_current_300_bar_cache_is_retained_without_unnecessary_full_query(monkeypatch):
    model, db = _terminal(
        [{"time": 100}, {"time": 200}],
        {"time": 200},
        [],
        monkeypatch,
    )
    chart = model.terminal_chart("BTCUSDT", "15m")
    assert [b["time"] for b in chart["bars"]] == [100, 200]
    assert db.history_reads == 0


def test_chart_rejects_invalid_requested_interval(monkeypatch):
    model, _db = _terminal([], None, [], monkeypatch)
    with pytest.raises(ValueError, match="CHART_QUERY_INVALID"):
        model.terminal_chart("BTCUSDT", "30m")


def test_candles_reject_out_of_order_and_future_rows(monkeypatch):
    now = datetime.now(UTC)

    def parse(cls, body, context=None):
        item = json.loads(body)
        opened = datetime.fromtimestamp(item["time"], UTC)
        return SimpleNamespace(
            symbol="BTCUSDT",
            timeframe=SimpleNamespace(value="15m"),
            open_at=opened,
            close_at=opened + timedelta(minutes=15) - timedelta(milliseconds=1),
            open=1,
            high=1.1,
            low=0.9,
            close=1,
            volume=100,
        )

    monkeypatch.setattr(module.ClosedBar, "model_validate_json", classmethod(parse))
    first = int((now - timedelta(hours=2)).timestamp())
    with pytest.raises(ValueError, match="CHART_SOURCE_INVALID"):
        module._render_bars([{"time": first}, {"time": first}], "BTCUSDT", "15m", now)
    with pytest.raises(ValueError, match="CHART_SOURCE_INVALID"):
        module._render_bars(
            [{"time": int((now + timedelta(hours=1)).timestamp())}], "BTCUSDT", "15m", now
        )

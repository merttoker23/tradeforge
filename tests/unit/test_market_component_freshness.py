"""Cost coverage and alpha admission require their own source evidence."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from tests.intelligence_fixtures import engine, frame, observation, universe
from tests.unit.test_alpha_handoff import feed_for
from tests.unit.test_momentum_alpha import change, evidence
from tradeforge.domain.market import MarketSnapshot, RegimeLabel, Timeframe
from tradeforge.market.runtime import GatewayPersistence, _merge_fresh
from tradeforge.market.streams import GLOBAL_STREAMS
from tradeforge.runtime.market_link import alpha_inputs, memory_payload

AT = datetime(2026, 10, 23, tzinfo=UTC)


def gateway(at=AT, symbols=("BTCUSDT",)):
    return SimpleNamespace(
        eligible=set(symbols),
        clock=lambda: at,
        monotonic=lambda: 1,
        clock_sync=SimpleNamespace(ready=lambda *args: True),
        stale_after=15,
        _latest={},
        _generations={},
        canonical=SimpleNamespace(books={}, flows={}),
        status=lambda: {"data_ready": True},
        view=SimpleNamespace(ready=False, observation=None),
    )


def rest_rows(at=AT, *, symbol="BTCUSDT", book_age=1, mark_age=2, volume_age=3):
    def stamp(age):
        return int((at - timedelta(seconds=age)).timestamp() * 1000)

    return (
        [{"symbol": symbol, "bidPrice": "100", "askPrice": "100.02", "time": stamp(book_age)}],
        [
            {
                "symbol": symbol,
                "markPrice": "100.01",
                "indexPrice": "100.01",
                "lastFundingRate": "0.0001",
                "time": stamp(mark_age),
            }
        ],
        [{"symbol": symbol, "volume": "123", "closeTime": stamp(volume_age)}],
    )


@pytest.mark.parametrize("ticker_kind", ["stale", "missing", "future", "malformed"])
def test_fresh_rest_costs_survive_unusable_volume_without_inventing_its_time(ticker_kind):
    worker = GatewayPersistence(gateway(), None)
    books, marks, tickers = rest_rows(volume_age=30)
    if ticker_kind == "missing":
        tickers = []
    elif ticker_kind == "future":
        tickers[0]["closeTime"] = int((AT + timedelta(seconds=1)).timestamp() * 1000)
    elif ticker_kind == "malformed":
        tickers[0]["volume"] = "NaN"
    rows = worker.rest_markets(books, marks, tickers, AT)
    assert len(rows) == 1
    market = rows[0]
    assert market.source_at == AT - timedelta(seconds=2)
    assert market.book_source_at == AT - timedelta(seconds=1)
    assert market.mark_source_at == AT - timedelta(seconds=2)
    assert market.costs_fresh(AT, 15)
    assert not market.volume_fresh(AT, 5)
    if ticker_kind == "stale":
        assert market.volume == Decimal("123")
        assert market.volume_source_at == AT - timedelta(seconds=30)
    else:
        assert market.volume is None and market.volume_source_at is None


@pytest.mark.parametrize("endpoint,age", [(0, 16), (0, -1), (1, 16), (1, -1)])
def test_stale_or_future_cost_source_still_fails_closed(endpoint, age):
    worker = GatewayPersistence(gateway(), None)
    rows = rest_rows()
    rows[endpoint][0]["time"] = int((AT - timedelta(seconds=age)).timestamp() * 1000)
    assert worker.rest_markets(*rows, AT) == ()


@pytest.mark.parametrize("missing", ["book_source_at", "mark_source_at", "volume_source_at"])
def test_partial_component_provenance_never_uses_legacy_fallback(missing):
    sample = evidence().market
    payload = {
        **sample.model_dump(),
        "book_source_at": sample.source_at,
        "mark_source_at": sample.source_at,
        "volume_source_at": sample.source_at,
    }
    del payload[missing]
    with pytest.raises(ValidationError):
        MarketSnapshot.model_validate(payload)


def test_future_component_time_is_rejected_even_with_old_composite_time():
    sample = evidence().market
    with pytest.raises(ValidationError):
        change(
            sample,
            book_source_at=sample.source_at,
            mark_source_at=sample.observed_at + timedelta(seconds=1),
            volume_source_at=sample.source_at,
        )


@pytest.mark.parametrize("volume_age", [None, 6, 30])
def test_alpha_handoff_rejects_fresh_costs_without_fresh_volume(volume_age, monkeypatch):
    sample = evidence()
    at = sample.as_of
    market = change(
        sample.market,
        source_at=at,
        book_source_at=at,
        mark_source_at=at,
        volume=None if volume_age is None else Decimal("123"),
        volume_source_at=None if volume_age is None else at - timedelta(seconds=volume_age),
    )
    feed = feed_for(sample)
    feed["markets"] = [market.model_dump(mode="json")]
    diagnostics = {}
    monkeypatch.setattr(
        "tradeforge.runtime.market_link.load_bars",
        lambda db, key, tf, at, limit: (
            sample.medium_bars if tf is Timeframe.M15 else sample.setup_bars
        ),
    )
    assert alpha_inputs(None, sample.features, feed, diagnostics=diagnostics) == ()
    assert diagnostics["input_rejections"] == [
        {"symbol": "ETHUSDT", "reason": "VOLUME_UNAVAILABLE"}
    ]


def test_ws_costs_do_not_wait_for_global_ticker():
    gw = gateway()
    _, marks, _ = rest_rows()
    mark = SimpleNamespace(
        event_time=AT - timedelta(seconds=2),
        stream=SimpleNamespace(name=GLOBAL_STREAMS[1]),
        generation=1,
        values={"p": Decimal("100.01"), "i": Decimal("100.01"), "r": Decimal("0.0001")},
    )
    gw._latest[(GLOBAL_STREAMS[1], "BTCUSDT")] = (mark, 1)
    gw._generations[GLOBAL_STREAMS[1]] = 1
    worker = GatewayPersistence(gw, None)
    books, _, _ = rest_rows()
    samples = worker.markets(books, AT)
    assert len(samples) == 1 and samples[0].costs_fresh(AT, 15)
    assert samples[0].volume is None
    mark.generation = 0
    assert worker.markets(books, AT) == ()


def test_rest_ws_merge_preserves_freshest_evidence_for_each_component():
    worker = GatewayPersistence(gateway(), None)
    rest = worker.rest_markets(*rest_rows(book_age=1, mark_age=3, volume_age=2), AT)
    ws = worker.rest_markets(*rest_rows(book_age=2, mark_age=1, volume_age=4), AT)
    merged = _merge_fresh(rest, ws, {"BTCUSDT"}, AT)
    assert len(merged) == 1
    assert merged[0].book_source_at == AT - timedelta(seconds=1)
    assert merged[0].mark_source_at == AT - timedelta(seconds=1)
    assert merged[0].volume_source_at == AT - timedelta(seconds=2)
    assert merged[0].source_at == AT - timedelta(seconds=1)
    aged = _merge_fresh(merged, (), {"BTCUSDT"}, AT + timedelta(seconds=14))
    assert len(aged) == 1 and not aged[0].volume_fresh(AT + timedelta(seconds=14), 15)
    assert aged[0].observed_at == AT
    assert _merge_fresh(merged, (), {"BTCUSDT"}, AT + timedelta(seconds=15)) == ()


@pytest.mark.parametrize("covered,ready", [(4, True), (3, False)])
def test_handoff_keeps_eighty_percent_cost_gate_independent_of_volume(covered, ready):
    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT")
    gw = gateway(symbols=symbols)
    worker = GatewayPersistence(gw, None)
    worker.rest_samples = tuple(
        worker.rest_markets(*rest_rows(symbol=symbol, volume_age=30), AT)[0]
        for symbol in symbols[:covered]
    )
    payload = memory_payload(gw, worker)
    assert payload["status"]["analysis_market_covered"] == covered
    assert payload["status"]["analysis_volume_covered"] == 0
    assert payload["status"]["analysis_coverage_ready"] is ready
    parsed = tuple(
        MarketSnapshot.model_validate_json(__import__("json").dumps(m)) for m in payload["markets"]
    )
    assert all(m.costs_fresh(AT, 15) and not m.volume_fresh(AT, 15) for m in parsed)


@pytest.mark.parametrize(
    "covered,label", [(4, RegimeLabel.TREND_EXPANSION), (3, RegimeLabel.UNKNOWN)]
)
def test_regime_counts_real_costs_at_eighty_percent_without_volume(covered, label):
    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT")
    detector = engine()
    for index in range(1, 13):
        base = frame(index)
        rows = []
        for i, symbol in enumerate(symbols):
            row = observation(index, symbol)
            market = change(
                row.market,
                book_source_at=row.market.source_at,
                mark_source_at=row.market.source_at,
                volume=None,
                volume_source_at=None,
            )
            rows.append(change(row, market=market if i < covered else None))
        current = change(base, universe=universe(symbols), observations=tuple(rows))
        result = detector.update(current)
    assert result.state.label is label
    assert dict(result.state.metrics)["stress_coverage"] == covered / 5
    assert ("STRESS_DATA_MISSING" in result.reason_codes) is (covered < 4)


@pytest.mark.parametrize("kind", ["momentum", "reversal"])
def test_direct_alpha_rejects_stale_volume_despite_fresh_costs(kind):
    from tests.unit.test_reversal_alpha import sample as reversal_sample
    from tradeforge.alpha.momentum import MomentumAlpha
    from tradeforge.alpha.reversal import ReversalAlpha
    from tradeforge.domain.decision import Side

    sample = evidence() if kind == "momentum" else reversal_sample()
    at = sample.as_of
    market = change(
        sample.market,
        source_at=at,
        book_source_at=at,
        mark_source_at=at,
        volume_source_at=at - timedelta(seconds=6),
    )
    candidate = change(sample, market=market)
    alpha = MomentumAlpha() if kind == "momentum" else ReversalAlpha()
    result = alpha.evaluate(candidate, side=Side.LONG, now=at)
    assert result.candidate is None
    assert "MARKET_UNAVAILABLE" in result.reason_codes


def test_regime_does_not_accept_cost_evidence_older_than_fifteen_seconds():
    base = frame(12)
    rows = []
    for row in base.observations:
        stamp = base.as_of - timedelta(seconds=16)
        market = change(
            row.market,
            source_at=stamp,
            book_source_at=stamp,
            mark_source_at=stamp,
            volume_source_at=stamp,
        )
        rows.append(change(row, market=market))
    result = engine().update(change(base, observations=tuple(rows)))
    assert dict(result.state.metrics)["stress_coverage"] == 0
    assert "STRESS_DATA_MISSING" in result.reason_codes


def test_ticker_endpoint_failure_does_not_discard_fresh_broad_costs():
    import asyncio

    from tests.unit.test_quote_recovery_progress import fixture, rows

    worker, at, symbols = fixture()
    body = rows(symbols, at[0])

    class Client:
        async def fetch(self, path, params):
            if path == "/fapi/v1/ticker/24hr":
                raise TimeoutError("offline ticker unavailable")
            return body[path], at[0]

    assert asyncio.run(worker.quote_once(Client())) is True
    assert len(worker.rest_samples) == 5
    assert all(m.costs_fresh(at[0], 15) and m.volume is None for m in worker.rest_samples)


@pytest.mark.parametrize("ticker_kind", ["stale", "future", "old_generation", "malformed"])
def test_ws_unusable_ticker_cannot_erase_fresh_costs(ticker_kind):
    gw = gateway()
    mark = SimpleNamespace(
        event_time=AT - timedelta(seconds=2),
        stream=SimpleNamespace(name=GLOBAL_STREAMS[1]),
        generation=1,
        values={"p": Decimal("100.01"), "i": Decimal("100.01"), "r": Decimal("0.0001")},
    )
    ticker = SimpleNamespace(
        event_time=AT - timedelta(seconds=30),
        stream=SimpleNamespace(name=GLOBAL_STREAMS[0]),
        generation=1,
        values={"v": Decimal("123")},
    )
    if ticker_kind == "future":
        ticker.event_time = AT + timedelta(seconds=1)
    elif ticker_kind == "old_generation":
        ticker.generation = 0
    elif ticker_kind == "malformed":
        ticker.values = {"v": Decimal("NaN")}
    gw._latest = {
        (GLOBAL_STREAMS[1], "BTCUSDT"): (mark, 1),
        (GLOBAL_STREAMS[0], "BTCUSDT"): (ticker, 1),
    }
    gw._generations = {GLOBAL_STREAMS[0]: 1, GLOBAL_STREAMS[1]: 1}
    worker = GatewayPersistence(gw, None)
    books, _, _ = rest_rows()
    snapshots = worker.markets(books, AT)
    assert len(snapshots) == 1
    assert snapshots[0].costs_fresh(AT, 15)
    assert not snapshots[0].volume_fresh(AT, 5)

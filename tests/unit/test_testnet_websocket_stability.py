"""Testnet WebSocket isolation and bounded subscription groups.

These checks require no Binance network access or trading credentials.
"""

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from tradeforge.domain.instrument import InstrumentKey
from tradeforge.market.clock_sync import ClockSync
from tradeforge.market.instrument_registry import RegistryView
from tradeforge.market.market_gateway import MarketGateway
from tradeforge.market.streams import decode_events


def _mark_row(symbol: str, now: datetime, *, expired: bool = False) -> dict:
    source_ms = int((now - timedelta(seconds=1)).timestamp() * 1000)
    return {
        "e": "markPriceUpdate",
        "E": source_ms,
        "s": symbol,
        "p": "100",
        "i": "100",
        "r": "0.001",
        "T": source_ms - 1 if expired else source_ms + 60_000,
    }


def _decode(rows: list[dict], now: datetime):
    return decode_events(
        json.dumps({"stream": "!markPrice@arr@1s", "data": rows}),
        route="market",
        received_at=now,
        generation=1,
        sequence=1,
        eligible=frozenset({"BTCUSDT", "ETHUSDT"}),
    )


def test_expired_funding_row_does_not_disconnect_valid_peers():
    now = datetime.now(UTC)
    result = _decode([_mark_row("BTCUSDT", now, expired=True), _mark_row("ETHUSDT", now)], now)
    assert [row.symbol for row in result] == ["ETHUSDT"]
    assert result[0].values["p"] == 100


def test_only_expired_funding_row_is_not_published():
    now = datetime.now(UTC)
    assert _decode([_mark_row("BTCUSDT", now, expired=True)], now) == ()


def test_malformed_funding_time_still_fails_closed():
    now = datetime.now(UTC)
    invalid = _mark_row("BTCUSDT", now)
    invalid["T"] = "invalid"
    with pytest.raises(ValueError):
        _decode([invalid, _mark_row("ETHUSDT", now)], now)


def _registry(symbols: int) -> RegistryView:
    # MarketGateway consumes ContractMetadata via the stable InstrumentKey fields.
    instruments = tuple(
        InstrumentKey(
            symbol=f"TF{i:04d}USDT",
            exchange="BINANCE",
            market_type="USD_M",
            quote="USDT",
            margin_asset="USDT",
            contract_type="PERPETUAL",
        )
        for i in range(symbols)
    )
    return RegistryView(
        SimpleNamespace(metadata=SimpleNamespace(instruments=instruments)),
        True,
        "UNIVERSE_READY",
    )


def test_testnet_517_symbols_use_bounded_socket_groups():
    gateway = MarketGateway(ClockSync(), environment="testnet")
    gateway.update_universe(_registry(517))
    assert gateway.plan is not None
    sizes = [len(group) for group in gateway.plan.groups]
    assert sizes == [2, 100, 100, 100, 100, 100, 17]
    assert gateway.status()["subscriptions"] == 519
    assert all(len({s.route for s in group}) == 1 for group in gateway.plan.groups)


def test_full_depth_testnet_subscribes_each_eligible_pair():
    gateway = MarketGateway(ClockSync(), environment="testnet", full_depth=True)
    gateway.update_universe(_registry(517))
    assert gateway.plan is not None
    assert len(gateway.plan.groups) <= 32
    assert max(map(len, gateway.plan.groups)) <= 100
    assert not gateway.plan.omitted
    assert gateway.canonical.books.keys() == gateway.eligible
    names = {stream.name for group in gateway.plan.groups for stream in group}
    for symbol in gateway.eligible:
        assert {
            f"{symbol.lower()}@{suffix}"
            for suffix in ("depth20@500ms", "aggTrade", "kline_5m", "kline_15m")
        } <= names


def test_live_subscription_budget_unchanged():
    gateway = MarketGateway(ClockSync(), environment="live")
    gateway.update_universe(_registry(517))
    assert gateway.plan is not None
    assert [len(group) for group in gateway.plan.groups] == [2, 517]


def test_local_depth_batches_rotate_without_dropping_broad_scan():
    elapsed = [0.0]
    gateway = MarketGateway(
        ClockSync(), environment="testnet", full_depth=True, depth_batch_size=24,
        monotonic=lambda: elapsed[0],
    )
    registry = _registry(517)
    protected = ("TF0516USDT",)
    gateway.update_universe(registry, deep=("TF0515USDT",), protected=protected)
    first = set(gateway.canonical.books)
    assert len(first) == 25
    assert {"TF0515USDT", *protected} <= first
    assert gateway.plan is not None
    assert sum("@kline_15m" in s.name for g in gateway.plan.groups for s in g) == 517
    elapsed[0] = 60.0
    gateway.update_universe(registry, deep=("TF0515USDT",), protected=protected)
    assert gateway.canonical.books.keys() == first
    observed = set(first)
    for _ in range(23):
        elapsed[0] += 300.0
        gateway.update_universe(registry, deep=("TF0515USDT",), protected=protected)
        observed.update(gateway.canonical.books)
        assert {"TF0515USDT", *protected} <= gateway.canonical.books.keys()
    assert observed == gateway.eligible

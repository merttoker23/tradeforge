"""Executable market contracts and cross-field invariants."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from tradeforge.domain.common import DataQuality, ReasonCode
from tradeforge.domain.instrument import ContractStatus, Instrument, InstrumentFilters
from tradeforge.domain.market import (
    ClosedBar,
    MarketRegimeState,
    MarketSnapshot,
    OrderBookSnapshot,
    RegimeLabel,
    StreamState,
    TradeFlowWindow,
    UniverseSnapshot,
)

from .conftest import NOW


def filters() -> InstrumentFilters:
    return InstrumentFilters(
        tick_size=Decimal("0.1"),
        step_size=Decimal("0.001"),
        min_price=Decimal("0.1"),
        max_price=Decimal("1000000"),
        min_qty=Decimal("0.001"),
        max_qty=Decimal("1000"),
        min_notional=Decimal("5"),
    )


@pytest.mark.parametrize(
    "patch",
    [
        {"market_type": "COIN_M"},
        {"quote": "USDC"},
        {"margin_asset": "BTC"},
        {"contract_type": "CURRENT_QUARTER"},
        {"symbol": "btcusdt"},
        {"symbol": "BTCUSDC"},
        {"exchange": "OTHER"},
    ],
)
def test_unsupported_instruments_rejected(
    event: dict[str, object], identity: dict[str, object], patch: dict[str, object]
) -> None:
    with pytest.raises(ValidationError):
        Instrument.model_validate(
            {
                **event,
                **identity,
                "contract_status": ContractStatus.TRADING,
                "filters": filters(),
                "onboard_at": NOW - timedelta(days=1),
                "delist_at": None,
                **patch,
            },
            context={"now": NOW},
        )


def test_instrument_lifecycle_and_filters(
    event: dict[str, object], identity: dict[str, object]
) -> None:
    data = {
        **event,
        **identity,
        "contract_status": ContractStatus.TRADING,
        "filters": filters(),
        "onboard_at": NOW - timedelta(days=1),
        "delist_at": NOW + timedelta(days=1),
    }
    record = Instrument.model_validate(data, context={"now": NOW})
    assert record.symbol == "BTCUSDT"
    with pytest.raises(ValidationError):
        Instrument.model_validate({**data, "delist_at": NOW - timedelta(days=2)})
    with pytest.raises(ValidationError):
        InstrumentFilters.model_validate({**filters().model_dump(), "max_qty": Decimal("0.0001")})


@pytest.mark.parametrize(
    "patch",
    [
        {"complete": False},
        {"complete": 1},
        {"timeframe": "2m"},
        {"high": Decimal("95")},
        {"low": Decimal("106")},
        {"close_at": NOW + timedelta(seconds=1)},
        {"open_at": NOW},
        {"open_at": NOW - timedelta(seconds=30)},
        {"close_at": NOW - timedelta(milliseconds=2)},
    ],
)
def test_partial_or_inconsistent_bars_fail(
    bar: dict[str, object], patch: dict[str, object]
) -> None:
    with pytest.raises(ValidationError):
        ClosedBar.model_validate({**bar, **patch}, context={"now": NOW})


def test_market_and_flow(event: dict[str, object], identity: dict[str, object]) -> None:
    market = {
        **event,
        **identity,
        "best_bid": Decimal("99"),
        "best_ask": Decimal("101"),
        "mark_price": Decimal("100"),
        "index_price": Decimal("100"),
        "volume": Decimal("10"),
        "funding": Decimal("-0.001"),
        "quality": DataQuality.OK,
    }
    assert MarketSnapshot.model_validate(market).funding < 0
    with pytest.raises(ValidationError):
        MarketSnapshot.model_validate({**market, "best_bid": Decimal("102")})
    flow = {
        **event,
        **identity,
        "period_seconds": 60,
        "signed_taker_flow": Decimal("-5"),
        "agg_volume": Decimal("10"),
        "count": 2,
        "source_start": NOW - timedelta(seconds=30),
        "source_end": NOW - timedelta(seconds=1),
        "validity": DataQuality.OK,
    }
    assert TradeFlowWindow.model_validate(flow).count == 2
    for patch in [
        {"signed_taker_flow": Decimal("11")},
        {"source_end": NOW + timedelta(seconds=1)},
        {"count": -1},
        {"period_seconds": 0},
    ]:
        with pytest.raises(ValidationError):
            TradeFlowWindow.model_validate({**flow, **patch})


def test_book_sequences_levels_and_sync(
    event: dict[str, object], identity: dict[str, object]
) -> None:
    data = {
        **event,
        **identity,
        "initial_update_id": 1,
        "last_update_id": 2,
        "captured_at": NOW,
        "bids": ((Decimal("99"), Decimal("2")),),
        "asks": ((Decimal("101"), Decimal("3")),),
        "stream_state": StreamState.READY,
    }
    record = OrderBookSnapshot.model_validate(data)
    assert record.stream_state is StreamState.READY
    for patch in [
        {"last_update_id": 0},
        {"asks": ()},
        {"asks": ((Decimal("98"), Decimal("1")),)},
        {"bids": ((Decimal("99"), Decimal("1")), (Decimal("100"), Decimal("1")))},
        {"captured_at": NOW + timedelta(seconds=1)},
    ]:
        with pytest.raises(ValidationError):
            OrderBookSnapshot.model_validate({**data, **patch})
    assert OrderBookSnapshot.model_validate(
        {**data, "bids": (), "asks": (), "stream_state": StreamState.GAP}
    )


def test_regime_and_universe(event: dict[str, object], identity: dict[str, object]) -> None:
    regime = {
        **event,
        "label": RegimeLabel.UNKNOWN,
        "metrics": (("breadth", 0.5),),
        "reason_codes": (ReasonCode.INSUFFICIENT_EVIDENCE,),
        "input_version": "v1",
    }
    assert MarketRegimeState.model_validate(regime).label is RegimeLabel.UNKNOWN
    with pytest.raises(ValidationError):
        MarketRegimeState.model_validate({**regime, "metrics": (("vol", float("nan")),)})
    universe = {
        **event,
        "eligible": (identity,),
        "watched": (identity,),
        "deep": (identity,),
        "filtering_rules": ("USDT_PERPETUAL",),
        "version_hash": "a" * 64,
    }
    record = UniverseSnapshot.model_validate(universe)
    assert len(record.eligible) == 1
    for patch in [{"eligible": ()}, {"watched": ()}, {"eligible": (identity, identity)}]:
        with pytest.raises(ValidationError):
            UniverseSnapshot.model_validate({**universe, **patch})


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), -0.0])
def test_non_money_stats_reject_invalid_numbers(event: dict[str, object], value: float) -> None:
    with pytest.raises(ValidationError):
        MarketRegimeState.model_validate(
            {
                **event,
                "label": RegimeLabel.UNKNOWN,
                "metrics": (("breadth", value),),
                "reason_codes": (ReasonCode.INSUFFICIENT_EVIDENCE,),
                "input_version": "v1",
            }
        )


def test_far_future_bar_open_is_a_validation_error(bar: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        ClosedBar.model_validate({**bar, "open_at": datetime.max.replace(tzinfo=UTC)})


def test_flow_decimal_bound_does_not_round(
    event: dict[str, object], identity: dict[str, object]
) -> None:
    data = {
        **event,
        **identity,
        "period_seconds": 60,
        "signed_taker_flow": Decimal("1.0000000000000000000000000001"),
        "agg_volume": Decimal("1"),
        "count": 1,
        "source_start": NOW - timedelta(seconds=30),
        "source_end": NOW - timedelta(seconds=1),
        "validity": DataQuality.OK,
    }
    with pytest.raises(ValidationError):
        TradeFlowWindow.model_validate(data)


def test_flow_period_rejects_unbounded_integer(
    event: dict[str, object], identity: dict[str, object]
) -> None:
    with pytest.raises(ValidationError):
        TradeFlowWindow.model_validate(
            {
                **event,
                **identity,
                "period_seconds": 10**20,
                "signed_taker_flow": Decimal("0"),
                "agg_volume": Decimal("0"),
                "count": 0,
                "source_start": NOW - timedelta(seconds=30),
                "source_end": NOW - timedelta(seconds=1),
                "validity": DataQuality.OK,
            }
        )

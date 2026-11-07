"""Hand-checkable paper portfolio inputs; no exchange credentials or network."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal as D
from uuid import UUID

from tradeforge.domain.decision import Side
from tradeforge.domain.instrument import InstrumentFilters, InstrumentKey

NOW = datetime(2026, 10, 20, 12, tzinfo=UTC)


def change(model, **patch):
    return type(model).model_validate({**model.model_dump(), **patch})


def request(**patch):
    from tradeforge.risk.models import EntryRequest

    return EntryRequest(
        instrument=InstrumentKey(
            symbol=patch.pop("symbol", "ETHUSDT"),
            exchange="BINANCE",
            market_type="USD_M",
            quote="USDT",
            margin_asset="USDT",
            contract_type="PERPETUAL",
        ),
        filters=InstrumentFilters(
            tick_size=D("0.1"),
            step_size=D("0.01"),
            min_price=D("0.1"),
            max_price=D("1000000"),
            min_qty=D("0.01"),
            max_qty=D("1000"),
            min_notional=D("5"),
        ),
        requested_qty=patch.pop("requested_qty", D("1")),
        entry_price=patch.pop("entry_price", D("100")),
        stop_price=patch.pop("stop_price", D("95")),
        side=patch.pop("side", Side.LONG),
        cost_per_unit=patch.pop("cost_per_unit", D("1")),
        leverage=patch.pop("leverage", D("1")),
        observed_at=patch.pop("observed_at", NOW),
        expires_at=patch.pop("expires_at", NOW + timedelta(seconds=30)),
        liquidity_scale=patch.pop("liquidity_scale", D("1")),
        spread_bps=patch.pop("spread_bps", D("2")),
        **patch,
    )


def factor(symbol="ETHUSDT", **patch):
    from tradeforge.risk.models import RiskFactor

    return RiskFactor(
        symbol=symbol,
        beta=patch.pop("beta", D("1")),
        realized_vol=patch.pop("realized_vol", D("0.5")),
        as_of=patch.pop("as_of", NOW),
        **patch,
    )


def position(symbol="BTCUSDT", **patch):
    from tradeforge.risk.models import ExposurePosition

    side = patch.pop("side", Side.LONG)
    return ExposurePosition(
        symbol=symbol,
        side=side,
        qty=patch.pop("qty", D("1")),
        entry_price=D("100"),
        mark_price=patch.pop("mark_price", D("100")),
        stop_price=patch.pop("stop_price", D("95") if side is Side.LONG else D("105")),
        cost_per_unit=D("1"),
        protected=patch.pop("protected", True),
        as_of=patch.pop("as_of", NOW),
        **patch,
    )


def correlation(left="BTCUSDT", right="ETHUSDT", **patch):
    from tradeforge.risk.models import Correlation

    return Correlation(
        left=left,
        right=right,
        rho=patch.pop("rho", D("0.9")),
        samples=patch.pop("samples", 100),
        as_of=patch.pop("as_of", NOW),
        **patch,
    )


def account(**patch):
    from tradeforge.risk.models import AccountSnapshot

    return AccountSnapshot(
        snapshot_id=UUID("00000000-0000-0000-0000-000000000012"),
        as_of=patch.pop("as_of", NOW),
        trading_day=patch.pop("trading_day", NOW.date()),
        equity=patch.pop("equity", D("10000")),
        available_balance=patch.pop("available_balance", D("10000")),
        day_start_equity=patch.pop("day_start_equity", D("10000")),
        realized_pnl_today=patch.pop("realized_pnl_today", D("0")),
        unrealized_pnl=patch.pop("unrealized_pnl", D("0")),
        positions_complete=patch.pop("positions_complete", True),
        protection_ready=patch.pop("protection_ready", True),
        positions=patch.pop("positions", ()),
        pending=patch.pop("pending", ()),
        factors=patch.pop("factors", (factor(),)),
        correlations=patch.pop("correlations", ()),
        **patch,
    )


def state(**patch):
    from tradeforge.risk.kill_switch import RiskState

    return RiskState(
        as_of=patch.pop("as_of", NOW), kill_switch=patch.pop("kill_switch", False), **patch
    )


def assess(req=None, acc=None, risk_state=None, **kwargs):
    from tradeforge.risk.governor import RiskGovernor

    return RiskGovernor(kwargs.pop("policy", None)).assess(
        req or request(),
        acc or account(),
        risk_state or state(),
        at=kwargs.pop("at", NOW),
        db_writable=kwargs.pop("db_writable", True),
        **kwargs,
    )

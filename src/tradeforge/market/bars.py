"""Closed-only wire conversion and complete, UTC-aligned 15m resampling."""

import hashlib
import json
from datetime import datetime, timedelta
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from tradeforge.domain.common import utc
from tradeforge.domain.instrument import InstrumentKey
from tradeforge.domain.market import ClosedBar, Timeframe
from tradeforge.market.exchange_info import milliseconds
from tradeforge.market.streams import MarketEvent, decimal_field


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def bar_hash(bar: ClosedBar) -> str:
    return hashlib.sha256(canonical(bar.model_dump(mode="json")).encode()).hexdigest()


def make_bar(
    key: InstrumentKey,
    timeframe: Timeframe,
    opened: datetime,
    closed: datetime,
    prices: dict[str, Decimal],
    source_at: datetime,
    observed_at: datetime,
    source: str,
) -> ClosedBar:
    data: dict[str, object] = dict(
        **key.model_dump(),
        schema_version=1,
        timeframe=timeframe,
        open_at=opened,
        close_at=closed,
        **prices,
        source_at=source_at,
        observed_at=observed_at,
        as_of=observed_at,
        source=source,
        complete=True,
    )
    identity = f"{key.key}/{timeframe}/{opened}/{source_at}/{observed_at}/{prices}/{source}"
    data["event_id"] = uuid5(NAMESPACE_URL, identity)
    return ClosedBar.model_validate(data, context={"now": observed_at})


def closed_kline(event: MarketEvent, key: InstrumentKey) -> ClosedBar | None:
    if event.symbol != key.symbol or "@kline_" not in event.stream.name:
        raise ValueError("KLINE_IDENTITY_MISMATCH")
    row = json.loads(event.payload_json)
    k = row.get("k")
    if not isinstance(k, dict) or type(k.get("x")) is not bool:
        raise ValueError("INVALID_KLINE")
    tf = Timeframe(event.stream.name.split("_", 1)[1])
    if k.get("s") != key.symbol or k.get("i") != tf.value:
        raise ValueError("KLINE_IDENTITY_MISMATCH")
    if not k["x"]:
        return None
    return make_bar(
        key,
        tf,
        milliseconds(k.get("t")),
        milliseconds(k.get("T")),
        {
            name: decimal_field(k, wire)
            for name, wire in (
                ("open", "o"),
                ("high", "h"),
                ("low", "l"),
                ("close", "c"),
                ("volume", "v"),
            )
        },
        event.event_time,
        event.received_at,
        "binance:ws",
    )


def resample(
    children: tuple[ClosedBar, ...],
    target: Timeframe,
    opened: datetime,
    as_of: datetime,
) -> ClosedBar | None:
    utc(opened)
    utc(as_of)
    if target not in (Timeframe.H1, Timeframe.H4, Timeframe.H12):
        raise ValueError("UNSUPPORTED_RESAMPLE")
    if opened.timestamp() % target.seconds:
        raise ValueError("UNALIGNED_RESAMPLE")
    if not children or any(b.timeframe is not Timeframe.M15 for b in children):
        return None
    if any(b.key != children[0].key for b in children):
        return None
    end = opened + timedelta(seconds=target.seconds)
    selected = tuple(
        sorted((b for b in children if opened <= b.open_at < end), key=lambda b: b.open_at)
    )
    count = target.seconds // Timeframe.M15.seconds
    if len(selected) != count or any(
        b.open_at != opened + timedelta(minutes=15 * i) or b.as_of > as_of or b.observed_at > as_of
        for i, b in enumerate(selected)
    ):
        return None
    provenance = hashlib.sha256("".join(bar_hash(b) for b in selected).encode()).hexdigest()
    return make_bar(
        InstrumentKey.model_validate(
            {k: getattr(selected[0], k) for k in InstrumentKey.model_fields}
        ),
        target,
        opened,
        end - timedelta(milliseconds=1),
        {
            "open": selected[0].open,
            "high": max(b.high for b in selected),
            "low": min(b.low for b in selected),
            "close": selected[-1].close,
            "volume": sum((b.volume for b in selected), Decimal(0)),
        },
        max(b.source_at for b in selected),
        max(b.observed_at for b in selected),
        "resample:15m:" + provenance,
    )

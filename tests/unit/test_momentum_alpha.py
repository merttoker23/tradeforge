"""Closed synthetic continuation evidence, including adversarial market inputs."""

from datetime import timedelta
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

import pytest
from pydantic import ValidationError

from tests.intelligence_fixtures import engine, frame
from tests.market_fixtures import instrument
from tradeforge.domain.common import DataQuality
from tradeforge.domain.decision import DecisionState, Side
from tradeforge.domain.market import OrderBookSnapshot, StreamState, Timeframe, TradeFlowWindow
from tradeforge.market.bars import make_bar


def change(model, **patch):
    return type(model).model_validate({**model.model_dump(), **patch})


def evidence(side=Side.LONG, stage="armed"):
    from tradeforge.alpha.base import MomentumInput

    sign = 1 if side is Side.LONG else -1
    worker, prior = engine(), None
    medium = []
    for n in range(1, 32):
        btc = 0.001 + (n % 7) * 0.0002
        prior = frame(
            n, moves=tuple(sign * v for v in (btc, btc + 0.003, btc + 0.001)), previous=prior
        )
        snap = worker.update(prior)
        medium.append(
            next(r.current for r in prior.observations if r.instrument.symbol == "ETHUSDT")
        )
    now, key = prior.as_of, instrument("ETHUSDT")
    prices = {
        "discovered": [100, 100.1, 100.2, 100.3, 100.4, 100.5, 100.6, 100.7],
        "watching": [100, 100.1, 100.2, 100.3, 100.4, 100.5, 100.6, 104],
        "armed": [100, 100.1, 100.2, 100.3, 100.4, 104, 103, 103.5],
        "false": [100, 100.1, 100.2, 100.3, 100.4, 104, 100, 100.2],
    }[stage]

    def series(tf, values):
        output = []
        for i, v in enumerate(values):
            opened = now - timedelta(seconds=(len(values) - i) * tf.seconds)
            closed = opened + timedelta(seconds=tf.seconds) - timedelta(milliseconds=1)
            center = Decimal(str(v))
            op = Decimal(str(values[max(0, i - 1)]))
            lo, hi = min(op, center) - Decimal("0.2"), max(op, center) + Decimal("0.2")
            # A pullback opens at the prior close but need not revisit breakout origin.
            if tf is Timeframe.M5 and stage in ("armed", "false") and i == 6:
                lo = min(center, op) - Decimal("0.2")
            if sign < 0:
                center, op, lo, hi = 200 - center, 200 - op, 200 - hi, 200 - lo
            volume = (
                Decimal("30")
                if tf is Timeframe.M5 and abs(v - values[max(0, i - 1)]) > 2
                else Decimal("10")
            )
            output.append(
                make_bar(
                    key,
                    tf,
                    opened,
                    closed,
                    {"open": op, "high": hi, "low": lo, "close": center, "volume": volume},
                    closed,
                    closed + timedelta(milliseconds=1),
                    "momentum-fixture",
                )
            )
        return tuple(output)

    lower = series(Timeframe.M5, prices)
    higher = series(Timeframe.H1, [100, 101, 102, 103, 104, 105, 106, 107])
    market = change(
        next(r.market for r in prior.observations if r.instrument.symbol == "ETHUSDT"),
        best_bid=lower[-1].close - Decimal("0.01"),
        best_ask=lower[-1].close + Decimal("0.01"),
        mark_price=lower[-1].close,
        index_price=lower[-1].close,
    )
    common = dict(**key.model_dump(), schema_version=1, source_at=now, observed_at=now, as_of=now)
    book = OrderBookSnapshot(
        **common,
        event_id=uuid5(NAMESPACE_URL, "book"),
        initial_update_id=1,
        last_update_id=10,
        captured_at=now,
        stream_state=StreamState.READY,
        bids=((market.best_bid, Decimal("1000")),),
        asks=((market.best_ask, Decimal("1000")),),
    )
    flow = TradeFlowWindow(
        **common,
        event_id=uuid5(NAMESPACE_URL, "flow"),
        period_seconds=60,
        signed_taker_flow=Decimal(sign * 60),
        agg_volume=Decimal("100"),
        count=100,
        source_start=now - timedelta(seconds=60),
        source_end=now,
        validity=DataQuality.OK,
    )
    return MomentumInput(
        instrument=key,
        as_of=now,
        features=snap,
        setup_bars=lower,
        medium_bars=tuple(medium[-8:]),
        higher_bars=higher,
        market=market,
        book=book,
        flow=flow,
        quality=DataQuality.OK,
    )


@pytest.mark.parametrize("side", [Side.LONG, Side.SHORT])
def test_strong_continuation_has_symmetric_trigger_stop_and_unproven_evidence(side):
    from tradeforge.alpha.momentum import MomentumAlpha

    sample = evidence(side)
    result = MomentumAlpha().evaluate(sample, side=side, now=sample.as_of)
    c = result.candidate
    assert c is not None and c.state is DecisionState.ARMED
    assert c.side is side
    if side is Side.LONG:
        assert c.stop_proposal < c.trigger < c.take_profit_proposal
        assert c.trigger == Decimal("104.2")
    else:
        assert c.take_profit_proposal < c.trigger < c.stop_proposal
        assert c.trigger == Decimal("95.8")
    assert 0 <= c.ordinal_quality <= 1
    assert c.confidence is None and c.proof_state.value == "UNPROVEN"
    assert c.source_feature_hash == sample.features.digest
    assert c.expires_at > sample.as_of and c.invalidations
    assert not c.trading_allowed
    assert "confidence_pct" not in c.model_dump() and "intent_id" not in c.model_dump()


@pytest.mark.parametrize(
    "stage,want", [("discovered", DecisionState.DISCOVERED), ("watching", DecisionState.WATCHING)]
)
def test_setup_stages_before_pullback(stage, want):
    from tradeforge.alpha.momentum import MomentumAlpha

    sample = evidence(stage=stage)
    assert (
        MomentumAlpha().evaluate(sample, side=Side.LONG, now=sample.as_of).candidate.state is want
    )


@pytest.mark.parametrize(
    "kind,reason",
    [
        ("false", "BREAKOUT_INVALIDATED"),
        ("weak_volume", "WEAK_PARTICIPATION"),
        ("stale_book", "BOOK_UNAVAILABLE"),
        ("spread", "SPREAD_GATE"),
        ("depth", "LIQUIDITY_GATE"),
        ("flow", "FLOW_OPPOSED"),
        ("regime", "REGIME_NOT_ALLOWED"),
        ("opposing", "TREND_MISALIGNED"),
        ("relative", "RELATIVE_STRENGTH_UNKNOWN"),
        ("quality", "DATA_UNAVAILABLE"),
    ],
)
@pytest.mark.parametrize("side", [Side.LONG, Side.SHORT])
def test_hard_gates_override_setup_score(kind, reason, side):
    from tradeforge.alpha.momentum import MomentumAlpha
    from tradeforge.domain.market import RegimeLabel

    d = evidence(side, stage="false" if kind == "false" else "armed")
    if kind == "weak_volume":
        d = change(
            d,
            setup_bars=tuple(
                change(b, volume=Decimal("1")) if i == 5 else b for i, b in enumerate(d.setup_bars)
            ),
        )
    if kind == "stale_book":
        d = change(d, book=change(d.book, captured_at=d.as_of - timedelta(seconds=20)))
    if kind == "spread":
        d = change(d, market=change(d.market, best_ask=d.market.best_ask + Decimal("2")))
    if kind == "depth":
        d = change(d, book=change(d.book, bids=((d.book.bids[0][0], Decimal("0.001")),)))
    if kind == "flow":
        d = change(
            d, flow=change(d.flow, signed_taker_flow=Decimal("-60" if side is Side.LONG else "60"))
        )
    if kind in ("regime", "opposing", "relative"):
        p = d.features.model_dump()
        if kind == "regime":
            p["state"]["label"] = RegimeLabel.RANGE
        elif kind == "opposing":
            p["features"] = tuple(
                {**f.model_dump(), "direction": -1 if side is Side.LONG else 1}
                for f in d.features.features
            )
        else:
            p["features"] = tuple(
                {**f.model_dump(), "beta": None, "relative_return": None}
                for f in d.features.features
            )
        d = change(d, features=type(d.features).model_validate(p))
    if kind == "quality":
        d = change(d, quality=DataQuality.GAP)
    result = MomentumAlpha().evaluate(d, side=side, now=d.as_of)
    assert result.candidate is None and reason in result.reason_codes


def test_duplicate_suppression_expiry_and_replay():
    from tradeforge.alpha.momentum import MomentumAlpha

    d = evidence()
    a = MomentumAlpha()
    first = a.evaluate(d, side=Side.LONG, now=d.as_of)
    assert a.evaluate(d, side=Side.LONG, now=d.as_of).reason_codes == ("DUPLICATE_SETUP",)
    assert first == MomentumAlpha().evaluate(d, side=Side.LONG, now=d.as_of)
    assert "EXPIRED" in a.evaluate(d, side=Side.LONG, now=first.candidate.expires_at).reason_codes


@pytest.mark.parametrize("kind", ["future", "gap", "timeframe", "identity", "partial"])
def test_invalid_input_cannot_be_used_as_closed_aligned_evidence(kind):
    from tradeforge.alpha.momentum import MomentumAlpha

    d = evidence()
    if kind == "future":
        with pytest.raises(ValueError, match="FUTURE_INPUT"):
            MomentumAlpha().evaluate(d, side=Side.LONG, now=d.as_of - timedelta(seconds=1))
        return
    p = d.model_dump()
    if kind == "gap":
        p["setup_bars"] = p["setup_bars"][:3] + p["setup_bars"][4:]
    if kind == "timeframe":
        p["medium_bars"] = p["higher_bars"]
    if kind == "identity":
        p["book"]["symbol"] = "BTCUSDT"
    if kind == "partial":
        p["setup_bars"][-1]["complete"] = False
    with pytest.raises(ValidationError):
        type(d).model_validate(p)


def append_pullback(d):
    """Advance only closed 5m bars; keep the last CLOSED 15m/1h evidence."""
    bars = list(d.setup_bars)
    for price in (Decimal("103"), Decimal("103.5")):
        previous = bars[-1]
        bars.append(
            change(
                previous,
                event_id=uuid5(NAMESPACE_URL, str((previous.open_at, price))),
                open_at=previous.open_at + timedelta(minutes=5),
                close_at=previous.close_at + timedelta(minutes=5),
                source_at=previous.source_at + timedelta(minutes=5),
                observed_at=previous.observed_at + timedelta(minutes=5),
                as_of=previous.as_of + timedelta(minutes=5),
                open=previous.close,
                close=price,
                high=max(previous.close, price) + Decimal("0.2"),
                low=min(previous.close, price) - Decimal("0.2"),
                volume=Decimal("10"),
            )
        )
    now = bars[-1].as_of
    fresh = dict(source_at=now, observed_at=now, as_of=now)
    return change(
        d,
        as_of=now,
        setup_bars=tuple(bars),
        market=change(d.market, **fresh, best_bid=Decimal("103.49"), best_ask=Decimal("103.51")),
        book=change(
            d.book,
            **fresh,
            captured_at=now,
            bids=((Decimal("103.49"), Decimal("1000")),),
            asks=((Decimal("103.51"), Decimal("1000")),),
        ),
        flow=change(d.flow, **fresh, source_start=now - timedelta(seconds=60), source_end=now),
    )


def test_watching_to_armed_keeps_identity_expiry_and_suppresses_stage_regression():
    from tradeforge.alpha.momentum import MomentumAlpha

    d = evidence(stage="watching")
    worker = MomentumAlpha()
    watching = worker.evaluate(d, side=Side.LONG, now=d.as_of).candidate
    later = append_pullback(d)
    armed = worker.evaluate(later, side=Side.LONG, now=later.as_of).candidate
    assert watching.state is DecisionState.WATCHING
    assert armed.state is DecisionState.ARMED
    assert armed.setup_id == watching.setup_id and armed.expires_at == watching.expires_at
    assert armed.input_hash != watching.input_hash
    assert worker.evaluate(later, side=Side.LONG, now=later.as_of).candidate is None
    with pytest.raises(ValueError, match="OUT_OF_ORDER_ALPHA"):
        worker.evaluate(d, side=Side.LONG, now=later.as_of)


def test_rejected_newer_evidence_prevents_rollback_to_previous_valid_setup():
    from tradeforge.alpha.momentum import MomentumAlpha

    d = evidence(stage="watching")
    bad = append_pullback(d)
    bad = change(bad, quality=DataQuality.STALE)
    worker = MomentumAlpha()
    assert worker.evaluate(bad, side=Side.LONG, now=bad.as_of).candidate is None
    with pytest.raises(ValueError, match="OUT_OF_ORDER_ALPHA"):
        worker.evaluate(d, side=Side.LONG, now=bad.as_of)


@pytest.mark.parametrize(
    "kind,reason",
    [
        ("stale_market", "MARKET_UNAVAILABLE"),
        ("stale_flow", "FLOW_UNAVAILABLE"),
        ("stale_bars", "STALE_BARS"),
        ("book_gap", "BOOK_UNAVAILABLE"),
        ("market_gap", "MARKET_UNAVAILABLE"),
        ("zero_flow", "FLOW_UNAVAILABLE"),
        ("no_expansion", "VOLATILITY_NOT_EXPANDING"),
        ("deep_pullback", "PULLBACK_TOO_DEEP"),
        ("trigger_passed", "TRIGGER_ALREADY_PASSED"),
    ],
)
def test_additional_market_failure_gates(kind, reason):
    from tradeforge.alpha.momentum import MomentumAlpha

    d = evidence()
    if kind == "stale_market":
        d = change(d, market=change(d.market, source_at=d.as_of - timedelta(seconds=6)))
    elif kind == "stale_flow":
        d = change(d, flow=change(d.flow, source_end=d.as_of - timedelta(seconds=6)))
    elif kind == "stale_bars":
        d = change(
            d,
            higher_bars=tuple(
                change(
                    b,
                    **{
                        name: getattr(b, name) - timedelta(hours=1)
                        for name in ("open_at", "close_at", "source_at", "observed_at", "as_of")
                    },
                )
                for b in d.higher_bars
            ),
        )
    elif kind == "book_gap":
        d = change(d, book=change(d.book, stream_state=StreamState.GAP))
    elif kind == "market_gap":
        d = change(d, market=change(d.market, quality=DataQuality.GAP))
    elif kind == "zero_flow":
        d = change(
            d, flow=change(d.flow, count=0, signed_taker_flow=Decimal(0), agg_volume=Decimal(0))
        )
    elif kind == "no_expansion":
        d = change(
            d,
            setup_bars=tuple(
                change(b, high=b.high + Decimal("10"), low=b.low - Decimal("10")) if i < 4 else b
                for i, b in enumerate(d.setup_bars)
            ),
        )
        # Keep a real breakout but require much stronger expansion than the fixture.
        from tradeforge.alpha.base import MomentumPolicy

        result = MomentumAlpha(MomentumPolicy(expansion_ratio=Decimal("100"))).evaluate(
            evidence(), side=Side.LONG, now=d.as_of
        )
        assert result.candidate is None and reason in result.reason_codes
        return
    elif kind == "deep_pullback":
        bars = list(d.setup_bars)
        bars[6] = change(bars[6], low=Decimal("101"))
        d = change(d, setup_bars=tuple(bars))
    else:
        bars = list(d.setup_bars)
        bars[-1] = change(bars[-1], close=Decimal("105"), high=Decimal("105.2"))
        d = change(d, setup_bars=tuple(bars))
    result = MomentumAlpha().evaluate(d, side=Side.LONG, now=d.as_of)
    assert result.candidate is None and reason in result.reason_codes


def test_fixed_evidence_groups_are_ordinal_and_do_not_double_count_price_indicators():
    from tradeforge.alpha.momentum import MomentumAlpha

    d = evidence()
    c = MomentumAlpha().evaluate(d, side=Side.LONG, now=d.as_of).candidate
    assert c.evidence_groups == (
        ("price_structure", 1.0),
        ("participation", 1.0),
        ("aggressive_flow", 0.6),
    )
    assert c.ordinal_quality == pytest.approx(0.92)


def test_conflicting_same_time_input_is_rejected_without_overwriting_dedup():
    from tradeforge.alpha.momentum import MomentumAlpha

    d = evidence()
    worker = MomentumAlpha()
    worker.evaluate(d, side=Side.LONG, now=d.as_of)
    with pytest.raises(ValueError, match="CONFLICTING_ALPHA_INPUT"):
        worker.evaluate(
            change(d, flow=change(d.flow, signed_taker_flow=Decimal("70"))),
            side=Side.LONG,
            now=d.as_of,
        )
    assert worker.evaluate(d, side=Side.LONG, now=d.as_of).reason_codes == ("DUPLICATE_SETUP",)


@pytest.mark.parametrize(
    "patch",
    [
        {"maximum_pullback": 0.05},
        {"minimum_flow": float("nan")},
        {"max_setups": True},
        {"spread_bps": Decimal("Infinity")},
        {"minimum_history": 3},
    ],
)
def test_invalid_policies_fail_closed(patch):
    from tradeforge.alpha.base import MomentumPolicy

    with pytest.raises(ValidationError):
        MomentumPolicy(**patch)


@pytest.mark.parametrize(
    "budget,reason",
    [
        ("max_setups", "SETUP_CAPACITY_EXHAUSTED"),
        ("max_instruments", "INSTRUMENT_CAPACITY_EXHAUSTED"),
    ],
)
def test_bounded_state_refuses_new_candidates_without_evicting_unexpired_dedup(budget, reason):
    from tradeforge.alpha.base import MomentumPolicy
    from tradeforge.alpha.momentum import MomentumAlpha

    worker = MomentumAlpha(MomentumPolicy(**{budget: 1}))
    a, b = evidence(Side.LONG), evidence(Side.SHORT)
    assert worker.evaluate(a, side=Side.LONG, now=a.as_of).candidate is not None
    r = worker.evaluate(b, side=Side.SHORT, now=b.as_of)
    assert r.candidate is None and reason in r.reason_codes
    assert worker.evaluate(a, side=Side.LONG, now=a.as_of).reason_codes == ("DUPLICATE_SETUP",)


def test_exact_volume_gate_boundary_and_unknown_relative_strength_do_not_get_defaults():
    from tradeforge.alpha.momentum import MomentumAlpha

    d = evidence()
    bars = list(d.setup_bars)
    bars[5] = change(bars[5], volume=Decimal("15"))
    d = change(d, setup_bars=tuple(bars))
    assert MomentumAlpha().evaluate(d, side=Side.LONG, now=d.as_of).candidate is not None
    bars[5] = change(bars[5], volume=Decimal("14.999999999999999999"))
    d = change(d, setup_bars=tuple(bars))
    assert MomentumAlpha().evaluate(d, side=Side.LONG, now=d.as_of).reason_codes == (
        "WEAK_PARTICIPATION",
    )


def test_future_nested_data_and_unaligned_feature_history_are_rejected():
    d = evidence()
    with pytest.raises(ValidationError, match="FUTURE_INPUT"):
        change(d, book=change(d.book, as_of=d.as_of + timedelta(seconds=1)))
    medium = list(d.medium_bars)
    medium[-1] = change(medium[-1], volume=Decimal("999"))
    with pytest.raises(ValidationError, match="FEATURE_BAR_MISMATCH"):
        change(d, medium_bars=tuple(medium))


def test_candidate_contract_refuses_approval_or_calibrated_confidence():
    from tradeforge.alpha.momentum import MomentumAlpha

    d = evidence()
    c = MomentumAlpha().evaluate(d, side=Side.LONG, now=d.as_of).candidate
    for patch in (
        {"state": DecisionState.APPROVED},
        {"confidence": 0.99},
        {"trading_allowed": True},
    ):
        with pytest.raises(ValidationError):
            change(c, **patch)


def test_revised_breakout_receipt_cannot_extend_setup_lifetime():
    from tradeforge.alpha.momentum import MomentumAlpha

    d = evidence(stage="watching")
    worker = MomentumAlpha()
    original = worker.evaluate(d, side=Side.LONG, now=d.as_of).candidate
    later = append_pullback(d)
    bars = list(later.setup_bars)
    bars[7] = change(bars[7], observed_at=later.as_of, as_of=later.as_of)
    later = change(later, setup_bars=tuple(bars))
    promoted = worker.evaluate(later, side=Side.LONG, now=later.as_of).candidate
    assert promoted.setup_id == original.setup_id
    assert promoted.expires_at == original.expires_at
    # A fresh worker/replay cannot renew the same setup either.
    assert MomentumAlpha().evaluate(
        later, side=Side.LONG, now=original.expires_at
    ).reason_codes == ("EXPIRED",)


@pytest.mark.parametrize("side", [Side.LONG, Side.SHORT])
def test_prior_closed_trigger_crossing_cannot_be_reused_after_price_returns(side):
    from tradeforge.alpha.momentum import MomentumAlpha

    d = evidence(side)
    sign = 1 if side is Side.LONG else -1

    def reflected(value):
        return Decimal(value) if sign > 0 else 200 - Decimal(value)

    bars = list(d.setup_bars)
    bars[6] = change(
        bars[6],
        close=reflected("105"),
        high=reflected("105.2" if sign > 0 else "103.8"),
        low=reflected("103.8" if sign > 0 else "105.2"),
    )
    bars[7] = change(
        bars[7],
        open=reflected("105"),
        close=reflected("104"),
        high=reflected("105.2" if sign > 0 else "103.8"),
        low=reflected("103.8" if sign > 0 else "105.2"),
    )
    d = change(d, setup_bars=tuple(bars))
    assert MomentumAlpha().evaluate(d, side=side, now=d.as_of).reason_codes == (
        "TRIGGER_ALREADY_PASSED",
    )


@pytest.mark.parametrize("side", [Side.LONG, Side.SHORT])
@pytest.mark.parametrize(
    "crossing,reason", [("trigger", "TRIGGER_ALREADY_PASSED"), ("stop", "STOP_ALREADY_BREACHED")]
)
def test_current_executable_quotes_invalidate_trigger_and_stop(side, crossing, reason):
    from tradeforge.alpha.momentum import MomentumAlpha

    d = evidence(side)
    center = Decimal("105" if crossing == "trigger" else "102")
    if side is Side.SHORT:
        center = 200 - center
    bid, ask = center - Decimal("0.01"), center + Decimal("0.01")
    d = change(
        d,
        market=change(d.market, best_bid=bid, best_ask=ask),
        book=change(d.book, bids=((bid, Decimal("1000")),), asks=((ask, Decimal("1000")),)),
    )
    assert MomentumAlpha().evaluate(d, side=side, now=d.as_of).reason_codes == (reason,)


def test_opposing_closed_higher_horizon_blocks_lower_timeframe_breakout():
    from tradeforge.alpha.momentum import MomentumAlpha

    d = evidence()
    higher = tuple(
        change(b, open=200 - b.open, close=200 - b.close, high=200 - b.low, low=200 - b.high)
        for b in d.higher_bars
    )
    d = change(d, higher_bars=higher)
    assert MomentumAlpha().evaluate(d, side=Side.LONG, now=d.as_of).reason_codes == (
        "TREND_MISALIGNED",
    )

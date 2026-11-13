"""Reversal safety against closed, synthetic range and hostile microstructure."""

from datetime import timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from tests.unit.test_momentum_alpha import change, evidence
from tradeforge.domain.common import DataQuality
from tradeforge.domain.decision import AlphaFamily, DecisionState, Side
from tradeforge.domain.market import RegimeLabel, StreamState
from tradeforge.intelligence.models import digest


def with_frame(features, frame):
    version = digest(
        {
            "frame": frame.model_dump(mode="json"),
            "policy": features.policy_hash,
            "previous": features.previous_hash,
            "algorithm": features.algorithm_version,
        }
    )
    return change(features, frame=frame, state=change(features.state, input_version=version))


def sample(side=Side.LONG, *, failed=False):
    from tradeforge.alpha.reversal import ReversalInput

    d = evidence(side)
    # Long prior range 99..101; low 97, closes at 100.5 then 100.8.
    rows = [(100, 101, 99, 100)] * 6 + [(100, 101, 97, 100.5), (100.5, 100.9, 100, 100.8)]
    if failed:
        rows[5] = (100, 101, 98, 98.5)
        rows[6] = (98.5, 101, 97, 100.5)

    def price(v):
        return Decimal(str(v)) if side is Side.LONG else 200 - Decimal(str(v))

    lower = tuple(
        change(
            bar,
            open=price(o),
            high=price(h if side is Side.LONG else low),
            low=price(low if side is Side.LONG else h),
            close=price(c),
            volume=Decimal("10"),
        )
        for bar, (o, h, low, c) in zip(d.setup_bars, rows, strict=True)
    )
    higher = tuple(
        change(b, open=Decimal("100"), high=Decimal("101"), low=Decimal("99"), close=Decimal("100"))
        for b in d.higher_bars
    )
    # Remove the trend from medium bars and their canonical frame as well.
    medium = tuple(
        change(b, open=Decimal("100"), high=Decimal("101"), low=Decimal("99"), close=Decimal("100"))
        for b in d.medium_bars
    )
    frame = change(
        d.features.frame,
        observations=tuple(
            change(r, current=medium[-1]) if r.instrument == d.instrument else r
            for r in d.features.frame.observations
        ),
    )
    features = with_frame(d.features, frame)
    features = change(features, state=change(features.state, label=RegimeLabel.RANGE))
    sign = 1 if side is Side.LONG else -1
    center = lower[-1].close
    market = change(d.market, best_bid=center - Decimal("0.01"), best_ask=center + Decimal("0.01"))
    book = change(
        d.book,
        bids=((market.best_bid, Decimal("1000")),),
        asks=((market.best_ask, Decimal("1000")),),
    )
    flow = change(d.flow, signed_taker_flow=Decimal(sign * 30))
    prior = change(
        flow,
        source_at=d.as_of - timedelta(seconds=60),
        observed_at=d.as_of - timedelta(seconds=60),
        as_of=d.as_of - timedelta(seconds=60),
        source_start=d.as_of - timedelta(seconds=120),
        source_end=d.as_of - timedelta(seconds=60),
        signed_taker_flow=Decimal(-sign * 60),
    )
    return ReversalInput.model_validate(
        {
            **d.model_dump(),
            "features": features,
            "setup_bars": lower,
            "medium_bars": medium,
            "higher_bars": higher,
            "market": market,
            "book": book,
            "flow": flow,
            "previous_flow": prior,
        }
    )


@pytest.mark.parametrize("side", [Side.LONG, Side.SHORT])
@pytest.mark.parametrize("failed", [False, True])
def test_clean_range_or_failed_breakout_emits_safe_symmetric_candidate(side, failed):
    from tradeforge.alpha.reversal import ReversalAlpha

    d = sample(side, failed=failed)
    c = ReversalAlpha().evaluate(d, side=side, now=d.as_of).candidate
    assert c is not None and c.state is DecisionState.ARMED
    assert c.alpha_family is AlphaFamily.REVERSAL and c.side is side
    assert c.trigger == Decimal("101" if side is Side.LONG else "99")
    assert c.stop_proposal == Decimal("97" if side is Side.LONG else "103")
    assert c.take_profit_proposal == Decimal("109" if side is Side.LONG else "91")
    assert "SWING_SWEEP_RECLAIM" in c.reason_codes
    if failed:
        assert "FAILED_BREAKOUT" in c.reason_codes
    assert c.confidence is None and c.proof_state.value == "UNPROVEN"
    assert c.trading_allowed is False and 0 < c.ordinal_quality <= 1
    assert c.source_feature_hash == d.features.digest and c.input_hash == d.digest
    assert c.expires_at == d.setup_bars[-2].close_at + timedelta(seconds=900)


@pytest.mark.parametrize("side", [Side.LONG, Side.SHORT])
@pytest.mark.parametrize(
    "kind,reason",
    [
        ("trend", "REGIME_NOT_ALLOWED"),
        ("panic", "REGIME_NOT_ALLOWED"),
        ("local_trend", "STRONG_TREND_FADE"),
        ("missing", "DATA_UNAVAILABLE"),
        ("history", "INSUFFICIENT_HISTORY"),
        ("fake", "NO_SWEEP_RECLAIM"),
        ("stretch", "STRETCH_GATE"),
        ("book", "BOOK_UNAVAILABLE"),
        ("empty_book", "BOOK_UNAVAILABLE"),
        ("liquidity", "LIQUIDITY_GATE"),
        ("spread", "SPREAD_GATE"),
        ("market", "MARKET_UNAVAILABLE"),
        ("flow", "FLOW_UNAVAILABLE"),
        ("opposed", "FLOW_NOT_STABILIZED"),
        ("no_turn", "FLOW_NOT_STABILIZED"),
        ("desync", "MICROSTRUCTURE_DESYNC"),
        ("book_quote", "MICROSTRUCTURE_DESYNC"),
        ("expired", "EXPIRED"),
        ("stale_regime", "STALE_REGIME"),
        ("stale_bars", "STALE_BARS"),
        ("universe", "UNIVERSE_UNAVAILABLE"),
        ("features", "FEATURE_UNAVAILABLE"),
    ],
)
def test_hard_vetoes_override_positive_reversal_features(side, kind, reason):
    from tradeforge.alpha.reversal import ReversalAlpha

    d = sample(side)
    now = d.as_of
    sign = 1 if side is Side.LONG else -1
    if kind in {"trend", "panic"}:
        d = change(
            d,
            features=change(
                d.features,
                state=change(
                    d.features.state,
                    label=RegimeLabel.TREND_EXPANSION if kind == "trend" else RegimeLabel.DISORDER,
                ),
            ),
        )
    if kind == "local_trend":
        d = change(d, higher_bars=evidence(Side.SHORT if sign > 0 else Side.LONG).higher_bars)
    if kind == "missing":
        d = change(d, quality=DataQuality.MISSING)
    if kind == "history":
        d = change(d, setup_bars=d.setup_bars[-3:])
    if kind == "fake":
        i = 6
        d = change(
            d,
            setup_bars=tuple(
                change(b, low=Decimal("99"))
                if n == i and sign > 0
                else change(b, high=Decimal("101"))
                if n == i
                else b
                for n, b in enumerate(d.setup_bars)
            ),
        )
    if kind == "stretch":
        d = change(
            d,
            setup_bars=tuple(
                change(b, low=Decimal("98.9"))
                if n == 6 and sign > 0
                else change(b, high=Decimal("101.1"))
                if n == 6
                else b
                for n, b in enumerate(d.setup_bars)
            ),
        )
    if kind == "book":
        d = change(d, book=change(d.book, captured_at=d.as_of - timedelta(seconds=20)))
    if kind == "empty_book":
        d = change(d, book=change(d.book, bids=(), asks=(), stream_state=StreamState.GAP))
    if kind == "liquidity":
        d = change(d, book=change(d.book, bids=((d.market.best_bid, Decimal("0.01")),)))
    if kind == "spread":
        d = change(d, market=change(d.market, best_ask=d.market.best_ask + Decimal("2")))
    if kind == "market":
        d = change(d, market=change(d.market, quality=DataQuality.STALE))
    if kind == "flow":
        d = change(d, flow=change(d.flow, validity=DataQuality.MISSING))
    if kind == "opposed":
        d = change(d, flow=change(d.flow, signed_taker_flow=Decimal(-sign * 30)))
    if kind == "no_turn":
        d = change(d, previous_flow=change(d.previous_flow, signed_taker_flow=Decimal(sign * 40)))
    if kind == "desync":
        d = change(
            d,
            book=change(
                d.book,
                source_at=d.as_of - timedelta(seconds=3),
                captured_at=d.as_of - timedelta(seconds=3),
            ),
        )
    if kind == "book_quote":
        d = change(
            d,
            book=change(
                d.book,
                bids=((Decimal("90"), Decimal("1000")),),
                asks=((Decimal("90.01"), Decimal("1000")),),
            ),
        )
    if kind == "expired":
        now += timedelta(seconds=900)
    if kind == "stale_regime":
        d = change(
            d,
            features=change(
                d.features, state=change(d.features.state, source_at=d.as_of - timedelta(hours=1))
            ),
        )
    if kind == "stale_bars":
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
    if kind == "universe":
        d = change(
            d,
            features=with_frame(
                d.features,
                change(
                    d.features.frame,
                    universe=change(
                        d.features.frame.universe,
                        observed_at=d.as_of - timedelta(days=2),
                        source_at=d.as_of - timedelta(days=2),
                    ),
                ),
            ),
        )
    if kind == "features":
        d = change(d, features=change(d.features, features=()))
    result = ReversalAlpha().evaluate(d, side=side, now=now)
    assert result.candidate is None and result.reason_codes == (reason,)


def test_replay_is_deterministic_duplicate_and_reordered_inputs_are_fenced():
    from tradeforge.alpha.reversal import ReversalAlpha

    d = sample()
    worker = ReversalAlpha()
    first = worker.evaluate(d, side=Side.LONG, now=d.as_of)
    assert first == ReversalAlpha().evaluate(d, side=Side.LONG, now=d.as_of)
    assert worker.evaluate(d, side=Side.LONG, now=d.as_of).reason_codes == ("DUPLICATE_SETUP",)
    newer = change(d, as_of=d.as_of + timedelta(seconds=1), quality=DataQuality.STALE)
    worker.evaluate(newer, side=Side.LONG, now=newer.as_of)
    with pytest.raises(ValueError, match="OUT_OF_ORDER_ALPHA"):
        worker.evaluate(d, side=Side.LONG, now=newer.as_of)
    with pytest.raises(ValueError, match="CONFLICTING_ALPHA_INPUT"):
        worker.evaluate(change(newer, quality=DataQuality.OK), side=Side.LONG, now=newer.as_of)


@pytest.mark.parametrize("kind", ["overlap", "identity", "future", "partial", "gap"])
def test_invalid_point_in_time_inputs_never_enter_alpha(kind):
    d = sample()
    with pytest.raises(ValidationError):
        if kind == "overlap":
            change(d, previous_flow=d.flow)
        elif kind == "identity":
            change(d, previous_flow=change(d.previous_flow, symbol="BTCUSDT"))
        elif kind == "future":
            change(d, as_of=d.as_of - timedelta(seconds=1))
        elif kind == "partial":
            change(d, setup_bars=(change(d.setup_bars[0], complete=False), *d.setup_bars[1:]))
        else:
            change(d, setup_bars=d.setup_bars[:2] + d.setup_bars[3:])


def test_family_conflict_and_dependent_features_are_never_independent_votes():
    from tradeforge.alpha.base import AlphaResult
    from tradeforge.alpha.feature_independence import combine_evidence
    from tradeforge.alpha.momentum import MomentumAlpha
    from tradeforge.alpha.reversal import ReversalAlpha

    d = sample()
    reversal = ReversalAlpha().evaluate(d, side=Side.LONG, now=d.as_of)
    m = evidence(Side.SHORT)
    momentum = MomentumAlpha().evaluate(m, side=Side.SHORT, now=m.as_of)
    # Comparable snapshot provenance; both families refer to the same source frame.
    momentum = change(
        momentum,
        candidate=change(
            momentum.candidate,
            source_feature_hash=d.features.digest,
            regime_version=d.features.state.input_version,
        ),
    )
    conflict = combine_evidence(momentum, reversal, now=d.as_of)
    assert conflict.side is Side.NO_TRADE and conflict.ordinal_quality == 0
    assert conflict.reason_codes == ("ALPHA_CONFLICT",) and not conflict.trading_allowed
    same = change(momentum, candidate=change(reversal.candidate, alpha_family=AlphaFamily.MOMENTUM))
    combined = combine_evidence(same, reversal, now=d.as_of)
    assert combined.ordinal_quality == reversal.candidate.ordinal_quality
    assert len(combined.evidence_groups) == 3 and combined.confidence is None
    empty = AlphaResult(candidate=None, reason_codes=("NO_SETUP",))
    assert combine_evidence(empty, empty, now=d.as_of).side is Side.NO_TRADE
    assert combine_evidence(empty, reversal, now=d.as_of + timedelta(hours=1)).side is Side.NO_TRADE


@pytest.mark.parametrize("side", [Side.LONG, Side.SHORT])
@pytest.mark.parametrize(
    "kind,reason",
    [
        ("weak_wick", "WEAK_WICK_REJECTION"),
        ("reclaim_lost", "RECLAIM_INVALIDATED"),
        ("closed_crossing", "TRIGGER_ALREADY_PASSED"),
        ("quote_crossing", "TRIGGER_ALREADY_PASSED"),
        ("stop_crossing", "STOP_ALREADY_BREACHED"),
        ("prior_missing", "FLOW_UNAVAILABLE"),
        ("prior_gap", "FLOW_WINDOW_GAP"),
        ("maximum_stretch", "STRETCH_GATE"),
    ],
)
def test_rejection_geometry_and_post_sweep_invalidations(side, kind, reason):
    from tradeforge.alpha.reversal import ReversalAlpha

    d = sample(side)
    sign = 1 if side is Side.LONG else -1

    def mirror(v):
        return Decimal(v) if sign > 0 else 200 - Decimal(v)

    if kind == "weak_wick":
        d = change(
            d,
            setup_bars=tuple(
                change(b, open=mirror("98")) if i == 6 else b for i, b in enumerate(d.setup_bars)
            ),
        )
    if kind == "reclaim_lost":
        last = change(
            d.setup_bars[-1],
            close=mirror("98.8"),
            **({"low": mirror("98.7")} if sign > 0 else {"high": mirror("98.7")}),
        )
        d = change(d, setup_bars=(*d.setup_bars[:-1], last))
    if kind == "closed_crossing":
        last = change(
            d.setup_bars[-1],
            close=mirror("101.1"),
            **({"high": mirror("101.2")} if sign > 0 else {"low": mirror("101.2")}),
        )
        d = change(d, setup_bars=(*d.setup_bars[:-1], last))
    if kind in {"quote_crossing", "stop_crossing"}:
        center = mirror("101.1" if kind == "quote_crossing" else "96.9")
        market = change(
            d.market, best_bid=center - Decimal("0.01"), best_ask=center + Decimal("0.01")
        )
        d = change(
            d,
            market=market,
            book=change(
                d.book,
                bids=((market.best_bid, Decimal("1000")),),
                asks=((market.best_ask, Decimal("1000")),),
            ),
        )
    if kind == "prior_missing":
        d = change(d, previous_flow=change(d.previous_flow, validity=DataQuality.MISSING))
    if kind == "prior_gap":
        d = change(
            d,
            previous_flow=change(
                d.previous_flow,
                source_start=d.previous_flow.source_start - timedelta(seconds=20),
                source_end=d.previous_flow.source_end - timedelta(seconds=20),
            ),
        )
    if kind == "maximum_stretch":
        d = change(
            d,
            setup_bars=tuple(
                change(b, **({"low": mirror("90")} if sign > 0 else {"high": mirror("90")}))
                if i == 6
                else b
                for i, b in enumerate(d.setup_bars)
            ),
        )
    assert ReversalAlpha().evaluate(d, side=side, now=d.as_of).reason_codes == (reason,)


def test_receipt_revisions_cannot_extend_a_closed_setup_lifetime():
    from tradeforge.alpha.reversal import ReversalAlpha

    d = sample()
    later = d.as_of + timedelta(seconds=900)
    recovered = change(
        d,
        as_of=later,
        setup_bars=tuple(change(b, observed_at=later, as_of=later) for b in d.setup_bars),
    )
    assert ReversalAlpha().evaluate(recovered, side=Side.LONG, now=later).reason_codes == (
        "EXPIRED",
    )


@pytest.mark.parametrize(
    "limit,reason",
    [
        ("max_setups", "SETUP_CAPACITY_EXHAUSTED"),
        ("max_instruments", "INSTRUMENT_CAPACITY_EXHAUSTED"),
    ],
)
def test_alpha_state_is_bounded(limit, reason):
    from tradeforge.alpha.reversal import ReversalAlpha, ReversalPolicy

    worker = ReversalAlpha(ReversalPolicy(**{limit: 1}))
    d, other = sample(), sample(Side.SHORT)
    assert worker.evaluate(d, side=Side.LONG, now=d.as_of).candidate is not None
    assert worker.evaluate(other, side=Side.SHORT, now=other.as_of).reason_codes == (reason,)


@pytest.mark.parametrize("kind", ["future", "provenance", "family", "groups", "authority"])
def test_evidence_audit_cannot_bypass_time_provenance_or_authority(kind):
    from tradeforge.alpha.base import AlphaResult
    from tradeforge.alpha.feature_independence import combine_evidence
    from tradeforge.alpha.reversal import ReversalAlpha

    d = sample()
    r = ReversalAlpha().evaluate(d, side=Side.LONG, now=d.as_of)
    m = change(r, candidate=change(r.candidate, alpha_family=AlphaFamily.MOMENTUM))
    if kind == "future":
        assert combine_evidence(m, r, now=d.as_of - timedelta(seconds=1)).side is Side.NO_TRADE
    elif kind == "provenance":
        m = change(m, candidate=change(m.candidate, source_feature_hash="a" * 64))
        assert combine_evidence(m, r, now=d.as_of).side is Side.NO_TRADE
    elif kind == "family":
        with pytest.raises(ValueError, match="ALPHA_FAMILY_MISMATCH"):
            combine_evidence(r, m, now=d.as_of)
    elif kind == "groups":
        m = change(m, candidate=change(m.candidate, evidence_groups=(("rsi_1m", 1.0),)))
        with pytest.raises(ValueError, match="EVIDENCE_GROUP"):
            combine_evidence(m, r, now=d.as_of)
    else:
        audit = combine_evidence(m, r, now=d.as_of)
        with pytest.raises(ValidationError):
            change(audit, trading_allowed=True)
        with pytest.raises(ValidationError):
            change(r.candidate, state=DecisionState.APPROVED)
        empty = AlphaResult(candidate=None, reason_codes=("NO_SETUP",))
        assert combine_evidence(empty, r, now=d.as_of).side is Side.LONG


@pytest.mark.parametrize("side", [Side.LONG, Side.SHORT])
@pytest.mark.parametrize("crossing", ["stop", "trigger"])
def test_post_sweep_extreme_cannot_be_forgotten_after_recovery(side, crossing):
    from tradeforge.alpha.reversal import ReversalAlpha

    d = sample(side)
    long = side is Side.LONG
    patch = {"low": Decimal("96.5")} if long else {"high": Decimal("103.5")}
    if crossing == "trigger":
        patch = {"high": Decimal("101.5")} if long else {"low": Decimal("98.5")}
    last = change(d.setup_bars[-1], **patch)
    d = change(d, setup_bars=(*d.setup_bars[:-1], last))
    result = ReversalAlpha().evaluate(d, side=side, now=d.as_of)
    assert result.candidate is None
    assert result.reason_codes == (
        ("STOP_ALREADY_BREACHED",) if crossing == "stop" else ("TRIGGER_ALREADY_PASSED",)
    )


def test_positive_ohlc_with_nonpositive_short_target_is_rejected_safely():
    from tradeforge.alpha.reversal import ReversalAlpha

    d = sample(Side.SHORT)

    def translated(b):
        return change(
            b, **{name: getattr(b, name) - 92 for name in ("open", "high", "low", "close")}
        )

    lower, medium, higher = (
        tuple(translated(b) for b in bars) for bars in (d.setup_bars, d.medium_bars, d.higher_bars)
    )
    frame = change(
        d.features.frame,
        observations=tuple(
            change(r, current=medium[-1]) if r.instrument == d.instrument else r
            for r in d.features.frame.observations
        ),
    )
    market = change(d.market, best_bid=Decimal("7.199"), best_ask=Decimal("7.201"))
    d = change(
        d,
        setup_bars=lower,
        medium_bars=medium,
        higher_bars=higher,
        features=with_frame(d.features, frame),
        market=market,
        book=change(
            d.book,
            bids=((market.best_bid, Decimal("10000")),),
            asks=((market.best_ask, Decimal("10000")),),
        ),
    )
    result = ReversalAlpha().evaluate(d, side=Side.SHORT, now=d.as_of)
    assert result.candidate is None and result.reason_codes == ("INVALID_TARGET_GEOMETRY",)


@pytest.mark.parametrize("side", [Side.LONG, Side.SHORT])
def test_valid_range_reversal_not_rejected_for_asynchronous_trade_timestamps(side):
    """A trade stream does not tick at the same instant as a depth stream."""
    from tradeforge.alpha.reversal import ReversalAlpha

    d = sample(side)
    earlier = d.as_of - timedelta(seconds=3)
    # Quote/depth are synchronized; last aggregate trade is three seconds old
    # but remains inside the strict five-second flow freshness limit.
    current = change(d.flow, source_at=earlier, source_end=earlier)
    d = change(d, flow=current)
    result = ReversalAlpha().evaluate(d, side=side, now=d.as_of)
    assert result.candidate is not None
    assert result.candidate.state is DecisionState.ARMED


@pytest.mark.parametrize("side", [Side.LONG, Side.SHORT])
def test_adjacent_valid_flow_windows_can_have_five_second_trade_silence(side):
    from tradeforge.alpha.reversal import ReversalAlpha

    d = sample(side)
    earlier = d.as_of - timedelta(seconds=65)
    prior = change(d.previous_flow, source_at=earlier, source_end=earlier)
    d = change(d, previous_flow=prior)
    result = ReversalAlpha().evaluate(d, side=side, now=d.as_of)
    assert result.candidate is not None
    assert result.candidate.state is DecisionState.ARMED


@pytest.mark.parametrize("side", [Side.LONG, Side.SHORT])
def test_prolonged_gap_between_flow_windows_remains_hard_veto(side):
    from tradeforge.alpha.reversal import ReversalAlpha

    d = sample(side)
    earlier = d.as_of - timedelta(seconds=80)
    d = change(
        d,
        previous_flow=change(d.previous_flow, source_at=earlier, source_end=earlier),
    )
    result = ReversalAlpha().evaluate(d, side=side, now=d.as_of)
    assert result.candidate is None
    assert result.reason_codes == ("FLOW_WINDOW_GAP",)

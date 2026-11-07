from datetime import timedelta
from decimal import Decimal as D

import pytest

from tests.replay.fixtures import T, order, quote, universe


def tape(events=None):
    from tradeforge.research.replay import ReplayTape

    return ReplayTape(
        dataset_version="recording1",
        events=events
        or (
            universe(),
            quote(1),
            quote(2),
            quote(3, bid="94", ask="95", qty="10"),
        ),
    )


def decide(event, view):
    assert all(e.as_of <= view.at for e in view.history)
    return (order(),) if event.sequence == 1 else ()


def test_same_tape_policy_has_same_decision_hashes_pnl_and_truthful_report():
    from tradeforge.research.fill_simulator import FillScenario
    from tradeforge.research.replay import replay

    a = replay(tape(), decide, policy_version="policy1", scenario=FillScenario())
    b = replay(tape(), decide, policy_version="policy1", scenario=FillScenario())
    assert a == b
    assert a.decision_hashes and a.net_pnl < 0
    assert a.scorecard.status == "NEGATIVE"
    assert a.scorecard.mode == "BACKTEST" and a.scorecard.live_eligible is False
    assert {g.dimension for g in a.groups} == {"symbol", "strategy", "regime", "hour"}
    assert a.scorecard.fees > 0 and a.scorecard.drawdown > 0
    assert a.net_pnl == D("-3.09700075")
    assert a.scorecard.fees == D("0.04850075")


def test_reorder_duplicate_and_future_data_fail_closed():
    from tradeforge.research.replay import replay

    with pytest.raises(ValueError, match="ORDER"):
        tape((universe(), quote(2), quote(1)))
    with pytest.raises(ValueError, match="DUPLICATE"):
        tape((universe(), quote(1), quote(1)))
    with pytest.raises(ValueError, match="FUTURE"):
        replay(
            tape(), lambda e, v: (order(2),) if e.sequence == 1 else (), policy_version="policy1"
        )
    q = quote(1)
    broken = type(q).model_construct(**{**q.model_dump(), "source_at": T + timedelta(days=2)})
    with pytest.raises(ValueError):
        tape((universe(), broken))


def test_point_in_time_delisting_retains_historical_symbol_and_closes_position():
    from tradeforge.research.replay import replay

    result = replay(
        tape((universe(), quote(1), quote(2), universe(3, eligible=()), quote(4, qty="10"))),
        decide,
        policy_version="policy1",
    )
    assert result.trades[0].exit_reason == "DELISTED"
    assert result.trades[0].instrument.symbol == "BTCUSDT"
    with pytest.raises(ValueError, match="UNIVERSE"):
        replay(tape((universe(0, eligible=()), quote(1))), decide, policy_version="policy1")


def test_open_positions_are_marked_and_never_fabricated_as_closed():
    from tradeforge.research.replay import replay

    result = replay(tape((universe(), quote(1), quote(2))), decide, policy_version="policy1")
    assert not result.trades and result.open_positions == 1
    assert result.scorecard.status == "UNPROVEN"
    assert result.scorecard.unrealized_pnl < 0
    assert result.scorecard.fees > 0


def test_fee_revision_and_funding_settlement_use_only_available_evidence():
    from tests.replay.fixtures import KEY, event
    from tradeforge.research.fill_simulator import FeeSchedule, Funding
    from tradeforge.research.replay import replay

    charge = Funding(**event(3), **KEY.model_dump(), rate=D("0.001"), mark_price=D("100"))
    fees = FeeSchedule(
        version="fee2",
        observed_at=T + timedelta(seconds=3),
        effective_at=T + timedelta(seconds=3),
        maker_rate=D("0.001"),
        taker_rate=D("0.002"),
    )
    result = replay(
        tape(
            (
                universe(),
                quote(1),
                quote(2, qty="10"),
                charge,
                quote(4, bid="110", ask="111", qty="10", fees=fees),
            )
        ),
        decide,
        policy_version="policy1",
    )
    assert result.trades[0].funding == D("0.1")
    assert result.trades[0].fees > D("0.2")
    with pytest.raises(ValueError, match="FUTURE_FEE"):
        quote(1, fees=fees)


def test_closed_bars_never_become_visible_before_their_close():
    from tests.replay.fixtures import KEY, event
    from tradeforge.domain.market import ClosedBar, Timeframe

    with pytest.raises(ValueError, match="BAR_NOT_AVAILABLE"):
        tape(
            (
                universe(),
                ClosedBar(
                    **event(1),
                    **KEY.model_dump(),
                    timeframe=Timeframe.M1,
                    open_at=T,
                    close_at=T + timedelta(seconds=59, milliseconds=999),
                    open=D("100"),
                    high=D("101"),
                    low=D("99"),
                    close=D("100"),
                    volume=D("10"),
                    source="recorded",
                    complete=True,
                ),
            )
        )


def test_open_groups_include_fees_and_partial_exits_are_not_complete_outcomes():
    from tradeforge.research.replay import replay

    result = replay(
        tape((universe(), quote(1), quote(2, qty="10"), quote(3, bid="94", ask="95", qty="0.25"))),
        decide,
        policy_version="policy1",
    )
    assert result.open_positions == 1 and result.trades
    assert result.scorecard.closed_trades == 0 and result.scorecard.net_ev is None
    assert result.scorecard.net_pnl < 0
    for group in result.groups:
        assert group.scorecard.open_positions == 1
        assert group.scorecard.fees == result.scorecard.fees
        assert group.scorecard.unrealized_pnl < 0


def test_replay_view_history_is_bounded_without_changing_execution():
    from tradeforge.research.replay import replay

    def policy(event, view):
        assert len(view.history) <= 2
        return decide(event, view)

    result = replay(tape(), policy, policy_version="policy1", history_limit=2)
    assert result.net_pnl < 0

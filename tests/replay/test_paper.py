from datetime import timedelta
from decimal import Decimal as D

import pytest

from tests.replay.fixtures import KEY, T, order, quote


def broker(**patch):
    from tradeforge.execution.paper_broker import PaperBroker
    from tradeforge.research.fill_simulator import FillScenario

    return PaperBroker(FillScenario(**patch))


def test_partial_fill_latency_costs_stop_and_restart():
    from tradeforge.execution.paper_broker import PaperBroker

    b = broker(latency_ms=500, slippage_bps=D("10"))
    b.submit(order(), at=T + timedelta(seconds=1), eligible=(KEY,))
    b.on_quote(quote(1))
    assert not b.fills
    b.on_quote(quote(2))
    p = b.positions[0]
    assert p.qty == p.protected_qty == D("0.5")
    assert p.entry_price == D("100.1")
    assert b.fills[0].fee == D("0.025025")
    restored = PaperBroker.restore(b.checkpoint())
    assert restored.checkpoint() == b.checkpoint()
    for current in (b, restored):
        current.on_quote(quote(3, bid="94", ask="95"))
        assert not current.positions
        assert current.trades[0].exit_reason == "STOP"
        assert current.trades[0].net_pnl < D("-3")
        assert current.trades[0].fees > 0
        assert current.trades[0].slippage_cost > 0
        current.on_quote(quote(4))
        assert not current.positions  # unfilled remainder canceled by risk exit
    assert restored.checkpoint() == b.checkpoint()


def test_duplicate_order_conflict_and_membership_fail_closed():
    b = broker()
    o = order()
    b.submit(o, at=o.intent.created_at, eligible=(KEY,))
    b.submit(o, at=o.intent.created_at, eligible=(KEY,))
    with pytest.raises(ValueError, match="CONFLICT"):
        b.submit(order(qty="2"), at=o.intent.created_at, eligible=(KEY,))
    with pytest.raises(ValueError, match="UNIVERSE"):
        broker().submit(o, at=o.intent.created_at, eligible=())
    with pytest.raises(ValueError, match="FUTURE"):
        broker().submit(order(2), at=T, eligible=(KEY,))
    b.on_quote(quote(2, qty="10"))
    assert len(b.fills) == 1 and b.positions[0].qty == D("1")


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_every_roundtrip_pays_both_fees_and_slippage(side):
    from tradeforge.domain.decision import Side

    b = broker(slippage_bps=D("10"))
    b.submit(order(side=Side(side)), at=T + timedelta(seconds=1), eligible=(KEY,))
    b.on_quote(quote(2, qty="10"))
    b.close(KEY, at=T + timedelta(seconds=2))
    b.on_quote(quote(3, qty="10"))
    trade = b.trades[0]
    assert trade.net_pnl == trade.gross_pnl - trade.fees - trade.funding
    assert trade.net_pnl < 0 and trade.slippage_cost > 0
    assert len(b.fills) == 2 and all(f.fee > 0 for f in b.fills)


def test_maker_requires_trade_through_limit_and_uses_dated_fee():
    b = broker()
    b.on_quote(quote(1, bid="99", ask="101"))
    b.submit(order(maker=True), at=T + timedelta(seconds=1), eligible=(KEY,))
    b.on_quote(quote(2))  # merely touching is not proof of queue execution
    assert not b.fills
    b.on_quote(quote(3, bid="98", ask="99", qty="10"))
    assert b.fills[0].price == D("100")
    assert b.fills[0].fee == D("0.02")


def test_maker_needs_passive_submission_evidence():
    with pytest.raises(ValueError, match="MAKER"):
        broker().submit(order(maker=True), at=T + timedelta(seconds=1), eligible=(KEY,))
    b = broker()
    b.on_quote(quote(1))
    with pytest.raises(ValueError, match="MAKER"):
        b.submit(order(maker=True), at=T + timedelta(seconds=1), eligible=(KEY,))


def test_delayed_quote_cannot_fill_using_pre_order_liquidity():
    b = broker(latency_ms=500)
    b.submit(order(2), at=T + timedelta(seconds=2), eligible=(KEY,))
    b.on_quote(quote(3, source_at=T + timedelta(seconds=1)))
    assert not b.fills
    b.on_quote(quote(4, source_at=T + timedelta(seconds=2, milliseconds=200)))
    assert not b.fills  # order is still in transit at this quote's source time
    b.on_quote(quote(5, qty="10"))
    assert b.positions[0].qty == D("1")


def test_manual_exit_cannot_execute_before_request_or_latency():
    b = broker(latency_ms=500)
    b.submit(order(), at=T + timedelta(seconds=1), eligible=(KEY,))
    b.on_quote(quote(2, qty="10"))
    b.close(KEY, at=T + timedelta(seconds=3))
    with pytest.raises(ValueError, match="ORDER"):
        b.on_quote(quote(2, sequence=4))
    b.on_quote(quote(3, sequence=5))
    assert b.positions
    b.on_quote(quote(4, qty="10"))
    assert not b.positions


def test_funding_same_timestamp_sequence_and_zero_short_pnl_are_valid():
    from tests.replay.fixtures import event
    from tradeforge.domain.decision import Side
    from tradeforge.research.fill_simulator import Funding

    b = broker(slippage_bps=D("0"), latency_ms=0)
    b.submit(order(side=Side.SHORT), at=T + timedelta(seconds=1), eligible=(KEY,))
    b.on_quote(quote(2, bid="100", ask="101", qty="10"))
    b.on_funding(
        Funding(**{**event(2), "sequence": 3}, **KEY.model_dump(), rate=D("0"), mark_price=D("100"))
    )
    b.close(KEY, at=T + timedelta(seconds=2))
    b.on_quote(quote(3, bid="99", ask="100", qty="10"))
    assert b.trades[0].gross_pnl == D("0") and b.trades[0].net_pnl < 0


def test_partial_exit_and_expiry_keep_only_actual_protection():
    b = broker()
    b.submit(order(), at=T + timedelta(seconds=1), eligible=(KEY,))
    b.on_quote(quote(2, qty="10"))
    b.on_quote(quote(3, bid="94", ask="95", qty="0.25"))
    assert b.positions[0].qty == b.positions[0].protected_qty == D("0.75")
    b.on_quote(quote(4, bid="94", ask="95", qty="10"))
    assert not b.positions and sum(t.qty for t in b.trades) == D("1")
    b2 = broker()
    b2.submit(order(), at=T + timedelta(seconds=1), eligible=(KEY,))
    b2.on_quote(quote(62))
    assert not b2.fills


def test_corrupt_checkpoint_and_reordered_quote_are_rejected():
    from tradeforge.execution.paper_broker import PaperBroker

    b = broker()
    b.on_quote(quote(2))
    with pytest.raises(ValueError, match="ORDER"):
        b.on_quote(quote(1))
    state = b.checkpoint()
    with pytest.raises(ValueError):
        PaperBroker.restore(state.replace('"digest":"', '"digest":"0'))


def test_checkpoint_reconciliation_rejects_inconsistent_inventory():
    import json

    from tradeforge.execution.paper_broker import PaperBroker
    from tradeforge.intelligence.models import digest

    b = broker()
    b.submit(order(), at=T + timedelta(seconds=1), eligible=(KEY,))
    b.on_quote(quote(2, qty="10"))
    assert b.fills[0].instrument == KEY
    assert b.fills[0].intent_id == order().intent.intent_id
    payload = json.loads(b.checkpoint())
    payload["state"]["positions"][0]["qty"] = "2"
    payload["digest"] = digest(payload["state"])
    with pytest.raises(ValueError, match="INVENTORY"):
        PaperBroker.restore(json.dumps(payload))


def test_slippage_staleness_and_empty_liquidity_cannot_fake_a_fill():
    b = broker(slippage_bps=D("400"))
    b.submit(order(), at=T + timedelta(seconds=1), eligible=(KEY,))
    b.on_quote(quote(2, qty="10"))
    assert not b.fills
    b = broker(max_quote_age_ms=100)
    b.submit(order(), at=T + timedelta(seconds=1), eligible=(KEY,))
    b.on_quote(quote(2, qty="0"))
    b.on_quote(quote(4, source_at=T + timedelta(seconds=2)))
    assert not b.fills
    with pytest.raises(ValueError, match="SOURCE_ORDER"):
        b.on_quote(quote(5, source_at=T + timedelta(seconds=1)))


@pytest.mark.parametrize("side,bid,ask", [("LONG", "111", "112"), ("SHORT", "88", "89")])
def test_target_exits_and_funding_credits(side, bid, ask):
    from tests.replay.fixtures import event
    from tradeforge.domain.decision import Side
    from tradeforge.research.fill_simulator import Funding

    b = broker()
    b.submit(order(side=Side(side)), at=T + timedelta(seconds=1), eligible=(KEY,))
    b.on_quote(quote(2, qty="10"))
    rate = D("-0.001") if side == "LONG" else D("0.001")
    b.on_funding(Funding(**event(3), **KEY.model_dump(), rate=rate, mark_price=D("100")))
    b.on_quote(quote(4, bid=bid, ask=ask, qty="10"))
    assert b.trades[0].exit_reason == "TARGET"
    assert b.trades[0].funding == D("-0.1")
    assert b.trades[0].net_pnl > 0

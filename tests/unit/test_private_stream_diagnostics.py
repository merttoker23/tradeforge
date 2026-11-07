"""Transport connectivity must remain distinct from account reconciliation."""

from datetime import timedelta

from tests.market_fixtures import START
from tradeforge.execution.private_stream import PrivateStream


def test_connected_socket_with_unready_account_is_reported_as_transport_ready():
    now = [START]
    stream = PrivateStream(None, lambda _: None, clock=lambda: now[0], latch_gap=False)
    stream.connected = True
    assert not stream.ready
    assert stream.health()["transport_ready"] is True
    assert stream.health()["connected"] is True
    now[0] += timedelta(seconds=61)
    assert stream.health()["transport_ready"] is False
    assert stream.health()["transport_age_seconds"] == 61
    stream.gap()
    assert stream.health()["connected"] is False

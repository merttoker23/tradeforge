"""SIGTERM requests cooperative shutdown and restores the prior handler."""

import signal
from threading import Event


def test_sigterm_sets_stop_and_restores_handler():
    from tradeforge.runtime.signals import shutdown_signals

    previous = signal.getsignal(signal.SIGTERM)
    stop = Event()
    with shutdown_signals(stop):
        signal.raise_signal(signal.SIGTERM)
        assert stop.is_set()
    assert signal.getsignal(signal.SIGTERM) == previous

"""Cooperative process shutdown; systemd owns restart and process cleanup."""

import asyncio
import signal
from collections.abc import Iterator
from contextlib import contextmanager
from threading import Event
from types import FrameType


@contextmanager
def shutdown_signals(stop: Event | asyncio.Event) -> Iterator[None]:
    def request_stop(number: int, frame: FrameType | None) -> None:
        stop.set()

    previous = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
    try:
        for number in previous:
            signal.signal(number, request_stop)
        yield
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)

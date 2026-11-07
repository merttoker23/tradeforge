"""Protection scheduling cannot queue behind optional executor work."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Event

from apps.execution_guardian import runtime


def test_protection_refresh_runs_while_optional_work_is_blocked():
    async def scenario():
        loop = asyncio.get_running_loop()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
        entered, release, protected = Event(), Event(), Event()

        def optional():
            entered.set()
            assert release.wait(5)

        class Recovery:
            ready = True

            def reconcile(self):
                protected.set()

        class Stream:
            def mark_recovered(self, ready):
                pass

        blocked = asyncio.create_task(asyncio.to_thread(optional))
        protection = None
        try:
            while not entered.is_set():
                await asyncio.sleep(0)
            refresh = getattr(runtime, "protection_loop", None)
            assert callable(refresh), "guardian has no independent protection schedule"
            protection = asyncio.create_task(refresh(Recovery(), Stream(), interval=0.01))
            deadline = loop.time() + 1
            while not protected.is_set() and loop.time() < deadline:
                await asyncio.sleep(0.001)
            assert protected.is_set(), "optional work starved account protection"
        finally:
            release.set()
            await blocked
            if protection is not None:
                protection.cancel()
                await asyncio.gather(protection, return_exceptions=True)

    asyncio.run(scenario())


def test_protection_shutdown_waits_for_inflight_exchange_work():
    async def scenario():
        entered, release, finished = Event(), Event(), Event()

        class Recovery:
            ready = True

            def reconcile(self):
                entered.set()
                assert release.wait(5)
                finished.set()

        class Stream:
            def mark_recovered(self, ready):
                pass

        task = asyncio.create_task(runtime.protection_loop(Recovery(), Stream()))
        try:
            while not entered.is_set():
                await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
        finally:
            release.set()
            result = await asyncio.gather(task, return_exceptions=True)
        assert finished.is_set()
        assert isinstance(result[0], asyncio.CancelledError)

    asyncio.run(scenario())


def test_unexpected_protection_failure_propagates_to_service_supervisor():
    async def scenario():
        class Recovery:
            ready = True

            def reconcile(self):
                raise RuntimeError("failed protection worker")

        class Stream:
            def mark_recovered(self, ready):
                raise AssertionError("failed recovery cannot recover stream")

        import pytest

        with pytest.raises(RuntimeError, match="failed protection worker"):
            await runtime.protection_loop(Recovery(), Stream())

    asyncio.run(scenario())

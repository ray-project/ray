import asyncio
import warnings
import sys

import pytest
import ray

from ray._common.utils import (
    get_or_create_event_loop,
    run_background_task,
    _BACKGROUND_TASKS,
)

# Export test utility classes for use by other test files
__all__ = ["SignalActor", "Semaphore"]


# Test utility classes (moved from _common/test_utils.py)
@ray.remote(num_cpus=0)
class SignalActor:
    def __init__(self):
        self.ready_event = asyncio.Event()
        self.num_waiters = 0

    def send(self, clear: bool = False):
        self.ready_event.set()
        if clear:
            self.ready_event.clear()

    async def wait(self, should_wait: bool = True):
        if should_wait:
            self.num_waiters += 1
            await self.ready_event.wait()
            self.num_waiters -= 1

    async def cur_num_waiters(self) -> int:
        return self.num_waiters


@ray.remote(num_cpus=0)
class Semaphore:
    def __init__(self, value: int = 1):
        self._sema = asyncio.Semaphore(value=value)

    async def acquire(self):
        await self._sema.acquire()

    async def release(self):
        self._sema.release()

    async def locked(self) -> bool:
        return self._sema.locked()


# Tests for utility functions
class TestGetOrCreateEventLoop:
    def test_existing_event_loop(self):
        # With running event loop
        expect_loop = asyncio.new_event_loop()
        expect_loop.set_debug(True)
        asyncio.set_event_loop(expect_loop)
        with warnings.catch_warnings():
            # Assert no deprecating warnings raised for python>=3.10.
            warnings.simplefilter("error")
            actual_loop = get_or_create_event_loop()

            assert actual_loop == expect_loop, "Loop should not be recreated."

    def test_new_event_loop(self):
        with warnings.catch_warnings():
            # Assert no deprecating warnings raised for python>=3.10.
            warnings.simplefilter("error")
            loop = get_or_create_event_loop()
            assert loop is not None, "new event loop should be created."


@pytest.mark.asyncio
async def test_run_background_task():
    result = {}

    async def co():
        result["start"] = 1
        await asyncio.sleep(0)
        result["end"] = 1

    run_background_task(co())

    # Background task is running.
    assert len(_BACKGROUND_TASKS) == 1
    # co executed.
    await asyncio.sleep(0)
    # await asyncio.sleep(0) from co is reached.
    await asyncio.sleep(0)
    # co finished and callback called.
    await asyncio.sleep(0)
    # The task should be removed from the set once it finishes.
    assert len(_BACKGROUND_TASKS) == 0

    assert result.get("start") == 1
    assert result.get("end") == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))

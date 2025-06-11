import asyncio
import concurrent.futures
import sys
import threading

import pytest

from ray._common.test_utils import wait_for_condition
from ray.serve._private.utils import run_coroutine_or_future_threadsafe


@pytest.fixture
def separate_loop():
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever)
    thread.start()
    yield loop
    loop.call_soon_threadsafe(loop.stop)
    thread.join()
    loop.close()


@pytest.mark.asyncio
async def test_run_coroutine_threadsafe_with_basic_coroutine(separate_loop):
    async def sample_coro():
        await asyncio.sleep(0.01)
        return "ok"

    future = run_coroutine_or_future_threadsafe(sample_coro(), separate_loop)
    result = future.result(timeout=1)

    assert isinstance(future, concurrent.futures.Future)
    assert result == "ok"


@pytest.mark.asyncio
async def test_run_coroutine_threadsafe_with_future(separate_loop):
    async_future = asyncio.Future(loop=separate_loop)
    async_future.set_result("ok2")
    future = run_coroutine_or_future_threadsafe(async_future, separate_loop)
    result = future.result(timeout=1)
    assert result == "ok2"


@pytest.mark.asyncio
async def test_run_coroutine_threadsafe_with_task(separate_loop):
    async def sample_coro():
        await asyncio.sleep(0.01)
        return "ok"

    async_future = separate_loop.create_task(sample_coro())
    future = run_coroutine_or_future_threadsafe(async_future, separate_loop)
    result = future.result(timeout=1)
    assert result == "ok"


@pytest.mark.asyncio
async def test_run_coroutine_threadsafe_cancellation(separate_loop):
    async def cancelled_coro():
        await asyncio.sleep(5)

    async_future = separate_loop.create_task(cancelled_coro())
    future = run_coroutine_or_future_threadsafe(async_future, separate_loop)
    future.cancel()
    assert future.cancelled()
    wait_for_condition(lambda: async_future.cancelled())


@pytest.mark.asyncio
async def test_run_coroutine_threadsafe_with_future_from_other_loop(separate_loop):
    future = asyncio.Future(loop=asyncio.get_running_loop())
    future.set_result("ok")
    with pytest.raises(AssertionError):
        run_coroutine_or_future_threadsafe(future, separate_loop)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))

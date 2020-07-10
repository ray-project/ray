# coding: utf-8
import asyncio
import sys
import threading

import pytest

import ray
from ray.test_utils import SignalActor


def test_asyncio_actor(ray_start_regular_shared):
    @ray.remote
    class AsyncBatcher:
        def __init__(self):
            self.batch = []
            self.event = asyncio.Event()

        async def add(self, x):
            self.batch.append(x)
            if len(self.batch) >= 3:
                self.event.set()
            else:
                await self.event.wait()
            return sorted(self.batch)

    a = AsyncBatcher.remote()
    x1 = a.add.remote(1)
    x2 = a.add.remote(2)
    x3 = a.add.remote(3)
    r1 = ray.get(x1)
    r2 = ray.get(x2)
    r3 = ray.get(x3)
    assert r1 == [1, 2, 3]
    assert r1 == r2 == r3


def test_asyncio_actor_same_thread(ray_start_regular_shared):
    @ray.remote
    class Actor:
        def sync_thread_id(self):
            return threading.current_thread().ident

        async def async_thread_id(self):
            return threading.current_thread().ident

    a = Actor.remote()
    sync_id, async_id = ray.get(
        [a.sync_thread_id.remote(),
         a.async_thread_id.remote()])
    assert sync_id == async_id


def test_asyncio_actor_concurrency(ray_start_regular_shared):
    @ray.remote
    class RecordOrder:
        def __init__(self):
            self.history = []

        async def do_work(self):
            self.history.append("STARTED")
            # Force a context switch
            await asyncio.sleep(0)
            self.history.append("ENDED")

        def get_history(self):
            return self.history

    num_calls = 10

    a = RecordOrder.options(max_concurrency=1).remote()
    ray.get([a.do_work.remote() for _ in range(num_calls)])
    history = ray.get(a.get_history.remote())

    # We only care about ordered start-end-start-end sequence because
    # coroutines may be executed out of enqueued order.
    answer = []
    for _ in range(num_calls):
        for status in ["STARTED", "ENDED"]:
            answer.append(status)

    assert history == answer


def test_asyncio_actor_high_concurrency(ray_start_regular_shared):
    # This tests actor can handle concurrency above recursionlimit.

    @ray.remote
    class AsyncConcurrencyBatcher:
        def __init__(self, batch_size):
            self.batch = []
            self.event = asyncio.Event()
            self.batch_size = batch_size

        async def add(self, x):
            self.batch.append(x)
            if len(self.batch) >= self.batch_size:
                self.event.set()
            else:
                await self.event.wait()
            return sorted(self.batch)

    batch_size = sys.getrecursionlimit() * 4
    actor = AsyncConcurrencyBatcher.options(max_concurrency=batch_size *
                                            2).remote(batch_size)
    result = ray.get([actor.add.remote(i) for i in range(batch_size)])
    assert result[0] == list(range(batch_size))
    assert result[-1] == list(range(batch_size))


@pytest.mark.asyncio
async def test_asyncio_get(ray_start_regular_shared, event_loop):
    loop = event_loop
    asyncio.set_event_loop(loop)
    loop.set_debug(True)

    # Test Async Plasma
    @ray.remote
    def task():
        return 1

    assert await ray.async_compat.get_async(task.remote()) == 1

    @ray.remote
    def task_throws():
        1 / 0

    with pytest.raises(ray.exceptions.RayTaskError):
        await ray.async_compat.get_async(task_throws.remote())

    # Test actor calls.
    str_len = 200 * 1024

    @ray.remote
    class Actor:
        def echo(self, i):
            return i

        def big_object(self):
            # 100Kb is the limit for direct call
            return "a" * (str_len)

        def throw_error(self):
            1 / 0

    actor = Actor.remote()

    actor_call_future = ray.async_compat.get_async(actor.echo.remote(2))
    assert await actor_call_future == 2

    promoted_to_plasma_future = ray.async_compat.get_async(
        actor.big_object.remote())
    assert await promoted_to_plasma_future == "a" * str_len

    with pytest.raises(ray.exceptions.RayTaskError):
        await ray.async_compat.get_async(actor.throw_error.remote())


def test_asyncio_actor_async_get(ray_start_regular_shared):
    @ray.remote
    def remote_task():
        return 1

    @ray.remote
    class AsyncGetter:
        async def get(self):
            return await remote_task.remote()

        async def plasma_get(self, plasma_object):
            return await plasma_object[0]

    plasma_object = ray.put(2)
    getter = AsyncGetter.remote()
    assert ray.get(getter.get.remote()) == 1
    assert ray.get(getter.plasma_get.remote([plasma_object])) == 2


@pytest.mark.asyncio
async def test_asyncio_double_await(ray_start_regular_shared):
    # This is a regression test for
    # https://github.com/ray-project/ray/issues/8841

    signal = SignalActor.remote()
    waiting = signal.wait.remote()

    future = waiting.as_future()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(future, timeout=0.1)
    assert future.cancelled()

    # We are explicitly waiting multiple times here to test asyncio state
    # override.
    await signal.send.remote()
    await waiting
    await waiting


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))

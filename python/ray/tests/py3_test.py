# coding: utf-8
import asyncio
import threading
import pytest
import sys

import ray
import ray.cluster_utils
import ray.test_utils


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
def test_args_force_positional(ray_start_regular):
    def force_positional(*, a="hello", b="helxo", **kwargs):
        return a, b, kwargs

    class TestActor():
        def force_positional(self, a="hello", b="heo", *args, **kwargs):
            return a, b, args, kwargs

    def test_function(fn, remote_fn):
        assert fn(a=1, b=3, c=5) == ray.get(remote_fn.remote(a=1, b=3, c=5))
        assert fn(a=1) == ray.get(remote_fn.remote(a=1))
        assert fn(a=1) == ray.get(remote_fn.remote(a=1))

    remote_test_function = ray.remote(test_function)

    remote_force_positional = ray.remote(force_positional)
    test_function(force_positional, remote_force_positional)
    ray.get(
        remote_test_function.remote(force_positional, remote_force_positional))

    remote_actor_class = ray.remote(TestActor)
    remote_actor = remote_actor_class.remote()
    actor_method = remote_actor.force_positional
    local_actor = TestActor()
    local_method = local_actor.force_positional
    test_function(local_method, actor_method)
    ray.get(remote_test_function.remote(local_method, actor_method))


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": False
    }, {
        "local_mode": True
    }],
    indirect=True)
def test_args_intertwined(ray_start_regular):
    def args_intertwined(a, *args, x="hello", **kwargs):
        return a, args, x, kwargs

    class TestActor():
        def args_intertwined(self, a, *args, x="hello", **kwargs):
            return a, args, x, kwargs

        @classmethod
        def cls_args_intertwined(cls, a, *args, x="hello", **kwargs):
            return a, args, x, kwargs

    def test_function(fn, remote_fn):
        assert fn(
            1, 2, 3, x="hi", y="hello") == ray.get(
                remote_fn.remote(1, 2, 3, x="hi", y="hello"))
        assert fn(
            1, 2, 3, y="1hello") == ray.get(
                remote_fn.remote(1, 2, 3, y="1hello"))
        assert fn(1, y="1hello") == ray.get(remote_fn.remote(1, y="1hello"))

    remote_test_function = ray.remote(test_function)

    remote_args_intertwined = ray.remote(args_intertwined)
    test_function(args_intertwined, remote_args_intertwined)
    ray.get(
        remote_test_function.remote(args_intertwined, remote_args_intertwined))

    remote_actor_class = ray.remote(TestActor)
    remote_actor = remote_actor_class.remote()
    actor_method = remote_actor.args_intertwined
    local_actor = TestActor()
    local_method = local_actor.args_intertwined
    test_function(local_method, actor_method)
    ray.get(remote_test_function.remote(local_method, actor_method))

    actor_method = remote_actor.cls_args_intertwined
    local_actor = TestActor()
    local_method = local_actor.cls_args_intertwined
    test_function(local_method, actor_method)
    ray.get(remote_test_function.remote(local_method, actor_method))


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

    a = AsyncBatcher.options(is_direct_call=True).remote()
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

    a = Actor.options(is_direct_call=True).remote()
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

    a = RecordOrder.options(is_direct_call=True, max_concurrency=1).remote()
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
    actor = AsyncConcurrencyBatcher.options(
        max_concurrency=batch_size * 2, is_direct_call=True).remote(batch_size)
    result = ray.get([actor.add.remote(i) for i in range(batch_size)])
    assert result[0] == list(range(batch_size))
    assert result[-1] == list(range(batch_size))


@pytest.mark.asyncio
async def test_asyncio_get(ray_start_regular_shared, event_loop):
    loop = event_loop
    asyncio.set_event_loop(loop)
    loop.set_debug(True)

    # This is needed for async plasma
    from ray.experimental.async_api import _async_init
    await _async_init()

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

    # Test Direct Actor Call
    str_len = 200 * 1024

    @ray.remote
    class DirectActor:
        def echo(self, i):
            return i

        def big_object(self):
            # 100Kb is the limit for direct call
            return "a" * (str_len)

        def throw_error(self):
            1 / 0

    direct = DirectActor.options(is_direct_call=True).remote()

    direct_actor_call_future = ray.async_compat.get_async(
        direct.echo.remote(2))
    assert await direct_actor_call_future == 2

    promoted_to_plasma_future = ray.async_compat.get_async(
        direct.big_object.remote())
    assert await promoted_to_plasma_future == "a" * str_len

    with pytest.raises(ray.exceptions.RayTaskError):
        await ray.async_compat.get_async(direct.throw_error.remote())


def test_asyncio_actor_async_get(ray_start_regular_shared):
    @ray.remote
    def remote_task():
        return 1

    plasma_object = ray.put(2)

    @ray.remote
    class AsyncGetter:
        async def get(self):
            return await remote_task.remote()

        async def plasma_get(self):
            return await plasma_object

    getter = AsyncGetter.options().remote()
    assert ray.get(getter.get.remote()) == 1
    assert ray.get(getter.plasma_get.remote()) == 2

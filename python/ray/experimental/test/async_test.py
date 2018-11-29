from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import asyncio
import time

import pytest

import ray
from ray.experimental import async_api


@pytest.fixture
def init():
    ray.init(num_cpus=4)
    asyncio.get_event_loop().set_debug(False)
    asyncio.get_event_loop().run_until_complete(async_api.init())
    yield
    async_api.shutdown()
    ray.shutdown()


def gen_tasks(time_scale=0.1):
    @ray.remote
    def f(n):
        time.sleep(n * time_scale)
        return n

    tasks = [f.remote(i) for i in range(5)]
    return tasks


def test_get(init):
    tasks = gen_tasks()
    fut = async_api.get(tasks)
    results = asyncio.get_event_loop().run_until_complete(fut)
    assert all(a == b for a, b in zip(results, ray.get(tasks)))


def test_get_benchmark(init):
    @ray.remote
    def f(n):
        time.sleep(0.001 * n)
        return 42

    async def test_async():
        sum_time = 0.
        for _ in range(50):
            tasks = [f.remote(n) for n in range(20)]
            start = time.time()
            await async_api.get(tasks)
            sum_time += time.time() - start
        return sum_time

    def baseline():
        sum_time = 0.
        for _ in range(50):
            tasks = [f.remote(n) for n in range(20)]
            start = time.time()
            ray.get(tasks)
            sum_time += time.time() - start
        return sum_time

    # warm up
    baseline()
    # async get
    sum_time_1 = asyncio.get_event_loop().run_until_complete(test_async())
    # get
    sum_time_2 = baseline()

    # Ensure the new implementation is not too slow.
    assert sum_time_2 * 1.2 > sum_time_1


def test_wait(init):
    tasks = gen_tasks()
    fut = async_api.wait(tasks, num_returns=len(tasks))
    results, _ = asyncio.get_event_loop().run_until_complete(fut)
    assert set(results) == set(tasks)


def test_wait_timeout(init):
    tasks = gen_tasks(10)
    fut = async_api.wait(tasks, timeout=5000, num_returns=len(tasks))
    results, _ = asyncio.get_event_loop().run_until_complete(fut)
    assert results[0] == tasks[0]


def test_get_mixup(init):
    @ray.remote
    def f(n):
        time.sleep(n * 0.1)
        return n

    async def g(n):
        await asyncio.sleep(n * 0.1)
        return n

    tasks = [f.remote(1), g(2), f.remote(3), g(4)]
    results = asyncio.get_event_loop().run_until_complete(async_api.get(tasks))
    assert results == [1, 2, 3, 4]


def test_wait_mixup(init):
    @ray.remote
    def f(n):
        time.sleep(n)
        return n

    def g(n):
        async def _g(_n):
            await asyncio.sleep(_n)
            return _n

        return asyncio.ensure_future(_g(n))

    tasks = [f.remote(0.1), g(7), f.remote(5), g(2)]
    ready, _ = asyncio.get_event_loop().run_until_complete(
        async_api.wait(tasks, timeout=4000, num_returns=4))
    assert set(ready) == {tasks[0], tasks[-1]}

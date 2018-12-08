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
    async_api.init()
    asyncio.get_event_loop().set_debug(False)
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


def test_simple(init):
    @ray.remote
    def f():
        time.sleep(1)
        return {"key1": ["value"]}

    future = async_api.as_future(f.remote())
    result = asyncio.get_event_loop().run_until_complete(future)
    assert result["key1"] == ["value"]


def test_gather(init):
    loop = asyncio.get_event_loop()
    tasks = gen_tasks()
    futures = [async_api.as_future(obj_id) for obj_id in tasks]
    results = loop.run_until_complete(asyncio.gather(*futures))
    assert all(a == b for a, b in zip(results, ray.get(tasks)))


def test_gather_benchmark(init):
    @ray.remote
    def f(n):
        time.sleep(0.001 * n)
        return 42

    async def test_async():
        sum_time = 0.
        for _ in range(50):
            tasks = [f.remote(n) for n in range(20)]
            start = time.time()
            futures = [async_api.as_future(obj_id) for obj_id in tasks]
            await asyncio.gather(*futures)
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
    loop = asyncio.get_event_loop()
    tasks = gen_tasks()
    futures = [async_api.as_future(obj_id) for obj_id in tasks]
    results, _ = loop.run_until_complete(asyncio.wait(futures))
    assert set(results) == set(futures)


def test_wait_timeout(init):
    loop = asyncio.get_event_loop()
    tasks = gen_tasks(10)
    futures = [async_api.as_future(obj_id) for obj_id in tasks]
    fut = asyncio.wait(futures, timeout=5)
    results, _ = loop.run_until_complete(fut)
    assert list(results)[0] == futures[0]


def test_gather_mixup(init):
    loop = asyncio.get_event_loop()

    @ray.remote
    def f(n):
        time.sleep(n * 0.1)
        return n

    async def g(n):
        await asyncio.sleep(n * 0.1)
        return n

    tasks = [
        async_api.as_future(f.remote(1)),
        g(2),
        async_api.as_future(f.remote(3)),
        g(4)
    ]
    results = loop.run_until_complete(asyncio.gather(*tasks))
    assert results == [1, 2, 3, 4]


def test_wait_mixup(init):
    loop = asyncio.get_event_loop()

    @ray.remote
    def f(n):
        time.sleep(n)
        return n

    def g(n):
        async def _g(_n):
            await asyncio.sleep(_n)
            return _n

        return asyncio.ensure_future(_g(n))

    tasks = [
        async_api.as_future(f.remote(0.1)),
        g(7),
        async_api.as_future(f.remote(5)),
        g(2)
    ]
    ready, _ = loop.run_until_complete(asyncio.wait(tasks, timeout=4))
    assert set(ready) == {tasks[0], tasks[-1]}

import asyncio
import time
import pytest

import numpy as np

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
        return n, np.zeros(1024 * 1024, dtype=np.uint8)

    return [f.remote(i) for i in range(5)]


def test_simple(init):
    @ray.remote
    def f():
        time.sleep(1)
        return np.zeros(1024 * 1024, dtype=np.uint8)

    future = async_api.as_future(f.remote())
    result = asyncio.get_event_loop().run_until_complete(future)
    assert isinstance(result, np.ndarray)


def test_gather(init):
    loop = asyncio.get_event_loop()
    tasks = gen_tasks()
    futures = [async_api.as_future(obj_id) for obj_id in tasks]
    results = loop.run_until_complete(asyncio.gather(*futures))
    assert all(a[0] == b[0] for a, b in zip(results, ray.get(tasks)))


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
        return n, np.zeros(1024 * 1024, dtype=np.uint8)

    async def g(n):
        await asyncio.sleep(n * 0.1)
        return n, np.zeros(1024 * 1024, dtype=np.uint8)

    tasks = [
        async_api.as_future(f.remote(1)),
        g(2),
        async_api.as_future(f.remote(3)),
        g(4)
    ]
    results = loop.run_until_complete(asyncio.gather(*tasks))
    assert [result[0] for result in results] == [1, 2, 3, 4]


def test_wait_mixup(init):
    loop = asyncio.get_event_loop()

    @ray.remote
    def f(n):
        time.sleep(n)
        return n, np.zeros(1024 * 1024, dtype=np.uint8)

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

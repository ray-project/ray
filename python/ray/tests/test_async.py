import asyncio
import sys
import time

import numpy as np

import pytest

import ray


@pytest.fixture
def init():
    ray.init(num_cpus=4)
    asyncio.get_event_loop().set_debug(False)
    yield
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

    future = f.remote().as_future()
    result = asyncio.get_event_loop().run_until_complete(future)
    assert isinstance(result, np.ndarray)


def test_gather(init):
    loop = asyncio.get_event_loop()
    tasks = gen_tasks()
    futures = [obj_ref.as_future() for obj_ref in tasks]
    results = loop.run_until_complete(asyncio.gather(*futures))
    assert all(a[0] == b[0] for a, b in zip(results, ray.get(tasks)))


def test_wait(init):
    loop = asyncio.get_event_loop()
    tasks = gen_tasks()
    futures = [obj_ref.as_future() for obj_ref in tasks]
    results, _ = loop.run_until_complete(asyncio.wait(futures))
    assert set(results) == set(futures)


def test_wait_timeout(init):
    loop = asyncio.get_event_loop()
    tasks = gen_tasks(10)
    futures = [obj_ref.as_future() for obj_ref in tasks]
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

    tasks = [f.remote(1).as_future(), g(2), f.remote(3).as_future(), g(4)]
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

    tasks = [f.remote(0.1).as_future(), g(7), f.remote(5).as_future(), g(2)]
    ready, _ = loop.run_until_complete(asyncio.wait(tasks, timeout=4))
    assert set(ready) == {tasks[0], tasks[-1]}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ray_start_regular_shared", [{
        "object_store_memory": 100 * 1024 * 1024,
    }],
    indirect=True)
async def test_garbage_collection(ray_start_regular_shared):
    # This is a regression test for
    # https://github.com/ray-project/ray/issues/9134

    @ray.remote
    def f():
        return np.zeros(40 * 1024 * 1024, dtype=np.uint8)

    for _ in range(10):
        await f.remote()
    for _ in range(10):
        put_id = ray.put(np.zeros(40 * 1024 * 1024, dtype=np.uint8))
        await put_id


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))

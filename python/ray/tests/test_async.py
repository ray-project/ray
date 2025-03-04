import asyncio
import concurrent.futures
import sys
import time

import numpy as np

import pytest

import ray
from ray._private.test_utils import wait_for_condition
from ray._private.utils import (
    get_or_create_event_loop,
    run_background_task,
    background_tasks,
)


@pytest.fixture
def init():
    ray.init(num_cpus=4)
    get_or_create_event_loop().set_debug(False)
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
    result = get_or_create_event_loop().run_until_complete(future)
    assert isinstance(result, np.ndarray)


def test_gather(init):
    loop = get_or_create_event_loop()
    tasks = gen_tasks()
    futures = [obj_ref.as_future() for obj_ref in tasks]
    results = loop.run_until_complete(asyncio.gather(*futures))
    assert all(a[0] == b[0] for a, b in zip(results, ray.get(tasks)))


def test_wait(init):
    loop = get_or_create_event_loop()
    tasks = gen_tasks()
    futures = [obj_ref.as_future() for obj_ref in tasks]
    results, _ = loop.run_until_complete(asyncio.wait(futures))
    assert set(results) == set(futures)


def test_wait_timeout(init):
    loop = get_or_create_event_loop()
    tasks = gen_tasks(10)
    futures = [obj_ref.as_future() for obj_ref in tasks]
    fut = asyncio.wait(futures, timeout=5)
    results, _ = loop.run_until_complete(fut)
    assert list(results)[0] == futures[0]


def test_gather_mixup(init):
    loop = get_or_create_event_loop()

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
    loop = get_or_create_event_loop()

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
    "ray_start_regular_shared",
    [
        {
            "object_store_memory": 100 * 1024 * 1024,
        }
    ],
    indirect=True,
)
async def test_garbage_collection(ray_start_regular_shared):
    # This is a regression test for
    # https://github.com/ray-project/ray/issues/9134

    @ray.remote
    def f():
        return np.zeros(20 * 1024 * 1024, dtype=np.uint8)

    for _ in range(10):
        await f.remote()
    for _ in range(10):
        put_id = ray.put(np.zeros(20 * 1024 * 1024, dtype=np.uint8))
        await put_id


def test_concurrent_future(ray_start_regular_shared):
    ref = ray.put(1)

    fut = ref.future()
    assert isinstance(fut, concurrent.futures.Future)
    wait_for_condition(lambda: fut.done())

    global_result = None

    def cb(fut):
        nonlocal global_result
        global_result = fut.result()

    fut.add_done_callback(cb)
    assert global_result == 1
    assert fut.result() == 1


def test_concurrent_future_many(ray_start_regular_shared):
    @ray.remote
    def task(i):
        return i

    refs = [task.remote(i) for i in range(100)]
    futs = [ref.future() for ref in refs]
    result = set()

    for fut in concurrent.futures.as_completed(futs):
        assert fut.done()
        result.add(fut.result())
    assert result == set(range(100))


@pytest.mark.asyncio
async def test_run_backgroun_job():
    """Test `run_backgroun_job` works as expected."""
    result = {}

    async def co():
        result["start"] = 1
        await asyncio.sleep(0)
        result["end"] = 1

    run_background_task(co())

    # Backgroun job is registered.
    assert len(background_tasks) == 1
    # co executed.
    await asyncio.sleep(0)
    # await asyncio.sleep(0) from co is reached.
    await asyncio.sleep(0)
    # co finished and callback called.
    await asyncio.sleep(0)
    # The callback should be cleaned.
    assert len(background_tasks) == 0

    assert result.get("start") == 1
    assert result.get("end") == 1


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))

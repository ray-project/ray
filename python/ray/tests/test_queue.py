import pytest

import ray
from ray.exceptions import GetTimeoutError, RayActorError
from ray.util.queue import Queue, Empty, Full
from ray.test_utils import wait_for_condition


# Remote helper functions for testing concurrency
@ray.remote
def async_get(queue):
    return queue.get(block=True)


@ray.remote
def async_put(queue, item):
    return queue.put(item, block=True)


def test_simple_usage(ray_start_regular_shared):

    q = Queue()

    items = list(range(10))

    for item in items:
        q.put(item)

    for item in items:
        assert item == q.get()


def test_get(ray_start_regular_shared):

    q = Queue()

    item = 0
    q.put(item)
    assert q.get(block=False) == item

    item = 1
    q.put(item)
    assert q.get(timeout=0.2) == item

    with pytest.raises(ValueError):
        q.get(timeout=-1)

    with pytest.raises(Empty):
        q.get_nowait()

    with pytest.raises(Empty):
        q.get(timeout=0.2)


@pytest.mark.asyncio
async def test_get_async(ray_start_regular_shared):

    q = Queue()

    item = 0
    await q.put_async(item)
    assert await q.get_async(block=False) == item

    item = 1
    await q.put_async(item)
    assert await q.get_async(timeout=0.2) == item

    with pytest.raises(ValueError):
        await q.get_async(timeout=-1)

    with pytest.raises(Empty):
        await q.get_async(block=False)

    with pytest.raises(Empty):
        await q.get_async(timeout=0.2)


def test_put(ray_start_regular_shared):

    q = Queue(1)

    item = 0
    q.put(item, block=False)
    assert q.get() == item

    item = 1
    q.put(item, timeout=0.2)
    assert q.get() == item

    with pytest.raises(ValueError):
        q.put(0, timeout=-1)

    q.put(0)
    with pytest.raises(Full):
        q.put_nowait(1)

    with pytest.raises(Full):
        q.put(1, timeout=0.2)


@pytest.mark.asyncio
async def test_put_async(ray_start_regular_shared):

    q = Queue(1)

    item = 0
    await q.put_async(item, block=False)
    assert await q.get_async() == item

    item = 1
    await q.put_async(item, timeout=0.2)
    assert await q.get_async() == item

    with pytest.raises(ValueError):
        await q.put_async(0, timeout=-1)

    await q.put_async(0)
    with pytest.raises(Full):
        await q.put_async(1, block=False)

    with pytest.raises(Full):
        await q.put_async(1, timeout=0.2)


def test_concurrent_get(ray_start_regular_shared):
    q = Queue()
    future = async_get.remote(q)

    with pytest.raises(Empty):
        q.get_nowait()

    with pytest.raises(GetTimeoutError):
        ray.get(future, timeout=0.1)  # task not canceled on timeout.

    q.put(1)
    assert ray.get(future) == 1


def test_concurrent_put(ray_start_regular_shared):
    q = Queue(1)
    q.put(1)
    future = async_put.remote(q, 2)

    with pytest.raises(Full):
        q.put_nowait(3)

    with pytest.raises(GetTimeoutError):
        ray.get(future, timeout=0.1)  # task not canceled on timeout.

    assert q.get() == 1
    assert q.get() == 2


def test_batch(ray_start_regular_shared):
    q = Queue(1)

    with pytest.raises(Full):
        q.put_nowait_batch([1, 2])

    with pytest.raises(Empty):
        q.get_nowait_batch(1)

    big_q = Queue(100)
    big_q.put_nowait_batch(list(range(100)))
    assert big_q.get_nowait_batch(100) == list(range(100))


def test_qsize(ray_start_regular_shared):

    q = Queue()

    items = list(range(10))
    size = 0

    assert q.qsize() == size

    for item in items:
        q.put(item)
        size += 1
        assert q.qsize() == size

    for item in items:
        assert q.get() == item
        size -= 1
        assert q.qsize() == size


def test_shutdown(ray_start_regular_shared):
    q = Queue()
    actor = q.actor
    q.shutdown()
    assert q.actor is None
    with pytest.raises(RayActorError):
        ray.get(actor.empty.remote())


def test_custom_resources(ray_start_regular_shared):
    current_resources = ray.available_resources()
    assert current_resources["CPU"] == 1.0

    # By default an actor should not reserve any resources.
    q = Queue()
    current_resources = ray.available_resources()
    assert current_resources["CPU"] == 1.0
    q.shutdown()

    # Specify resource requirement. The queue should now reserve 1 CPU.
    q = Queue(actor_options={"num_cpus": 1})

    def no_cpu_in_resources():
        return "CPU" not in ray.available_resources()

    wait_for_condition(no_cpu_in_resources)
    q.shutdown()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))

import pytest

import ray
from ray.exceptions import GetTimeoutError
from ray.util.queue import Queue, Empty, Full


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


def test_async_get(ray_start_regular_shared):
    q = Queue()
    future = async_get.remote(q)

    with pytest.raises(Empty):
        q.get_nowait()

    with pytest.raises(GetTimeoutError):
        ray.get(future, timeout=0.1)  # task not canceled on timeout.

    q.put(1)
    assert ray.get(future) == 1


def test_async_put(ray_start_regular_shared):
    q = Queue(1)
    q.put(1)
    future = async_put.remote(q, 2)

    with pytest.raises(Full):
        q.put_nowait(3)

    with pytest.raises(GetTimeoutError):
        ray.get(future, timeout=0.1)  # task not canceled on timeout.

    assert q.get() == 1
    assert q.get() == 2


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


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))

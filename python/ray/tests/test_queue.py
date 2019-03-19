from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import time

import ray
from ray.experimental.queue import Queue, Empty, Full


def test_queue(ray_start_regular):
    @ray.remote
    def get_async(queue, block, timeout, sleep):
        time.sleep(sleep)
        return queue.get(block, timeout)

    @ray.remote
    def put_async(queue, item, block, timeout, sleep):
        time.sleep(sleep)
        queue.put(item, block, timeout)

    # Test simple usage.

    q = Queue()

    items = list(range(10))

    for item in items:
        q.put(item)

    for item in items:
        assert item == q.get()

    # Test asynchronous usage.

    q = Queue()

    items = set(range(10))
    producers = [  # noqa
        put_async.remote(q, item, True, None, 0.5) for item in items
    ]
    consumers = [get_async.remote(q, True, None, 0) for _ in items]

    result = set(ray.get(consumers))

    assert items == result

    # Test put.

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

    q.get()
    q.put(1)

    get_id = get_async.remote(q, False, None, 0.2)
    q.put(2)

    assert ray.get(get_id) == 1

    # Test get.

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

    item = 0
    put_async.remote(q, item, True, None, 0.2)
    assert q.get() == item

    # Test qsize.

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

import sys
import time

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import BatchQueue
from ray.exceptions import GetTimeoutError, RayActorError
from ray.util.queue import Empty, Full, Queue


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


def test_pull_from_streaming_batch_queue(ray_start_regular_shared):
    class QueueBatchPuller:
        def __init__(self, batch_size, queue):
            self.batch_size = batch_size
            self.queue = queue

        def __iter__(self):
            pending = []
            is_done = False
            while True:
                if not pending:
                    for item in self.queue.get_batch(self.batch_size, total_timeout=0):
                        if item is None:
                            is_done = True
                            break
                        else:
                            pending.append(item)
                    if is_done:
                        break
                ready, pending = ray.wait(pending, num_returns=1)
                yield ray.get(ready[0])

    @ray.remote
    class QueueConsumer:
        def __init__(self, batch_size, queue):
            self.batch_puller = QueueBatchPuller(batch_size, queue)
            self.data = []

        def consume(self):
            for item in self.batch_puller:
                self.data.append(item)
                time.sleep(0.3)

        def get_data(self):
            return self.data

    @ray.remote
    def dummy(x):
        return x

    q = BatchQueue()
    num_batches = 5
    batch_size = 4
    consumer = QueueConsumer.remote(batch_size, q)
    consumer.consume.remote()
    data = list(range(batch_size * num_batches))
    for idx in range(0, len(data), batch_size):
        time.sleep(1)
        q.put_nowait_batch(
            [dummy.remote(item) for item in data[idx : idx + batch_size]]
        )
    q.put_nowait(None)
    consumed_data = ray.get(consumer.get_data.remote())
    assert len(consumed_data) == len(data)
    assert set(consumed_data) == set(data)


def test_len(ray_start_regular_shared):
    q = Queue()
    assert len(q) == 0
    q.put(1)
    assert len(q) == 1
    q.put(2)
    assert len(q) == 2
    q.get()
    assert len(q) == 1
    q.shutdown()


def test_empty_and_full(ray_start_regular_shared):
    q = Queue(2)
    assert q.empty() is True
    assert q.full() is False
    q.put(1)
    assert q.empty() is False
    assert q.full() is False
    q.put(2)
    assert q.full() is True
    q.get()
    assert q.full() is False
    q.shutdown()


def test_clear(ray_start_regular_shared):
    q = Queue()
    for i in range(10):
        q.put(i)
    assert q.qsize() == 10
    q.clear()
    assert q.qsize() == 0
    assert q.empty() is True
    # Ensure queue still works after clear
    q.put(42)
    assert q.get() == 42
    q.shutdown()


def test_clear_empty_queue(ray_start_regular_shared):
    q = Queue()
    # Should not raise on empty queue
    q.clear()
    assert q.qsize() == 0
    q.shutdown()


def test_peek(ray_start_regular_shared):
    q = Queue()
    q.put("hello")
    # peek should return item without removing it
    assert q.peek() == "hello"
    assert q.qsize() == 1
    assert q.peek() == "hello"
    # get should still return the same item
    assert q.get() == "hello"
    q.shutdown()


def test_peek_empty(ray_start_regular_shared):
    q = Queue()
    with pytest.raises(Empty):
        q.peek()
    q.shutdown()


@pytest.mark.asyncio
async def test_peek_async(ray_start_regular_shared):
    q = Queue()
    await q.put_async("world")
    assert await q.peek_async() == "world"
    assert q.qsize() == 1
    q.shutdown()


@pytest.mark.asyncio
async def test_peek_async_empty(ray_start_regular_shared):
    q = Queue()
    with pytest.raises(Empty):
        await q.peek_async()
    q.shutdown()


def test_init_invalid_maxsize(ray_start_regular_shared):
    with pytest.raises(TypeError):
        Queue(maxsize="invalid")
    with pytest.raises(ValueError):
        Queue(maxsize=-1)


def test_init_invalid_actor_options(ray_start_regular_shared):
    with pytest.raises(TypeError):
        Queue(actor_options="not_a_dict")
    with pytest.raises(TypeError):
        Queue(actor_options=[1, 2, 3])


def test_get_nowait_batch_invalid_args(ray_start_regular_shared):
    q = Queue()
    with pytest.raises(TypeError):
        q.get_nowait_batch("not_an_int")
    with pytest.raises(ValueError):
        q.get_nowait_batch(-1)
    q.shutdown()


def test_put_nowait_batch_non_iterable(ray_start_regular_shared):
    q = Queue()
    with pytest.raises(TypeError):
        q.put_nowait_batch(42)
    q.shutdown()


def test_shutdown_force(ray_start_regular_shared):
    q = Queue()
    actor = q.actor
    q.shutdown(force=True)
    assert q.actor is None
    with pytest.raises(RayActorError):
        ray.get(actor.empty.remote())


def test_shutdown_idempotent(ray_start_regular_shared):
    q = Queue()
    q.shutdown()
    # Calling shutdown again should not raise
    q.shutdown()
    assert q.actor is None


def test_put_get_complex_objects(ray_start_regular_shared):
    q = Queue()
    # Test with various Python types
    test_items = [
        None,
        True,
        3.14,
        "string",
        [1, 2, 3],
        {"key": "value"},
        (1, "two", 3.0),
        set(),
    ]
    for item in test_items:
        q.put(item)
    for item in test_items:
        result = q.get()
        assert result == item
    q.shutdown()


def test_batch_operations_ordering(ray_start_regular_shared):
    q = Queue(100)
    items = list(range(50))
    q.put_nowait_batch(items)
    result = q.get_nowait_batch(50)
    assert result == items
    q.shutdown()


def test_get_nowait_batch_zero(ray_start_regular_shared):
    q = Queue()
    q.put(1)
    result = q.get_nowait_batch(0)
    assert result == []
    # Item should still be in queue
    assert q.qsize() == 1
    q.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))

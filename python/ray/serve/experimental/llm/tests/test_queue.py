import pytest
import time
from threading import Thread

from ray.serve.experimental.llm.queue import RequestQueue


def test_request_queue():
    queue = RequestQueue()
    assert queue.peek() is None

    for i in range(10):
        assert len(queue._queue) == i
        queue.push(i)
        assert queue.peek() == 0

    for i in range(10):
        assert len(queue._queue) == 10 - i
        assert queue.peek() == i
        assert queue.pop() == i

    for i in range(10):
        assert len(queue._queue) == i
        queue.reverse_push(i)
        assert queue.peek() == i

    for i in range(10):
        assert len(queue._queue) == 10 - i
        assert queue.peek() == 9 - i
        assert queue.pop() == 9 - i


def test_wait_queue():
    queue = RequestQueue()

    start = time.time()
    queue.wait(timeout=0.5)
    elapsed = time.time() - start
    assert elapsed >= 0.4
    assert queue.empty()

    def run(queue):
        time.sleep(0.5)
        queue.push(0)

    t = Thread(target=run, args=(queue,))
    t.start()

    queue.wait()
    assert queue.peek() == 0
    assert len(queue._queue) == 1

    t.join()

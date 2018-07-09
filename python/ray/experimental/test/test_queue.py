from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
import unittest

import ray

from ray.experimental.queue import Queue, Empty, Full


@ray.remote
def get_async(queue, block, timeout, sleep):
    time.sleep(sleep)
    return queue.get(block, timeout)


@ray.remote
def put_async(queue, item, block, timeout, sleep):
    time.sleep(sleep)
    queue.put(item, block, timeout)


class TestQueue(unittest.TestCase):
    def test_simple_use(self):
        q = Queue()

        items = list(range(10))

        for item in items:
            q.put(item)

        for item in items:
            self.assertEqual(item, q.get())

    def test_async(self):
        q = Queue()

        items = set(range(10))
        producers = [   # noqa
                put_async.remote(q, item, True, None, 0.5)
                for item in items
        ]
        consumers = [get_async.remote(q, True, None, 0) for _ in items]

        result = set(ray.get(consumers))

        self.assertEqual(items, result)

    def test_put(self):
        q = Queue(1)

        item = 0
        q.put(item, block=False)
        self.assertEqual(q.get(), item)

        item = 1
        q.put(item, timeout=0.2)
        self.assertEqual(q.get(), item)

        with self.assertRaises(ValueError):
            q.put(0, timeout=-1)

        q.put(0)
        with self.assertRaises(Full):
            q.put_nowait(1)

        with self.assertRaises(Full):
            q.put(1, timeout=0.2)

        q.get()
        q.put(1)

        get_id = get_async.remote(q, False, None, 0.2)
        q.put(2)

        self.assertEqual(ray.get(get_id), 1)

    def test_get(self):
        q = Queue()

        item = 0
        q.put(item)
        self.assertEqual(q.get(block=False), item)

        item = 1
        q.put(item)
        self.assertEqual(q.get(timeout=0.2), item)

        with self.assertRaises(ValueError):
            q.get(timeout=-1)

        with self.assertRaises(Empty):
            q.get_nowait()

        with self.assertRaises(Empty):
            q.get(timeout=0.2)

        item = 0
        put_async.remote(q, item, True, None, 0.2)
        self.assertEqual(q.get(), item)

    def test_qsize(self):
        q = Queue()

        items = list(range(10))
        size = 0

        self.assertEqual(q.qsize(), size)

        for item in items:
            q.put(item)
            size += 1
            self.assertEqual(q.qsize(), size)

        for item in items:
            self.assertEqual(q.get(), item)
            size -= 1
            self.assertEqual(q.qsize(), size)


if __name__ == "__main__":
    ray.init()
    unittest.main()

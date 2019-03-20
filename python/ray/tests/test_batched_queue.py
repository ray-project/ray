from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import ray
from ray.experimental.streaming.batched_queue import BatchedQueue


@ray.remote
class Reader(object):
    def __init__(self, queue):
        self.queue = queue
        self.num_reads = 0
        self.start = time.time()

    def read(self, read_slowly):
        expected_value = 0
        for _ in range(1000):
            x = self.queue.read_next()
            assert x == expected_value, (x, expected_value)
            expected_value += 1
            self.num_reads += 1
            if read_slowly:
                time.sleep(0.001)


def test_batched_queue(ray_start_regular):
    # Batched queue parameters
    max_queue_size = 10000  # Max number of batches in queue
    max_batch_size = 1000  # Max number of elements per batch
    batch_timeout = 0.001  # 1ms flush timeout
    prefetch_depth = 10  # Number of batches to prefetch from plasma
    background_flush = False  # Don't use daemon thread for flushing
    # Two tests: one with a big queue and slow reader, and
    # a second one with a small queue and a faster reader
    for read_slowly in [True, False]:
        # Construct the batched queue
        queue = BatchedQueue(
            max_size=max_queue_size,
            max_batch_size=max_batch_size,
            max_batch_time=batch_timeout,
            prefetch_depth=prefetch_depth,
            background_flush=background_flush)
        # Create and start the reader
        reader = Reader.remote(queue)
        object_id = reader.read.remote(read_slowly=read_slowly)
        value = 0
        for _ in range(1000):
            queue.put_next(value)
            value += 1
        queue._flush_writes()
        ray.get(object_id)
        # Test once more with a very small queue size and a faster reader
        max_queue_size = 10

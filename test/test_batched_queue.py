from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import time

import ray
from ray.experimental.streaming.batched_queue import BatchedQueue

@ray.remote
class Reader(object):
    def __init__(self, queue, max_reads_per_second=float("inf")):
        self.queue = queue
        self.max_reads_per_second = max_reads_per_second
        self.num_reads = 0
        self.start = time.time()

    def read_forever(self):
        expected_value = 0
        while True:
            N = 100000
            for _ in range(N):
                x = self.queue.read_next()
                assert x == expected_value, (x, expected_value)
                expected_value += 1
                self.num_reads += 1
                while (self.num_reads / (time.time() - self.start) >
                       self.max_reads_per_second):
                    time.sleep(0.1)

def test_batched_queue():
    # Batched queue parameters
    max_queue_size = 10000  # Max number of batches in queue
    max_batch_size = 1000  # Max number of elements per batch
    batch_timeout = 0.001  # 1ms flush timeout
    prefetch_depth = 10  # Number of batches to prefetch from plasma
    background_flush = False  # Don't use daemon thread for flushing
    max_reads_per_second = float("inf")

    ray.init()
    ray.register_custom_serializer(BatchedQueue, use_pickle=True)

    for _ in range(2):
        # Construct the batched queue
        queue = BatchedQueue(
            max_size=max_queue_size,
            max_batch_size=max_batch_size,
            max_batch_time=batch_timeout,
            prefetch_depth=prefetch_depth,
            background_flush=background_flush)
        # Create and start the reader
        reader = Reader.remote(queue, max_reads_per_second)
        reader.read_forever.remote()
        value = 0
        for _ in range(5):
            N = 100000
            for _ in range(N):
                queue.put_next(value)
                value += 1
        queue._flush_writes()
        # Test once more with backpressure
        max_reads_per_second = 50000  # Max throughput
    ray.shutdown()

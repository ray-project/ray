from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time

import ray
from ray.experimental.slib.batched_queue import BatchedQueue

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

@ray.remote
class Reader(object):
    def __init__(self, queue, queue2, max_reads_per_second=999999):
        self.queue = queue
        self.max_reads_per_second = max_reads_per_second
        self.num_reads = 0
        self.num_writes = 0
        self.start = time.time()
        self.out_queue = queue2

    def read_forever(self):
        expected_value = 0
        while True:
            start = time.time()
            N = 100000
            for _ in range(N):
                x = self.queue.read_next()
                assert x == expected_value, (x, expected_value)
                expected_value += 1
                self.num_reads += 1
                while (self.num_reads / (time.time() - self.start) >
                       self.max_reads_per_second):
                    logger.debug(
                        "[reader] Reads throttled to {} reads/s".format(
                            self.max_reads_per_second))
                    time.sleep(0.1)
            logger.info("[reader] Reads per second {}".format(
                N / (time.time() - start)))

    def get_queue(self):
        return self.out_queue

    def read_write_forever(self):
        if self.out_queue:
            self.enable_writes()
        expected_value = 0
        while True:
            start = time.time()
            for _ in range(10):
                N = 100000
                for _ in range(N):
                    x = self.queue.read_next()
                    assert x == expected_value, (x, expected_value)
                    expected_value += 1
                    self.num_reads += 1
                    if self.out_queue:
                        self.out_queue.put_next(x)
                    self.num_writes += 1
                    while (self.num_reads / (time.time() - self.start) >
                           self.max_reads_per_second):
                        logger.debug(
                            "[reader] Reads throttled to {} reads/s".format(
                                self.max_reads_per_second))
                        time.sleep(0.1)
                logger.info("[reader] Reads per second {}".format(
                    N / (time.time() - start)))
            queue._flush_writes()

def test_max_throughput():

    queue = BatchedQueue(
        max_size=10000,
        max_batch_size=10000,
        max_batch_time=0.001,
        prefetch_depth=10,
        background_flush=False)

    queue2 = BatchedQueue(
                max_size=10000,
                max_batch_size=10000,
                max_batch_time=0.001,
                prefetch_depth=10,
                background_flush=False)

    reader = Reader.remote(queue, queue2,max_reads_per_second=float("inf"))
    reader.read_forever.remote()

    value = 0
    for _ in range(10):
        N = 100000
        start = time.time()
        for i in range(N):
            queue.put_next(value)
            value += 1
        logger.info("[writer] Puts per second {}".format(
            N / (time.time() - start)))
    queue._flush_writes()


def test_backpressure():
    queue = BatchedQueue(
        max_size=10000,
        max_batch_size=1000,
        max_batch_time=0.001,
        prefetch_depth=10,
        background_flush=False)
    reader = Reader.remote(queue, None, max_reads_per_second=50000)
    reader.read_forever.remote()
    value = 0
    for _ in range(5):
        N = 100000
        start = time.time()
        for i in range(N):
            queue.put_next(value)
            value += 1
        logger.info("[writer] Puts per second {}".format(
            N / (time.time() - start)))
    queue._flush_writes()


if __name__ == "__main__":
    ray.init()
    ray.register_custom_serializer(BatchedQueue, use_pickle=True)

    print("== Testing max throughput ==")
    test_max_throughput()
    time.sleep(1)

    print("== Testing backpressure ==")
    test_backpressure()

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import time

import ray
from ray.experimental.streaming.batched_queue import BatchedQueue

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--rounds", default=10, help="the number of experiment rounds")
parser.add_argument(
    "--num-queues", default=1, help="the number of queues in the chain")
parser.add_argument(
    "--queue-size", default=10000, help="the queue size in number of batches")
parser.add_argument(
    "--batch-size", default=1000, help="the batch size in number of elements")
parser.add_argument(
    "--flush-timeout", default=0.001, help="the timeout to flush a batch")
parser.add_argument(
    "--prefetch-depth",
    default=10,
    help="the number of batches to prefetch from plasma")
parser.add_argument(
    "--background-flush",
    default=False,
    help="whether to flush in the backrgound or not")
parser.add_argument(
    "--max-throughput",
    default="inf",
    help="maximum read throughput (elements/s)")


@ray.remote
class Node(object):
    """An actor that reads from an input queue and writes to an output queue.

    Attributes:
        id (int): The id of the actor.
        queue (BatchedQueue): The input queue.
        out_queue (BatchedQueue): The output queue.
        max_reads_per_second (int): The max read throughput (default: inf).
        num_reads (int): Number of elements read.
        num_writes (int): Number of elements written.
    """

    def __init__(self,
                 id,
                 in_queue,
                 out_queue,
                 max_reads_per_second=float("inf")):
        self.id = id
        self.queue = in_queue
        self.out_queue = out_queue
        self.max_reads_per_second = max_reads_per_second
        self.num_reads = 0
        self.num_writes = 0
        self.start = time.time()

    def read_write_forever(self):
        debug_log = "[actor {}] Reads throttled to {} reads/s"
        log = ""
        if self.out_queue is not None:
            self.out_queue.enable_writes()
            log += "[actor {}] Reads/Writes per second {}"
        else:  # It's just a reader
            log += "[actor {}] Reads per second {}"
        # Start spinning
        expected_value = 0
        while True:
            start = time.time()
            N = 100000
            for _ in range(N):
                x = self.queue.read_next()
                assert x == expected_value, (x, expected_value)
                expected_value += 1
                self.num_reads += 1
                if self.out_queue is not None:
                    self.out_queue.put_next(x)
                    self.num_writes += 1
                while (self.num_reads / (time.time() - self.start) >
                       self.max_reads_per_second):
                    logger.debug(
                        debug_log.format(self.id, self.max_reads_per_second))
                    time.sleep(0.1)
            logger.info(log.format(self.id, N / (time.time() - start)))
            # Flush any remaining elements
            if self.out_queue is not None:
                self.out_queue._flush_writes()


def test_max_throughput(rounds,
                        max_queue_size,
                        max_batch_size,
                        batch_timeout,
                        prefetch_depth,
                        background_flush,
                        num_queues,
                        max_reads_per_second=float("inf")):
    assert num_queues >= 1
    first_queue = BatchedQueue(
        max_size=max_queue_size,
        max_batch_size=max_batch_size,
        max_batch_time=batch_timeout,
        prefetch_depth=prefetch_depth,
        background_flush=background_flush)
    previous_queue = first_queue
    for i in range(num_queues):
        # Construct the batched queue
        in_queue = previous_queue
        out_queue = None
        if i < num_queues - 1:
            out_queue = BatchedQueue(
                max_size=max_queue_size,
                max_batch_size=max_batch_size,
                max_batch_time=batch_timeout,
                prefetch_depth=prefetch_depth,
                background_flush=background_flush)

        node = Node.remote(i, in_queue, out_queue, max_reads_per_second)
        node.read_write_forever.remote()
        previous_queue = out_queue

    value = 0
    # Feed the chain
    for round in range(rounds):
        logger.info("Round {}".format(round))
        N = 100000
        start = time.time()
        for i in range(N):
            first_queue.put_next(value)
            value += 1
        log = "[writer] Puts per second {}"
        logger.info(log.format(N / (time.time() - start)))
    first_queue._flush_writes()


if __name__ == "__main__":
    ray.init()
    ray.register_custom_serializer(BatchedQueue, use_pickle=True)

    args = parser.parse_args()

    rounds = int(args.rounds)
    max_queue_size = int(args.queue_size)
    max_batch_size = int(args.batch_size)
    batch_timeout = float(args.flush_timeout)
    prefetch_depth = int(args.prefetch_depth)
    background_flush = bool(args.background_flush)
    num_queues = int(args.num_queues)
    max_reads_per_second = float(args.max_throughput)

    logger.info("== Parameters ==")
    logger.info("Rounds: {}".format(rounds))
    logger.info("Max queue size: {}".format(max_queue_size))
    logger.info("Max batch size: {}".format(max_batch_size))
    logger.info("Batch timeout: {}".format(batch_timeout))
    logger.info("Prefetch depth: {}".format(prefetch_depth))
    logger.info("Background flush: {}".format(background_flush))
    logger.info("Max read throughput: {}".format(max_reads_per_second))

    # Estimate the ideal throughput
    value = 0
    start = time.time()
    for round in range(rounds):
        N = 100000
        for _ in range(N):
            value += 1
    logger.info("Ideal throughput: {}".format(value / (time.time() - start)))

    logger.info("== Testing max throughput ==")
    start = time.time()
    test_max_throughput(rounds, max_queue_size, max_batch_size, batch_timeout,
                        prefetch_depth, background_flush, num_queues,
                        max_reads_per_second)
    logger.info("Elapsed time: {}".format(time.time() - start))

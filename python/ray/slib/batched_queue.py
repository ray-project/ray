from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import numpy as np
import threading
import time

import ray
from ray.experimental import internal_kv    # What's the internal_kv?

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

def plasma_prefetch(oid):
    """Tells plasma to prefetch the given oid."""
    # if ray.global_state.use_raylet: # What's reaylet and when is it used?
    local_sched_client = ray.worker.global_worker.raylet_client
    ray_obj_id = ray.ObjectID(oid)
    local_sched_client.fetch_or_reconstruct([ray_obj_id], True)
    # else:
    #     plasma_id = ray.pyarrow.plasma.ObjectID(oid)
    #     ray.worker.global_worker.plasma_client.fetch([plasma_id])


def plasma_get(oid):
    """Get an object directly from plasma without going through object table.

    Precondition: plasma_prefetch(oid) has been called before."""
    client = ray.worker.global_worker.plasma_client
    pid = ray.pyarrow.plasma.ObjectID(oid)
    while not client.contains(pid):
        pass
    return client.get(pid)


# TODO: doing the timer in Python land is a bit slow
class FlushThread(threading.Thread):
    def __init__(self, interval, flush_fn):
        threading.Thread.__init__(self)
        self.interval = interval        # Interval is the max_batch_time
        self.flush_fn = flush_fn
        self.daemon = True              # ?

    def run(self):
        while True:
            time.sleep(self.interval)   # Flushing period
            self.flush_fn()


class BatchedQueue(object):
    def __init__(self,
                 max_size=999999,           # Max queue size in number of batches
                 max_batch_size=99999,      # Max batch size in number of elements
                 max_batch_time=0.01,       # Is this a timeout for writing a batch to plasma?
                 prefetch_depth=10,
                 background_flush=True):    # ??
        self.max_size = max_size
        self.max_batch_size = max_batch_size
        self.max_batch_time = max_batch_time
        self.prefetch_depth = prefetch_depth    # Number of consecutive batches to prefetch
        self.background_flush = background_flush

        # Common queue metadata -- This serves as the unique id of the queue
        self.base = np.random.randint(0, 2**32 - 1, size=5, dtype="uint32")
        self.base[-2] = 0
        self.base[-1] = 0
        self.read_ack_key = np.ndarray.tobytes(self.base)

        # Reader state
        self.prefetch_batch_offset = 0  # What's that?
        self.read_item_offset = 0
        self.read_batch_offset = 0
        self.read_buffer = []

        # Writer state
        self.write_item_offset = 0
        self.write_batch_offset = 0
        self.write_buffer = []
        self.last_flush_time = 0.0
        self.cached_remote_offset = 0

        self.flush_lock = threading.RLock()
        self.flush_thread = FlushThread(self.max_batch_time,
                                    self._flush_writes)
    def __getstate__(self):
        state = dict(self.__dict__)
        del state["flush_lock"]
        del state["flush_thread"]
        del state["write_buffer"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

    # This is to enable writing functionality in case the queue is not created by the writer
    # The reason is that python locks cannot be serialized
    def enable_writes(self):
        self.write_buffer = []
        self.flush_lock = threading.RLock()
        self.flush_thread = FlushThread(self.max_batch_time,
                                    self._flush_writes)

    # Batch ids consist of a unique queue id used as prefix along with
    # two numbers generated using the batch offset in the queue
    def _batch_id(self, batch_offset):
        oid = self.base.copy()
        oid[-2] = batch_offset // 2**32 # ?
        oid[-1] = batch_offset % 2**32  # ?
        return np.ndarray.tobytes(oid)

    def _flush_writes(self):
        with self.flush_lock:
            if not self.write_buffer:
                return
            batch_id = self._batch_id(self.write_batch_offset)
            ray.worker.global_worker.put_object(
                ray.ObjectID(batch_id), self.write_buffer)
            logger.debug("[writer] Flush batch {} offset {} size {}".format(
                self.write_batch_offset, self.write_item_offset,
                len(self.write_buffer)))
            self.write_buffer = []
            self.write_batch_offset += 1
            self._wait_for_reader()             # Check for backpressure
            self.last_flush_time = time.time()

    def _wait_for_reader(self):
        if self.max_size <= 0:  # Unlimited queue
            return
        if self.write_item_offset - self.cached_remote_offset <= self.max_size: # Haven't reached max size
            return
        remote_offset = internal_kv._internal_kv_get(self.read_ack_key)
        if remote_offset is None:
            logger.debug("[writer] Waiting for reader to start...")
            while remote_offset is None:
                time.sleep(0.01)
                remote_offset = internal_kv._internal_kv_get(self.read_ack_key)
        remote_offset = int(remote_offset)
        if self.write_item_offset - remote_offset > self.max_size:  # Backpressure?
            logger.debug(
                "[writer] Waiting for reader to catch up {} to {} - {}".format(
                    remote_offset, self.write_item_offset, self.max_size))
            while self.write_item_offset - remote_offset > self.max_size:
                time.sleep(0.01)
                remote_offset = int(
                    internal_kv._internal_kv_get(self.read_ack_key))
        self.cached_remote_offset = remote_offset

    def _read_next_batch(self):
        while (self.prefetch_batch_offset <
               self.read_batch_offset + self.prefetch_depth):
            plasma_prefetch(self._batch_id(self.prefetch_batch_offset))
            self.prefetch_batch_offset += 1
        self.read_buffer = plasma_get(self._batch_id(self.read_batch_offset))
        self.read_batch_offset += 1
        logger.debug("[reader] Fetched batch {} offset {} size {}".format(
            self.read_batch_offset, self.read_item_offset,
            len(self.read_buffer)))
        self._ack_reads(self.read_item_offset + len(self.read_buffer))

    # Reader acks the key it reads so that writer knows reader's offset.
    # This is to cap queue size and simulate backpressure
    def _ack_reads(self, offset):
        if self.max_size > 0:
            internal_kv._internal_kv_put(
                self.read_ack_key, offset, overwrite=True)

    def put_next(self, item):
        with self.flush_lock:
            if self.background_flush and not self.flush_thread.is_alive():
                logger.debug("[writer] Starting batch flush thread")
                self.flush_thread.start()
            self.write_buffer.append(item)
            self.write_item_offset += 1
            if not self.last_flush_time:
                self.last_flush_time = time.time()
            delay = time.time() - self.last_flush_time
            if (len(self.write_buffer) > self.max_batch_size
                    or delay > self.max_batch_time):
                self._flush_writes()

    def read_next(self):
        if not self.read_buffer:
            self._read_next_batch()
            assert self.read_buffer
        self.read_item_offset += 1
        return self.read_buffer.pop(0)


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

    # reader = Reader.remote(queue, queue2,max_reads_per_second=float("inf"))
    # reader.read_forever.remote()

    # FIXME (john): the following blocks
    reader1 = Reader.remote(queue, queue2,max_reads_per_second=float("inf"))
    reader1.read_write_forever.remote()
    reader2 = Reader.remote(queue2, None, max_reads_per_second=float("inf"))
    reader2.read_write_forever.remote()

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

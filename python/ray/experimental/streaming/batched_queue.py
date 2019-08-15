from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import numpy as np
import threading
import time

import ray
from ray.experimental import internal_kv

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def plasma_prefetch(object_id):
    """Tells plasma to prefetch the given object_id."""
    local_sched_client = ray.worker.global_worker.raylet_client
    ray_obj_id = ray.ObjectID(object_id)
    local_sched_client.fetch_or_reconstruct([ray_obj_id], True)


def plasma_get(object_id):
    """Get an object directly from plasma without going through object table.

    Precondition: plasma_prefetch(object_id) has been called before.
    """
    client = ray.worker.global_worker.plasma_client
    plasma_id = ray.pyarrow.plasma.ObjectID(object_id)
    while not client.contains(plasma_id):
        pass
    return client.get(plasma_id)


# TODO: doing the timer in Python land is a bit slow
class FlushThread(threading.Thread):
    """A thread that flushes periodically to plasma.

    Attributes:
         interval: The flush timeout per batch.
         flush_fn: The flush function.
    """

    def __init__(self, interval, flush_fn):
        threading.Thread.__init__(self)
        self.interval = interval  # Interval is the max_batch_time
        self.flush_fn = flush_fn
        self.daemon = True

    def run(self):
        while True:
            time.sleep(self.interval)  # Flushing period
            self.flush_fn()


class BatchedQueue(object):
    """A batched queue for actor to actor communication.

    Attributes:
         max_size (int): The maximum size of the queue in number of batches
         (if exceeded, backpressure kicks in)
         max_batch_size (int): The size of each batch in number of records.
         max_batch_time (float): The flush timeout per batch.
         prefetch_depth (int): The  number of batches to prefetch from plasma.
         background_flush (bool): Denotes whether a daemon flush thread should
         be used (True) to flush batches to plasma.
         base (ndarray): A unique signature for the queue.
         read_ack_key (bytes): The signature of the queue in bytes.
         prefetch_batch_offset (int): The number of the last read prefetched
         batch.
         read_batch_offset (int): The number of the last read batch.
         read_item_offset (int): The number of the last read record inside a
         batch.
         write_batch_offset (int): The number of the last written batch.
         write_item_offset (int): The numebr of the last written item inside a
         batch.
         write_buffer (list): The write buffer, i.e. an in-memory batch.
         last_flush_time (float): The time the last flushing to plasma took
         place.
         cached_remote_offset (int): The number of the last read batch as
         recorded by the writer after the previous flush.
         flush_lock (RLock): A python lock used for flushing batches to plasma.
         flush_thread (Threading): The python thread used for flushing batches
         to plasma.
    """

    def __init__(self,
                 max_size=999999,
                 max_batch_size=99999,
                 max_batch_time=0.01,
                 prefetch_depth=10,
                 background_flush=True):
        self.max_size = max_size
        self.max_batch_size = max_batch_size
        self.max_batch_time = max_batch_time
        self.prefetch_depth = prefetch_depth
        self.background_flush = background_flush

        # Common queue metadata -- This serves as the unique id of the queue
        self.base = np.random.randint(0, 2**32 - 1, size=5, dtype="uint32")
        self.base[-2] = 0
        self.base[-1] = 0
        self.read_ack_key = np.ndarray.tobytes(self.base)

        # Reader state
        self.prefetch_batch_offset = 0
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

    # This is to enable writing functionality in
    # case the queue is not created by the writer
    # The reason is that python locks cannot be serialized
    def enable_writes(self):
        """Restores the state of the batched queue for writing."""
        self.write_buffer = []
        self.flush_lock = threading.RLock()
        self.flush_thread = FlushThread(self.max_batch_time,
                                        self._flush_writes)

    # Batch ids consist of a unique queue id used as prefix along with
    # two numbers generated using the batch offset in the queue
    def _batch_id(self, batch_offset):
        oid = self.base.copy()
        oid[-2] = batch_offset // 2**32
        oid[-1] = batch_offset % 2**32
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
            self._wait_for_reader()
            self.last_flush_time = time.time()

    def _wait_for_reader(self):
        """Checks for backpressure by the downstream reader."""
        if self.max_size <= 0:  # Unlimited queue
            return
        if self.write_item_offset - self.cached_remote_offset <= self.max_size:
            return  # Hasn't reached max size
        remote_offset = internal_kv._internal_kv_get(self.read_ack_key)
        if remote_offset is None:
            # logger.debug("[writer] Waiting for reader to start...")
            while remote_offset is None:
                time.sleep(0.01)
                remote_offset = internal_kv._internal_kv_get(self.read_ack_key)
        remote_offset = int(remote_offset)
        if self.write_item_offset - remote_offset > self.max_size:
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

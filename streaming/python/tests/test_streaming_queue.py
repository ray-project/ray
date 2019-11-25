from __future__ import print_function

import pickle
import threading
import time
from abc import ABCMeta

import ray
import ray.streaming.runtime.queue.queue_utils as queue_utils
from ray.function_manager import FunctionDescriptor
from ray.streaming.runtime.queue.queue_interface import QueueID
from ray.streaming.runtime.queue.streaming_queue import QueueLinkImpl


class Worker:
    __metaclass__ = ABCMeta

    def __init__(self):
        self.queue_link = None

    def on_streaming_transfer(self, buffer):
        """used in direct call mode"""
        self.queue_link.on_streaming_transfer(buffer.to_pybytes())

    def on_streaming_transfer_sync(self, buffer):
        """used in direct call mode"""
        if self.queue_link is None:
            return b' ' * 4  # special flag to indicate this actor not ready
        result = self.queue_link.on_streaming_transfer_sync(buffer.to_pybytes())
        return result


@ray.remote
class WriterWorker(Worker):
    def __init__(self):
        super().__init__()
        sync_func = FunctionDescriptor(
            __name__, self.on_streaming_transfer_sync.__name__, "ReaderWorker")
        async_func = FunctionDescriptor(
            __name__, self.on_streaming_transfer.__name__, "ReaderWorker")
        self.queue_link = QueueLinkImpl(sync_func, async_func)
        self.producer = None
        self.output_queue_id = None

    def init(self, output_queue, reader_actor):
        self.output_queue_id = QueueID(output_queue)
        self.producer = self.queue_link.register_queue_producer(
            [output_queue], [pickle.loads(reader_actor)])

    def start_write(self, msg_nums):
        self.t = threading.Thread(target=self.run, args=[msg_nums], daemon=True)
        self.t.start()

    def is_finished(self):
        return not self.t.is_alive()

    def run(self, msg_nums):
        for i in range(msg_nums):
            self.producer.produce(self.output_queue_id, pickle.dumps(i))
        print("WriterWorker done.")


@ray.remote
class ReaderWorker(Worker):
    def __init__(self):
        super().__init__()
        sync_func = FunctionDescriptor(
            __name__, self.on_streaming_transfer_sync.__name__, "WriterWorker")
        async_func = FunctionDescriptor(
            __name__, self.on_streaming_transfer.__name__, "WriterWorker")
        self.queue_link = QueueLinkImpl(sync_func, async_func)
        self.consumer = None

    def init(self, input_queue, writer_actor):
        self.consumer = self.queue_link.register_queue_consumer(
            [input_queue], [pickle.loads(writer_actor)])

    def start_read(self, msg_nums):
        self.t = threading.Thread(target=self.run, args=[msg_nums], daemon=True)
        self.t.start()

    def is_finished(self):
        return not self.t.is_alive()

    def run(self, msg_nums):
        count = 0
        msg = None
        while count != msg_nums:
            queue_item = self.consumer.pull(100)
            if queue_item is None:
                time.sleep(0.01)
            else:
                msg = pickle.loads(queue_item.body())
                count += 1
        assert msg == msg_nums - 1
        print("ReaderWorker done.")


def test_queue():
    writer = WriterWorker._remote(is_direct_call=True)
    reader = ReaderWorker._remote(is_direct_call=True)
    queue_str = queue_utils.gen_random_qid()
    inits = [writer.init.remote(queue_str, pickle.dumps(reader)),
             reader.init.remote(queue_str, pickle.dumps(writer))]
    ray.get(inits)
    msg_nums = 1000
    print("start read/write")
    reader.start_read.remote(msg_nums)
    writer.start_write.remote(msg_nums)
    while not ray.get(reader.is_finished.remote()):
        time.sleep(0.1)


if __name__ == "__main__":
    ray.init()
    test_queue()

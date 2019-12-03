from __future__ import print_function

import pickle
import threading
import time
from abc import ABCMeta

import ray
import ray.streaming._streaming as _streaming
from ray.streaming.config import Config
import ray.streaming.runtime.channel as channel
from ray.function_manager import FunctionDescriptor


class Worker:
    __metaclass__ = ABCMeta

    def __init__(self):
        self.writer_client = None
        self.reader_client = None

    def on_reader_message(self, buffer: ray._raylet.Buffer):
        """used in direct call mode"""
        self.reader_client.on_reader_message(buffer)

    def on_reader_message_sync(self, buffer: ray._raylet.Buffer):
        """used in direct call mode"""
        if self.reader_client is None:
            return b' ' * 4  # special flag to indicate this actor not ready
        result = self.reader_client.on_reader_message_sync(buffer)
        return result

    def on_writer_message(self, buffer: ray._raylet.Buffer):
        """used in direct call mode"""
        self.writer_client.on_writer_message(buffer)

    def on_writer_message_sync(self, buffer: ray._raylet.Buffer):
        """used in direct call mode"""
        if self.writer_client is None:
            return b' ' * 4  # special flag to indicate this actor not ready
        result = self.writer_client.on_writer_message_sync(buffer)
        return result


@ray.remote
class WriterWorker(Worker):
    def __init__(self):
        super().__init__()
        core_worker = ray.worker.global_worker.core_worker
        writer_async_func = FunctionDescriptor(__name__, self.on_writer_message.__name__,
                                               "WriterWorker")
        writer_sync_func = FunctionDescriptor(__name__, self.on_writer_message_sync.__name__,
                                              "WriterWorker")
        self.writer_client = _streaming.WriterClient(core_worker,
                                                     writer_async_func,
                                                     writer_sync_func)
        reader_async_func = FunctionDescriptor(__name__, self.on_reader_message.__name__,
                                               "ReaderWorker")
        reader_sync_func = FunctionDescriptor(__name__, self.on_reader_message_sync.__name__,
                                              "ReaderWorker")
        self.reader_client = _streaming.ReaderClient(core_worker,
                                                     reader_async_func,
                                                     reader_sync_func)
        self.writer = None
        self.output_channel_id = None

    def init(self, output_channel, reader_actor):
        conf = {
            Config.TASK_JOB_ID: ray.runtime_context._get_runtime_context().current_driver_id,
            Config.CHANNEL_TYPE: Config.NATIVE_CHANNEL
        }
        self.writer = channel.DataWriter([output_channel], [pickle.loads(reader_actor)], conf)
        self.output_channel_id = channel.ChannelID(output_channel)

    def start_write(self, msg_nums):
        self.t = threading.Thread(target=self.run, args=[msg_nums], daemon=True)
        self.t.start()

    def is_finished(self):
        return not self.t.is_alive()

    def run(self, msg_nums):
        for i in range(msg_nums):
            self.writer.write(self.output_channel_id, pickle.dumps(i))
        print("WriterWorker done.")


@ray.remote
class ReaderWorker(Worker):
    def __init__(self):
        super().__init__()
        core_worker = ray.worker.global_worker.core_worker
        writer_async_func = FunctionDescriptor(__name__, self.on_writer_message.__name__,
                                               "WriterWorker")
        writer_sync_func = FunctionDescriptor(__name__, self.on_writer_message_sync.__name__,
                                              "WriterWorker")
        self.writer_client = _streaming.WriterClient(core_worker,
                                                     writer_async_func,
                                                     writer_sync_func)
        reader_async_func = FunctionDescriptor(__name__, self.on_reader_message.__name__,
                                               "ReaderWorker")
        reader_sync_func = FunctionDescriptor(__name__, self.on_reader_message_sync.__name__,
                                              "ReaderWorker")
        self.reader_client = _streaming.ReaderClient(core_worker,
                                                     reader_async_func,
                                                     reader_sync_func)
        self.reader = None

    def init(self, input_channel, writer_actor):
        conf = {
            Config.TASK_JOB_ID: ray.runtime_context._get_runtime_context().current_driver_id,
            Config.CHANNEL_TYPE: Config.NATIVE_CHANNEL
        }
        self.reader = channel.DataReader([input_channel], [pickle.loads(writer_actor)], conf)

    def start_read(self, msg_nums):
        self.t = threading.Thread(target=self.run, args=[msg_nums], daemon=True)
        self.t.start()

    def is_finished(self):
        return not self.t.is_alive()

    def run(self, msg_nums):
        count = 0
        msg = None
        while count != msg_nums:
            item = self.reader.read(100)
            if item is None:
                time.sleep(0.01)
            else:
                msg = pickle.loads(item.body())
                count += 1
        assert msg == msg_nums - 1
        print("ReaderWorker done.")


def test_queue():
    writer = WriterWorker._remote(is_direct_call=True)
    reader = ReaderWorker._remote(is_direct_call=True)
    channel_id_str = channel.ChannelID.gen_random_id()
    inits = [writer.init.remote(channel_id_str, pickle.dumps(reader)),
             reader.init.remote(channel_id_str, pickle.dumps(writer))]
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

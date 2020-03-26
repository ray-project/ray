import pickle
import threading
import time

import ray
import ray.streaming._streaming as _streaming
import ray.streaming.runtime.transfer as transfer
from ray._raylet import PythonFunctionDescriptor
from ray.streaming.config import Config


@ray.remote
class Worker:
    def __init__(self):
        core_worker = ray.worker.global_worker.core_worker
        writer_async_func = PythonFunctionDescriptor(
            __name__, self.on_writer_message.__name__, self.__class__.__name__)
        writer_sync_func = PythonFunctionDescriptor(
            __name__, self.on_writer_message_sync.__name__,
            self.__class__.__name__)
        self.writer_client = _streaming.WriterClient(
            core_worker, writer_async_func, writer_sync_func)
        reader_async_func = PythonFunctionDescriptor(
            __name__, self.on_reader_message.__name__, self.__class__.__name__)
        reader_sync_func = PythonFunctionDescriptor(
            __name__, self.on_reader_message_sync.__name__,
            self.__class__.__name__)
        self.reader_client = _streaming.ReaderClient(
            core_worker, reader_async_func, reader_sync_func)
        self.writer = None
        self.output_channel_id = None
        self.reader = None

    def init_writer(self, output_channel, reader_actor):
        conf = {
            Config.TASK_JOB_ID: ray.worker.global_worker.current_job_id,
            Config.CHANNEL_TYPE: Config.NATIVE_CHANNEL
        }
        self.writer = transfer.DataWriter([output_channel],
                                          [pickle.loads(reader_actor)], conf)
        self.output_channel_id = transfer.ChannelID(output_channel)

    def init_reader(self, input_channel, writer_actor):
        conf = {
            Config.TASK_JOB_ID: ray.worker.global_worker.current_job_id,
            Config.CHANNEL_TYPE: Config.NATIVE_CHANNEL
        }
        self.reader = transfer.DataReader([input_channel],
                                          [pickle.loads(writer_actor)], conf)

    def start_write(self, msg_nums):
        self.t = threading.Thread(
            target=self.run_writer, args=[msg_nums], daemon=True)
        self.t.start()

    def run_writer(self, msg_nums):
        for i in range(msg_nums):
            self.writer.write(self.output_channel_id, pickle.dumps(i))
        print("WriterWorker done.")

    def start_read(self, msg_nums):
        self.t = threading.Thread(
            target=self.run_reader, args=[msg_nums], daemon=True)
        self.t.start()

    def run_reader(self, msg_nums):
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

    def is_finished(self):
        return not self.t.is_alive()

    def on_reader_message(self, buffer: bytes):
        """used in direct call mode"""
        self.reader_client.on_reader_message(buffer)

    def on_reader_message_sync(self, buffer: bytes):
        """used in direct call mode"""
        if self.reader_client is None:
            return b" " * 4  # special flag to indicate this actor not ready
        result = self.reader_client.on_reader_message_sync(buffer)
        return result.to_pybytes()

    def on_writer_message(self, buffer: bytes):
        """used in direct call mode"""
        self.writer_client.on_writer_message(buffer)

    def on_writer_message_sync(self, buffer: bytes):
        """used in direct call mode"""
        if self.writer_client is None:
            return b" " * 4  # special flag to indicate this actor not ready
        result = self.writer_client.on_writer_message_sync(buffer)
        return result.to_pybytes()


def test_queue():
    ray.init()
    writer = Worker._remote(is_direct_call=True)
    reader = Worker._remote(is_direct_call=True)
    channel_id_str = transfer.ChannelID.gen_random_id()
    inits = [
        writer.init_writer.remote(channel_id_str, pickle.dumps(reader)),
        reader.init_reader.remote(channel_id_str, pickle.dumps(writer))
    ]
    ray.get(inits)
    msg_nums = 1000
    print("start read/write")
    reader.start_read.remote(msg_nums)
    writer.start_write.remote(msg_nums)
    while not ray.get(reader.is_finished.remote()):
        time.sleep(0.1)
    ray.shutdown()


if __name__ == "__main__":
    test_queue()

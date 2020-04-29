import logging
import random
from queue import Queue
from typing import List

import ray
import ray.streaming._streaming as _streaming
import ray.streaming.generated.streaming_pb2 as streaming_pb
from ray.actor import ActorHandle
from ray.streaming.config import Config
from ray._raylet import JavaFunctionDescriptor
from ray._raylet import PythonFunctionDescriptor
from ray._raylet import Language

CHANNEL_ID_LEN = 20


class ChannelID:
    """
    ChannelID is used to identify a transfer channel between
     a upstream worker and downstream worker.
    """

    def __init__(self, channel_id_str: str):
        """
        Args:
            channel_id_str: string representation of channel id
        """
        self.channel_id_str = channel_id_str
        self.object_qid = ray.ObjectID(channel_id_str_to_bytes(channel_id_str))

    def __eq__(self, other):
        if other is None:
            return False
        if type(other) is ChannelID:
            return self.channel_id_str == other.channel_id_str
        else:
            return False

    def __hash__(self):
        return hash(self.channel_id_str)

    def __repr__(self):
        return self.channel_id_str

    @staticmethod
    def gen_random_id():
        """Generate a random channel id string
        """
        res = ""
        for i in range(CHANNEL_ID_LEN * 2):
            res += str(chr(random.randint(0, 5) + ord("A")))
        return res

    @staticmethod
    def gen_id(from_index, to_index, ts):
        """Generate channel id, which is 20 character"""
        channel_id = bytearray(20)
        for i in range(11, 7, -1):
            channel_id[i] = ts & 0xff
            ts >>= 8
        channel_id[16] = (from_index & 0xffff) >> 8
        channel_id[17] = (from_index & 0xff)
        channel_id[18] = (to_index & 0xffff) >> 8
        channel_id[19] = (to_index & 0xff)
        return channel_bytes_to_str(bytes(channel_id))


def channel_id_str_to_bytes(channel_id_str):
    """
    Args:
        channel_id_str: string representation of channel id

    Returns:
        bytes representation of channel id
    """
    assert type(channel_id_str) in [str, bytes]
    if isinstance(channel_id_str, bytes):
        return channel_id_str
    qid_bytes = bytes.fromhex(channel_id_str)
    assert len(qid_bytes) == CHANNEL_ID_LEN
    return qid_bytes


def channel_bytes_to_str(id_bytes):
    """
    Args:
        id_bytes: bytes representation of channel id

    Returns:
        string representation of channel id
    """
    assert type(id_bytes) in [str, bytes]
    if isinstance(id_bytes, str):
        return id_bytes
    return bytes.hex(id_bytes)


class DataMessage:
    """
    DataMessage represents data between upstream and downstream operator
    """

    def __init__(self,
                 body,
                 timestamp,
                 channel_id,
                 message_id_,
                 is_empty_message=False):
        self.__body = body
        self.__timestamp = timestamp
        self.__channel_id = channel_id
        self.__message_id = message_id_
        self.__is_empty_message = is_empty_message

    def __len__(self):
        return len(self.__body)

    def body(self):
        """Message data"""
        return self.__body

    def timestamp(self):
        """Get timestamp when item is written by upstream DataWriter
        """
        return self.__timestamp

    def channel_id(self):
        """Get string id of channel where data is coming from
        """
        return self.__channel_id

    def is_empty_message(self):
        """Whether this message is an empty message.
        Upstream DataWriter will send an empty message when this is no data
         in specified interval.
        """
        return self.__is_empty_message

    @property
    def message_id(self):
        return self.__message_id


class ChannelCreationParametersBuilder:
    """
    wrap initial parameters needed by a streaming queue
    """
    _java_reader_async_function_descriptor = JavaFunctionDescriptor(
        "io.ray.streaming.runtime.worker.JobWorker", "onReaderMessage",
        "([B)V")
    _java_reader_sync_function_descriptor = JavaFunctionDescriptor(
        "io.ray.streaming.runtime.worker.JobWorker", "onReaderMessageSync",
        "([B)[B")
    _java_writer_async_function_descriptor = JavaFunctionDescriptor(
        "io.ray.streaming.runtime.worker.JobWorker", "onWriterMessage",
        "([B)V")
    _java_writer_sync_function_descriptor = JavaFunctionDescriptor(
        "io.ray.streaming.runtime.worker.JobWorker", "onWriterMessageSync",
        "([B)[B")
    _python_reader_async_function_descriptor = PythonFunctionDescriptor(
        "ray.streaming.runtime.worker", "on_reader_message", "JobWorker")
    _python_reader_sync_function_descriptor = PythonFunctionDescriptor(
        "ray.streaming.runtime.worker", "on_reader_message_sync", "JobWorker")
    _python_writer_async_function_descriptor = PythonFunctionDescriptor(
        "ray.streaming.runtime.worker", "on_writer_message", "JobWorker")
    _python_writer_sync_function_descriptor = PythonFunctionDescriptor(
        "ray.streaming.runtime.worker", "on_writer_message_sync", "JobWorker")

    def get_parameters(self):
        return self._parameters

    def __init__(self):
        self._parameters = []

    def build_input_queue_parameters(self, queue_ids_dict):
        self.build_parameters(queue_ids_dict,
                              self._java_writer_async_function_descriptor,
                              self._java_writer_sync_function_descriptor,
                              self._python_writer_async_function_descriptor,
                              self._python_writer_sync_function_descriptor)
        return self

    def build_output_queue_parameters(self, to_actors):
        self.build_parameters(to_actors,
                              self._java_reader_async_function_descriptor,
                              self._java_reader_sync_function_descriptor,
                              self._python_reader_async_function_descriptor,
                              self._python_reader_sync_function_descriptor)
        return self

    def build_parameters(self, actors, java_async_func, java_sync_func,
                         py_async_func, py_sync_func):
        for handle in actors:
            parameter = None
            if handle._ray_actor_language == Language.PYTHON:
                parameter = _streaming.ChannelCreationParameter(
                    handle._ray_actor_id, py_async_func, py_sync_func)
            else:
                parameter = _streaming.ChannelCreationParameter(
                    handle._ray_actor_id, java_async_func, java_sync_func)
            self._parameters.append(parameter)
        return self

    @staticmethod
    def set_python_writer_function_descriptor(async_function, sync_function):
        ChannelCreationParametersBuilder. \
            _python_writer_async_function_descriptor = async_function
        ChannelCreationParametersBuilder. \
            _python_writer_sync_function_descriptor = sync_function

    @staticmethod
    def set_python_reader_function_descriptor(async_function, sync_function):
        ChannelCreationParametersBuilder. \
            _python_reader_async_function_descriptor = async_function
        ChannelCreationParametersBuilder. \
            _python_reader_sync_function_descriptor = sync_function


logger = logging.getLogger(__name__)


class DataWriter:
    """Data Writer is a wrapper of streaming c++ DataWriter, which sends data
     to downstream workers
    """

    def __init__(self, output_channels, to_actors: List[ActorHandle],
                 conf: dict):
        """Get DataWriter of output channels
        Args:
            output_channels: output channels ids
            to_actors: downstream output actors
        Returns:
            DataWriter
        """
        assert len(output_channels) > 0
        py_output_channels = [
            channel_id_str_to_bytes(qid_str) for qid_str in output_channels
        ]
        creation_parameters = ChannelCreationParametersBuilder()
        creation_parameters.build_output_queue_parameters(to_actors)
        channel_size = conf.get(Config.CHANNEL_SIZE,
                                Config.CHANNEL_SIZE_DEFAULT)
        py_msg_ids = [0 for _ in range(len(output_channels))]
        config_bytes = _to_native_conf(conf)
        is_mock = conf[Config.CHANNEL_TYPE] == Config.MEMORY_CHANNEL
        self.writer = _streaming.DataWriter.create(
            py_output_channels, creation_parameters.get_parameters(),
            channel_size, py_msg_ids, config_bytes, is_mock)

        logger.info("create DataWriter succeed")

    def write(self, channel_id: ChannelID, item: bytes):
        """Write data into native channel
        Args:
            channel_id: channel id
            item: bytes data
        Returns:
            msg_id
        """
        assert type(item) == bytes
        msg_id = self.writer.write(channel_id.object_qid, item)
        return msg_id

    def stop(self):
        logger.info("stopping channel writer.")
        self.writer.stop()
        # destruct DataWriter
        self.writer = None

    def close(self):
        logger.info("closing channel writer.")


class DataReader:
    """Data Reader is wrapper of streaming c++ DataReader, which read data
    from channels of upstream workers
    """

    def __init__(self, input_channels: List, from_actors: List[ActorHandle],
                 conf: dict):
        """Get DataReader of input channels
        Args:
            input_channels:  input channels
            from_actors:  upstream input actors
        Returns:
            DataReader
        """
        assert len(input_channels) > 0
        py_input_channels = [
            channel_id_str_to_bytes(qid_str) for qid_str in input_channels
        ]
        creation_parameters = ChannelCreationParametersBuilder()
        creation_parameters.build_input_queue_parameters(from_actors)
        py_seq_ids = [0 for _ in range(len(input_channels))]
        py_msg_ids = [0 for _ in range(len(input_channels))]
        timer_interval = int(conf.get(Config.TIMER_INTERVAL_MS, -1))
        is_recreate = bool(conf.get(Config.IS_RECREATE, False))
        config_bytes = _to_native_conf(conf)
        self.__queue = Queue(10000)
        is_mock = conf[Config.CHANNEL_TYPE] == Config.MEMORY_CHANNEL
        self.reader = _streaming.DataReader.create(
            py_input_channels, creation_parameters.get_parameters(),
            py_seq_ids, py_msg_ids, timer_interval, is_recreate, config_bytes,
            is_mock)
        logger.info("create DataReader succeed")

    def read(self, timeout_millis):
        """Read data from channel
        Args:
            timeout_millis: timeout millis when there is no data in channel
             for this duration
        Returns:
            channel item
        """
        if self.__queue.empty():
            msgs = self.reader.read(timeout_millis)
            for msg in msgs:
                msg_bytes, msg_id, timestamp, qid_bytes = msg
                data_msg = DataMessage(msg_bytes, timestamp,
                                       channel_bytes_to_str(qid_bytes), msg_id)
                self.__queue.put(data_msg)
        if self.__queue.empty():
            return None
        return self.__queue.get()

    def stop(self):
        logger.info("stopping Data Reader.")
        self.reader.stop()
        # destruct DataReader
        self.reader = None

    def close(self):
        logger.info("closing Data Reader.")


def _to_native_conf(conf):
    config = streaming_pb.StreamingConfig()
    if Config.STREAMING_JOB_NAME in conf:
        config.job_name = conf[Config.STREAMING_JOB_NAME]
    if Config.STREAMING_WORKER_NAME in conf:
        config.worker_name = conf[Config.STREAMING_WORKER_NAME]
    if Config.STREAMING_OP_NAME in conf:
        config.op_name = conf[Config.STREAMING_OP_NAME]
    # TODO set operator type
    if Config.STREAMING_RING_BUFFER_CAPACITY in conf:
        config.ring_buffer_capacity = \
            conf[Config.STREAMING_RING_BUFFER_CAPACITY]
    if Config.STREAMING_EMPTY_MESSAGE_INTERVAL in conf:
        config.empty_message_interval = \
            conf[Config.STREAMING_EMPTY_MESSAGE_INTERVAL]
    if Config.FLOW_CONTROL_TYPE in conf:
        conf.flow_control_type = conf[Config.FLOW_CONTROL_TYPE]
    if Config.WRITER_CONSUMED_STEP in conf:
        conf.writer_consumed_step = \
            conf[Config.WRITER_CONSUMED_STEP]
    if Config.READER_CONSUMED_STEP in conf:
        conf.reader_consumed_step = \
            conf[Config.READER_CONSUMED_STEP]
    logger.info("conf: %s", str(config))
    return config.SerializeToString()


class ChannelInitException(Exception):
    def __init__(self, msg, abnormal_channels):
        self.abnormal_channels = abnormal_channels
        self.msg = msg


class ChannelInterruptException(Exception):
    def __init__(self, msg=None):
        self.msg = msg

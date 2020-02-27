import logging
import random
from queue import Queue
from typing import List

import ray
import ray.streaming._streaming as _streaming
import ray.streaming.generated.streaming_pb2 as streaming_pb
from ray.actor import ActorHandle, ActorID
from ray.streaming.config import Config

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
        output_actor_ids: List[ActorID] = [
            handle._ray_actor_id for handle in to_actors
        ]
        channel_size = conf.get(Config.CHANNEL_SIZE,
                                Config.CHANNEL_SIZE_DEFAULT)
        py_msg_ids = [0 for _ in range(len(output_channels))]
        config_bytes = _to_native_conf(conf)
        is_mock = conf[Config.CHANNEL_TYPE] == Config.MEMORY_CHANNEL
        self.writer = _streaming.DataWriter.create(
            py_output_channels, output_actor_ids, channel_size, py_msg_ids,
            config_bytes, is_mock)

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
        input_actor_ids: List[ActorID] = [
            handle._ray_actor_id for handle in from_actors
        ]
        py_seq_ids = [0 for _ in range(len(input_channels))]
        py_msg_ids = [0 for _ in range(len(input_channels))]
        timer_interval = int(conf.get(Config.TIMER_INTERVAL_MS, -1))
        is_recreate = bool(conf.get(Config.IS_RECREATE, False))
        config_bytes = _to_native_conf(conf)
        self.__queue = Queue(10000)
        is_mock = conf[Config.CHANNEL_TYPE] == Config.MEMORY_CHANNEL
        self.reader = _streaming.DataReader.create(
            py_input_channels, input_actor_ids, py_seq_ids, py_msg_ids,
            timer_interval, is_recreate, config_bytes, is_mock)
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
    if Config.TASK_JOB_ID in conf:
        job_id = conf[Config.TASK_JOB_ID]
        config.task_job_id = job_id.hex()
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

import random
import logging
from queue import Queue
from typing import List
from abc import ABCMeta, abstractmethod

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

    def __init__(self, str_qid: str):
        """
        Args:
            str_qid: string representation of queue id
        """
        self.str_qid = str_qid
        self.object_qid = ray.ObjectID(qid_str_to_bytes(str_qid))

    def __eq__(self, other):
        if other is None:
            return False
        if type(other) is ChannelID:
            return self.str_qid == other.str_qid
        else:
            return False

    def __hash__(self):
        return hash(self.str_qid)

    def __repr__(self):
        return self.str_qid


def qid_str_to_bytes(qid_str):
    """
    Args:
        qid_str: string representation of queue id

    Returns:
        bytes representation of queue id
    """
    assert type(qid_str) in [str, bytes]
    if isinstance(qid_str, bytes):
        return qid_str
    qid_bytes = bytes.fromhex(qid_str)
    assert len(qid_bytes) == CHANNEL_ID_LEN
    return qid_bytes


def qid_bytes_to_str(qid_bytes):
    """
    Args:
        qid_bytes: bytes representation of queue id

    Returns:
        string representation of queue id
    """
    assert type(qid_bytes) in [str, bytes]
    if isinstance(qid_bytes, str):
        return qid_bytes
    return bytes.hex(qid_bytes)


def gen_random_qid():
    """Generate a random queue id string
    """
    res = ""
    for i in range(CHANNEL_ID_LEN * 2):
        res += str(chr(random.randint(0, 5) + ord('A')))
    return res


def generate_qid(from_index, to_index, ts):
    """Generate queue id, which is 20 character"""
    queue_id = bytearray(20)
    for i in range(11, 7, -1):
        queue_id[i] = ts & 0xff
        ts >>= 8
    queue_id[16] = (from_index & 0xffff) >> 8
    queue_id[17] = (from_index & 0xff)
    queue_id[18] = (to_index & 0xffff) >> 8
    queue_id[19] = (to_index & 0xff)
    return qid_bytes_to_str(bytes(queue_id))


def create_data_writer(output_queue_ids, to_actors):
    """Get queue producer of output queue ids
    Args:
        output_queue_ids: output queue ids
        to_actors: downstream output actors
    Returns:
        DataWriter
    """


def create_data_reader(input_queue_ids, from_actors):
    """Get queue consumer of input queues
    Args:
        input_queue_ids:  input queue ids
        from_actors:  upstream input actors
    Returns:
        DataReader
    """


class QueueItem:
    """
    QueueItem interface lists all methods that a QueueItem subclass need to implement.
    QueueItem represents an item pulled from queue. It maybe a data message or control message.
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    def body(self):
        """Get queue item data
        """
        pass

    @abstractmethod
    def timestamp(self):
        """Get timestamp when item is written by upstream StreamingWriter
        """
        pass


class QueueMessage(QueueItem):
    """
    Queue data message interface
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def queue_id(self):
        """Get id of queue where data is coming from
        """
        pass


class QueueMessageImpl(QueueMessage):
    """
    queue message interface
    """

    def __init__(self, body, timestamp, queue_id, message_id_, is_empty_message=False):
        super().__init__()
        self.__body = body
        self.__timestamp = timestamp
        self.__queue_id = queue_id
        self.__message_id = message_id_
        self.__is_empty_message = is_empty_message

    def __len__(self):
        return len(self.__body)

    def body(self):
        return self.__body

    def timestamp(self):
        return self.__timestamp

    def queue_id(self):
        return self.__queue_id

    def is_empty_message(self):
        return self.__is_empty_message

    @property
    def message_id(self):
        return self.__message_id


logger = logging.getLogger(__name__)


class DataWriter:
    """
    Data Writer
    """

    def __init__(self, output_channels, to_actors: List[ActorHandle], conf: dict):
        assert len(output_channels) > 0
        py_output_channels = [qid_str_to_bytes(qid_str) for qid_str in output_channels]
        output_actor_ids: List[ActorID] = [handle._ray_actor_id for handle in to_actors]
        queue_size = conf.get(Config.QUEUE_SIZE, Config.QUEUE_SIZE_DEFAULT)
        py_msg_ids = [0 for _ in range(len(output_channels))]
        config_bytes = _to_native_conf(conf)
        is_mock = conf[Config.QUEUE_TYPE] == Config.MEMORY_QUEUE
        self.writer = _streaming.DataWriter.create(py_output_channels, output_actor_ids,
                                                   queue_size, py_msg_ids, config_bytes, is_mock)

        logger.info("create DataWriter succeed")

    def write(self, channel_id: ChannelID, item: bytes):
        """
        write data into native channel
        :param channel_id: channel id
        :param item: data
        :return: msg_id
        """
        assert type(item) == bytes
        msg_id = self.writer.write(channel_id.object_qid, item)
        return msg_id

    def stop(self):
        logger.info("stopping channel writer.")
        self.writer.stop()

    def close(self):
        logger.info("closing channel writer.")


class DataReader:
    """
    Data Reader
    """

    def __init__(self, input_channels: List, from_actors: List[ActorHandle], conf: dict):
        assert len(input_channels) > 0
        py_input_queues = [qid_str_to_bytes(qid_str) for qid_str in input_channels]
        input_actor_ids: List[ActorID] = [handle._ray_actor_id for handle in from_actors]
        py_seq_ids = [0 for _ in range(len(input_channels))]
        py_msg_ids = [0 for _ in range(len(input_channels))]
        timer_interval = int(conf.get(Config.TIMER_INTERVAL_MS, -1))
        is_recreate = bool(conf.get(Config.IS_RECREATE, False))
        config_bytes = _to_native_conf(conf)
        self.__queue = Queue(10000)
        is_mock = conf[Config.QUEUE_TYPE] == Config.MEMORY_QUEUE
        self.reader = _streaming.DataReader.create(
            py_input_queues, input_actor_ids,
            py_seq_ids, py_msg_ids, timer_interval, is_recreate, config_bytes, is_mock)
        logger.info("create DataReader succeed")

    def read(self, timeout_millis):
        """Read data from queue
        Args:
            timeout_millis: timeout millis when there is no data in channel for this duration
        Returns:
            queue item
        """
        if self.__queue.empty():
            msgs = self.reader.read(timeout_millis)
            for msg in msgs:
                msg_bytes, msg_id, timestamp, qid_bytes = msg
                queue_msg = QueueMessageImpl(msg_bytes, timestamp, qid_bytes_to_str(qid_bytes), msg_id)
                self.__queue.put(queue_msg)
        if self.__queue.empty():
            return None
        return self.__queue.get()

    def stop(self):
        logger.info("stopping Data Reader.")
        self.reader.stop()

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
        config.ring_buffer_capacity = conf[Config.STREAMING_RING_BUFFER_CAPACITY]
    if Config.STREAMING_EMPTY_MESSAGE_INTERVAL in conf:
        config.empty_message_interval = conf[Config.STREAMING_EMPTY_MESSAGE_INTERVAL]
    logger.info("conf: %s", str(config))
    return config.SerializeToString()


class ChannelInitException(Exception):
    def __init__(self, msg, abnormal_channels):
        self.abnormal_channels = abnormal_channels
        self.msg = msg


class ChannelInterruptException(Exception):
    def __init__(self, msg=None):
        self.msg = msg

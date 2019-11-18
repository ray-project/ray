from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from abc import ABCMeta, abstractmethod
from enum import Enum

import ray
import ray.streaming.queue.queue_utils as queue_utils


class QueueConfig:
    """
    queue config
    """
    # operator type
    OPERATOR_TYPE = "operator_type"

    # reliability level
    RELIABILITY_LEVEL = "reliability_level"


class OperatorType(Enum):
    """
    operator type
    """
    SOURCE = 1
    TRANSFORM = 2
    SINK = 3


class ReliabilityLevel(Enum):
    """
    reliability level
    """
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2
    EXACTLY_SAME = 3


class QueueID:
    def __init__(self, str_qid: str):
        self.str_qid = str_qid
        self.object_qid = ray.ObjectID(queue_utils.qid_str_to_bytes(str_qid))

    def __eq__(self, other):
        if other is None:
            return False
        if type(other) is QueueID:
            return self.str_qid == other.str_qid
        else:
            return False

    def __hash__(self):
        return hash(self.str_qid)

    def __repr__(self):
        return self.str_qid


class QueueItem:
    """
    queue item interface
    """

    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    def body(self):
        pass

    @abstractmethod
    def timestamp(self):
        pass


class QueueMessage(QueueItem):
    """
    queue message interface
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def queue_id(self):
        pass


class QueueLink:
    """
    queue link interface
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def set_ray_runtime(self, runtime):
        """
        Set ray runtime config
        :param runtime:  ray runtime config
        """
        pass

    @abstractmethod
    def set_configuration(self, conf):
        """
        Set queue configuration
        :param conf:  queue configuration
        """
        pass

    @abstractmethod
    def register_queue_consumer(self, input_queue_ids, from_actors):
        """
        Get queue consumer of input queues
        :param input_queue_ids:  input queue ids
        :param from_actors:  upstream input actors
        :return:  queue consumer
        """
        pass

    @abstractmethod
    def register_queue_producer(self, output_queue_ids, to_actors):
        """
        Get queue producer of output queue ids
        :param output_queue_ids: output queue ids
        :param to_actors: downstream output actors
        :return:  queue producer
        """
        pass

    @abstractmethod
    def on_streaming_transfer(self, buffer: bytes):
        """used in direct call mode"""
        pass

    @abstractmethod
    def on_streaming_transfer_sync(self, buffer: bytes):
        """used in direct call mode"""
        pass

class QueueProducer:
    """
    queue producer interface
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def produce(self, queue_id: QueueID, item):
        """
        Produce msg into the special queue
        :param queue_id:  the specified queue id
        :param item:  the message
        """
        pass

    @abstractmethod
    def stop(self):
        """
        stop produce to avoid blocking
        :return: None
        """
        pass

    @abstractmethod
    def close(self):
        """
        Close the queue producer to release resource
        """
        pass


class QueueConsumer:
    """
    queue consumer interface
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def pull(self, timeout_millis):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def close(self):
        pass


class QueueInitException(Exception):
    def __init__(self, msg, abnormal_queues):
        self.abnormal_queues = abnormal_queues
        self.msg = msg


class QueueInterruptException(Exception):
    def __init__(self, msg=None):
        self.msg = msg

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from abc import ABCMeta, abstractmethod

import ray
import ray.streaming.runtime.queue.queue_utils as queue_utils


class QueueID:
    """
    QueueID is used to identify a queue which is the transfer channel between
     a upstream operator and downstream operator.
    """
    def __init__(self, str_qid: str):
        """
        Args:
            str_qid: string representation of queue id
        """
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
        """Get queue consumer of input queues
        Args:
            input_queue_ids:  input queue ids
            from_actors:  upstream input actors
        Returns:
            queue consumer
        """
        pass

    @abstractmethod
    def register_queue_producer(self, output_queue_ids, to_actors):
        """Get queue producer of output queue ids
        Args:
            output_queue_ids: output queue ids
            to_actors: downstream output actors
        Returns:
            queue producer
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
    Queue producer interface
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def produce(self, queue_id: QueueID, item):
        """Produce msg into the downstream queue
        Args:
            queue_id:  the downstream queue id
            item:  the data message
        """
        pass

    @abstractmethod
    def stop(self):
        """Stop producer to avoid blocking
        """
        pass

    @abstractmethod
    def close(self):
        """Close the queue producer to release resource
        """
        pass


class QueueConsumer:
    """
    Queue consumer interface
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def pull(self, timeout_millis):
        """Pull data from queue
        Args:
            timeout_millis: timeout millis when there is no data in queue for this duration
        Returns:
            queue item
        """
        pass

    @abstractmethod
    def stop(self):
        """
        stop queue consumer
        """
        pass

    @abstractmethod
    def close(self):
        """
        close queue consumer
        """
        pass


class QueueInitException(Exception):
    def __init__(self, msg, abnormal_queues):
        self.abnormal_queues = abnormal_queues
        self.msg = msg


class QueueInterruptException(Exception):
    def __init__(self, msg=None):
        self.msg = msg

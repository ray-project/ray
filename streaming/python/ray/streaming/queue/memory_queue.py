from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import logging
from time import sleep
from queue import Queue

from ray.streaming.queue.queue_interface import QueueMessage
from ray.streaming.queue.queue_interface import QueueLink
from ray.streaming.queue.queue_interface import QueueConsumer
from ray.streaming.queue.queue_interface import QueueProducer


class MemQueueMessageImpl(QueueMessage):
    """
    memory queue message impl
    """
    def __init__(self,
                 queue_id,
                 body):
        self.__queue_id = queue_id
        self.__body = body

    def queue_id(self):
        return self.__queue_id

    def body(self):
        return self.__body

    def timestamp(self):
        pass


class MemQueueLinkImpl(QueueLink):
    """
    memory queue link impl
    """
    msg_queue_map = dict()
    recover_info_queue = Queue()

    def __init__(self):
        pass

    def set_configuration(self, conf):
        pass

    def register_queue_consumer(self, input_queue_ids):
        return MemQueueConsumerImpl(input_queue_ids, MemQueueLinkImpl.msg_queue_map)

    def register_queue_producer(self, output_queue_ids):
        return MemQueueProducerImpl(output_queue_ids, MemQueueLinkImpl.msg_queue_map)


class MemQueueProducerImpl(QueueProducer):
    """
    memory queue producer impl
    """
    QUEUE_SIZE_MAX = 100
    LOGGER = logging.getLogger("MemQueueProducerImpl")

    def __init__(self,
                 output_queue_ids,
                 msg_queue_map):
        self.__output_queue_ids = output_queue_ids
        self.__msg_queue_map = msg_queue_map

        for queue_id in output_queue_ids:
            if self.__msg_queue_map.get(queue_id) is None:
                self.__msg_queue_map[queue_id] = Queue(self.QUEUE_SIZE_MAX)

    def produce(self, queue_id, item):
        queue = self.__msg_queue_map.get(queue_id)
        if queue is None:
            MemQueueProducerImpl.LOGGER.error("Queue id {} is not find.".format(queue_id))
            raise ValueError("Queue id" + queue_id + " is not find")

        # check if back pressure
        while queue.qsize() > self.QUEUE_SIZE_MAX:
            try:
                sleep(1)
            except Exception as e:
                # do nothing
                pass
        queue.put(MemQueueMessageImpl(queue_id, item))

    def stop(self):
        pass

    def close(self):
        pass


class MemQueueConsumerImpl(QueueConsumer):
    """
    memory queue consumer impl
    """
    def __init__(self,
                 input_queue_ids,
                 msg_queue_map):
        self.__input_queue_ids = input_queue_ids
        self.__msg_queue_map = msg_queue_map
        self.__index = 0

    def pull(self, timeout_millis):
        queue_id = self.__input_queue_ids[self.__index % len(self.__input_queue_ids)]
        self.__index += 1
        queue = self.__msg_queue_map.get(queue_id)

        if queue is None or queue.empty():
            return None

        return queue.get()

    def stop(self):
        pass

    def close(self):
        pass

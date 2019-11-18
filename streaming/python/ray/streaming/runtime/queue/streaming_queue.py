import logging
from queue import Queue
from typing import List

import ray
import ray.streaming as streaming
import ray.streaming.runtime.queue.queue_utils as qutils
from ray.actor import ActorHandle, ActorID
from ray.streaming.config import Config
from ray.streaming.runtime.queue.queue_interface import QueueLink, QueueConsumer, QueueProducer, QueueMessage, QueueID


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


class QueueLinkImpl(QueueLink):
    """
    streaming queue link impl
    """

    def __init__(self, sync_func, async_func):
        self.__configuration = dict()
        core_worker = ray.worker.global_worker.core_worker
        logger.info("current worker actor id %s", core_worker.get_actor_id())
        self.queue_link = streaming.QueueLink(core_worker)
        self.producer = None
        self.consumer = None
        self.sync_func = sync_func
        self.async_func = async_func

    def set_configuration(self, conf):
        for (k, v) in conf.items():
            self.__configuration[k] = v

    def register_queue_producer(self, output_queues, to_actors: List[ActorHandle]):
        assert len(output_queues) > 0
        py_output_queues = qutils.qid_str_list_to_bytes_list(output_queues)
        output_actor_ids: List[ActorID] = [handle._ray_actor_id for handle in to_actors]
        queue_size = self.__configuration.get(Config.QUEUE_SIZE, Config.QUEUE_SIZE_DEFAULT)
        py_seq_ids = [0 for _ in range(len(output_queues))]
        config_bytes = _to_native_conf(self.__configuration)
        producer = self.queue_link.create_producer(
            py_output_queues,
            output_actor_ids,
            queue_size,
            py_seq_ids,
            config_bytes,
            self.async_func,
            self.sync_func)
        self.producer = QueueProducerImpl(producer)
        logger.info("create QueueProducer succeed")
        return self.producer

    def register_queue_consumer(self, input_queue_ids, from_actors: List[ActorHandle]):
        assert len(input_queue_ids) > 0
        py_input_queues = qutils.qid_str_list_to_bytes_list(input_queue_ids)
        input_actor_ids: List[ActorID] = [handle._ray_actor_id for handle in from_actors]
        py_seq_ids = [0 for _ in range(len(input_queue_ids))]
        py_msg_ids = [0 for _ in range(len(input_queue_ids))]
        timer_interval = int(self.__configuration.get(Config.TIMER_INTERVAL_MS, -1))
        is_recreate = bool(self.__configuration.get(Config.IS_RECREATE, False))
        config_bytes = _to_native_conf(self.__configuration)
        consumer = self.queue_link.create_consumer(
            py_input_queues,
            input_actor_ids,
            py_seq_ids,
            py_msg_ids,
            timer_interval,
            is_recreate,
            config_bytes,
            self.async_func,
            self.sync_func)
        self.consumer = QueueConsumerImpl(consumer)
        logger.info("create QueueConsumer succeed")
        return self.consumer

    def set_ray_runtime(self, runtime):
        self.__configuration[Config.TASK_JOB_ID] = runtime[Config.TASK_JOB_ID]

    def on_streaming_transfer(self, buffer):
        """used in direct call mode"""
        self.queue_link.on_streaming_transfer(buffer)

    def on_streaming_transfer_sync(self, buffer):
        """used in direct call mode"""
        return self.queue_link.on_streaming_transfer_sync(buffer)


class QueueProducerImpl(QueueProducer):
    """
    queue producer impl
    """

    def __init__(self, native_producer):
        self.__native_producer = native_producer

    def produce(self, queue_id: QueueID, item: bytes):
        """
        produce data into native queue
        :param queue_id: queue id
        :param item: data
        :return: msg_id
        """
        assert type(item) == bytes
        msg_id = self.__native_producer.produce(queue_id.object_qid, item)
        return msg_id

    def stop(self):
        logger.info("stopping queue producer.")
        self.__native_producer.stop()

    def close(self):
        logger.info("closing queue producer.")


class QueueConsumerImpl(QueueConsumer):
    """
    queue consumer impl
    """

    def __init__(self, consumer):
        self.__queue = Queue(10000)
        self.__native_consumer = consumer

    def pull(self, timeout_millis):
        """
        pull message from native queue
        :param timeout_millis: timeout millis
        :return: message
        """
        if self.__queue.empty():
            msgs = self.__native_consumer.pull(100)
            for msg in msgs:
                msg_bytes, msg_id, timestamp, qid_bytes = msg
                queue_msg = QueueMessageImpl(msg_bytes, timestamp, qutils.qid_bytes_to_str(qid_bytes), msg_id)
                self.__queue.put(queue_msg)
        if self.__queue.empty():
            return None
        return self.__queue.get()

    def stop(self):
        logger.info("stopping queue consumer.")
        self.__native_consumer.stop()

    def close(self):
        logger.info("closing queue consumer.")


def _to_native_conf(conf):
    return b""

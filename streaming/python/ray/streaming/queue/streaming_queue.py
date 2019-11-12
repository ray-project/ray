import logging
from queue import Queue

import ray
from ray.function_manager import FunctionDescriptor

import ray.streaming.queue.queue_utils as qutils
from ray.streaming.queue.exception import QueueInitException, QueueInterruptException
from ray.streaming.config import Config
from ray.streaming.queue.queue_constants import QueueConstants, QueueStatus, QueueBundleType
from ray.streaming.queue.queue_constants import QueueCreatorType
from ray.streaming.queue.queue_interface import QueueConsumer
from ray.streaming.queue.queue_interface import QueueLink
from ray.streaming.queue.queue_interface import QueueMessage
from ray.streaming.queue.queue_interface import QueueProducer
import ray.streaming as streaming


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

    def __init__(self):
        self.__configuration = dict()
        core_worker = ray.worker.global_worker.core_worker
        self.queue_link = streaming.QueueLink(core_worker)
        self.sync_func = FunctionDescriptor("ray.streaming.operator_instance",
                                            "on_streaming_transfer", "OperatorInstance")
        self.async_func = FunctionDescriptor("ray.streaming.operator_instance",
                                             "on_streaming_transfer_sync", "OperatorInstance")

    def set_configuration(self, conf):
        for (k, v) in conf.items():
            self.__configuration[k] = v

    def register_queue_consumer(self, input_queue_ids, from_actors):
        input_queue_ids = qutils.qid_str_list_to_bytes_list(input_queue_ids)
        seq_id_list = []
        msg_id_list = []

        timer_interval = -1
        if Config.TIMER_INTERVAL_MS in self.__configuration:
            timer_interval = self.__configuration.get(Config.TIMER_INTERVAL_MS)
        logger.info("create consumer, queues={}, seq_ids={}, msg_ids={}."
                    .format(input_queue_ids, seq_id_list, msg_id_list))
        try:
            return QueueConsumerImpl(
                input_queue_ids,
                seq_id_list,
                msg_id_list,
                timer_interval,
                _to_native_conf(self.__configuration)
            )
        except QueueInitException as e:
            logger.error("native consumer failed")
            self.__abnormal_input_queues += e.abnormal_queues

    def register_queue_producer(self, output_queues, to_actors):
        py_output_queues = qutils.qid_str_list_to_bytes_list(output_queues)
        # output_actor_ids

        # list[bytes] py_output_queues,
        #                         list[uint64_t] output_actor_ids,
        #                         uint64_t queue_size,
        #                         list[int] py_seq_ids,
        #                         bytes config_bytes,
        #                         async_func: FunctionDescriptor,
        #                         sync_func: FunctionDescriptor):

        creator_types = [QueueCreatorType.RECREATE] * py_output_queues.__len__()
        # RECONSTRUCT by default if has cp
        if self.__output_checkpoints.__len__() != 0:
            creator_types = [QueueCreatorType.RECONSTRUCT] * py_output_queues.__len__()
        # abnormal queues use RECREATE_AND_CLEAR
        i = 0
        for qid in py_output_queues:
            if qid in self.__last_abnormal_queues:
                creator_types[i] = QueueCreatorType.RECREATE_AND_CLEAR
            i += 1

        logger.info("register producer, createType: {}, queues:{}, seqIds: {}".format(
            creator_types, py_output_queues, self.__output_checkpoints
        ))
        try:
            return QueueProducerImpl(
                py_output_queues,
                self.__output_checkpoints,
                int(self.__configuration.get(Config.QUEUE_SIZE)),
                creator_types,
                _to_native_conf(self.__configuration)
            )
        except QueueInitException as e:
            logger.error("native producer failed")
            self.__abnormal_input_queues = e.abnormal_queues

    def set_ray_runtime(self, runtime):
        self.__configuration[Config.TASK_JOB_ID] = \
            runtime[Config.RAY_RUNTIME_TASK_JOB_ID]


class QueueProducerImpl(QueueProducer):
    """
    queue producer impl
        Args:
            output_queue_ids    : str or bytes array
            store_path_list     : plasma store socket name list
            output_points       : output queue message id offset
            queue_size          : int, default 10 ** 8 bytes
            creator_types : Enum list, default recreate
    """
    LOGGER = logging.getLogger(__name__)

    def __init__(self, output_queue_ids, store_path_list, output_points, queue_size,
                 creator_types, fbs_conf_bytes):
        pass

    # queue_id: str
    # item: bytes
    def produce(self, queue_id, item):
        queue_id = qutils.qid_str_to_bytes(queue_id)
        ret = self.__writer.write_message(queue_id, item, item.__len__())
        if ret is None:
            raise QueueInterruptException("producer has been stopped")

    def stop(self):
        QueueProducerImpl.LOGGER.info("stopping queue producer.")
        self.__writer.stop()

    def close(self):
        QueueProducerImpl.LOGGER.info("closing queue producer.")


class QueueConsumerImpl(QueueConsumer):
    """
    queue consumer impl
    """
    LOGGER = logging.getLogger(__name__)

    def __init__(self, producer):
        self.__queue = Queue(10000)
        self.producer = producer

    def pull(self, timeout_millis):
        if self.__queue.empty():
            msgs = self.producer.pull(100)
            for msg in msgs:
                msg_bytes, msg_id, timestamp, qid_bytes = msg
                queue_msg = QueueMessageImpl(msg_bytes, timestamp, qutils.qid_bytes_to_str(qid_bytes), msg_id)
                self.__queue.put(queue_msg)
        if self.__queue.empty():
            return None
        return self.__queue.get()

    def stop(self):
        QueueConsumerImpl.LOGGER.info("stopping queue consumer.")
        self.producer.stop()

    def close(self):
        QueueConsumerImpl.LOGGER.info("closing queue consumer.")


def _to_native_conf(conf):
    pass

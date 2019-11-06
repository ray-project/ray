import logging
from queue import Queue

from ray import streaming
from streaming.queue.queue_interface import QueueMessage
from streaming.queue.queue_interface import QueueLink
from streaming.queue.queue_interface import QueueConsumer
from streaming.queue.queue_interface import QueueProducer
import streaming.queue.queue_utils as qutils
from streaming.queue.config import Config
from streaming.queue.fbs_config_converter import FbsConfigConverter
from streaming.queue.queue_constants import QueueCreatorType
import streaming.queue.queue_constants as qc
from streaming.queue.queue_constants import QueueConstants, QueueStatus, QueueBundleType
from streaming.exception import QueueInitException, QueueInterruptException


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
        self.__input_checkpoints = dict()
        self.__output_checkpoints = dict()

    def set_configuration(self, conf):
        for (k, v) in conf.items():
            self.__configuration[k] = v

    def register_queue_consumer(self, input_queue_ids):
        input_queue_ids = qutils.qid_str_list_to_bytes_list(input_queue_ids)
        is_recreate = False
        if Config.IS_RECREATE in self.__configuration:
            is_recreate = self.__configuration[Config.IS_RECREATE]
        seq_id_list = []
        msg_id_list = []

        timer_interval = -1
        if Config.TIMER_INTERVAL_MS in self.__configuration:
            timer_interval = self.__configuration.get(Config.TIMER_INTERVAL_MS)
        logger.info("create consumer, queues={}, seq_ids={}, msg_ids={}."
                    .format(input_queue_ids, seq_id_list, msg_id_list))
        try:
            return QueueConsumerImpl(
                self.__configuration.get(Config.PLASMA_STORE_PATH),
                input_queue_ids,
                seq_id_list,
                msg_id_list,
                timer_interval,
                is_recreate,
                FbsConfigConverter.map2bytes(self.__configuration)
            )
        except QueueInitException as e:
            logger.error("native consumer failed")
            self.__abnormal_input_queues += e.abnormal_queues

    def register_queue_producer(self, output_queue_ids):
        output_queue_ids = qutils.qid_str_list_to_bytes_list(output_queue_ids)
        store_path = self.__configuration.get(Config.PLASMA_STORE_PATH)

        creator_types = [QueueCreatorType.RECREATE] * output_queue_ids.__len__()
        # RECONSTRUCT by default if has cp
        if self.__output_checkpoints.__len__() != 0:
            creator_types = [QueueCreatorType.RECONSTRUCT] * output_queue_ids.__len__()
        # abnormal queues use RECREATE_AND_CLEAR
        i = 0
        for qid in output_queue_ids:
            if qid in self.__last_abnormal_queues:
                creator_types[i] = QueueCreatorType.RECREATE_AND_CLEAR
            i += 1

        logger.info("register producer, createType: {}, queues:{}, seqIds: {}".format(
            creator_types, output_queue_ids, self.__output_checkpoints
        ))
        try:
            return QueueProducerImpl(
                output_queue_ids,
                store_path,
                self.__output_checkpoints,
                int(self.__configuration.get(Config.QUEUE_SIZE)),
                creator_types,
                FbsConfigConverter.map2bytes(self.__configuration)
            )
        except QueueInitException as e:
            logger.error("native producer failed")
            self.__abnormal_input_queues = e.abnormal_queues

    def set_ray_runtime(self, runtime):
        self.__configuration[Config.PLASMA_STORE_PATH] = \
            runtime[Config.RAY_RUNTIME_OBJECT_STORE_ADDRESS]
        self.__configuration[Config.RAYLET_SOCKET_NAME] = \
            runtime[Config.RAY_RUNTIME_RAYLET_SOCKET_NAME]
        self.__configuration[Config.TASK_JOB_ID] = \
            runtime[Config.RAY_RUNTIME_TASK_JOB_ID]


class QueueProducerImpl(QueueProducer):
    """
    plasma queue producer impl
        Args:
            output_queue_ids    : str or bytes array
            store_path_list     : plasma store socket name list
            output_points       : output queue message id offset
            queue_size          : int, default 10 ** 8 bytes
            creator_types : Enum list, default recreate
    """
    QUEUE_SIZE_MAX = 100
    LOGGER = logging.getLogger(__name__)

    def __init__(self, output_queue_ids, store_path_list, output_points, queue_size,
                 creator_types, fbs_conf_bytes):

        self.__output_queue_ids = output_queue_ids
        self.__writer = streaming.StreamingWriter()
        ret = self.__writer.run(
            output_queue_ids,
            store_path_list,
            qutils.get_offset_list(output_queue_ids, output_points),
            queue_size,
            creator_types,
            fbs_conf_bytes
        )
        assert ret.__contains__(QueueConstants.KEY_CREATE_PRODUCER_STATUS)
        assert QueueStatus(ret[QueueConstants.KEY_CREATE_PRODUCER_STATUS])
        if ret.__contains__(QueueConstants.KEY_CREATE_ABNORMAL_QUEUES) \
                and len(ret[QueueConstants.KEY_CREATE_ABNORMAL_QUEUES]) > 0:
            raise QueueInitException(
                "remaining queues fail to be created : {}".format(
                    ret[QueueConstants.KEY_CREATE_ABNORMAL_QUEUES]
                ),
                ret[QueueConstants.KEY_CREATE_ABNORMAL_QUEUES]
            )

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

    def __init__(self, store_path, input_queue_ids, seq_id_list, msg_id_list,
                 timer_interval, is_recreate, fbs_conf_bytes):

        self.__store_path = store_path
        self.__input_queue_ids = input_queue_ids
        self.__reader = streaming.StreamingReader()
        ret = self.__reader.run(
            store_path,
            input_queue_ids,
            seq_id_list,
            msg_id_list,
            timer_interval,
            int(is_recreate),
            fbs_conf_bytes
        )
        if QueueConstants.KEY_CREATE_ABNORMAL_QUEUES in ret:
            raise QueueInitException(
                "reader init failed, abnormal_queues={}".format(
                    ret[QueueConstants.KEY_CREATE_ABNORMAL_QUEUES]
                ),
                ret[QueueConstants.KEY_CREATE_ABNORMAL_QUEUES]
            )
        self.__queue = Queue()

    def pull(self, timeout_millis):
        if self.__queue.empty():
            data = self.__reader.read_bundle(timeout_millis, 0)
            if data.__contains__(QueueConstants.KEY_PULL_STATUS):
                error_code = data[QueueConstants.KEY_PULL_STATUS]
                if error_code == QueueStatus.INTERRUPTED:
                    raise QueueInterruptException("queue reader has interrupted")
                if error_code == QueueConstants.READ_TIME_OUT:
                    return None
                if error_code not in QueueConstants.NORMAL_STATUS_SET:
                    raise RuntimeError("pull data error, error code : {}".format(error_code))

            if QueueConstants.KEY_BUNDLE_TYPE in data:
                if QueueBundleType(data[QueueConstants.KEY_BUNDLE_TYPE]) == \
                        QueueBundleType.EMPTY:

                    self.__queue.put(
                        QueueMessageImpl(None,
                                         data[QueueConstants.KEY_PROCESS_TIME],
                                         data[QueueConstants.KEY_FROM_Q_ID],
                                         data[QueueConstants.KEY_LAST_MESSAGE_ID],
                                         True
                                         )
                    )

                elif QueueBundleType(data[QueueConstants.KEY_BUNDLE_TYPE]) == \
                        QueueBundleType.BUNDLE:
                    start_message_id = data[QueueConstants.KEY_LAST_MESSAGE_ID] - \
                                       len(data[QueueConstants.KEY_DATA]) + 1
                    for i, item in enumerate(data[QueueConstants.KEY_DATA]):
                        self.__queue.put(
                            QueueMessageImpl(item,
                                             data[QueueConstants.KEY_PROCESS_TIME],
                                             data[QueueConstants.KEY_FROM_Q_ID],
                                             i + start_message_id
                                             )
                        )
                else:
                    raise RuntimeError("no such bundle type")

        if self.__queue.empty():
            return None
        return self.__queue.get()

    def stop(self):
        QueueConsumerImpl.LOGGER.info("stopping queue consumer.")
        self.__reader.stop()

    def close(self):
        QueueConsumerImpl.LOGGER.info("closing queue consumer.")

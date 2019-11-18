import enum

KEY_MSG_ID = b"msg_id"
KEY_SEQ_ID = b"seq_id"
QUEUE_ID_LEN = 20


class QueueCreatorType(enum.IntEnum):
    RECREATE = 0
    RECONSTRUCT = 1
    RECREATE_AND_CLEAR = 2


class QueueStatus(enum.IntEnum):
    OK = 0
    RECONSTRUCT_TIME_OUT = 1
    FULL_PLASMA_STORE = 2
    QUEUE_ID_NOT_FOUND = 3
    RESUBSCRIBE_FAILED = 4
    EMPTY_RING_BUFFER = 5
    FULL_PLASMA_QUEUE = 6
    NO_SUCH_ITEM = 7
    INIT_QUEUE_FAILED = 8
    GET_BUNDLE_TIME_OUT = 9
    SKIP_SEND_EMPTY_MESSAGE = 10
    INTERRUPTED = 11
    WAIT_QUEUE_TIME_OUT = 12

    def __bool__(self):
        return self.value == QueueStatus.OK.value


class QueueBundleType(enum.IntEnum):
    EMPTY = 1
    BARRIER = 2
    BUNDLE = 3


class QueueConstants(object):
    KEY_CREATE_PRODUCER_STATUS = b"status"
    KEY_CREATE_ABNORMAL_QUEUES = b"abnormal_queues"
    KEY_BUNDLE_TYPE = b"bundle_type"
    KEY_PROCESS_TIME = b"process_time"
    KEY_FROM_Q_ID = b"from_q_id"
    KEY_DATA = b"data"
    KEY_BARRIER_ID = b"barrier_id"
    KEY_OFFSET = b"offset"
    KEY_LAST_MESSAGE_ID = b"last_message_id"
    KEY_PULL_STATUS = b"error_code"
    # error_code 0 is OK, 9 is read timeout
    READ_TIME_OUT = 9
    NORMAL_STATUS_SET = [0, 9]

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class Config:
    # queue
    QUEUE_TYPE = "queue_type"
    MEMORY_QUEUE = "memory_queue"
    NATIVE_QUEUE = "streaming_queue"
    QUEUE_SIZE = "queue_size"
    QUEUE_SIZE_DEFAULT = 10 ** 8
    TASK_JOB_ID = "TaskJobID"

    TIMER_INTERVAL_MS = "timerIntervalMs"
    IS_RECREATE = "isRecreate"
    STREAMING_PERSISTENCE_PATH = "StreamingPersistencePath"
    STREAMING_JOB_NAME = "StreamingJobName"
    STREAMING_OP_NAME = "StreamingOpName"
    STREAMING_WORKER_NAME = "StreamingWorkerName"
    STREAMING_LOG_PATH = "StreamingLogPath"

    # uint config
    STREAMING_LOG_LEVEL = "StreamingLogLevel"
    STREAMING_RING_BUFFER_CAPACITY = "StreamingRingBufferCapacity"
    STREAMING_EMPTY_MESSAGE_TIME_INTERVAL = "StreamingEmptyMessageTimeInterval"
    STREAMING_FULL_QUEUE_TIME_INTERVAL = "StreamingFullQueueTimeInterval"
    STREAMING_WRITER_CONSUMED_STEP = "StreamingWriterConsumedStep"
    STREAMING_READER_CONSUMED_STEP = "StreamingReaderConsumedStep"
    STREAMING_READER_CONSUMED_STEP_UPDATER = "StreamingReaderConsumedStepUpdater"
    STREAMING_FLOW_CONTROL = "StreamingFlowControl"

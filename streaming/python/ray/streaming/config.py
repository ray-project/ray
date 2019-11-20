from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class Config:
    STREAMING_JOB_NAME = "streaming.job.name"
    STREAMING_OP_NAME = "streaming.op_name"
    TASK_JOB_ID = "streaming.task_job_id"
    STREAMING_WORKER_NAME = "streaming.worker_name"
    # queue
    QUEUE_TYPE = "queue_type"
    MEMORY_QUEUE = "memory_queue"
    NATIVE_QUEUE = "streaming_queue"
    QUEUE_SIZE = "queue_size"
    QUEUE_SIZE_DEFAULT = 10 ** 8
    IS_RECREATE = "streaming.is_recreate"
    # return from StreamingReader.getBundle if only empty message read in this interval.
    TIMER_INTERVAL_MS = "timer_interval_ms"

    STREAMING_RING_BUFFER_CAPACITY = "streaming.ring_buffer_capacity"
    # write an empty message if there is no data to be written in this interval.
    STREAMING_EMPTY_MESSAGE_INTERVAL = "streaming.empty_message_interval"

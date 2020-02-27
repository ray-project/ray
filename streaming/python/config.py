class Config:
    STREAMING_JOB_NAME = "streaming.job.name"
    STREAMING_OP_NAME = "streaming.op_name"
    TASK_JOB_ID = "streaming.task_job_id"
    STREAMING_WORKER_NAME = "streaming.worker_name"
    # channel
    CHANNEL_TYPE = "channel_type"
    MEMORY_CHANNEL = "memory_channel"
    NATIVE_CHANNEL = "native_channel"
    CHANNEL_SIZE = "channel_size"
    CHANNEL_SIZE_DEFAULT = 10**8
    IS_RECREATE = "streaming.is_recreate"
    # return from StreamingReader.getBundle if only empty message read in this
    # interval.
    TIMER_INTERVAL_MS = "timer_interval_ms"
    READ_TIMEOUT_MS = "read_timeout_ms"
    DEFAULT_READ_TIMEOUT_MS = "10"
    STREAMING_RING_BUFFER_CAPACITY = "streaming.ring_buffer_capacity"
    # write an empty message if there is no data to be written in this
    # interval.
    STREAMING_EMPTY_MESSAGE_INTERVAL = "streaming.empty_message_interval"

    # operator type
    OPERATOR_TYPE = "operator_type"

    # flow control
    FLOW_CONTROL_TYPE = "streaming.flow_control_type"
    WRITER_CONSUMED_STEP = "streaming.writer.consumed_step"
    READER_CONSUMED_STEP = "streaming.reader.consumed_step"

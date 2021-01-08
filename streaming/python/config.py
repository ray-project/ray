class Config:
    STREAMING_JOB_NAME = "streaming.job.name"
    STREAMING_OP_NAME = "streaming.op_name"
    STREAMING_WORKER_NAME = "streaming.worker_name"
    # channel
    CHANNEL_TYPE = "channel_type"
    MEMORY_CHANNEL = "memory_channel"
    NATIVE_CHANNEL = "native_channel"
    CHANNEL_SIZE = "channel_size"
    CHANNEL_SIZE_DEFAULT = 10**8
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

    # state backend
    CP_STATE_BACKEND_TYPE = "streaming.context-backend.type"
    CP_STATE_BACKEND_MEMORY = "memory"
    CP_STATE_BACKEND_LOCAL_FILE = "local_file"
    CP_STATE_BACKEND_DEFAULT = CP_STATE_BACKEND_MEMORY

    # local disk
    FILE_STATE_ROOT_PATH = "streaming.context-backend.file-state.root"
    FILE_STATE_ROOT_PATH_DEFAULT = "/tmp/ray_streaming_state"

    # checkpoint
    JOB_WORKER_CONTEXT_KEY = "jobworker_context_"

    # reliability level
    REQUEST_ROLLBACK_RETRY_TIMES = 3

    # checkpoint prefix key
    JOB_WORKER_OP_CHECKPOINT_PREFIX_KEY = "jobwk_op_"


class ConfigHelper(object):
    @staticmethod
    def get_cp_local_file_root_dir(conf):
        value = conf.get(Config.FILE_STATE_ROOT_PATH)
        if value is not None:
            return value
        return Config.FILE_STATE_ROOT_PATH_DEFAULT

    @staticmethod
    def get_cp_context_backend_type(conf):
        value = conf.get(Config.CP_STATE_BACKEND_TYPE)
        if value is not None:
            return value
        return Config.CP_STATE_BACKEND_DEFAULT

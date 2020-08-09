from ray.streaming.constants import StreamingConstants


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


class ConfigHelper(object):

    @staticmethod
    def get_cp_local_file_root_dir(conf):
        value = conf.get(StreamingConstants.FILE_STATE_ROOT_PATH)
        if value is not None:
            return value
        return StreamingConstants.FILE_STATE_ROOT_PATH_DEFAULT

    @staticmethod
    def get_cp_state_backend_type(conf):
        value = conf.get(StreamingConstants.CP_STATE_BACKEND_TYPE)
        if value is not None:
            return value
        return StreamingConstants.CP_STATE_BACKEND_DEFAULT

    @staticmethod
    def get_buffer_pool_size(conf):
        if StreamingConstants.BUFFER_POOL_SIZE in conf:
            buffer_pool_size = int(conf[StreamingConstants.BUFFER_POOL_SIZE])
        else:
            queue_size = int(conf.get(StreamingConstants.QUEUE_SIZE, StreamingConstants.QUEUE_SIZE_DEFAULT))
            buffer_pool_size = int(queue_size * StreamingConstants.BUFFER_POOL_QUEUE_RATIO)
        assert buffer_pool_size < 2 ** 32 - 1
        return buffer_pool_size

    @staticmethod
    def get_buffer_pool_min_buffer_size(conf):
        if StreamingConstants.BUFFER_POOL_MIN_BUFFER_SIZE in conf:
            return int(conf[StreamingConstants.BUFFER_POOL_MIN_BUFFER_SIZE])
        else:
            max_buffers = int(conf.get(StreamingConstants.BUFFER_POOL_MAX_BUFFERS,
                                       StreamingConstants.BUFFER_POOL_MAX_BUFFERS_DEFAULT))
            buffer_pool_size = ConfigHelper.get_buffer_pool_size(conf)
            min_buffer_size = buffer_pool_size // max_buffers
            assert min_buffer_size < 2 ** 32 - 1
            return min_buffer_size

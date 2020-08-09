from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class StreamingConstants:
    """
    streaming constants
    """

    # checkpoint
    JOB_WORKER_CONTEXT_KEY = "jobworker_context_"

    # queue
    QUEUE_TYPE = "queue_type"
    MEMORY_QUEUE = "memory_queue"
    MOCK_QUEUE = "mock_queue"
    STREAMING_QUEUE = "streaming_queue"
    QUEUE_SIZE = "queue_size"
    QUEUE_SIZE_DEFAULT = 10**8
    QUEUE_PULL_TIMEOUT = "queue_pull_timeout"
    QUEUE_PULL_TIMEOUT_DEAULT = 300000
    BUFFER_POOL_SIZE = "streaming.queue.buffer-pool.size"
    BUFFER_POOL_QUEUE_RATIO = 1.1
    BUFFER_POOL_MIN_BUFFER_SIZE = "streaming.queue.buffer-pool.buffer.size.min"
    BUFFER_POOL_MAX_BUFFERS = "streaming.queue.buffer-pool.buffers.max"
    BUFFER_POOL_MAX_BUFFERS_DEFAULT = 5

    # reliability level
    RELIABILITY_LEVEL = "Reliability_Level"
    EXACTLY_SAME = "EXACTLY_SAME"
    EXACTLY_ONCE = "EXACTLY_ONCE"
    REQUEST_ROLLBACK_RETRY_TIMES = 3

    # checkpoint prefix key
    JOB_WORKER_OP_CHECKPOINT_PREFIX_KEY = "jobwk_op_"

    # state backend
    CP_STATE_BACKEND_TYPE = "streaming.state-backend.type"
    CP_STATE_BACKEND_MEMORY = "memory"
    CP_STATE_BACKEND_LOCAL_FILE = "local_file"
    CP_STATE_BACKEND_DEFAULT = CP_STATE_BACKEND_MEMORY

    # local disk
    FILE_STATE_ROOT_PATH = "streaming.state-backend.file-state.root"
    FILE_STATE_ROOT_PATH_DEFAULT = "/tmp/ray_streaming_state"

    # job master/worker context
    JOB_NAME = "job_name"
    WORKER_ID = "worker_id"
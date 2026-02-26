from enum import Enum

from ray._common.logging_constants import (
    LOGRECORD_STANDARD_ATTRS,  # noqa: F401 (re-export)
)

LOGGER_FLATTEN_KEYS = {
    "ray_serve_extra_fields",
}


class LogKey(str, Enum):
    # Core context
    JOB_ID = "job_id"
    WORKER_ID = "worker_id"
    NODE_ID = "node_id"
    ACTOR_ID = "actor_id"
    TASK_ID = "task_id"
    ACTOR_NAME = "actor_name"
    TASK_NAME = "task_name"
    TASK_FUNCTION_NAME = "task_func_name"

    # Logger built-in context
    ASCTIME = "asctime"
    LEVELNAME = "levelname"
    MESSAGE = "message"
    FILENAME = "filename"
    LINENO = "lineno"
    EXC_TEXT = "exc_text"
    PROCESS = "process"

    # Ray logging context
    TIMESTAMP_NS = "timestamp_ns"

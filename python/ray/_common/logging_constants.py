"""Logging-related constants shared across Ray libraries.

Used to distinguish standard Python logging attributes from Ray or user context.
See: https://docs.python.org/3/library/logging.html#logrecord-attributes
"""

from enum import Enum

LOGRECORD_STANDARD_ATTRS = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "message",
        "module",
        "msecs",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
        "taskName",
    }
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

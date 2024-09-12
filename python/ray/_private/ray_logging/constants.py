from enum import Enum

# A set containing the standard attributes of a LogRecord. This is used to
# help us determine which attributes constitute Ray or user-provided context.
# http://docs.python.org/library/logging.html#logrecord-attributes
LOGRECORD_STANDARD_ATTRS = {
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

    # Logger built-in context
    ASCTIME = "asctime"
    LEVELNAME = "levelname"
    MESSAGE = "message"
    FILENAME = "filename"
    LINENO = "lineno"
    EXC_TEXT = "exc_text"

    # Ray logging context
    TIMESTAMP_NS = "timestamp_ns"

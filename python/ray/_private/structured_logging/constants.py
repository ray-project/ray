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


LOG_MODE_DICT = {
    "TEXT": lambda log_level: {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "text": {
                "()": "ray._private.structured_logging.formatters.TextFormatter",
            },
        },
        "filters": {
            "core_context": {
                "()": "ray._private.structured_logging.filters.CoreContextFilter",
            },
        },
        "handlers": {
            "console": {
                "level": log_level,
                "class": "logging.StreamHandler",
                "formatter": "text",
                "filters": ["core_context"],
            },
        },
        "root": {
            "level": log_level,
            "handlers": ["console"],
        },
    },
}

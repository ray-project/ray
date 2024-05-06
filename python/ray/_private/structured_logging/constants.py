from enum import Enum


class LogKey(str, Enum):
    # Core context
    JOB_ID = "job_id"
    WORKER_ID = "worker_id"
    NODE_ID = "node_id"
    ACTOR_ID = "actor_id"
    TASK_ID = "task_id"

    # Logger built-in context
    TS = "ts"
    LEVEL = "level"
    MSG = "msg"
    FILENAME = "filename"
    LINENO = "lineno"
    EXC_TEXT = "exc_text"

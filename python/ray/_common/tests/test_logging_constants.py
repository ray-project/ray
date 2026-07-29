import sys

import pytest

from ray._common.logging_constants import (
    LOGGER_FLATTEN_KEYS,
    LOGRECORD_STANDARD_ATTRS,
    LogKey,
)


def test_logrecord_standard_attrs_is_frozenset():
    assert isinstance(LOGRECORD_STANDARD_ATTRS, frozenset)


def test_logrecord_standard_attrs_contains_standard_names():
    expected = frozenset(
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
    assert LOGRECORD_STANDARD_ATTRS == expected


def test_logrecord_standard_attrs_has_expected_size():
    assert len(LOGRECORD_STANDARD_ATTRS) == 23


def test_logger_flatten_keys_is_set():
    assert isinstance(LOGGER_FLATTEN_KEYS, set)
    assert "ray_serve_extra_fields" in LOGGER_FLATTEN_KEYS


def test_logkey_is_enum():
    from enum import Enum

    assert issubclass(LogKey, Enum)
    expected = {
        "JOB_ID": "job_id",
        "WORKER_ID": "worker_id",
        "NODE_ID": "node_id",
        "ACTOR_ID": "actor_id",
        "TASK_ID": "task_id",
        "ACTOR_NAME": "actor_name",
        "TASK_NAME": "task_name",
        "TASK_FUNCTION_NAME": "task_func_name",
        "ASCTIME": "asctime",
        "LEVELNAME": "levelname",
        "MESSAGE": "message",
        "FILENAME": "filename",
        "LINENO": "lineno",
        "EXC_TEXT": "exc_text",
        "PROCESS": "process",
        "TIMESTAMP_NS": "timestamp_ns",
    }
    actual = {member.name: member.value for member in LogKey}
    assert actual == expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))

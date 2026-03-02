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
    expected = {"message", "levelname", "name", "pathname", "lineno", "exc_info"}
    assert expected.issubset(LOGRECORD_STANDARD_ATTRS)


def test_logrecord_standard_attrs_has_expected_size():
    assert len(LOGRECORD_STANDARD_ATTRS) == 23


def test_logger_flatten_keys_is_set():
    assert isinstance(LOGGER_FLATTEN_KEYS, set)
    assert "ray_serve_extra_fields" in LOGGER_FLATTEN_KEYS


def test_logkey_is_enum():
    from enum import Enum

    assert issubclass(LogKey, Enum)
    assert LogKey.JOB_ID.value == "job_id"
    assert LogKey.WORKER_ID.value == "worker_id"
    assert LogKey.NODE_ID.value == "node_id"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))

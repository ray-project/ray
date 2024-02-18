import logging
import os
import re
from datetime import datetime
from unittest.mock import patch

import pytest

import ray
from ray.data._internal.dataset_logger import (
    DatasetLogger,
    SystemException,
    UserCodeException,
)
from ray.exceptions import RayTaskError
from ray.tests.conftest import *  # noqa


def test_dataset_logger(shutdown_only):
    ray.init()
    log_name, msg = "test_name", "test_message_1234"
    logger = DatasetLogger(log_name)
    logger.get_logger().info(msg)

    # Read from log file, and parse each component of emitted log row
    session_dir = ray._private.worker._global_node.get_session_dir_path()
    log_file_path = os.path.join(session_dir, DatasetLogger.DEFAULT_DATASET_LOG_PATH)
    with open(log_file_path, "r") as f:
        raw_logged_msg = f.read()
    (
        logged_ds,
        logged_ts,
        logged_level,
        logged_filepath,
        sep,
        logged_msg,
    ) = raw_logged_msg.split()

    # Could not use freezegun to test exact timestamp value
    # (values off by some milliseconds), so instead we check
    # for correct timestamp format.
    try:
        datetime.strptime(f"{logged_ds} {logged_ts}", "%Y-%m-%d %H:%M:%S,%f")
    except ValueError:
        raise Exception(f"Invalid log timestamp: {logged_ds} {logged_ts}")

    assert logged_level == logging.getLevelName(logging.INFO)
    assert re.match(r"test_logger.py:\d+", logged_filepath)
    assert logged_msg == msg


def test_skip_internal_stack_frames(ray_start_regular_shared):
    def f(x):
        1 / 0
        return x

    with pytest.raises(ZeroDivisionError) as exc_info:
        ray.data.range(10).map(f).take_all()
        assert isinstance(exc_info, RayTaskError)
        assert isinstance(exc_info, UserCodeException)

    class FakeException(Exception):
        pass

    # Mock `ExecutionPlan.execute()` to raise an exception, to emulate
    # an error in internal Ray Data code.
    with patch(
        "ray.data._internal.plan.ExecutionPlan.execute",
        side_effect=FakeException("fake exception"),
    ):
        with pytest.raises(FakeException, match="fake exception") as exc_info:
            ray.data.range(10).materialize()
            assert isinstance(exc_info, SystemException)
            assert isinstance(exc_info, FakeException)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

import logging
import os
import re
from datetime import datetime
from unittest.mock import patch

import pytest

import ray
from ray.data._internal.dataset_logger import (
    DatasetLogger,
    RayDataInternalException,
    UserCodeException,
    skip_internal_stack_frames,
)
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

    try:
        ray.data.range(10).map(f).take_all()
    except Exception as e:
        ex_internal_skipped = skip_internal_stack_frames(e)
        assert isinstance(ex_internal_skipped, UserCodeException), ex_internal_skipped

    # Mock `ExecutionPlan.execute()`` to raise an exception, to emulate
    # an error in internal Ray Data code.
    with patch(
        "ray.data._internal.plan.ExecutionPlan.execute",
        side_effect=Exception("fake exception"),
    ):
        # Ideally, we would check that `RayDataInternalException` is raised, but the
        # patch above unfortunately cannot trick `skip_internal_stack_frames` into
        # thinking the side_effect comes from the actual `ExecutionPlan.execute()`
        # method, and still believes it's from user code.
        # Instead, we can check that the mocked exception is raised from the
        # side effect message, and confirm the type of the stripped exception
        # is `RayDataInternalException`.
        with pytest.raises(Exception, match="fake exception"):
            try:
                ray.data.range(10).materialize()
            except Exception as e:
                ex_internal_skipped = skip_internal_stack_frames(e)
                assert isinstance(
                    ex_internal_skipped, RayDataInternalException
                ), ex_internal_skipped
                raise ex_internal_skipped


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

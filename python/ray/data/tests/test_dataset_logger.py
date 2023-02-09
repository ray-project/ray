import pytest
from ray.tests.conftest import *  # noqa

import os
import re
import logging

from datetime import datetime

import ray
from ray.data._internal.dataset_logger import DatasetLogger


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
    assert re.match(r"test_dataset_logger.py:\d+", logged_filepath)
    assert logged_msg == msg


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

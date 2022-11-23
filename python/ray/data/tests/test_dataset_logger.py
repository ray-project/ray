import pytest
from ray.tests.conftest import *  # noqa
from unittest.mock import patch, mock_open

import os
import re
import logging

from datetime import datetime
import pytz
from freezegun import freeze_time

import ray
from ray.data._internal.dataset_logger import DatasetLogger


def test_dataset_logger(shutdown_only):
    ray.init()
    log_name, msg = "test_name", "test message 1234"
    with patch("logging.open", mock_open(), create=True) as open_mock:

        logger = DatasetLogger(log_name).logger
        logger.info(msg)

    session_dir = ray._private.worker._global_node.get_session_dir_path()
    log_file_path = os.path.join(session_dir, DatasetLogger.DEFAULT_DATASET_LOG_PATH)
    open_mock.assert_called_with(log_file_path, "a", encoding=None)
    open_mock.return_value.write.assert_called_once()

    # Parse each component of emitted log row
    raw_logged_msg = open_mock.return_value.write.call_args.args[0].strip()
    log_header, logged_msg = raw_logged_msg.split(" -- ")
    logged_ds, logged_ts, logged_info, logged_callpath = log_header.split()

    # Could not use freezegun to test exact timestamp value
    # (values off by some milliseconds), so instead we check
    # for correct timestamp format.
    try:
        datetime.strptime(f"{logged_ds} {logged_ts}", "%Y-%m-%d %H:%M:%S,%f")
    except ValueError:
        raise Exception(f"Invalid log timestamp: {logged_ds} {logged_ts}")

    assert logged_info == logging.getLevelName(logging.INFO)
    assert re.match(r"test_dataset_logger.py:\d+", logged_callpath)
    assert logged_msg == msg


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

import io
import logging
import re
from datetime import datetime

import pytest

import ray
from ray.data._internal.logging import (
    configure_logging,
    get_log_path,
    is_logging_configured,
    reset_logging,
)
from ray.tests.conftest import *  # noqa


@pytest.fixture
def setup_logging():
    if not is_logging_configured():
        configure_logging()
    yield
    reset_logging()


def test_messages_logged_to_file(setup_logging, shutdown_only):
    ray.init()
    logger = logging.getLogger("ray.data.spam")

    logger.debug("ham")

    log_path = get_log_path()
    with open(log_path) as file:
        log_contents = file.read()
    assert "ham" in log_contents


def test_messages_are_propagated(setup_logging):
    base_logger = logging.getLogger("ray")
    stream = io.StringIO()
    base_logger.addHandler(logging.StreamHandler(stream))
    logger = logging.getLogger("ray.data.spam")

    logger.info("ham")

    assert "ham" in stream.getvalue()


def test_message_format(setup_logging, shutdown_only):
    ray.init()
    logger = logging.getLogger("ray.data.spam")

    logger.info("ham")

    log_path = get_log_path()
    with open(log_path, "r") as f:
        log_contents = f.read()
    (
        logged_ds,
        logged_ts,
        logged_level,
        logged_filepath,
        sep,
        logged_msg,
    ) = log_contents.split()

    try:
        datetime.strptime(f"{logged_ds} {logged_ts}", "%Y-%m-%d %H:%M:%S,%f")
    except ValueError:
        raise Exception(f"Invalid log timestamp: {logged_ds} {logged_ts}")

    assert logged_level == logging.getLevelName(logging.INFO)
    assert re.match(r"test_logging.py:\d+", logged_filepath)
    assert logged_msg == "ham"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

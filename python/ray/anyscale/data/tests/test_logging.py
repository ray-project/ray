import logging
import os
from unittest.mock import patch

import pytest

import ray
from ray.anyscale.data._internal.logging import configure_anyscale_logging
from ray.data._internal.logging import configure_logging, get_log_directory
from ray.data.exceptions import UserCodeException
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


@pytest.fixture(name="reset_logging")
def reset_logging_fixture():
    from ray.data._internal.logging import reset_logging

    yield
    reset_logging()


def test_internal_error_logged_to_error_log(reset_logging, ray_start_regular):
    configure_logging()

    class FakeException(Exception):
        pass

    with pytest.raises(FakeException):
        with patch(
            "ray.data._internal.plan.ExecutionPlan.has_computed_output",
            side_effect=FakeException("fake exception"),
        ):
            ray.data.range(1).materialize()

    log_path = os.path.join(get_log_directory(), "ray-data-errors.log")
    with open(log_path) as file:
        assert "Traceback (most recent call last)" in file.read()


def test_user_error_logged_to_error_log(reset_logging, ray_start_regular):
    configure_logging()

    def fn(row):
        assert False

    with pytest.raises(UserCodeException):
        ray.data.range(1).map(fn).materialize()

    log_path = os.path.join(get_log_directory(), "ray-data-errors.log")
    with open(log_path) as file:
        text = file.read()
        assert "Traceback (most recent call last)" in text
        assert "AssertionError" in text


def test_ignored_error_logged_to_error_log(
    reset_logging, ray_start_regular, restore_data_context
):
    configure_logging()

    def fn(row):
        if row["id"] == 0:
            assert False
        return row

    ray.data.DataContext.get_current().max_errored_blocks = 1
    ray.data.range(2, override_num_blocks=2).map(fn).materialize()

    log_path = os.path.join(get_log_directory(), "ray-data-errors.log")
    with open(log_path) as file:
        text = file.read()
        assert "Traceback (most recent call last)" in text
        assert "AssertionError" in text


def test_info_not_logged_to_error_log(reset_logging, ray_start_regular):
    configure_logging()
    logger = logging.getLogger("ray.data.ham")

    logger.info("eggs")

    log_path = os.path.join(get_log_directory(), "ray-data-errors.log")
    if os.path.exists(log_path):
        with open(log_path) as file:
            assert "eggs" not in file.read()


def test_json_logging_configuration(
    capsys, reset_logging, monkeypatch, shutdown_only, propagate_logs
):
    import json

    monkeypatch.setenv("RAY_DATA_LOG_ENCODING", "JSON")
    ray.init()

    configure_anyscale_logging()

    logger = logging.getLogger("ray.data")

    # Ensure handlers correctly setup
    handlers = logger.handlers
    assert sum(handler.name == "file_json" for handler in handlers) == 1
    assert sum(handler.name == "error_file_json" for handler in handlers) == 1
    assert sum(handler.name == "console" for handler in handlers) == 1

    logger.info("ham")
    logger.debug("turkey")
    logger.error("spam")

    log_path = os.path.join(get_log_directory(), "ray-data.log")
    with open(log_path) as file:
        log_contents = file.read()

    error_log_path = os.path.join(get_log_directory(), "ray-data-errors.log")
    with open(error_log_path) as file:
        error_log_contents = file.read()

    # Validate the log is in JSON format (a basic check for JSON)
    messages = []
    for log_line in log_contents.splitlines():
        log_dict = json.loads(log_line)  # will error if not a json line
        messages.append(log_dict["message"])

    error_messages = []
    for log_line in error_log_contents.splitlines():
        log_dict = json.loads(log_line)  # will error if not a json line
        error_messages.append(log_dict["message"])

    assert "ham" in messages
    assert "turkey" in messages
    assert "spam" in error_messages

    # Validate console logs are in text mode
    console_log_output = capsys.readouterr().err
    for log_line in console_log_output.splitlines():
        with pytest.raises(json.JSONDecodeError):
            json.loads(log_line)

    assert "ham" in console_log_output
    assert "spam" in console_log_output
    assert "turkey" not in console_log_output


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

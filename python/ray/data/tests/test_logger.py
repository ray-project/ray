import logging
import os
import re
from datetime import datetime
from unittest.mock import patch

import pytest

import ray
from ray.data._internal.dataset_logger import DatasetLogger
from ray.data.exceptions import SystemException, UserCodeException
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


def check_full_stack_trace_logged_to_file():
    # Checks that the prefix text for the full stack trace is present
    # in the Ray Data log file.
    log_path = ray.data.exceptions.data_exception_logger._datasets_log_path
    with open(log_path, "r") as file:
        data = file.read()
        assert "Full stack trace:" in data


def check_exception_text_logged_to_stdout(text, mock_calls):
    # Checks that the exception text is present in the log output.
    found_exception_call = False
    for call_args in mock_calls:
        if text in call_args.args[0]:
            found_exception_call = True
            break
    assert found_exception_call, (
        f"Searched logs for the following text, but did not find: `{text}` "
        f"All calls: {mock_calls}"
    )


def test_omit_traceback_stdout_user_exception(ray_start_regular_shared):
    def f(x):
        1 / 0
        return x

    def run_with_patch():
        with pytest.raises(UserCodeException) as exc_info:
            ray.data.range(10).map(f).take_all()
        return exc_info

    with patch.object(logging.Logger, "error") as mock_logger:
        exc_info = run_with_patch()

        assert issubclass(exc_info.type, RayTaskError)
        assert issubclass(exc_info.type, UserCodeException)
        assert ZeroDivisionError.__name__ in str(exc_info.value)

        check_exception_text_logged_to_stdout(
            "Exception occurred in user code,", mock_logger.mock_calls
        )

    # To check the output log file, we need to run the code again without
    # the logging method patched, so that the logger actually writes to the
    # log file.
    run_with_patch()
    check_full_stack_trace_logged_to_file()


def test_omit_traceback_stdout_system_exception(ray_start_regular_shared):
    class FakeException(Exception):
        pass

    def run_with_patch():
        with pytest.raises(FakeException) as exc_info:
            with patch(
                (
                    "ray.data._internal.execution.legacy_compat."
                    "get_legacy_lazy_block_list_read_only"
                ),
                side_effect=FakeException("fake exception"),
            ):
                ray.data.range(10).materialize()
            assert issubclass(exc_info.type, FakeException)
            assert issubclass(exc_info.type, SystemException)

    # Mock `ExecutionPlan.execute()` to raise an exception, to emulate
    # an error in internal Ray Data code.
    with patch.object(logging.Logger, "error") as mock_logger:
        run_with_patch()
        check_exception_text_logged_to_stdout(
            "Exception occurred in Ray Data or Ray Core internal code.",
            mock_logger.mock_calls,
        )

    # To check the output log file, we need to run the code again without
    # the logging method patched, so that the logger actually writes to the
    # log file.
    run_with_patch()
    check_full_stack_trace_logged_to_file()


@patch("ray.data.exceptions._is_ray_debugger_enabled", return_value=True)
def test_omit_traceback_skipped_with_ray_debugger(ray_start_regular_shared):
    def f(x):
        1 / 0
        return x

    def run_with_patch():
        with pytest.raises(Exception) as exc_info:
            ray.data.range(10).map(f).take_all()
        return exc_info

    exc_info = run_with_patch()
    assert issubclass(exc_info.type, RayTaskError)
    assert issubclass(exc_info.type, UserCodeException)
    assert ZeroDivisionError.__name__ in str(exc_info.value)

    # To check the output log file, we need to run the code again without
    # the logging method patched, so that the logger actually writes to the
    # log file.
    run_with_patch()
    check_full_stack_trace_logged_to_file()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

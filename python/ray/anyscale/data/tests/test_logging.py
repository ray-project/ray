import logging
import os
from unittest.mock import patch

import pytest

import ray
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

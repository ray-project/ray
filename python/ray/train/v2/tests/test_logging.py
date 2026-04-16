import builtins
import logging
import os

import pytest

import ray
from ray.runtime_context import get_runtime_context
from ray.train.v2._internal.logging import LoggingManager
from ray.train.v2._internal.logging.patch_print import patch_print_function
from ray.train.v2.api.config import LoggingConfig, RunConfig
from ray.train.v2.tests.util import (
    DummyTrainContext,
    create_dummy_run_context,
    create_dummy_train_context,
)


@pytest.fixture(name="worker_logging")
def worker_logging_fixture():
    # Save the current root logger settings
    root_logger = logging.getLogger()
    original_handlers = root_logger.handlers[:]
    original_level = root_logger.level
    # Save the current train logger settings
    train_logger = logging.getLogger("ray.train")
    original_handlers = train_logger.handlers[:]
    original_level = train_logger.level
    # Save the original print function
    original_print = builtins.print
    yield
    # Rest the root logger back to its original state
    root_logger.handlers = original_handlers
    root_logger.setLevel(original_level)
    # Rest the train logger back to its original state
    train_logger.handlers = original_handlers
    train_logger.setLevel(original_level)
    # Rest the print function back to its original state
    builtins.print = original_print


@pytest.fixture(name="controller_logging")
def controller_logging_fixture():
    # Save the current root logger settings
    train_logger = logging.getLogger("ray.train")
    original_handlers = train_logger.handlers[:]
    original_level = train_logger.level
    yield
    # Rest the root logger back to its original state
    train_logger.handlers = original_handlers
    train_logger.setLevel(original_level)


@pytest.fixture(autouse=True)
def ray_start():
    ray.init()
    yield
    ray.shutdown()


def get_log_directory() -> str:
    """
    Return the directory where Ray Train writes log files.
    Only works if Ray is initialized.
    """
    global_node = ray._private.worker._global_node
    session_dir = global_node.get_session_dir_path()
    return os.path.join(session_dir, "logs", "train")


def get_file_contents(file_name: str) -> str:
    log_path = os.path.join(get_log_directory(), file_name)
    with open(log_path, encoding="utf-8") as file:
        log_contents = file.read()
    return log_contents


def test_controller_sys_logged_to_file(controller_logging):
    """
    Test that system messages are logged to the correct file on Controller process.
    """
    LoggingManager.configure_controller_logger(create_dummy_run_context())
    worker_id = get_runtime_context().get_worker_id()
    train_logger = logging.getLogger("ray.train.spam")
    train_logger.info("ham")

    log_contents = get_file_contents(f"ray-train-sys-controller-{worker_id}.log")
    assert "ham" in log_contents


def test_controller_sys_not_logged_to_file(controller_logging):
    """
    Test that system messages are not logged on Controller process when logging not configured.
    """
    worker_id = get_runtime_context().get_worker_id()
    train_logger = logging.getLogger("ray.train.spam")
    train_logger.info("ham")

    with pytest.raises(FileNotFoundError):
        get_file_contents(f"ray-train-sys-controller-{worker_id}.log")


def test_worker_sys_logged_to_file(worker_logging):
    """
    Test that system messages are logged to the correct file on Worker process.
    """
    LoggingManager.configure_worker_logger(create_dummy_train_context())
    worker_id = get_runtime_context().get_worker_id()
    train_logger = logging.getLogger("ray.train.spam")
    train_logger.info("ham")

    log_contents = get_file_contents(f"ray-train-sys-worker-{worker_id}.log")
    assert "ham" in log_contents


def test_worker_sys_not_logged_to_file(worker_logging):
    """
    Test that system messages are not logged on Worker process when logging not configured.
    """
    worker_id = get_runtime_context().get_worker_id()
    train_logger = logging.getLogger("ray.train.spam")
    train_logger.info("ham")

    with pytest.raises(FileNotFoundError):
        get_file_contents(f"ray-train-sys-worker-{worker_id}.log")


def test_worker_app_logged_to_file(worker_logging):
    """
    Test that worker messages are logged to the correct file.
    Only root logger on worker processes is configured with the train context.
    """
    LoggingManager.configure_worker_logger(create_dummy_train_context())
    worker_id = get_runtime_context().get_worker_id()
    root_logger = logging.getLogger()
    # print(root_logger.handlers)
    root_logger.info("ham")

    log_contents = get_file_contents(f"ray-train-app-worker-{worker_id}.log")
    assert "ham" in log_contents


def test_worker_app_print_redirect(worker_logging):
    """Test the print statement can be captured on the worker processes."""
    LoggingManager.configure_worker_logger(create_dummy_train_context())
    patch_print_function()
    worker_id = get_runtime_context().get_worker_id()
    print("ham")

    log_contents = get_file_contents(f"ray-train-app-worker-{worker_id}.log")
    assert "ham" in log_contents, log_contents
    assert "ham\\n" not in log_contents, log_contents


def _create_run_context_with_log_level(log_level):
    """Helper to create a TrainRunContext with a specific log level."""
    return create_dummy_run_context(
        run_config=RunConfig(
            name="test", logging_config=LoggingConfig(log_level=log_level)
        )
    )


def _create_train_context_with_log_level(log_level):
    """Helper to create a DummyTrainContext with a specific log level."""
    ctx = DummyTrainContext()
    ctx.train_run_context = _create_run_context_with_log_level(log_level)
    return ctx


def test_default_log_level_is_info(controller_logging):
    """Test that the default log level is INFO when nothing is configured."""
    context = create_dummy_run_context()
    config = LoggingManager._get_controller_logger_config_dict(context)
    assert config["loggers"]["ray.train"]["level"] == "DEBUG"
    assert config["filters"]["train_log_level_filter"]["log_level"] == "INFO"
    assert "level" not in config["handlers"]["console"]
    assert "level" not in config["handlers"]["file_train_app_controller"]
    assert "level" not in config["handlers"]["file_train_sys_controller"]


def test_log_level_from_run_config_controller(controller_logging):
    """Test that log level from RunConfig applies to the controller via filter."""
    context = _create_run_context_with_log_level("DEBUG")
    config = LoggingManager._get_controller_logger_config_dict(context)
    assert config["loggers"]["ray.train"]["level"] == "DEBUG"
    assert config["filters"]["train_log_level_filter"]["log_level"] == "DEBUG"
    assert "train_log_level_filter" in config["handlers"]["console"]["filters"]
    assert (
        "train_log_level_filter"
        in config["handlers"]["file_train_app_controller"]["filters"]
    )


def test_log_level_from_run_config_worker(worker_logging):
    """Test that log level from RunConfig applies to the worker via filter."""
    context = _create_train_context_with_log_level("DEBUG")
    config = LoggingManager._get_worker_logger_config_dict(context)
    assert config["loggers"]["ray.train"]["level"] == "DEBUG"
    assert config["filters"]["train_log_level_filter"]["log_level"] == "DEBUG"
    assert "train_log_level_filter" in config["handlers"]["console"]["filters"]
    assert (
        "train_log_level_filter"
        in config["handlers"]["file_train_app_worker"]["filters"]
    )
    assert "train_log_level_filter" not in config["handlers"][
        "file_train_sys_worker"
    ].get("filters", [])


def test_sys_handlers_always_capture_all_levels(controller_logging):
    """Test that sys handlers are not affected by the log level filter."""
    context = _create_run_context_with_log_level("WARNING")
    config = LoggingManager._get_controller_logger_config_dict(context)
    assert config["filters"]["train_log_level_filter"]["log_level"] == "WARNING"
    assert "train_log_level_filter" not in config["handlers"][
        "file_train_sys_controller"
    ].get("filters", [])
    assert "level" not in config["handlers"]["file_train_sys_controller"]


def test_invalid_log_level_string_raises():
    """Test that an invalid log level string raises ValueError."""
    with pytest.raises(ValueError, match="Invalid log_level"):
        LoggingConfig(log_level="INVALID_LEVEL")


def test_invalid_log_level_type_raises():
    """Test that a non-string log level raises ValueError."""
    with pytest.raises(ValueError, match="Invalid log_level"):
        LoggingConfig(log_level=10)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

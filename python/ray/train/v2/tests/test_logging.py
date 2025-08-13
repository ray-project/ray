import builtins
import logging
import os

import pytest

import ray
from ray.runtime_context import get_runtime_context
from ray.train.v2._internal.logging import LoggingManager
from ray.train.v2._internal.logging.patch_print import patch_print_function
from ray.train.v2.tests.util import create_dummy_run_context, create_dummy_train_context


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

import builtins
import logging
import os
from unittest.mock import MagicMock

import pytest

import ray
from ray.runtime_context import get_runtime_context
from ray.train import RunConfig
from ray.train.v2._internal.execution.context import TrainContext, TrainRunContext
from ray.train.v2._internal.logging.logging import (
    configure_controller_logger,
    configure_worker_logger,
)
from ray.train.v2._internal.logging.patch_print import patch_print_function


@pytest.fixture(name="worker_logging")
def worker_logging_fixture():
    # Save the current root logger settings
    root_logger = logging.getLogger()
    original_handlers = root_logger.handlers[:]
    original_level = root_logger.level
    # Save the current root logger settings
    train_logger = logging.getLogger("ray.train")
    original_handlers = train_logger.handlers[:]
    original_level = train_logger.level
    # Save the original print function
    original_print = builtins.print
    yield
    # Rest the root logger back to its original state
    root_logger.handlers = original_handlers
    root_logger.setLevel(original_level)
    # Rest the root logger back to its original state
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


def get_dummy_run_context():
    """Create dummy train run context objects for testing."""
    return TrainRunContext(run_config=RunConfig(name="test"))


def get_dummy_train_context():
    """Mock a dummy train context objects for testing."""
    train_context = MagicMock(spec=TrainContext)
    train_context.get_run_config.return_value = RunConfig(name="test")
    train_context.get_world_rank.return_value = 0
    train_context.get_local_rank.return_value = 0
    train_context.get_node_rank.return_value = 0
    return train_context


def get_file_contents(file_name: str) -> str:
    log_path = os.path.join(get_log_directory(), file_name)
    with open(log_path, encoding="utf-8") as file:
        log_contents = file.read()
    return log_contents


@pytest.mark.parametrize("context", [get_dummy_run_context()])
def test_controller_sys_logged_to_file(controller_logging, context):
    """
    Test that system messages are logged to the correct file on Controller process.
    """
    configure_controller_logger(context)
    worker_id = get_runtime_context().get_worker_id()
    train_logger = logging.getLogger("ray.train.spam")
    train_logger.info("ham")

    log_path = os.path.join(
        get_log_directory(), f"ray-train-sys-controller-{worker_id}.log"
    )
    with open(log_path, encoding="utf-8") as file:
        log_contents = file.read()
    assert "ham" in log_contents


@pytest.mark.parametrize("context", [get_dummy_train_context()])
def test_worker_sys_logged_to_file(worker_logging, context):
    """
    Test that system messages are logged to the correct file on Worker process.
    """
    configure_worker_logger(context)
    worker_id = get_runtime_context().get_worker_id()
    train_logger = logging.getLogger("ray.train.spam")
    train_logger.info("ham")

    log_contents = get_file_contents(f"ray-train-sys-worker-{worker_id}.log")
    assert "ham" in log_contents


@pytest.mark.parametrize("context", [get_dummy_train_context()])
def test_worker_app_logged_to_file(worker_logging, context):
    """
    Test that worker messages are logged to the correct file.
    Only root logger on worker processes is configured with the train context.
    """
    configure_worker_logger(context)
    worker_id = get_runtime_context().get_worker_id()
    root_logger = logging.getLogger()
    # print(root_logger.handlers)
    root_logger.info("ham")

    log_contents = get_file_contents(f"ray-train-app-worker-{worker_id}.log")
    assert "ham" in log_contents


@pytest.mark.parametrize("context", [get_dummy_train_context()])
def test_worker_app_print_redirect(worker_logging, context):
    """Test the print statement can be captured on the worker processes."""
    configure_worker_logger(context)
    patch_print_function()
    worker_id = get_runtime_context().get_worker_id()
    print("ham")

    log_contents = get_file_contents(f"ray-train-app-worker-{worker_id}.log")
    assert "ham" in log_contents


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

import builtins
import logging
import os

import pytest

import ray
from ray.runtime_context import get_runtime_context
from ray.train.v2._internal.logging import LoggingManager
from ray.train.v2._internal.logging.logging import TrainLogLevelFilter
from ray.train.v2._internal.logging.patch_print import patch_print_function
from ray.train.v2.api.config import LoggingConfig, RunConfig
from ray.train.v2.tests.util import (
    DummyTrainContext,
    create_dummy_run_context,
    create_dummy_train_context,
)

# ============================================================
# Fixtures
# ============================================================


@pytest.fixture(name="worker_logging")
def worker_logging_fixture():
    root_logger = logging.getLogger()
    original_root_handlers = root_logger.handlers[:]
    original_root_level = root_logger.level
    train_logger = logging.getLogger("ray.train")
    original_train_handlers = train_logger.handlers[:]
    original_train_level = train_logger.level
    original_print = builtins.print
    yield
    root_logger.handlers = original_root_handlers
    root_logger.setLevel(original_root_level)
    train_logger.handlers = original_train_handlers
    train_logger.setLevel(original_train_level)
    builtins.print = original_print


@pytest.fixture(name="controller_logging")
def controller_logging_fixture():
    train_logger = logging.getLogger("ray.train")
    original_handlers = train_logger.handlers[:]
    original_level = train_logger.level
    yield
    train_logger.handlers = original_handlers
    train_logger.setLevel(original_level)


@pytest.fixture(autouse=True)
def ray_start():
    ray.init()
    yield
    ray.shutdown()


# ============================================================
# Helpers
# ============================================================


def get_log_directory() -> str:
    global_node = ray._private.worker._global_node
    session_dir = global_node.get_session_dir_path()
    return os.path.join(session_dir, "logs", "train")


def get_file_contents(file_name: str) -> str:
    log_path = os.path.join(get_log_directory(), file_name)
    with open(log_path, encoding="utf-8") as file:
        return file.read()


def file_exists(file_name: str) -> bool:
    log_path = os.path.join(get_log_directory(), file_name)
    return os.path.exists(log_path)


def _create_run_context_with_log_level(log_level):
    return create_dummy_run_context(
        run_config=RunConfig(
            name="test", logging_config=LoggingConfig(log_level=log_level)
        )
    )


def _create_train_context_with_log_level(log_level):
    ctx = DummyTrainContext()
    ctx.train_run_context = _create_run_context_with_log_level(log_level)
    return ctx


# ============================================================
# Validation tests
# ============================================================


def test_invalid_log_level_string_raises():
    """Test that an invalid log level string raises ValueError."""
    with pytest.raises(ValueError, match="Invalid log_level"):
        LoggingConfig(log_level="INVALID_LEVEL")


def test_invalid_log_level_type_raises():
    """Test that a non-string log level raises ValueError."""
    with pytest.raises(ValueError, match="Invalid log_level"):
        LoggingConfig(log_level=10)


def test_log_level_normalized_to_uppercase():
    """Test that log_level is normalized to uppercase after construction."""
    config = LoggingConfig(log_level="debug")
    assert config.log_level == "DEBUG"


# ============================================================
# Config structure tests
# ============================================================


def test_default_log_level_is_info(controller_logging):
    """Test that the default log level filter is INFO when nothing is configured."""
    context = create_dummy_run_context()
    config = LoggingManager._get_controller_logger_config_dict(context)
    assert config["loggers"]["ray.train"]["level"] == "DEBUG"
    assert config["filters"]["train_log_level_filter"]["log_level"] == "INFO"


def test_log_level_filter_on_controller_handlers(controller_logging):
    """Test that the log level filter is attached to console and app handlers
    but not to sys handlers on the controller."""
    context = _create_run_context_with_log_level("WARNING")
    config = LoggingManager._get_controller_logger_config_dict(context)
    assert config["filters"]["train_log_level_filter"]["log_level"] == "WARNING"
    assert "train_log_level_filter" in config["handlers"]["console"]["filters"]
    assert (
        "train_log_level_filter"
        in config["handlers"]["file_train_app_controller"]["filters"]
    )
    assert "train_log_level_filter" not in config["handlers"][
        "file_train_sys_controller"
    ].get("filters", [])


def test_log_level_filter_on_worker_handlers(worker_logging):
    """Test that the log level filter is attached to console and app handlers
    but not to sys handlers on the worker."""
    context = _create_train_context_with_log_level("WARNING")
    config = LoggingManager._get_worker_logger_config_dict(context)
    assert config["filters"]["train_log_level_filter"]["log_level"] == "WARNING"
    assert "train_log_level_filter" in config["handlers"]["console"]["filters"]
    assert (
        "train_log_level_filter"
        in config["handlers"]["file_train_app_worker"]["filters"]
    )
    assert "train_log_level_filter" not in config["handlers"][
        "file_train_sys_worker"
    ].get("filters", [])


def test_no_handler_level_set(controller_logging):
    """Test that no handler has an explicit 'level' key. Log level filtering
    is done entirely through TrainLogLevelFilter."""
    context = _create_run_context_with_log_level("DEBUG")
    config = LoggingManager._get_controller_logger_config_dict(context)
    for handler_name, handler_config in config["handlers"].items():
        assert (
            "level" not in handler_config
        ), f"Handler '{handler_name}' should not have an explicit level"


def test_ray_train_logger_propagate_false(worker_logging):
    """Test that the ray.train logger has propagate=False after configuration,
    which is the foundation for isolation from the ray and root loggers."""
    context = create_dummy_run_context()
    config = LoggingManager._get_controller_logger_config_dict(context)
    assert config["loggers"]["ray.train"]["propagate"] is False

    context = _create_train_context_with_log_level("INFO")
    config = LoggingManager._get_worker_logger_config_dict(context)
    assert config["loggers"]["ray.train"]["propagate"] is False


# ============================================================
# TrainLogLevelFilter unit tests
# ============================================================


def test_filter_blocks_low_level_train_logs():
    """Test that the filter blocks ray.train logs below the configured level."""
    f = TrainLogLevelFilter("WARNING")
    record = logging.LogRecord("ray.train.foo", logging.INFO, "", 0, "msg", (), None)
    assert not f.filter(record)


def test_filter_passes_high_level_train_logs():
    """Test that the filter passes ray.train logs at or above the configured level."""
    f = TrainLogLevelFilter("WARNING")
    record = logging.LogRecord("ray.train.foo", logging.WARNING, "", 0, "msg", (), None)
    assert f.filter(record)


def test_filter_passes_user_logs_regardless_of_level():
    """Test that non-ray.train logs (user application logs) always pass through."""
    f = TrainLogLevelFilter("WARNING")
    record = logging.LogRecord("myapp", logging.DEBUG, "", 0, "msg", (), None)
    assert f.filter(record)


# ============================================================
# Controller log routing tests
# ============================================================


def test_controller_no_logs_without_configuration(controller_logging):
    """Test that no log files are created when logging is not configured."""
    worker_id = get_runtime_context().get_worker_id()
    train_logger = logging.getLogger("ray.train.spam")
    train_logger.info("ham")

    assert not file_exists(f"ray-train-sys-controller-{worker_id}.log")
    assert not file_exists(f"ray-train-app-controller-{worker_id}.log")


def test_controller_train_logs_routed_to_all_files(controller_logging):
    """Test that ray.train INFO logs appear in both sys and app controller files."""
    LoggingManager.configure_controller_logger(create_dummy_run_context())
    worker_id = get_runtime_context().get_worker_id()

    train_logger = logging.getLogger("ray.train.spam")
    train_logger.info("info_message")

    sys_contents = get_file_contents(f"ray-train-sys-controller-{worker_id}.log")
    app_contents = get_file_contents(f"ray-train-app-controller-{worker_id}.log")
    assert "info_message" in sys_contents
    assert "info_message" in app_contents


def test_controller_debug_only_in_sys_when_log_level_warning(controller_logging):
    """Test that DEBUG ray.train logs appear in the sys file but are filtered
    from the app file when log_level is WARNING."""
    context = _create_run_context_with_log_level("WARNING")
    LoggingManager.configure_controller_logger(context)
    worker_id = get_runtime_context().get_worker_id()

    train_logger = logging.getLogger("ray.train.spam")
    train_logger.debug("debug_message")
    train_logger.warning("warning_message")

    sys_contents = get_file_contents(f"ray-train-sys-controller-{worker_id}.log")
    app_contents = get_file_contents(f"ray-train-app-controller-{worker_id}.log")

    # Sys file captures everything
    assert "debug_message" in sys_contents
    assert "warning_message" in sys_contents

    # App file only captures WARNING and above
    assert "debug_message" not in app_contents
    assert "warning_message" in app_contents


# ============================================================
# Worker log routing tests
# ============================================================


def test_worker_no_logs_without_configuration(worker_logging):
    """Test that no log files are created when logging is not configured."""
    worker_id = get_runtime_context().get_worker_id()
    train_logger = logging.getLogger("ray.train.spam")
    train_logger.info("ham")

    assert not file_exists(f"ray-train-sys-worker-{worker_id}.log")
    assert not file_exists(f"ray-train-app-worker-{worker_id}.log")


def test_worker_train_logs_routed_to_all_files(worker_logging):
    """Test that ray.train INFO logs appear in both sys and app worker files."""
    LoggingManager.configure_worker_logger(create_dummy_train_context())
    worker_id = get_runtime_context().get_worker_id()

    train_logger = logging.getLogger("ray.train.spam")
    train_logger.info("info_message")

    sys_contents = get_file_contents(f"ray-train-sys-worker-{worker_id}.log")
    app_contents = get_file_contents(f"ray-train-app-worker-{worker_id}.log")
    assert "info_message" in sys_contents
    assert "info_message" in app_contents


def test_worker_debug_only_in_sys_when_log_level_warning(worker_logging):
    """Test that DEBUG ray.train logs appear in the sys file but are filtered
    from the app file when log_level is WARNING."""
    context = _create_train_context_with_log_level("WARNING")
    LoggingManager.configure_worker_logger(context)
    worker_id = get_runtime_context().get_worker_id()

    train_logger = logging.getLogger("ray.train.spam")
    train_logger.debug("debug_message")
    train_logger.warning("warning_message")

    sys_contents = get_file_contents(f"ray-train-sys-worker-{worker_id}.log")
    app_contents = get_file_contents(f"ray-train-app-worker-{worker_id}.log")

    # Sys file captures everything
    assert "debug_message" in sys_contents
    assert "warning_message" in sys_contents

    # App file only captures WARNING and above
    assert "debug_message" not in app_contents
    assert "warning_message" in app_contents


def test_worker_user_logs_in_app_not_sys(worker_logging):
    """Test that user application logs (root logger) appear in the app file
    but not in the sys file, and are not filtered by log_level."""
    context = _create_train_context_with_log_level("WARNING")
    LoggingManager.configure_worker_logger(context)
    worker_id = get_runtime_context().get_worker_id()

    # Emit a ray.train log to ensure the sys file is created
    train_logger = logging.getLogger("ray.train.spam")
    train_logger.info("train_log")

    root_logger = logging.getLogger()
    root_logger.info("user_info_message")

    app_contents = get_file_contents(f"ray-train-app-worker-{worker_id}.log")
    sys_contents = get_file_contents(f"ray-train-sys-worker-{worker_id}.log")

    # User logs appear in app file, unaffected by log_level
    assert "user_info_message" in app_contents
    # User logs do not appear in sys file (sys is only for ray.train)
    assert "user_info_message" not in sys_contents


def test_worker_print_redirect(worker_logging):
    """Test that print statements are captured in the worker app log file."""
    LoggingManager.configure_worker_logger(create_dummy_train_context())
    patch_print_function()
    worker_id = get_runtime_context().get_worker_id()
    print("ham")

    log_contents = get_file_contents(f"ray-train-app-worker-{worker_id}.log")
    assert "ham" in log_contents, log_contents
    assert "ham\\n" not in log_contents, log_contents


# ============================================================
# Isolation from ray.init tests
# ============================================================


def test_ray_init_log_level_does_not_affect_train(controller_logging):
    """Test that ray.init(logging_config=LoggingConfig(log_level=...))
    does not change the ray.train logger configuration."""
    ray.shutdown()
    ray.init(logging_config=ray.LoggingConfig(log_level="WARNING"))

    context = create_dummy_run_context()
    config = LoggingManager._get_controller_logger_config_dict(context)

    # ray.train logger should still be DEBUG regardless of ray.init setting
    assert config["loggers"]["ray.train"]["level"] == "DEBUG"
    # Default Train filter should be INFO, not WARNING
    assert config["filters"]["train_log_level_filter"]["log_level"] == "INFO"


def test_train_log_level_does_not_affect_ray_logger(worker_logging):
    """Test that setting ray.train LoggingConfig does not change the ray logger."""
    ray_logger = logging.getLogger("ray")
    original_level = ray_logger.level

    context = _create_train_context_with_log_level("DEBUG")
    LoggingManager.configure_worker_logger(context)

    assert ray_logger.level == original_level


@pytest.mark.parametrize("log_level", ["DEBUG", "WARNING", "ERROR"])
def test_train_log_level_does_not_affect_root_logger_level(worker_logging, log_level):
    """Test that the root logger level is always INFO regardless of log_level."""
    context = _create_train_context_with_log_level(log_level)
    config = LoggingManager._get_worker_logger_config_dict(context)
    assert config["root"]["level"] == "INFO"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

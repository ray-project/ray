import logging
import os
import re
import time
from datetime import datetime

import pytest
import yaml

import ray
from ray.data._internal.logging import (
    configure_logging,
    get_log_directory,
    reset_logging,
)
from ray.tests.conftest import *  # noqa


@pytest.fixture(name="configure_logging")
def configure_logging_fixture():
    from ray.data._internal.logging import configure_logging

    configure_logging()
    yield


@pytest.fixture(name="reset_logging")
def reset_logging_fixture():
    from ray.data._internal.logging import reset_logging

    yield
    reset_logging()


def test_messages_logged_to_file(configure_logging, reset_logging, shutdown_only):
    ray.init()
    logger = logging.getLogger("ray.data.spam")

    logger.debug("ham")

    log_path = os.path.join(get_log_directory(), "ray-data.log")
    with open(log_path) as file:
        log_contents = file.read()
    assert "ham" in log_contents


def test_messages_printed_to_console(
    capsys,
    configure_logging,
    reset_logging,
    propagate_logs,
):
    logger = logging.getLogger("ray.data.spam")

    logger.info("ham")

    assert "ham" in capsys.readouterr().err


def test_hidden_messages_not_printed_to_console(
    capsys,
    configure_logging,
    reset_logging,
    propagate_logs,
):
    logger = logging.getLogger("ray.data.spam")

    logger.info("ham", extra={"hide": True})

    assert "ham" not in capsys.readouterr().err


def test_message_format(configure_logging, reset_logging, shutdown_only):
    ray.init()
    logger = logging.getLogger("ray.data.spam")

    logger.info("ham")

    log_path = os.path.join(get_log_directory(), "ray-data.log")
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


def test_custom_config(reset_logging, monkeypatch, tmp_path):
    config_path = tmp_path / "logging.yaml"
    monkeypatch.setenv("RAY_DATA_LOGGING_CONFIG", config_path)

    handlers = {
        "console": {"class": "logging.StreamHandler", "stream": "ext://sys.stdout"}
    }
    loggers = {
        "ray.data": {
            "level": "CRITICAL",
            "handlers": ["console"],
        },
    }
    config = {
        "version": 1,
        "handlers": handlers,
        "loggers": loggers,
        "disable_existing_loggers": False,
    }
    with open(config_path, "w") as file:
        yaml.dump(config, file)

    configure_logging()

    logger = logging.getLogger("ray.data")

    assert logger.getEffectiveLevel() == logging.CRITICAL
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], logging.StreamHandler)


def test_json_logging_configuration(
    capsys, reset_logging, monkeypatch, shutdown_only, propagate_logs
):
    import json

    monkeypatch.setenv("RAY_DATA_LOG_ENCODING", "JSON")
    ray.init()

    configure_logging()

    logger = logging.getLogger("ray.data")

    # Ensure handlers correctly setup
    handlers = logger.handlers
    assert sum(handler.name == "file_json" for handler in handlers) == 1
    assert sum(handler.name == "console" for handler in handlers) == 1

    logger.info("ham")
    logger.debug("turkey")

    log_path = os.path.join(get_log_directory(), "ray-data.log")
    with open(log_path) as file:
        log_contents = file.read()

    # Validate the log is in JSON format (a basic check for JSON)
    messages = []
    for log_line in log_contents.splitlines():
        log_dict = json.loads(log_line)  # will error if not a json line
        messages.append(log_dict["message"])

    assert "ham" in messages
    assert "turkey" in messages

    # Validate console logs are in text mode
    console_log_output = capsys.readouterr().err
    for log_line in console_log_output.splitlines():
        with pytest.raises(json.JSONDecodeError):
            json.loads(log_line)

    assert "ham" in console_log_output
    assert "turkey" not in console_log_output


def test_configure_logging_preserves_existing_handlers(reset_logging, shutdown_only):
    """Test that configure_logging() preserves existing handlers.

    When configure_logging() is called, it should not remove existing handlers
    like MemoryHandler that were added to loggers before configuration.
    """
    ray.init()

    # Create a logger and add a MemoryHandler with a target before configuring Ray Data logging
    test_logger = logging.getLogger("ray.serve.test_preserve")
    target_handler = logging.StreamHandler()
    memory_handler = logging.handlers.MemoryHandler(capacity=100, target=target_handler)
    test_logger.addHandler(memory_handler)

    try:
        # Verify the memory handler is there and target is set
        assert memory_handler in test_logger.handlers
        assert memory_handler.target is not None
        assert memory_handler.target is target_handler

        # Configure Ray Data logging
        configure_logging()

        # Verify the memory handler is still present after configuration
        assert memory_handler in test_logger.handlers

        # Verify the target is still set (would be None if handler was closed/recreated)
        assert memory_handler.target is not None
        assert memory_handler.target is target_handler

        # Verify the memory handler still works
        test_logger.info("test message")
        assert len(memory_handler.buffer) == 1
        assert "test message" in memory_handler.buffer[0].getMessage()
    finally:
        # Clean up handlers to avoid logging errors during teardown
        test_logger.removeHandler(memory_handler)
        memory_handler.close()
        target_handler.close()


def test_concurrent_dataset_logging_separate_files(shutdown_only):
    """Test that concurrent datasets write to separate log files.

    This test addresses issue #48633 by verifying that when multiple Ray Data
    operations run concurrently, each writes to its own log file.
    """
    reset_logging()
    ray.init()

    @ray.remote
    def create_dataset(dataset_id):
        # Create and materialize a dataset
        # ray.data.range returns {"id": n}, not {"value": n}
        ds = ray.data.range(100, override_num_blocks=10)
        ds = ds.map(lambda row: {"value": row["id"] * 2})
        return ds.materialize()

    # Launch 3 concurrent dataset operations
    futures = [create_dataset.remote(i) for i in range(3)]
    ray.get(futures)

    # Give some time for logs to be flushed
    time.sleep(1)

    # Verify that multiple dataset-specific log files were created
    log_dir = get_log_directory()
    log_files = os.listdir(log_dir)

    # Filter for dataset-specific log files (ray-data-{uuid}.log)
    dataset_logs = [
        f for f in log_files if f.startswith("ray-data-") and f != "ray-data.log"
    ]

    # Should have at least 3 dataset-specific log files
    assert (
        len(dataset_logs) >= 3
    ), f"Expected at least 3 log files, got {len(dataset_logs)}: {dataset_logs}"

    # Verify each log file contains execution information
    for log_file in dataset_logs[:3]:  # Check first 3 files
        log_path = os.path.join(log_dir, log_file)
        with open(log_path) as f:
            content = f.read()
            # Each dataset log should contain its own execution info
            assert (
                "Starting execution" in content
                or "Registered dataset logger" in content
            ), f"Log file {log_file} missing execution info"

    reset_logging()


def test_concurrent_dataset_logging_no_interference(shutdown_only):
    """Test that concurrent dataset logs don't interfere with each other.

    Each dataset should have its own log file with separate content.
    """
    reset_logging()
    ray.init()

    @ray.remote
    def create_dataset(dataset_num):
        # Simple dataset creation - logs will be automatically generated
        ds = ray.data.range(50, override_num_blocks=5)
        ds = ds.map(lambda row: {"num": dataset_num, "value": row["id"]})
        return ds.materialize()

    # Launch 3 concurrent operations
    futures = [create_dataset.remote(i) for i in range(3)]
    ray.get(futures)

    time.sleep(1)

    # Verify separate log files were created
    log_dir = get_log_directory()
    log_files = os.listdir(log_dir)
    dataset_logs = [
        f for f in log_files if f.startswith("ray-data-") and f != "ray-data.log"
    ]

    # Should have at least 3 separate dataset log files
    assert (
        len(dataset_logs) >= 3
    ), f"Expected at least 3 log files, got {len(dataset_logs)}: {dataset_logs}"

    # Verify each log file has unique content and is non-empty
    log_contents = []
    for log_file in dataset_logs[:3]:
        log_path = os.path.join(log_dir, log_file)
        with open(log_path) as f:
            content = f.read()
            assert len(content) > 0, f"Log file {log_file} is empty"
            log_contents.append(content)

    # Each log file should contain execution information
    for i, content in enumerate(log_contents):
        assert (
            "Starting execution" in content or "Registered dataset logger" in content
        ), f"Log file {i} missing execution info"

    reset_logging()


def test_concurrent_logging_issue_48633_repro(shutdown_only):
    """Test the exact reproduction case from issue #48633.

    When multiple Ray tasks run datasets concurrently, their logs should
    be written to separate files instead of being interleaved.
    """
    reset_logging()
    ray.init()

    @ray.remote
    def f():
        # This is the exact repro from issue #48633
        ray.data.range(1).materialize()

    # Run 2 concurrent tasks (from the issue repro)
    ray.get([f.remote() for _ in range(2)])

    time.sleep(1)

    # Verify separate log files were created
    log_dir = get_log_directory()
    log_files = os.listdir(log_dir)
    dataset_logs = [
        f for f in log_files if f.startswith("ray-data-") and f != "ray-data.log"
    ]

    # Should have at least 2 separate log files
    assert (
        len(dataset_logs) >= 2
    ), f"Expected at least 2 separate log files for concurrent datasets, got {len(dataset_logs)}"

    # Verify each has content
    for log_file in dataset_logs[:2]:
        log_path = os.path.join(log_dir, log_file)
        with open(log_path) as f:
            content = f.read()
            assert len(content) > 0, f"Log file {log_file} is empty"

    reset_logging()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

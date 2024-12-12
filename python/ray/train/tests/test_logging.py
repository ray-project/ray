import logging
import os

import pytest
import yaml

import ray
from ray.train._internal.logging import (
    LOG_CONFIG_PATH_ENV,
    LOG_ENCODING_ENV,
    configure_logging,
    get_log_directory,
)


@pytest.fixture(name="configure_logging")
def configure_logging_fixture():
    from ray.train._internal.logging import configure_logging

    configure_logging()
    yield


@pytest.fixture(name="reset_logging")
def reset_logging_fixture():
    yield
    logger = logging.getLogger("ray.train")
    logger.handlers.clear()
    logger.setLevel(logging.NOTSET)


def test_messages_logged_to_file(configure_logging, reset_logging, shutdown_only):
    ray.init()
    logger = logging.getLogger("ray.train.spam")

    logger.debug("ham")
    log_path = os.path.join(get_log_directory(), "ray-train.log")
    with open(log_path) as file:
        log_contents = file.read()
    assert "ham" in log_contents


def test_messages_printed_to_console(
    capsys,
    configure_logging,
    reset_logging,
    propagate_logs,
):
    logger = logging.getLogger("ray.train.spam")

    logger.info("ham")

    assert "ham" in capsys.readouterr().err


def test_hidden_messages_not_printed_to_console(
    capsys,
    configure_logging,
    reset_logging,
    propagate_logs,
):
    logger = logging.getLogger("ray.train.spam")

    logger.info("ham", extra={"hide": True})

    assert "ham" not in capsys.readouterr().err


def test_custom_config(reset_logging, monkeypatch, tmp_path):
    config_path = tmp_path / "logging.yaml"
    monkeypatch.setenv(LOG_CONFIG_PATH_ENV, config_path)

    handlers = {
        "console": {"class": "logging.StreamHandler", "stream": "ext://sys.stdout"}
    }
    loggers = {
        "ray.train": {
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

    logger = logging.getLogger("ray.train")

    assert logger.getEffectiveLevel() == logging.CRITICAL
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], logging.StreamHandler)


def test_json_logging_configuration(
    capsys, reset_logging, monkeypatch, shutdown_only, propagate_logs
):
    import json

    monkeypatch.setenv(LOG_ENCODING_ENV, "JSON")
    ray.init()

    configure_logging()

    logger = logging.getLogger("ray.train")

    # Ensure handlers correctly setup
    handlers = logger.handlers
    assert sum(handler.name == "file" for handler in handlers) == 1
    assert sum(handler.name == "console_json" for handler in handlers) == 1

    logger.info("ham")
    logger.debug("turkey")

    log_path = os.path.join(get_log_directory(), "ray-train.log")
    with open(log_path) as file:
        log_contents = file.read()

    # Validate the log is in JSON format (a basic check for JSON)
    messages = []
    for log_line in log_contents.splitlines():
        log_dict = json.loads(log_line)  # will error if not a json line
        messages.append(log_dict["message"])

    assert "ham" in messages
    assert "turkey" in messages


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

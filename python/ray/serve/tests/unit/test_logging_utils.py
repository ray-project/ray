import logging
import logging.config
import logging.handlers
import sys
from types import SimpleNamespace

import pytest

import ray
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.logging_utils import (
    configure_component_logger,
    get_component_file_name,
    get_component_logger_file_path,
)
from ray.serve.schema import LoggingConfig


@pytest.fixture
def serve_logger_in(tmp_path, monkeypatch):
    """Configure the Serve component logger to write under tmp_path, which also
    stands in for the Ray logs directory."""
    monkeypatch.setattr(
        ray._private.worker,
        "_global_node",
        SimpleNamespace(get_logs_dir_path=lambda: str(tmp_path)),
        raising=False,
    )
    logger = logging.getLogger(SERVE_LOGGER_NAME)
    saved_handlers, saved_propagate = list(logger.handlers), logger.propagate
    yield tmp_path
    for handler in logger.handlers:
        handler.close()
    logger.handlers[:] = saved_handlers
    logger.propagate = saved_propagate


def _configure(logs_dir, **kwargs):
    configure_component_logger(
        component_name="test_component",
        component_id="abc123",
        logging_config=LoggingConfig(logs_dir=str(logs_dir)),
        max_bytes=1024,
        backup_count=1,
        **kwargs,
    )


def test_logger_file_path_survives_dictconfig(serve_logger_in):
    """A library calling logging.config.dictConfig must not break the lookup.

    dictConfig closes every existing handler, which leaves Serve's MemoryHandler
    attached with target=None. vLLM does this at startup, so in an LLM replica
    Replica.is_allocated() used to raise AttributeError and the controller
    stopped healthy replicas during recovery.
    """
    _configure(serve_logger_in)
    expected = "/" + get_component_file_name(
        component_name="test_component",
        component_id="abc123",
        component_type=None,
        suffix=".log",
    )
    assert get_component_logger_file_path() == expected

    logging.config.dictConfig(
        {"version": 1, "disable_existing_loggers": False, "loggers": {"vllm": {}}}
    )
    memory_handlers = [
        h
        for h in logging.getLogger(SERVE_LOGGER_NAME).handlers
        if isinstance(h, logging.handlers.MemoryHandler)
    ]
    assert memory_handlers and memory_handlers[0].target is None

    assert get_component_logger_file_path() == expected


def test_logger_file_path_is_none_for_closed_unrecorded_handler(serve_logger_in):
    """A closed MemoryHandler the path was not recorded for gives None, not an error."""
    _configure(serve_logger_in, stream_handler_only=True)
    handler = logging.handlers.MemoryHandler(capacity=1, target=None)
    logging.getLogger(SERVE_LOGGER_NAME).addHandler(handler)

    assert get_component_logger_file_path() is None


def test_logger_file_path_is_none_for_stream_only(serve_logger_in):
    _configure(serve_logger_in, stream_handler_only=True)

    assert get_component_logger_file_path() is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))

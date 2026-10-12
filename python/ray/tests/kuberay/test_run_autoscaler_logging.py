import io
import logging
import os
import sys

import pytest

from ray._private import ray_constants
from ray.autoscaler._private.kuberay.run_autoscaler import _setup_logging


@pytest.fixture
def clean_root_logger():
    """Snapshot and restore the global logging state that _setup_logging mutates."""
    root = logging.root
    saved_handlers = root.handlers[:]
    saved_level = root.level
    ray_logger = logging.getLogger("ray")
    saved_ray_handlers = ray_logger.handlers[:]
    saved_ray_propagate = ray_logger.propagate
    root.handlers.clear()
    try:
        yield
    finally:
        # _setup_logging attaches monitor.log's RotatingFileHandler to the root logger,
        # so closing everything here also closes that file handle.
        for handler in root.handlers:
            handler.close()
        root.handlers[:] = saved_handlers
        root.setLevel(saved_level)
        ray_logger.handlers[:] = saved_ray_handlers
        ray_logger.propagate = saved_ray_propagate


@pytest.fixture
def emit_records(tmp_path, clean_root_logger, monkeypatch):
    """Return a function that runs _setup_logging and emits records at given levels.

    The returned function accepts the levels to emit plus a ``logger_level``, and returns
    ``(stdout_text, stderr_text, monitor_log_text)``.

    ``logger_level`` is always applied, so the tests do not depend on the ambient
    ``ray_constants.LOGGER_LEVEL``, which honors ``RAY_LOGGER_LEVEL`` from the
    environment.
    """

    def run(*levels, logger_level="info"):
        monkeypatch.setattr(ray_constants, "LOGGER_LEVEL", logger_level)
        stdout, stderr = io.StringIO(), io.StringIO()
        real_stdout, real_stderr = sys.stdout, sys.stderr
        sys.stdout, sys.stderr = stdout, stderr
        try:
            _setup_logging(str(tmp_path))
            logger = logging.getLogger("ray.autoscaler._private.kuberay.run_autoscaler")
            for level in levels:
                logger.log(level, "%s_RECORD", logging.getLevelName(level))
            for handler in logging.root.handlers:
                handler.flush()
        finally:
            sys.stdout, sys.stderr = real_stdout, real_stderr

        monitor_log = os.path.join(str(tmp_path), ray_constants.MONITOR_LOG_FILE_NAME)
        with open(monitor_log) as f:
            return stdout.getvalue(), stderr.getvalue(), f.read()

    return run


ALL_LEVELS = (
    logging.DEBUG,
    logging.INFO,
    logging.WARNING,
    logging.ERROR,
    logging.CRITICAL,
)


def test_info_goes_to_stdout(emit_records):
    """INFO must not go to stderr.

    Container log collectors derive a line's severity from the stream it arrived on, so
    INFO on stderr makes routine autoscaler output look like errors.
    """
    stdout, stderr, _ = emit_records(logging.INFO)
    assert "INFO_RECORD" in stdout
    assert "INFO_RECORD" not in stderr


def test_warning_and_above_go_to_stderr(emit_records):
    stdout, stderr, _ = emit_records(logging.WARNING, logging.ERROR, logging.CRITICAL)
    for name in ("WARNING", "ERROR", "CRITICAL"):
        assert f"{name}_RECORD" in stderr
        assert f"{name}_RECORD" not in stdout


def test_monitor_log_still_receives_all_levels(emit_records):
    """monitor.log backs the dashboard and the State API, so it must keep everything."""
    _, _, monitor_log = emit_records(*ALL_LEVELS)
    for name in ("INFO", "WARNING", "ERROR", "CRITICAL"):
        assert f"{name}_RECORD" in monitor_log


def test_default_level_filters_debug(emit_records):
    """At the default level of INFO, DEBUG is dropped everywhere."""
    stdout, stderr, monitor_log = emit_records(*ALL_LEVELS, logger_level="info")
    assert "DEBUG_RECORD" not in stdout
    assert "DEBUG_RECORD" not in stderr
    assert "DEBUG_RECORD" not in monitor_log


@pytest.mark.parametrize(
    "logger_level,expected_stdout_level,expected_stderr_level",
    [
        ("debug", logging.DEBUG, logging.WARNING),
        ("info", logging.INFO, logging.WARNING),
        ("warning", logging.WARNING, logging.WARNING),
        ("error", logging.ERROR, logging.ERROR),
        ("critical", logging.CRITICAL, logging.CRITICAL),
    ],
)
def test_handler_types_and_levels_track_logger_level(
    tmp_path,
    clean_root_logger,
    monkeypatch,
    logger_level,
    expected_stdout_level,
    expected_stderr_level,
):
    """stdout uses StreamHandler; stderr stays lazy and floors at WARNING.

    This asserts handler configuration rather than emitted output, because the "ray"
    logger carries its own explicit level and that, not the root logger's level, decides
    whether a sub-INFO record gets created at all.
    """
    monkeypatch.setattr(ray_constants, "LOGGER_LEVEL", logger_level)
    _setup_logging(str(tmp_path))
    handlers = [
        h
        for h in logging.root.handlers
        if getattr(h, "stream", None) in (sys.stdout, sys.stderr)
    ]
    by_stream = {
        "stdout": next(h for h in handlers if h.stream is sys.stdout),
        "stderr": next(h for h in handlers if h.stream is sys.stderr),
    }
    assert len(handlers) == 2
    assert type(by_stream["stdout"]) is logging.StreamHandler
    assert type(by_stream["stderr"]) is logging._StderrHandler
    assert by_stream["stdout"].level == expected_stdout_level
    assert by_stream["stderr"].level == expected_stderr_level


@pytest.mark.parametrize("logger_level", ["error", "critical"])
def test_level_stricter_than_warning_suppresses_warnings(emit_records, logger_level):
    """A LOGGER_LEVEL above WARNING must still be honored on stderr."""
    stdout, stderr, _ = emit_records(*ALL_LEVELS, logger_level=logger_level)
    assert "WARNING_RECORD" not in stderr
    assert "WARNING_RECORD" not in stdout
    assert "CRITICAL_RECORD" in stderr
    assert stdout == ""


def test_split_is_numeric_not_name_based(emit_records):
    """A custom level between INFO and WARNING routes to stdout.

    Level 25 is deliberately left unregistered so this does not mutate the global level
    name table; logging renders it as "Level 25".
    """
    stdout, stderr, _ = emit_records(25, logging.WARNING)
    assert "Level 25_RECORD" in stdout
    assert "Level 25_RECORD" not in stderr


def test_invalid_logger_level_raises(emit_records):
    """An unrecognized RAY_LOGGER_LEVEL fails loudly, as it did before the split."""
    with pytest.raises(ValueError, match="Unknown level"):
        emit_records(logging.INFO, logger_level="bogus")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))

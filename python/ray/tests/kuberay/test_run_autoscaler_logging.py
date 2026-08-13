import io
import logging
import os
import sys

import pytest

from ray._private import ray_constants
from ray.autoscaler._private.kuberay.run_autoscaler import (
    _LazyStreamHandler,
    _setup_logging,
)


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

    The returned function accepts an optional ``logger_level`` to override
    ``ray_constants.LOGGER_LEVEL``, plus the levels to emit, and returns
    ``(stdout_text, stderr_text, monitor_log_text)``.
    """

    def run(*levels, logger_level=None):
        if logger_level is not None:
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
    """The default level is INFO, so DEBUG is dropped everywhere."""
    assert ray_constants.LOGGER_LEVEL == "info"
    stdout, stderr, monitor_log = emit_records(*ALL_LEVELS)
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
def test_handler_levels_track_logger_level(
    tmp_path,
    clean_root_logger,
    monkeypatch,
    logger_level,
    expected_stdout_level,
    expected_stderr_level,
):
    """stdout follows LOGGER_LEVEL; stderr follows it but never drops below WARNING.

    This asserts handler configuration rather than emitted output, because the "ray"
    logger carries its own explicit level and that, not the root logger's level, decides
    whether a sub-INFO record gets created at all.
    """
    monkeypatch.setattr(ray_constants, "LOGGER_LEVEL", logger_level)
    _setup_logging(str(tmp_path))
    handlers = [h for h in logging.root.handlers if isinstance(h, _LazyStreamHandler)]
    by_stream = {
        "stdout": next(h for h in handlers if h.stream is sys.stdout),
        "stderr": next(h for h in handlers if h.stream is sys.stderr),
    }
    assert len(handlers) == 2
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


def test_lazy_stream_handler_follows_stream_reassignment():
    """The handler must resolve sys.stdout at emit time, not construction time.

    ray#33652 chose logging._StderrHandler so handlers keep working when the streams are
    replaced after construction. _LazyStreamHandler preserves that for stdout too.
    """
    handler = _LazyStreamHandler("stdout")
    original, replacement = sys.stdout, io.StringIO()
    assert handler.stream is original
    sys.stdout = replacement
    try:
        assert handler.stream is replacement
        handler.emit(
            logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname=__file__,
                lineno=1,
                msg="REDIRECTED",
                args=(),
                exc_info=None,
            )
        )
    finally:
        sys.stdout = original
    assert "REDIRECTED" in replacement.getvalue()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))

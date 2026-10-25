import logging
from unittest.mock import patch

import pytest

import ray
from ray.data.exceptions import SystemException, UserCodeException
from ray.exceptions import RayTaskError
from ray.tests.conftest import *  # noqa


@pytest.mark.parametrize("log_internal_stack_trace", [True, False])
def test_user_exception(
    log_internal_stack_trace,
    caplog,
    propagate_logs,
    restore_data_context,
    ray_start_regular_shared,
):
    ctx = ray.data.DataContext.get_current()
    ctx.log_internal_stack_trace = log_internal_stack_trace

    def f(row):
        _ = 1 / 0

    with pytest.raises(UserCodeException) as exc_info:
        ray.data.range(1).map(f).take_all()

    assert issubclass(exc_info.type, RayTaskError)
    assert issubclass(exc_info.type, UserCodeException)
    assert ZeroDivisionError.__name__ in str(exc_info.value)

    if not log_internal_stack_trace:
        assert any(
            record.levelno == logging.ERROR
            and "Exception occurred in user code" in record.message
            for record in caplog.records
        ), caplog.records

    # For a user-code error the "Full stack trace:" record is always hidden from
    # stdout (``hide=True``); the flag only controls the log-file content.
    assert any(
        record.levelno == logging.ERROR
        and "Full stack trace:" in record.message
        and getattr(record, "hide", False) is True
        for record in caplog.records
    ), caplog.records

    # The "Full stack trace:" record (written to the log file only): by default it
    # carries just the cleaned worker-side traceback inlined into the message, with
    # the driver-side propagation frames omitted. When the user opts in via
    # `log_internal_stack_trace`, the full driver + worker traceback is attached to
    # the log-file record via exc_info instead.
    full_trace_records = [
        record
        for record in caplog.records
        if record.levelno == logging.ERROR and "Full stack trace:" in record.message
    ]
    assert full_trace_records, caplog.records
    full_trace_record = full_trace_records[0]
    if log_internal_stack_trace:
        assert full_trace_record.exc_info is not None
    else:
        assert full_trace_record.exc_info is None
        assert "ZeroDivisionError" in full_trace_record.message


def test_system_exception(caplog, propagate_logs, ray_start_regular_shared):
    class FakeException(Exception):
        pass

    with pytest.raises(FakeException) as exc_info:
        with patch(
            "ray.data.dataset._ExecutionCache.get_bundle",
            side_effect=FakeException(),
        ):
            ray.data.range(1).materialize()
            assert issubclass(exc_info.type, FakeException)
            assert issubclass(exc_info.type, SystemException)

    assert any(
        record.levelno == logging.ERROR
        and "Exception occurred in Ray Data or Ray Core internal code."
        in record.message
        for record in caplog.records
    ), caplog.records

    assert any(
        record.levelno == logging.ERROR
        and "Full stack trace:" in record.message
        and not getattr(record, "hide", False)
        for record in caplog.records
    ), caplog.records


def test_full_traceback_logged_with_ray_debugger(
    caplog, propagate_logs, ray_start_regular_shared, monkeypatch
):
    monkeypatch.setenv("RAY_DEBUG_POST_MORTEM", 1)

    def f(row):
        _ = 1 / 0
        return row

    with pytest.raises(Exception) as exc_info:
        ray.data.range(1).map(f).take_all()

    assert issubclass(exc_info.type, RayTaskError)
    assert issubclass(exc_info.type, UserCodeException)
    assert ZeroDivisionError.__name__ in str(exc_info.value)

    assert any(
        record.levelno == logging.ERROR
        and "Full stack trace:" in record.message
        and not getattr(record, "hide", False)
        for record in caplog.records
    ), caplog.records


def test_raise_original_map_exception_env_var(
    caplog, propagate_logs, restore_data_context, ray_start_regular_shared, monkeypatch
):
    monkeypatch.setenv("RAY_DATA_RAISE_ORIGINAL_MAP_EXCEPTION", "1")
    ctx = ray.data.DataContext.get_current()
    ctx.raise_original_map_exception = (
        True  # Ensure that the context picks up the environment variable
    )

    def f(row):
        raise ValueError("This is a test error.")

    with pytest.raises(ValueError) as exc_info:
        ray.data.range(1).map(f).take_all()

    assert issubclass(exc_info.type, ValueError)
    assert "This is a test error." in str(exc_info.value)

    # Ensure that the stack trace is not cleared or replaced by UserCodeException
    assert not any(
        record.levelno == logging.ERROR
        and "Exception occurred in user code" in record.message
        for record in caplog.records
    ), caplog.records


def test_deprecated_log_internal_stack_trace_alias(restore_data_context):
    # The old `log_internal_stack_trace_to_stdout` field name is deprecated but
    # still forwards to `log_internal_stack_trace` (with a warning) for
    # backwards compatibility.
    ctx = ray.data.DataContext.get_current()
    with pytest.warns(DeprecationWarning, match="log_internal_stack_trace_to_stdout"):
        ctx.log_internal_stack_trace_to_stdout = False
    assert ctx.log_internal_stack_trace is False


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

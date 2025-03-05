import logging
from unittest.mock import patch

import pytest

import ray
from ray.data.exceptions import SystemException, UserCodeException
from ray.exceptions import RayTaskError
from ray.tests.conftest import *  # noqa


@pytest.mark.parametrize("log_internal_stack_trace_to_stdout", [True, False])
def test_user_exception(
    log_internal_stack_trace_to_stdout,
    caplog,
    propagate_logs,
    restore_data_context,
    ray_start_regular_shared,
):
    ctx = ray.data.DataContext.get_current()
    ctx.log_internal_stack_trace_to_stdout = log_internal_stack_trace_to_stdout

    def f(row):
        _ = 1 / 0

    with pytest.raises(UserCodeException) as exc_info:
        ray.data.range(1).map(f).take_all()

    assert issubclass(exc_info.type, RayTaskError)
    assert issubclass(exc_info.type, UserCodeException)
    assert ZeroDivisionError.__name__ in str(exc_info.value)

    if not log_internal_stack_trace_to_stdout:
        assert any(
            record.levelno == logging.ERROR
            and "Exception occurred in user code" in record.message
            for record in caplog.records
        ), caplog.records

    assert any(
        record.levelno == logging.ERROR
        and "Full stack trace:" in record.message
        and getattr(record, "hide", False) == (not log_internal_stack_trace_to_stdout)
        for record in caplog.records
    ), caplog.records


def test_system_exception(caplog, propagate_logs, ray_start_regular_shared):
    class FakeException(Exception):
        pass

    with pytest.raises(FakeException) as exc_info:
        with patch(
            "ray.data._internal.plan.ExecutionPlan.has_computed_output",
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

import logging
from unittest.mock import patch

import pytest

import ray
from ray.data.exceptions import SystemException, UserCodeException
from ray.exceptions import RayTaskError
from ray.tests.conftest import *  # noqa


def test_user_exception(caplog, propagate_logs, ray_start_regular_shared):
    def f(row):
        1 / 0
        return row

    with pytest.raises(UserCodeException) as exc_info:
        ray.data.range(1).map(f).take_all()

    assert issubclass(exc_info.type, RayTaskError)
    assert issubclass(exc_info.type, UserCodeException)
    assert ZeroDivisionError.__name__ in str(exc_info.value)

    assert any(
        record.levelno == logging.ERROR
        and "Exception occurred in user code" in record.message
        for record in caplog.records
    ), caplog.records

    assert any(
        record.levelno == logging.DEBUG and "Full stack trace:" in record.message
        for record in caplog.records
    ), caplog.records


def test_system_exception(caplog, propagate_logs, ray_start_regular_shared):
    class FakeException(Exception):
        pass

    with pytest.raises(FakeException) as exc_info:
        with patch(
            (
                "ray.data._internal.execution.legacy_compat."
                "get_legacy_lazy_block_list_read_only"
            ),
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
        record.levelno == logging.DEBUG and "Full stack trace:" in record.message
        for record in caplog.records
    ), caplog.records


def test_full_traceback_logged_with_ray_debugger(
    caplog, propagate_logs, ray_start_regular_shared, monkeypatch
):
    monkeypatch.setenv("RAY_PDB", 1)

    def f(row):
        1 / 0
        return row

    with pytest.raises(Exception) as exc_info:
        ray.data.range(1).map(f).take_all()

    assert issubclass(exc_info.type, RayTaskError)
    assert issubclass(exc_info.type, UserCodeException)
    assert ZeroDivisionError.__name__ in str(exc_info.value)

    assert any(
        record.levelno == logging.ERROR and "Full stack trace:" in record.message
        for record in caplog.records
    ), caplog.records


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

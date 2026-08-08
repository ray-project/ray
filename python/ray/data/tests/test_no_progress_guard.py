import time

import pytest

import ray
from ray.data.context import DataContext
from ray.data.exceptions import ExecutionTimeoutError
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

# Deterministic coverage of the guard's timing logic lives in
# `tests/unit/test_no_progress_guard.py`, which drives it with an injected
# clock. This file covers the wiring into `StreamingExecutor` against a real
# cluster.
#
# These use `ray_start_regular` rather than the shared cluster: a UDF blocked
# forever keeps holding its CPU after the execution fails, which would starve
# whichever test ran next.


def test_hanging_udf_fails_execution(
    ray_start_regular, restore_data_context  # noqa: F811
):
    DataContext.get_current().execution_no_progress_timeout_s = 2

    def hang(row):
        time.sleep(10**6)
        return row

    ds = ray.data.range(1).map(hang)
    with pytest.raises(ExecutionTimeoutError, match="made no progress"):
        ds.take(1)


@pytest.mark.parametrize(
    "make_dataset,expected_enabled",
    [
        (lambda: ray.data.range(10).map(lambda row: row), True),
        (lambda: ray.data.range(10).sort("id"), False),
    ],
    ids=["map", "sort"],
)
def test_all_to_all_operator_disables_the_guard(
    make_dataset,
    expected_enabled,
    ray_start_regular,
    restore_data_context,  # noqa: F811
):
    DataContext.get_current().execution_no_progress_timeout_s = 600

    bundles, _, executor = make_dataset()._execute_to_iterator()
    list(bundles)

    assert executor._no_progress_guard.enabled is expected_enabled


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

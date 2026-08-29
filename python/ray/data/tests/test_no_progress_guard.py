import time

import pytest

import ray
from ray.data._internal.execution.interfaces import ExecutionOptions
from ray.data._internal.execution.no_progress_guard import NoProgressGuard
from ray.data._internal.execution.streaming_executor_state import (
    build_streaming_topology,
)
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.exceptions import ExecutionTimeoutError
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.conftest import noop_counter
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


# GPU_SHUFFLE is excluded because building its plan needs GPUs in the cluster.
@pytest.mark.parametrize(
    "shuffle_strategy",
    [s for s in ShuffleStrategy if s is not ShuffleStrategy.GPU_SHUFFLE],
)
def test_legacy_shuffle_operators_disable_the_guard(
    shuffle_strategy,
    restore_data_context,  # noqa: F811
):
    # Only the V2 hash shuffle implementation is supported by the guard.
    expected_enabled = shuffle_strategy is ShuffleStrategy.SHUFFLE_V2
    DataContext.get_current().shuffle_strategy = shuffle_strategy

    ds = ray.data.range(1).groupby("id").count()
    physical_plan, _ = get_execution_plan(ds._logical_plan)
    topology = build_streaming_topology(
        physical_plan.dag, ExecutionOptions(), noop_counter()
    )

    guard = NoProgressGuard(topology, timeout_s=1)

    assert guard.enabled is expected_enabled


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

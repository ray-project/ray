import os
from unittest.mock import MagicMock

import pytest

import ray
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)


def test_min_max_resource_requirements(ray_start_regular_shared, restore_data_context):
    data_context = ray.data.DataContext.get_current()
    op = TaskPoolMapOperator(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
        ray_remote_args={"num_cpus": 1},
    )
    op._metrics = MagicMock(obj_store_mem_max_pending_output_per_task=3)

    (
        min_resource_usage_bound,
        max_resource_usage_bound,
    ) = op.min_max_resource_requirements()

    # At a minimum, you need enough processors to run one task and enough object
    # store memory for a pending task.
    assert min_resource_usage_bound == ExecutionResources(
        cpu=1, gpu=0, object_store_memory=3
    )
    # For CPU-only operators, max GPU/memory is 0 (not inf) to prevent hoarding.
    assert max_resource_usage_bound == ExecutionResources.for_limits(gpu=0, memory=0)


def test_operator_id_env_var_injected(ray_start_regular_shared, restore_data_context):
    """Verify __RAY_DATA_OPERATOR_ID is set inside map task UDFs."""
    ds = ray.data.range(1)

    def check_env(row):
        op_id = os.environ.get("__RAY_DATA_OPERATOR_ID")
        assert op_id is not None, "__RAY_DATA_OPERATOR_ID not set"
        row["op_id"] = op_id
        return row

    result = ds.map(check_env).take_all()
    assert all(r["op_id"] for r in result)


def test_operator_id_env_var_merges_with_user_env(
    ray_start_regular_shared, restore_data_context
):
    """Verify __RAY_DATA_OPERATOR_ID merges with user-specified env_vars."""
    ds = ray.data.range(10)
    user_var_value = "hello"

    def check_both(row):
        assert os.environ.get("MY_VAR") == "hello"
        assert "__RAY_DATA_OPERATOR_ID" in os.environ
        row["ok"] = True
        return row

    result = ds.map(
        check_both,
        ray_remote_args_fn=lambda: {
            "runtime_env": {"env_vars": {"MY_VAR": user_var_value}}
        },
    ).take_all()
    assert all(r["ok"] for r in result)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

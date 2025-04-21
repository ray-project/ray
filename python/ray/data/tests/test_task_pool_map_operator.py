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
        target_max_block_size=None,
        ray_remote_args={"num_cpus": 1},
    )
    op._metrics = MagicMock(obj_store_mem_max_pending_output_per_task=1)

    (
        min_resource_usage_bound,
        max_resource_usage_bound,
    ) = op.min_max_resource_requirements()

    assert (
        # At a minimum, you need enough processors to run one task and enough object
        # store memory for a pending task.
        min_resource_usage_bound == ExecutionResources(cpu=1, object_store_memory=1)
        # As long as the operator is still receiving inputs, it can't determine an
        # upper bound on the resource usage.
        and max_resource_usage_bound == ExecutionResources.for_limits()
    )


def test_min_max_resource_requirements_with_inputs_complete(
    ray_start_regular_shared, restore_data_context
):
    data_context = ray.data.DataContext.get_current()
    op = TaskPoolMapOperator(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
        target_max_block_size=None,
        ray_remote_args={"num_cpus": 1},
    )
    op._metrics = MagicMock(obj_store_mem_max_pending_output_per_task=1)
    op.num_active_tasks = MagicMock(return_value=2)
    op.all_inputs_done()

    (
        min_resource_usage_bound,
        max_resource_usage_bound,
    ) = op.min_max_resource_requirements()

    assert min_resource_usage_bound == ExecutionResources(cpu=1, object_store_memory=1)
    # If the operator is done receiving inputs, it knows it doesn't need more resources
    # than to run the active tasks.
    assert max_resource_usage_bound == ExecutionResources(cpu=2, object_store_memory=2)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

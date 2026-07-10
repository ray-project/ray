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


def test_dynamic_remote_args_inject_context_label_selector(
    ray_start_regular_shared, restore_data_context
):
    """ExecutionOptions.label_selector should propagate to ray_remote_args."""
    data_context = ray.data.DataContext.get_current()
    data_context.execution_options.label_selector = {"subcluster": "train"}

    op = TaskPoolMapOperator(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
        ray_remote_args={"num_cpus": 1},
    )
    args = op._get_dynamic_ray_remote_args()
    assert args["label_selector"] == {"subcluster": "train"}


def test_dynamic_remote_args_op_wins_on_collision(
    ray_start_regular_shared, restore_data_context
):
    """Operator-level label_selector wins on key conflict with ExecutionOptions."""
    data_context = ray.data.DataContext.get_current()
    data_context.execution_options.label_selector = {"subcluster": "train"}

    op = TaskPoolMapOperator(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
        ray_remote_args={
            "num_cpus": 1,
            "label_selector": {"subcluster": "val", "node": "X"},
        },
    )
    args = op._get_dynamic_ray_remote_args()
    # Operator's "subcluster" wins; "node" is preserved.
    assert args["label_selector"] == {"subcluster": "val", "node": "X"}


def test_dynamic_remote_args_no_label_when_unset(
    ray_start_regular_shared, restore_data_context
):
    """No label_selector key is added when ExecutionOptions.label_selector is unset."""
    data_context = ray.data.DataContext.get_current()
    # Ensure unset.
    data_context.execution_options.label_selector = None

    op = TaskPoolMapOperator(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
        ray_remote_args={"num_cpus": 1},
    )
    args = op._get_dynamic_ray_remote_args()
    assert "label_selector" not in args


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

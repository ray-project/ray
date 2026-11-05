from typing import cast
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

import ray
from ray.core.generated import common_pb2
from ray.data._internal.execution.interfaces import ExecutionResources, RefBundle
from ray.data._internal.execution.interfaces.ref_bundle import BlockEntry
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import (
    get_safe_default_logical_memory,
)
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.data.block import Block, BlockMetadata
from ray.types import ObjectRef


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


@pytest.mark.parametrize(
    "user_labels",
    [
        None,
        {"team": "ingest"},
        {TaskPoolMapOperator._OPERATOR_ID_LABEL_KEY: "not-this-op"},
    ],
)
def test_dynamic_remote_args_keep_operator_id_label(
    ray_start_regular_shared, restore_data_context, user_labels
):
    """`.options()` replaces `_labels`, so the operator id must be merged back in."""
    data_context = ray.data.DataContext.get_current()
    ray_remote_args = {"num_cpus": 1}
    if user_labels is not None:
        ray_remote_args["_labels"] = user_labels

    op = TaskPoolMapOperator(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
        ray_remote_args=ray_remote_args,
    )
    labels = op._get_dynamic_ray_remote_args()["_labels"]

    # User labels survive; the operator id is always present and always wins.
    assert labels == {
        **(user_labels or {}),
        TaskPoolMapOperator._OPERATOR_ID_LABEL_KEY: op.id,
    }


def _lineage_reconstruction_task(operator_id: str):
    task = common_pb2.LineageReconstructionTask()
    task.labels[TaskPoolMapOperator._OPERATOR_ID_LABEL_KEY] = operator_id
    return task


@pytest.mark.parametrize("num_reconstruction_tasks", [0, 1, 3])
def test_extra_resource_usage_counts_lineage_reconstruction_tasks(
    ray_start_regular_shared, restore_data_context, num_reconstruction_tasks
):
    """Lineage reconstruction tasks are charged to the operator that owns them."""
    data_context = ray.data.DataContext.get_current()
    op = TaskPoolMapOperator(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
        ray_remote_args={"num_cpus": 1, "num_gpus": 2},
    )

    task_infos = []
    if num_reconstruction_tasks:
        task_infos.append(
            (_lineage_reconstruction_task(op.id), num_reconstruction_tasks)
        )

    with patch(
        "ray.data._internal.execution.operators.task_pool_map_operator"
        ".get_local_ongoing_lineage_reconstruction_tasks",
        return_value=task_infos,
    ):
        assert op.extra_resource_usage() == ExecutionResources(
            cpu=1 * num_reconstruction_tasks, gpu=2 * num_reconstruction_tasks
        )


def test_extra_resource_usage_respects_context_switch(
    ray_start_regular_shared, restore_data_context
):
    """The switch turns the accounting off without touching anything else."""
    data_context = ray.data.DataContext.get_current()
    data_context.enable_lineage_reconstruction_resource_accounting = False

    op = TaskPoolMapOperator(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
        ray_remote_args={"num_cpus": 1},
    )

    with patch(
        "ray.data._internal.execution.operators.task_pool_map_operator"
        ".get_local_ongoing_lineage_reconstruction_tasks",
        return_value=[(_lineage_reconstruction_task(op.id), 4)],
    ):
        assert op.extra_resource_usage() == ExecutionResources.zero()

        # Flipping it back on re-enables the accounting on the same operator.
        data_context.enable_lineage_reconstruction_resource_accounting = True
        assert op.extra_resource_usage() == ExecutionResources(cpu=4, gpu=0)


def test_extra_resource_usage_ignores_other_operators(
    ray_start_regular_shared, restore_data_context
):
    """Reconstruction tasks belonging to a different operator aren't counted."""
    data_context = ray.data.DataContext.get_current()
    op = TaskPoolMapOperator(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
        ray_remote_args={"num_cpus": 1},
    )

    task_infos = [
        (_lineage_reconstruction_task("some_other_operator_id"), 5),
        (_lineage_reconstruction_task(op.id), 2),
        # A task with no operator label at all, e.g. one submitted outside Data.
        (common_pb2.LineageReconstructionTask(), 7),
    ]

    with patch(
        "ray.data._internal.execution.operators.task_pool_map_operator"
        ".get_local_ongoing_lineage_reconstruction_tasks",
        return_value=task_infos,
    ):
        assert op.extra_resource_usage() == ExecutionResources(cpu=2, gpu=0)


def _bundle(*estimates):
    """A bundle with one block per argument, each carrying that estimate."""
    blocks = tuple(
        BlockEntry(
            ref=cast(ObjectRef[Block], ray.put(pa.table({"id": [1]}))),
            metadata=BlockMetadata(
                num_rows=1, size_bytes=1, exec_stats=None, consumer_memory_bytes=e
            ),
        )
        for e in estimates
    )
    return RefBundle(blocks=blocks, owns_blocks=True, schema=None)


def _memory_estimating_op(data_context, ray_remote_args=None, **kwargs):
    return TaskPoolMapOperator(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
        ray_remote_args=ray_remote_args or {"num_cpus": 1},
        memory_from_input_estimate=True,
        **kwargs,
    )


def test_memory_from_input_estimate(ray_start_regular_shared, restore_data_context):
    """A task asks for its input blocks' estimates summed, capped at the safe default,
    in place of the default memory; a memory the caller set wins."""
    data_context = ray.data.DataContext.get_current()

    def memory(op, *estimates):
        bundle = _bundle(*estimates) if estimates else None
        return op._get_dynamic_ray_remote_args(input_bundle=bundle).get("memory")

    op = _memory_estimating_op(data_context)
    cap = get_safe_default_logical_memory(op._ray_remote_args)
    assert [memory(op, 100), memory(op, 50, 60)] == [100, 110]
    assert memory(op, 10 * cap) == cap
    # Without an estimate on every block, or without a bundle, the static args stand.
    assert memory(op, 50, None) is None
    assert memory(op) is None
    # The per-task figure the resource manager admits tasks on stays static.
    assert op.per_task_resource_allocation().memory == 0

    # The default logical memory gives way to an estimate and stands without one.
    op = _memory_estimating_op(data_context, default_logical_memory_enabled=True)
    default = get_safe_default_logical_memory(op._ray_remote_args)
    assert [memory(op, 100), memory(op, None)] == [100, default]
    op = _memory_estimating_op(
        data_context, ray_remote_args={"num_cpus": 1, "memory": 7}
    )
    assert memory(op, 100) == 7


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

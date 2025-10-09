import logging
import warnings
from typing import Iterable, List

import ray
from ray import ObjectRef
from ray.data._internal.compute import TaskPoolStrategy
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.execution.util import memory_string
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.util import _warn_on_high_parallelism
from ray.data.block import Block, BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource.datasource import ReadTask
from ray.experimental.locations import get_local_object_locations
from ray.util.debug import log_once

TASK_SIZE_WARN_THRESHOLD_BYTES = 1024 * 1024  # 1 MiB

logger = logging.getLogger(__name__)


def _derive_metadata(read_task: ReadTask, read_task_ref: ObjectRef) -> BlockMetadata:
    # NOTE: Use the `get_local_object_locations` API to get the size of the
    # serialized ReadTask, instead of pickling.
    # Because the ReadTask may capture ObjectRef objects, which cannot
    # be serialized out-of-band.
    locations = get_local_object_locations([read_task_ref])
    task_size = locations[read_task_ref]["object_size"]
    if task_size > TASK_SIZE_WARN_THRESHOLD_BYTES and log_once(
        f"large_read_task_{read_task.read_fn.__name__}"
    ):
        warnings.warn(
            "The serialized size of your read function named "
            f"'{read_task.read_fn.__name__}' is {memory_string(task_size)}. This size "
            "is relatively large. As a result, Ray might excessively "
            "spill objects during execution. To fix this issue, avoid accessing "
            f"`self` or other large objects in '{read_task.read_fn.__name__}'."
        )

    return BlockMetadata(
        num_rows=1,
        size_bytes=task_size,
        exec_stats=None,
        input_files=None,
    )


def plan_read_op(
    op: Read,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for Read.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """
    assert len(physical_children) == 0

    def get_input_data(target_max_block_size) -> List[RefBundle]:
        parallelism = op.get_detected_parallelism()
        assert (
            parallelism is not None
        ), "Read parallelism must be set by the optimizer before execution"

        # Get the original read tasks
        read_tasks = op._datasource_or_legacy_reader.get_read_tasks(
            parallelism, per_task_row_limit=op._per_block_limit
        )

        _warn_on_high_parallelism(parallelism, len(read_tasks))

        ret = []
        for read_task in read_tasks:
            read_task_ref = ray.put(read_task)
            ref_bundle = RefBundle(
                (
                    (
                        # TODO: figure out a better way to pass read
                        # tasks other than ray.put().
                        read_task_ref,
                        _derive_metadata(read_task, read_task_ref),
                    ),
                ),
                # `owns_blocks` is False, because these refs are the root of the
                # DAG. We shouldn't eagerly free them. Otherwise, the DAG cannot
                # be reconstructed.
                owns_blocks=False,
                schema=None,
            )
            ret.append(ref_bundle)
        return ret

    inputs = InputDataBuffer(data_context, input_data_factory=get_input_data)

    def do_read(blocks: Iterable[ReadTask], _: TaskContext) -> Iterable[Block]:
        for read_task in blocks:
            yield from read_task()

    # Create a MapTransformer for a read operator
    map_transformer = MapTransformer(
        [
            BlockMapTransformFn(
                do_read,
                is_udf=False,
                output_block_size_option=OutputBlockSizeOption.of(
                    target_max_block_size=data_context.target_max_block_size,
                ),
            ),
        ]
    )

    return MapOperator.create(
        map_transformer,
        inputs,
        data_context,
        name=op.name,
        compute_strategy=TaskPoolStrategy(op._concurrency),
        ray_remote_args=op._ray_remote_args,
    )

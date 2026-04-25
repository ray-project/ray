import logging
import warnings
from typing import Iterable, List, Sequence

import ray
from ray import ObjectRef
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.execution.util import memory_string
from ray.data._internal.logical.operators import Read
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.util import _warn_on_high_parallelism
from ray.data.block import Block, BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource.datasource import ReadTask
from ray.experimental.locations import get_local_object_locations
from ray.util.debug import log_once

TASK_SIZE_WARN_THRESHOLD_BYTES = 1024 * 1024  # 1 MiB

logger = logging.getLogger(__name__)


def _derive_block_metadata(
    read_tasks: Sequence[ReadTask],
    read_task_refs: Sequence[ObjectRef],
) -> List[BlockMetadata]:
    """Return `BlockMetadata` for a batch of read tasks.

    Issues a single `get_local_object_locations` call for all refs and then
    consults the result per-task. At typical read parallelism (tens of
    thousands of tasks) the per-call overhead of `get_local_object_locations`
    becomes a significant fraction of pipeline setup time; one batched call
    collapses that to a single core-worker lookup.
    """
    # NOTE: Use `get_local_object_locations` to get the serialized size
    # rather than pickling — the `ReadTask` may capture `ObjectRef`
    # objects that cannot be serialized out-of-band.
    locations = get_local_object_locations(list(read_task_refs))

    metadata: List[BlockMetadata] = []
    for read_task, read_task_ref in zip(read_tasks, read_task_refs):
        task_size = locations[read_task_ref]["object_size"]
        if task_size > TASK_SIZE_WARN_THRESHOLD_BYTES and log_once(
            f"large_read_task_{read_task.read_fn.__name__}"
        ):
            warnings.warn(
                "The serialized size of your read function named "
                f"'{read_task.read_fn.__name__}' is {memory_string(task_size)}. "
                "This size is relatively large. As a result, Ray might "
                "excessively spill objects during execution. To fix this "
                "issue, avoid accessing `self` or other large objects in "
                f"'{read_task.read_fn.__name__}'."
            )
        metadata.append(
            BlockMetadata(
                num_rows=1,
                size_bytes=task_size,
                exec_stats=None,
                input_files=None,
            )
        )
    return metadata


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
        read_tasks = op.datasource_or_legacy_reader.get_read_tasks(
            parallelism,
            per_task_row_limit=op.per_block_limit,
            data_context=data_context,
        )

        _warn_on_high_parallelism(parallelism, len(read_tasks))

        # Materialize to a list so subsequent code can iterate ``read_tasks``
        # more than once (the comprehension below puts each into the object
        # store, then ``_derive_block_metadata`` zips it with the refs).
        # Pristine ray-2.55.0's loop iterated only once, so any
        # ``Sequence[ReadTask]`` worked. The batched path requires a
        # replayable container; without this conversion, an
        # iterable-with-``__len__``-but-non-replayable-``__iter__`` (e.g., a
        # third-party datasource that returns ``(rt for rt in ...)``-style
        # output and lies about being a ``Sequence``) would silently
        # produce zero ``RefBundle``s on the second pass.
        read_tasks = list(read_tasks)

        # TODO: figure out a better way to pass read tasks other than
        # ray.put().
        read_task_refs = [ray.put(rt) for rt in read_tasks]
        block_metadata = _derive_block_metadata(read_tasks, read_task_refs)

        ret = [
            RefBundle(
                ((read_task_ref, meta),),
                # `owns_blocks` is False because these refs are the root
                # of the DAG. We shouldn't eagerly free them; otherwise
                # the DAG cannot be reconstructed.
                owns_blocks=False,
                schema=None,
            )
            for read_task_ref, meta in zip(read_task_refs, block_metadata)
        ]
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
        compute_strategy=op.compute,
        ray_remote_args=op.ray_remote_args,
    )

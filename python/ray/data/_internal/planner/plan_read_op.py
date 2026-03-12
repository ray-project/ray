import logging
import warnings
from typing import Iterable, List

import ray
from ray import ObjectRef
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import (
    MapOperator,
)
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.execution.util import (
    ShuffleRefBundler,
    concat_and_shuffle,
    memory_string,
)
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

    def get_input_data(_target_max_block_size) -> List[RefBundle]:
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

    # Compute effective block size: smaller blocks when sub-file shuffle is active
    shuffle_config = op.shuffle_config
    sub_file_shuffle = (
        shuffle_config is not None and shuffle_config.enable_sub_file_shuffle
    )
    if sub_file_shuffle:
        from ray.data.datasource.file_based_datasource import SubFileShuffleConfig

        if isinstance(shuffle_config, SubFileShuffleConfig):
            num_task_sources = shuffle_config.num_task_sources_per_batch
        else:
            num_task_sources = data_context.shuffle_merge_window
        effective_block_size = data_context.target_max_block_size // num_task_sources
    else:
        effective_block_size = data_context.target_max_block_size

    # Create a MapTransformer for a read operator
    map_transformer = MapTransformer(
        [
            BlockMapTransformFn(
                do_read,
                is_udf=False,
                output_block_size_option=OutputBlockSizeOption.of(
                    target_max_block_size=effective_block_size,
                ),
            ),
        ]
    )

    read_op = MapOperator.create(
        map_transformer,
        inputs,
        data_context,
        name=op.name,
        compute_strategy=op.compute,
        ray_remote_args=op.ray_remote_args,
        target_max_block_size_override=(
            effective_block_size if sub_file_shuffle else None
        ),
    )

    if not sub_file_shuffle:
        return read_op

    # TODO(xgui): for preserved_order, we should guarantee the ShuffleRefBundler includes blocks from different read tasks before flushing.
    if data_context.execution_options.preserve_order:
        raise ValueError(
            "Sub-file shuffle is not supported with preserve_order=True. "
            "Support for this combination is pending."
        )

    # Chain a shuffle operator that concatenates all blocks in a RefBundle
    # and shuffles rows.
    row_block_size = data_context.shuffle_row_block_size
    shuffle_seed = shuffle_config.get_seed()

    def _concat_and_shuffle(blocks: Iterable[Block], _: TaskContext) -> Iterable[Block]:
        return concat_and_shuffle(blocks, row_block_size, seed=shuffle_seed)

    shuffle_transformer = MapTransformer(
        [
            BlockMapTransformFn(
                _concat_and_shuffle,
                is_udf=False,
                output_block_size_option=OutputBlockSizeOption.of(
                    target_max_block_size=data_context.target_max_block_size,
                ),
            ),
        ]
    )

    return MapOperator.create(
        shuffle_transformer,
        read_op,
        data_context,
        name="SubFileShuffle",
        ref_bundler=ShuffleRefBundler(
            num_task_sources,
            seed=shuffle_seed,
        ),
    )

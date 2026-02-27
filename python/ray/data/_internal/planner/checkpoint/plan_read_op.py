from typing import Iterable, List

import numpy
import pyarrow.fs as fs

from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.execution.interfaces import PhysicalOperator, TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.logical.operators import Read
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.planner.plan_read_op import plan_read_op
from ray.data.block import Block
from ray.data.checkpoint.checkpoint_filter import (
    BatchBasedCheckpointFilter,
    IdColumnCheckpointLoader,
)
from ray.data.context import DataContext
from ray.data.datasource.path_util import _unwrap_protocol
from ray.types import ObjectRef


def plan_read_op_with_checkpoint_filter(
    op: Read,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    """Plan the read op to physical operators.
    1. If checkpoint is not enabled, or the checkpoint_path is an empty directory,
    return the original read physical operator.
    2. If the checkpoint is valid, translate the logical read operator into two
    physical operators read->map, where the map operator receives blocks from the
    read operator and outputs the filtered Blocks.
    The implementation of the map operator is `ActorPoolMapOperator`. At runtime
    the number of checkpoint-actors is dynamically scaled. The number of actors
    is in the range [checkpoint_actor_pool_min_size, checkpoint_actor_pool_max_size].
    """
    physical_read_op = plan_read_op(op, physical_children, data_context)

    # Return the read op directly if:
    # 1. the checkpoint directory is not existed.
    # 2. no valid files under checkpoint_path(for example, it is an empty directory).
    checkpoint_config = data_context.checkpoint_config
    info = checkpoint_config.filesystem.get_file_info(
        _unwrap_protocol(checkpoint_config.checkpoint_path)
    )
    # the directory is not existed
    if info.type == fs.FileType.NotFound:
        return physical_read_op

    loader = IdColumnCheckpointLoader(
        checkpoint_path=checkpoint_config.checkpoint_path,
        filesystem=checkpoint_config.filesystem,
        id_column=checkpoint_config.id_column,
        checkpoint_path_partition_filter=checkpoint_config.checkpoint_path_partition_filter,
    )
    # load checkpointed IDs as a numpy ndarray and store it to object store.
    checkpointed_ids_ref, checkpointed_ids_size = loader.load_checkpoint()
    # no valid files under checkpoint_path
    if not checkpointed_ids_ref:
        return physical_read_op

    # generate checkpoint op
    map_transformer = _get_checkpoint_map_transformer(
        data_context, checkpointed_ids_ref
    )

    checkpoint_op = MapOperator.create(
        map_transformer=map_transformer,
        input_op=physical_read_op,
        data_context=data_context,
        name="CheckpointFilter",
        compute_strategy=ActorPoolStrategy(
            min_size=data_context.checkpoint_actor_pool_min_size,
            max_size=data_context.checkpoint_actor_pool_max_size,
            max_tasks_in_flight_per_actor=data_context.checkpoint_actor_max_tasks_in_flight_per_actor,
        ),
        ray_remote_args={
            "memory": max(
                data_context.checkpoint_actor_memory_bytes,
                int(checkpointed_ids_size * 1.5),
            )
        },
    )
    return checkpoint_op


def _get_checkpoint_map_transformer(
    data_context: DataContext, checkpointed_ids_ref: ObjectRef[numpy.ndarray]
) -> MapTransformer:
    """Get the MapTransformer that performs checkpoint filtering.

    Args:
        data_context: DataContext.
        checkpointed_ids_ref: ObjectRef of the checkpointed ids.

    Returns:
        MapTransformer: A MapTransformer that performs checkpoint filtering.
    """

    # To make `init_checkpoint_filter` and `transform_logic` use the same `checkpoint_filter`,
    # we declare `checkpoint_filter` as a list. Note: nonlocal cannot be used here,
    # because after serialization/deserialization, the two methods would hold pointers
    # that refer to different `checkpoint_filter` instances.
    checkpoint_filter = []

    # The initialization method of the checkpoint-actor; Each actor has a checkpoint_filter.
    # This method will run only once for on single actor.
    def init_checkpoint_filter():
        checkpoint_filter.append(
            BatchBasedCheckpointFilter(
                data_context.checkpoint_config, checkpointed_ids_ref
            )
        )

    # The transform logic of the checkpoint-actor. The actor receives blocks from
    # the read operator, filters the blocks, and then outputs them.
    # This method will run multiple times on each actor.
    def transform_logic(blocks: Iterable[Block], ctx: TaskContext) -> Iterable[Block]:
        assert checkpoint_filter, "checkpoint filter was not initialized!"
        for block in blocks:
            filtered_block = checkpoint_filter[0].filter_rows_for_block(block)
            if filtered_block.num_rows > 0:
                yield filtered_block

    transformer_fn = BlockMapTransformFn(
        block_fn=transform_logic,
        output_block_size_option=OutputBlockSizeOption.of(
            target_max_block_size=data_context.target_max_block_size,
        ),
    )
    return MapTransformer(
        transform_fns=[transformer_fn],
        init_fn=init_checkpoint_filter,
    )

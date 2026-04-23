from typing import Iterable, List, Optional

import numpy
import pyarrow
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
    IdColumnCheckpointManager,
    NumpyArrayBasedCheckpointFilter,
)
from ray.data.context import DataContext
from ray.data.datasource.path_util import _unwrap_protocol
from ray.types import ObjectRef

CHECKPOINT_MEMORY_SAFETY_FACTOR = 1.5


def plan_read_op_with_checkpoint_filter(
    data_file_dir: Optional[str],
    data_file_filesystem: Optional["pyarrow.fs.FileSystem"],
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
    # 1. the checkpoint directory does not exist.
    # 2. no valid files under checkpoint_path(for example, it is an empty directory).
    checkpoint_config = data_context.checkpoint_config
    info = checkpoint_config.filesystem.get_file_info(
        _unwrap_protocol(checkpoint_config.checkpoint_path)
    )
    # the directory does not exist
    if info.type == fs.FileType.NotFound:
        return physical_read_op

    checkpoint_manager = IdColumnCheckpointManager(checkpoint_config=checkpoint_config)
    # load checkpointed IDs as a numpy ndarray and store it to object store.
    checkpointed_ids_ref, checkpointed_ids_size = checkpoint_manager.load_checkpoint(
        data_file_dir, data_file_filesystem
    )
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
            min_size=checkpoint_config.checkpoint_actor_pool_min_size,
            max_size=checkpoint_config.checkpoint_actor_pool_max_size,
        ),
        ray_remote_args={
            "memory": max(
                checkpoint_config.checkpoint_actor_memory_bytes,
                int(checkpointed_ids_size * CHECKPOINT_MEMORY_SAFETY_FACTOR),
            )
        },
        supports_fusion=False,
    )
    return checkpoint_op


class _CheckpointFilterFn:
    def __init__(
        self,
        checkpoint_config,
        checkpointed_ids_ref: ObjectRef[numpy.ndarray],
    ):
        self._config = checkpoint_config
        self._ref = checkpointed_ids_ref
        self._filter = None

    def init_checkpoint_filter(self):
        """Called once per actor worker to materialize the filter."""
        self._filter = NumpyArrayBasedCheckpointFilter(self._config, self._ref)

    def __call__(self, blocks: Iterable[Block], ctx: TaskContext) -> Iterable[Block]:
        assert self._filter is not None, "checkpoint filter was not initialized!"
        for block in blocks:
            filtered_block = self._filter.filter_rows_for_block(block)
            if filtered_block.num_rows > 0:
                yield filtered_block


def _get_checkpoint_map_transformer(
    data_context: DataContext, checkpointed_ids_ref: ObjectRef[numpy.ndarray]
) -> MapTransformer:
    fn = _CheckpointFilterFn(data_context.checkpoint_config, checkpointed_ids_ref)

    transformer_fn = BlockMapTransformFn(
        block_fn=fn,
        output_block_size_option=OutputBlockSizeOption.of(
            target_max_block_size=data_context.target_max_block_size,
        ),
    )
    return MapTransformer(
        transform_fns=[transformer_fn],
        init_fn=fn.init_checkpoint_filter,
    )

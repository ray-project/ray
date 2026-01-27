import logging
from typing import Iterable

from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.data.checkpoint.interfaces import (
    CheckpointConfig,
)

logger = logging.getLogger(__name__)


# Checkpoint keyword argument name
CHECKPOINT_EXISTED_KWARG_NAME = "is_checkpoint_existed"
CHECKPOINTED_IDS_KWARG_NAME = "checkpointed_ids"


def filter_checkpointed_rows_for_blocks(
    blocks: Iterable[Block],
    task_context: TaskContext,
    checkpoint_config: CheckpointConfig,
) -> Iterable[Block]:
    """For each block, filter rows that have already been checkpointed
    and yield the resulting block."""
    from ray.data.checkpoint.checkpoint_filter import (
        BatchBasedCheckpointFilter,
    )

    is_checkpoint_existed = task_context.kwargs[CHECKPOINT_EXISTED_KWARG_NAME]
    checkpointed_ids = task_context.kwargs[CHECKPOINTED_IDS_KWARG_NAME]

    if is_checkpoint_existed:
        ckpt_filter = BatchBasedCheckpointFilter(checkpoint_config)

        def filter_fn(block: Block) -> Block:
            return ckpt_filter.filter_rows_for_block(
                block=block,
                checkpointed_ids=checkpointed_ids,
            )

        for block in blocks:
            filtered_block = filter_fn(block)
            ba = BlockAccessor.for_block(filtered_block)
            if ba.num_rows() > 0:
                yield filtered_block
    else:
        for block in blocks:
            ba = BlockAccessor.for_block(block)
            if ba.num_rows() > 0:
                yield block


def filter_checkpointed_rows_for_batches(
    batches: Iterable[DataBatch],
    task_context: TaskContext,
    checkpoint_config: CheckpointConfig,
) -> Iterable[DataBatch]:
    """For each batch, filter rows that have already been checkpointed
    and yield the resulting batches."""
    from ray.data.checkpoint.checkpoint_filter import (
        BatchBasedCheckpointFilter,
    )

    is_checkpoint_existed = task_context.kwargs[CHECKPOINT_EXISTED_KWARG_NAME]

    if is_checkpoint_existed:
        ckpt_filter = BatchBasedCheckpointFilter(checkpoint_config)
        checkpointed_ids = task_context.kwargs[CHECKPOINTED_IDS_KWARG_NAME]

        def filter_fn(batch: DataBatch) -> DataBatch:
            return ckpt_filter.filter_rows_for_batch(
                batch=batch,
                checkpointed_ids=checkpointed_ids,
            )

        for batch in batches:
            filtered_batch = filter_fn(batch)
            yield filtered_batch
    else:
        for batch in batches:
            yield batch

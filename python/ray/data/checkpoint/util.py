import logging
from typing import Iterable

import ray
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.types import ObjectRef

logger = logging.getLogger(__name__)


# Checkpoint keyword argument name
CHECKPOINTED_IDS_KWARG_NAME = "checkpointed_ids"


def filter_checkpointed_rows_for_blocks(
    blocks: Iterable[Block],
    task_context: TaskContext,
) -> Iterable[Block]:
    """For each block, filter rows that have already been checkpointed
    and yield the resulting block."""

    job_id = ray.get_runtime_context().get_job_id()
    ckpt_filter_name = "checkpoint_filter_" + job_id
    try:
        ckpt_filter = ray.get_actor(ckpt_filter_name)
    except Exception as e:
        raise RuntimeError(
            f"Trying to get checkpoint filter {ckpt_filter_name} failed. "
            "This filter may have been killed due to OOM or other reasons. "
            f"Original error: {e}"
        ) from e

    def filter_fn(block: Block) -> ObjectRef[Block]:
        return ckpt_filter.filter_rows_for_block.remote(
            block=block,
        )

    for block in blocks:
        filtered_block = ray.get(filter_fn(block))
        ba = BlockAccessor.for_block(filtered_block)
        if ba.num_rows() > 0:
            yield filtered_block


def filter_checkpointed_rows_for_batches(
    batches: Iterable[DataBatch],
    task_context: TaskContext,
) -> Iterable[DataBatch]:
    """For each batch, filter rows that have already been checkpointed
    and yield the resulting batches."""
    job_id = ray.get_runtime_context().get_job_id()
    ckpt_filter_name = "checkpoint_filter_" + job_id
    try:
        ckpt_filter = ray.get_actor(ckpt_filter_name)
    except Exception as e:
        raise RuntimeError(
            f"Trying to get checkpoint filter {ckpt_filter_name} failed. "
            "This filter may have been killed due to OOM or other reasons. "
            f"Original error: {e}"
        ) from e

    def filter_fn(batch: DataBatch) -> DataBatch:
        return ckpt_filter.filter_rows_for_batch(
            batch=batch,
        )

    for batch in batches:
        filtered_batch = filter_fn(batch)
        yield filtered_batch

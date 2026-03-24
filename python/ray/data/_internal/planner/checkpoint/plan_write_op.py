import warnings
from typing import Iterable, List, Tuple

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
)
from ray.data._internal.logical.operators import Write
from ray.data._internal.planner.plan_write_op import (
    PENDING_CHECKPOINTS_KWARG_NAME,
    WRITE_UUID_KWARG_NAME,
    _plan_write_op_internal,
    generate_collect_write_stats_fn,
)
from ray.data.block import Block, BlockAccessor
from ray.data.checkpoint.checkpoint_writer import (
    CheckpointWriter,
    PendingCheckpoint,
)
from ray.data.checkpoint.interfaces import (
    InvalidCheckpointingOperators,
)
from ray.data.context import DataContext
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.file_datasink import _FileDatasink
from ray.data.datasource.filename_provider import _split_base_and_ext


def _validate_id_column_exists(id_column: str, block: Block) -> None:
    """Validate that the ID column exists in the block.

    Args:
        id_column: The name of the ID column to validate.
        block: The block to check.

    Raises:
        ValueError: If the ID column is not present in the block.
    """
    block_accessor = BlockAccessor.for_block(block)
    if id_column not in block_accessor.column_names():
        raise ValueError(
            f"ID column {id_column} is "
            f"absent in the block to be written. Do not drop or rename "
            f"this column."
        )


def _combine_blocks(
    blocks: Iterable[Block],
) -> Tuple[List[Block], Block]:
    """Combine multiple blocks into a single block.

    This is used by checkpoint transforms to match the behavior of _FileDatasink.write(),
    which combines all input blocks into one output file.

    Args:
        blocks: Iterable of blocks to combine.

    Returns:
        A tuple of (block_list, combined_block) where:
        - block_list: The original blocks as a list (for later iteration)
        - combined_block: A single block combining all input blocks
    """
    block_list = list(blocks)
    builder = DelegatingBlockBuilder()
    for block in block_list:
        builder.add_block(block)
    combined_block = builder.build()
    return block_list, combined_block


def plan_write_op_with_checkpoint_writer(
    op: Write, physical_children: List[PhysicalOperator], data_context: DataContext
) -> PhysicalOperator:
    """Plan a write operation with checkpoint support.

    For file-based datasinks (_FileDatasink):
        Uses 2-phase commit for atomicity:
        1. Pre-write: computes expected paths, write pending checkpoints
        2. Write: writes data files
        3. Post-write: commits checkpoints (renames pending -> committed)

    Writing the pending checkpoint BEFORE the data file is critical: the
    pending checkpoint is the source of truth for recovery. If failure occurs
    after data write but before commit, recovery finds the pending checkpoint,
    deletes the matching data files, and retries cleanly. Writing checkpoints
    after data files would be non-atomic — if failure occurs between data
    write and checkpoint write, there's no record of which data files are
    uncommitted.

    For non-file datasinks (SQLDatasink, etc.):
        Falls back to post-write checkpointing:
        1. Write: Write data to destination
        2. Post-write: Write checkpoints

    Non-file sinks (SQL, MongoDB, etc.) cannot predict a "file path" - data goes
    to database rows or documents. So we fall back to writing data first, then
    checkpointing. If failure occurs after data write but before checkpoint
    write, the same data may be written again on retry without removing the
    old data (at-least-once semantics for non-idempotent operations).
    """
    assert data_context.checkpoint_config is not None

    datasink = op.datasink_or_legacy_datasource
    if not isinstance(datasink, Datasink):
        raise InvalidCheckpointingOperators(
            f"To enable checkpointing, Write operation must use a "
            f"Datasink and not a legacy Datasource, but got: "
            f"{type(datasink)}"
        )

    checkpoint_writer = CheckpointWriter.create(data_context.checkpoint_config)
    collect_stats_fn = generate_collect_write_stats_fn()

    if isinstance(datasink, _FileDatasink):
        # File-based datasink: use 2-phase commit for atomicity
        # Pre-write transform: compute expected paths and write pending checkpoints
        prepare_checkpoint_fn = _generate_prepare_checkpoint_transform(
            data_context, datasink, checkpoint_writer
        )

        # Post-write transform: commit checkpoints
        commit_checkpoint_fn = _generate_commit_checkpoint_transform(checkpoint_writer)

        pre_transformations = [
            prepare_checkpoint_fn,
        ]
        post_transformations = [
            commit_checkpoint_fn,
            collect_stats_fn,
        ]
    else:
        # Non-file datasink (SQL, Mongo, etc.): fall back to non-atomic checkpoint
        # No 2-phase commit - write checkpoint after data write
        # This might cause duplicate writes if the write operation is retried.
        warnings.warn(
            f"Checkpointing with non-file datasink ({type(datasink).__name__}) "
            f"uses post-write checkpointing, which provides at-least-once "
            f"semantics. If a failure occurs after data is written but before "
            f"the checkpoint is saved, duplicate data may be written on retry. "
            f"This will be addressed in a future version."
        )
        write_checkpoint_fn = _generate_non_atomic_write_checkpoint_transform(
            data_context, checkpoint_writer
        )
        post_transformations = [
            write_checkpoint_fn,
            collect_stats_fn,
        ]
        pre_transformations = []

    physical_op = _plan_write_op_internal(
        op,
        physical_children,
        data_context,
        post_transformations=post_transformations,
        pre_transformations=pre_transformations,
    )

    return physical_op


def _generate_base_filename(
    datasink: _FileDatasink,
    ctx: TaskContext,
) -> str:
    """Compute the base filename (without extension) for this task's data files.

    This is called BEFORE writing to determine the filename prefix for data files
    that will be written by this task. Datasinks may write multiple files (with
    partitioning, max_rows_per_file, etc.), all sharing this base filename.

    Args:
        datasink: The file datasink being used.
        ctx: The task context.

    Returns:
        The base filename without extension (e.g., "write_uuid_000000_000000").
        Used both as a checkpoint ID for deterministic naming and as a prefix
        for matching data files during recovery.
    """
    write_uuid = ctx.kwargs.get(WRITE_UUID_KWARG_NAME)
    assert write_uuid is not None, "WRITE_UUID_KWARG_NAME is required"

    filename = datasink.filename_provider.get_filename_for_task(
        write_uuid, ctx.task_idx
    )

    # All file datasinks can potentially generate multiple files (e.g., with
    # partitioning, max_rows_per_file, etc.). Use prefix matching to handle
    # cases like "{filename}-{i}.parquet".
    base, _ = _split_base_and_ext(filename)
    return base


def _generate_prepare_checkpoint_transform(
    data_context: DataContext,
    datasink: _FileDatasink,
    checkpoint_writer: CheckpointWriter,
) -> BlockMapTransformFn:
    """Generate transform for preparing checkpoints BEFORE data write.

    This transform runs BEFORE the data write to enable rollback on failure.
    By recording the expected file path in a pending checkpoint first, we can
    clean up orphaned data files if the task fails after writing data but
    before committing.

    Steps:
    1. Combines all blocks (matching _FileDatasink behavior)
    2. Computes expected data file path prefix from FilenameProvider
    3. Writes pending checkpoint with expected path prefix as filename
    4. Stores pending checkpoint info in ctx.kwargs for later commit
    """

    def prepare_checkpoint(
        blocks: Iterable[Block], ctx: TaskContext
    ) -> Iterable[Block]:
        # Combine all blocks to match _FileDatasink.write() behavior
        # which combines all input blocks into one output file
        block_list, combined_block = _combine_blocks(blocks)
        ba = BlockAccessor.for_block(combined_block)

        if ba.num_rows() > 0:
            # Validate ID column exists
            id_column = data_context.checkpoint_config.id_column
            _validate_id_column_exists(id_column, combined_block)

            # Compute base filename using FilenameProvider
            # Note: This only depends on write_uuid and task_idx, NOT block content
            # base_filename is the filename without extension, used as checkpoint_id
            # for deterministic naming (same on retry, enabling idempotent writes)
            base_filename = _generate_base_filename(datasink, ctx)

            # Extract ID column data for checkpoint
            # Project to the single column first, then convert to Arrow to
            # avoid materializing the entire block as an Arrow table.
            id_column_data = BlockAccessor.for_block(
                ba.select(columns=[id_column])
            ).to_arrow()[id_column]

            # Write pending checkpoint with the base filename as checkpoint_id.
            # The checkpoint filename will be {base_filename}.pending.parquet.
            # During recovery, the pending checkpoint basename (without
            # .pending.parquet) is used as a prefix to match data files.
            pending = checkpoint_writer.write_pending_checkpoint(
                id_column_data,
                checkpoint_id=base_filename,
            )

            # Store pending checkpoint for commit phase
            if pending is not None:
                if PENDING_CHECKPOINTS_KWARG_NAME not in ctx.kwargs:
                    ctx.kwargs[PENDING_CHECKPOINTS_KWARG_NAME] = []
                ctx.kwargs[PENDING_CHECKPOINTS_KWARG_NAME].append(pending)

        # Return original blocks for the write transform
        return iter(block_list)

    return BlockMapTransformFn(
        prepare_checkpoint,
        is_udf=False,
        disable_block_shaping=True,
    )


def _generate_commit_checkpoint_transform(
    checkpoint_writer: CheckpointWriter,
) -> BlockMapTransformFn:
    """Generate transform for committing checkpoints AFTER data write.

    This transform runs AFTER the data write succeeds, completing the 2-phase
    commit. The commit operation (renaming pending -> committed) is the atomic
    point: once committed, the data is considered durably written. If failure
    occurs before this point, recovery will find the pending checkpoint and
    can safely delete the orphaned data files using the stored path.

    Steps:
    1. Retrieves pending checkpoints from ctx.kwargs
    2. Commits each pending checkpoint (rename pending -> committed)
    """

    def commit_checkpoints(
        blocks: Iterable[Block], ctx: TaskContext
    ) -> Iterable[Block]:
        # Get pending checkpoints written in pre-write phase
        pending_checkpoints: List[PendingCheckpoint] = ctx.kwargs.get(
            PENDING_CHECKPOINTS_KWARG_NAME, []
        )

        # Commit each pending checkpoint
        for pending in pending_checkpoints:
            checkpoint_writer.commit_checkpoint(pending)

        return blocks

    return BlockMapTransformFn(
        commit_checkpoints,
        is_udf=False,
        disable_block_shaping=True,
    )


def _generate_non_atomic_write_checkpoint_transform(
    data_context: DataContext,
    checkpoint_writer: CheckpointWriter,
) -> BlockMapTransformFn:
    """Generate transform for writing checkpoints AFTER data write (non-file datasinks).

    This is a fallback for non-file datasinks (SQL, Mongo, etc.) that don't
    support deletions. Unlike file-based sinks where we can delete orphaned
    data files during recovery, these sinks have no way to undo a write once
    data has been inserted into rows or documents.

    The checkpoint is written directly after the data write completes. This
    provides at-least-once semantics: if failure occurs after data write but
    before checkpoint write, the same data will be written again on retry
    without removing the old data.

    For idempotent operations (upserts with unique keys), this is safe. For
    non-idempotent operations (inserts), duplicates may result.

    TODO: For datasinks that support deletions (e.g., SQL DELETE by ID), we
    could store written IDs in pending checkpoints and delete them on recovery,
    avoiding duplicates even for non-idempotent operations.
    """

    def write_checkpoint(blocks: Iterable[Block], ctx: TaskContext) -> Iterable[Block]:
        per_block_write_returns = ctx.kwargs.get("_datasink_write_return_per_block")
        if per_block_write_returns is not None:
            block_list = list(blocks)
            if len(per_block_write_returns) != len(block_list):
                raise ValueError(
                    "Per-block write returns length mismatch: "
                    f"expected {len(block_list)} but got {len(per_block_write_returns)}"
                )

            id_column = data_context.checkpoint_config.id_column
            for i, block in enumerate(block_list):
                ba = BlockAccessor.for_block(block)
                if ba.num_rows() == 0:
                    continue

                _validate_id_column_exists(id_column, block)
                checkpoint_writer.write_block_checkpoint(
                    ba,
                    write_result=per_block_write_returns[i],
                )

            return iter(block_list)

        # Combine all blocks
        block_list, combined_block = _combine_blocks(blocks)
        ba = BlockAccessor.for_block(combined_block)

        if ba.num_rows() > 0:
            # Validate ID column exists
            id_column = data_context.checkpoint_config.id_column
            _validate_id_column_exists(id_column, combined_block)

            # Write checkpoint directly (no 2-phase commit)
            # No data_file_path since non-file datasinks don't have file paths
            checkpoint_writer.write_block_checkpoint(
                ba,
                write_result=ctx.kwargs.get("_datasink_write_return"),
            )

        return iter(block_list)

    return BlockMapTransformFn(
        write_checkpoint,
        is_udf=False,
        # NOTE: No need for block-shaping
        disable_block_shaping=True,
    )

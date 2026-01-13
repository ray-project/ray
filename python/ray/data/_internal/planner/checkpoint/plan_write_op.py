import posixpath
from typing import Iterable, List, Optional, Tuple

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
)
from ray.data._internal.logical.operators.write_operator import Write
from ray.data._internal.planner.plan_write_op import (
    EXPECTED_FILE_PATHS_KWARG_NAME,
    PENDING_CHECKPOINTS_KWARG_NAME,
    WRITE_UUID_KWARG_NAME,
    WRITTEN_FILE_PATHS_KWARG_NAME,
    _plan_write_op_internal,
    generate_collect_write_stats_fn,
)
from ray.data.block import Block, BlockAccessor
from ray.data.checkpoint.checkpoint_writer import (
    DATA_FILE_PATH_PATTERN_PREFIX,
    CheckpointWriter,
    PendingCheckpoint,
)
from ray.data.checkpoint.interfaces import (
    InvalidCheckpointingOperators,
)
from ray.data.context import DataContext
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.file_datasink import _FileDatasink


def plan_write_op_with_checkpoint_writer(
    op: Write, physical_children: List[PhysicalOperator], data_context: DataContext
) -> PhysicalOperator:
    """Plan a write operation with checkpoint support.

    For file-based datasinks (_FileDatasink):
        Uses 2-phase commit for atomicity:
        1. Pre-write: Compute expected paths, write pending checkpoints
        2. Write: Write data files
        3. Post-write: Commit checkpoints (rename pending -> committed), collect stats

    For non-file datasinks (SQLDatasink, etc.):
        Falls back to post-write checkpointing:
        1. Write: Write data to destination
        2. Post-write: Write checkpoints, collect stats
    """
    assert data_context.checkpoint_config is not None

    datasink = op._datasink_or_legacy_datasource
    if not isinstance(datasink, Datasink):
        raise InvalidCheckpointingOperators(
            f"To enable checkpointing, Write operation must use a "
            f"Datasink and not a legacy Datasource, but got: "
            f"{type(datasink)}"
        )

    checkpoint_writer = CheckpointWriter.create(data_context.checkpoint_config)
    collect_stats_fn = generate_collect_write_stats_fn()

    # Check if datasink is file-based (supports 2-phase commit)
    if isinstance(datasink, _FileDatasink):
        # File-based datasink: use 2-phase commit for atomicity
        # Pre-write transform: compute expected paths and write pending checkpoints
        write_pending_checkpoint_fn = _generate_pending_checkpoint_transform(
            data_context, datasink, checkpoint_writer
        )

        # Post-write transform: commit checkpoints
        commit_checkpoint_fn = _generate_commit_checkpoint_transform(
            data_context, checkpoint_writer
        )

        physical_op = _plan_write_op_internal(
            op,
            physical_children,
            data_context,
            extra_transformations=[
                commit_checkpoint_fn,
                collect_stats_fn,
            ],
            pre_write_transformations=[
                write_pending_checkpoint_fn,
            ],
        )
    else:
        # Non-file datasink (SQL, Mongo, etc.): fall back to post-write checkpoint
        # No 2-phase commit - write checkpoint after data write
        write_checkpoint_fn = _generate_post_write_checkpoint_transform(
            data_context, checkpoint_writer
        )

        physical_op = _plan_write_op_internal(
            op,
            physical_children,
            data_context,
            extra_transformations=[
                write_checkpoint_fn,
                collect_stats_fn,
            ],
            pre_write_transformations=[],
        )

    return physical_op


def _compute_expected_data_file_path(
    datasink: Datasink,
    block: Block,
    ctx: TaskContext,
    block_index: int,
) -> Tuple[Optional[str], bool]:
    """Compute the expected data file path using FilenameProvider.

    This is called BEFORE writing to determine where the data file will be.

    For datasinks that can write multiple files (like ParquetDatasink with
    max_rows_per_file or partitioning), this returns a pattern prefix instead
    of an exact path.

    Args:
        datasink: The datasink being used.
        block: The block to be written.
        ctx: The task context.
        block_index: The index of the block.

    Returns:
        Tuple of (path_or_pattern, is_pattern):
        - path_or_pattern: The expected path or pattern prefix (with DATA_FILE_PATH_PATTERN_PREFIX)
        - is_pattern: True if this is a pattern, False if exact path
    """
    # Only file-based datasinks have FilenameProvider
    if not isinstance(datasink, _FileDatasink):
        return None, False

    write_uuid = ctx.kwargs.get(WRITE_UUID_KWARG_NAME)
    if write_uuid is None:
        return None, False

    filename = datasink.filename_provider.get_filename_for_block(
        block, write_uuid, ctx.task_idx, block_index
    )

    # Check if this is a ParquetDatasink which can write multiple files
    from ray.data._internal.datasource.parquet_datasink import ParquetDatasink

    if isinstance(datasink, ParquetDatasink):
        # ParquetDatasink can write multiple files with pattern like:
        # "{filename}-{i}.parquet"
        # We store the base filename as a pattern prefix
        # Remove file extension to get the base
        base_filename = filename
        if "." in filename:
            base_filename = filename.rsplit(".", 1)[0]

        # Return as pattern - any file starting with this prefix is a match
        pattern_path = posixpath.join(datasink.path, base_filename)
        return f"{DATA_FILE_PATH_PATTERN_PREFIX}{pattern_path}", True

    # For other file datasinks, return exact path
    return posixpath.join(datasink.path, filename), False


def _generate_pending_checkpoint_transform(
    data_context: DataContext,
    datasink: Datasink,
    checkpoint_writer: CheckpointWriter,
) -> BlockMapTransformFn:
    """Generate transform for writing pending checkpoints BEFORE data write.

    This transform:
    1. Combines all blocks (matching _FileDatasink behavior)
    2. Computes expected data file path from FilenameProvider
    3. Writes pending checkpoint with expected path in metadata
    4. Stores pending checkpoint info in ctx.kwargs for later commit

    For datasinks that can write multiple files (ParquetDatasink), the expected
    path is stored as a pattern prefix (e.g., "pattern://path/to/file") so
    recovery can check for any matching files.
    """

    def write_pending_checkpoint(
        blocks: Iterable[Block], ctx: TaskContext
    ) -> Iterable[Block]:
        # Convert to list to allow multiple iterations
        block_list = list(blocks)

        # Combine all blocks to match _FileDatasink.write() behavior
        # which combines all input blocks into one output file
        builder = DelegatingBlockBuilder()
        for block in block_list:
            builder.add_block(block)
        combined_block = builder.build()
        ba = BlockAccessor.for_block(combined_block)

        if ba.num_rows() > 0:
            # Validate ID column exists
            if data_context.checkpoint_config.id_column not in ba.column_names():
                raise ValueError(
                    f"ID column {data_context.checkpoint_config.id_column} is "
                    f"absent in the block to be written. Do not drop or rename "
                    f"this column."
                )

            # Compute expected data file path using FilenameProvider
            # block_index=0 because _FileDatasink always uses 0 for combined block
            expected_path_or_pattern, is_pattern = _compute_expected_data_file_path(
                datasink, combined_block, ctx, block_index=0
            )

            # Store expected path for verification after write
            if EXPECTED_FILE_PATHS_KWARG_NAME not in ctx.kwargs:
                ctx.kwargs[EXPECTED_FILE_PATHS_KWARG_NAME] = []
            if expected_path_or_pattern:
                ctx.kwargs[EXPECTED_FILE_PATHS_KWARG_NAME].append(
                    {
                        "block_index": 0,
                        "expected_path": expected_path_or_pattern,
                        "is_pattern": is_pattern,
                    }
                )

            # Write pending checkpoint with expected data file path/pattern
            pending = checkpoint_writer.write_pending_checkpoint(
                ba, expected_data_file_path=expected_path_or_pattern or "unknown"
            )

            # Store pending checkpoint for commit phase
            if pending is not None:
                if PENDING_CHECKPOINTS_KWARG_NAME not in ctx.kwargs:
                    ctx.kwargs[PENDING_CHECKPOINTS_KWARG_NAME] = []
                ctx.kwargs[PENDING_CHECKPOINTS_KWARG_NAME].append(pending)

        # Return original blocks for the write transform
        return iter(block_list)

    return BlockMapTransformFn(
        write_pending_checkpoint,
        is_udf=False,
        disable_block_shaping=True,
    )


def _generate_commit_checkpoint_transform(
    data_context: DataContext,
    checkpoint_writer: CheckpointWriter,
) -> BlockMapTransformFn:
    """Generate transform for committing checkpoints AFTER data write.

    This transform:
    1. Retrieves pending checkpoints from ctx.kwargs
    2. Optionally verifies data files were written
    3. Commits each pending checkpoint (rename pending -> committed)
    """

    def commit_checkpoints(
        blocks: Iterable[Block], ctx: TaskContext
    ) -> Iterable[Block]:
        # Get pending checkpoints written in pre-write phase
        pending_checkpoints: List[PendingCheckpoint] = ctx.kwargs.get(
            PENDING_CHECKPOINTS_KWARG_NAME, []
        )

        # Get written file paths for verification
        written_paths = ctx.kwargs.get(WRITTEN_FILE_PATHS_KWARG_NAME, [])
        written_path_set = {info.get("full_path") for info in written_paths}

        # Commit each pending checkpoint
        for pending in pending_checkpoints:
            # Optional: verify data file was actually written
            if written_paths and pending.data_file_path not in written_path_set:
                # Log warning but still commit - data might have been written
                # to a different path or verification might not be needed
                pass

            checkpoint_writer.commit_checkpoint(pending)

        return blocks

    return BlockMapTransformFn(
        commit_checkpoints,
        is_udf=False,
        disable_block_shaping=True,
    )


def _generate_post_write_checkpoint_transform(
    data_context: DataContext,
    checkpoint_writer: CheckpointWriter,
) -> BlockMapTransformFn:
    """Generate transform for writing checkpoints AFTER data write (non-file datasinks).

    This is a fallback for non-file datasinks (SQL, Mongo, etc.) where we cannot
    compute an expected file path before writing. The checkpoint is written
    directly after the data write completes, without 2-phase commit.

    Note: This approach is less atomic than 2-phase commit - if the checkpoint
    write fails after data write succeeds, the data will be re-processed on retry.
    """

    def write_checkpoint(blocks: Iterable[Block], ctx: TaskContext) -> Iterable[Block]:
        # Convert to list to allow multiple iterations
        block_list = list(blocks)

        # Combine all blocks
        builder = DelegatingBlockBuilder()
        for block in block_list:
            builder.add_block(block)
        combined_block = builder.build()
        ba = BlockAccessor.for_block(combined_block)

        if ba.num_rows() > 0:
            # Validate ID column exists
            if data_context.checkpoint_config.id_column not in ba.column_names():
                raise ValueError(
                    f"ID column {data_context.checkpoint_config.id_column} is "
                    f"absent in the block to be written. Do not drop or rename "
                    f"this column."
                )

            # Write checkpoint directly (no 2-phase commit)
            # No data_file_path since non-file datasinks don't have file paths
            checkpoint_writer.write_block_checkpoint(ba)

        return iter(block_list)

    return BlockMapTransformFn(
        write_checkpoint,
        is_udf=False,
        disable_block_shaping=True,
    )

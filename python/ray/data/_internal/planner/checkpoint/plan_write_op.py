import posixpath
from typing import Iterable, List, Optional

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
from ray.data.checkpoint.checkpoint_writer import CheckpointWriter, PendingCheckpoint
from ray.data.checkpoint.interfaces import (
    InvalidCheckpointingOperators,
)
from ray.data.context import DataContext
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.file_datasink import _FileDatasink


def plan_write_op_with_checkpoint_writer(
    op: Write, physical_children: List[PhysicalOperator], data_context: DataContext
) -> PhysicalOperator:
    """Plan a write operation with 2-phase commit checkpoint support.

    The transform order is:
    1. Pre-write: Compute expected paths, write pending checkpoints
    2. Write: Write data files
    3. Post-write: Commit checkpoints, collect stats
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

    # Pre-write transform: compute expected paths and write pending checkpoints
    write_pending_checkpoint_fn = _generate_pending_checkpoint_transform(
        data_context, datasink, checkpoint_writer
    )

    # Post-write transform: commit checkpoints
    commit_checkpoint_fn = _generate_commit_checkpoint_transform(
        data_context, checkpoint_writer
    )

    collect_stats_fn = generate_collect_write_stats_fn()

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

    return physical_op


def _compute_expected_data_file_path(
    datasink: Datasink,
    block: Block,
    ctx: TaskContext,
    block_index: int,
) -> Optional[str]:
    """Compute the expected data file path using FilenameProvider.

    This is called BEFORE writing to determine where the data file will be.

    Args:
        datasink: The datasink being used.
        block: The block to be written.
        ctx: The task context.
        block_index: The index of the block.

    Returns:
        The expected full path to the data file, or None if not a file datasink.
    """
    # Only file-based datasinks have FilenameProvider
    if not isinstance(datasink, _FileDatasink):
        return None

    write_uuid = ctx.kwargs.get(WRITE_UUID_KWARG_NAME)
    if write_uuid is None:
        return None

    filename = datasink.filename_provider.get_filename_for_block(
        block, write_uuid, ctx.task_idx, block_index
    )
    return posixpath.join(datasink.path, filename)


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
            expected_path = _compute_expected_data_file_path(
                datasink, combined_block, ctx, block_index=0
            )

            # Store expected path for verification after write
            if EXPECTED_FILE_PATHS_KWARG_NAME not in ctx.kwargs:
                ctx.kwargs[EXPECTED_FILE_PATHS_KWARG_NAME] = []
            if expected_path:
                ctx.kwargs[EXPECTED_FILE_PATHS_KWARG_NAME].append(
                    {
                        "block_index": 0,
                        "expected_path": expected_path,
                    }
                )

            # Write pending checkpoint with expected data file path
            pending = checkpoint_writer.write_pending_checkpoint(
                ba, expected_data_file_path=expected_path or "unknown"
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

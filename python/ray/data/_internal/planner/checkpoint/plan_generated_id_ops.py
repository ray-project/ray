"""Planners for resuming ``CheckpointConfig(generated_id_column=...)`` jobs.

Generated row IDs name where each row lives (file, row group, position), so a
resumed job skips committed work as early as possible instead of filtering
every row through the actor-pool ``CheckpointFilter`` the ``id_column`` path
uses:

- ``ListFiles`` gets the done read unit ids and drops those files and row
  groups before they are listed (``EXCLUDED_READ_UNIT_IDS_KWARG_NAME``).
- Right after ``ReadFiles``, a ``DropCommittedRows`` step drops the committed
  rows of partly done row groups by looking up each row's generated ID in the
  row group's mask. It fuses into the read task.

Both read the checkpoint loaded once, when execution starts, through
``load_checkpoint``; it is passed as a task kwarg lazily because it isn't
loaded yet at planning time.
"""

from functools import partial
from typing import Callable, Iterable, List

from ray.data._internal.datasource_v2.interfaces.read_units import (
    EXCLUDED_READ_UNIT_IDS_KWARG_NAME,
)
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.logical.operators import ListFiles, ReadFiles
from ray.data._internal.planner.plan_list_files_op import plan_list_files_op
from ray.data._internal.planner.plan_read_files_op import plan_read_files_op
from ray.data.block import Block, BlockAccessor
from ray.data.checkpoint.generated_id import (
    GeneratedIdCheckpoint,
    _drop_committed_rows,
)
from ray.data.context import DataContext

# ``TaskContext.kwargs`` key for the masks of partly done row groups.
_PARTIAL_MASKS_KWARG_NAME = "generated_id_partial_masks"


def plan_list_files_op_with_done_read_units(
    op: ListFiles,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
    *,
    load_checkpoint: Callable[[], GeneratedIdCheckpoint],
) -> MapOperator:
    """Plan ``ListFiles`` so it leaves out the read units already done."""
    map_op = plan_list_files_op(op, physical_children, data_context)
    map_op.add_map_task_kwargs_fn(
        lambda: {EXCLUDED_READ_UNIT_IDS_KWARG_NAME: load_checkpoint().done_unit_ids}
    )
    return map_op


def _drop_committed_rows_in_blocks(
    blocks: Iterable[Block], ctx: TaskContext, *, id_column: str
) -> Iterable[Block]:
    partial_masks = ctx.kwargs.get(_PARTIAL_MASKS_KWARG_NAME)
    for block in blocks:
        if partial_masks:
            block = _drop_committed_rows(
                BlockAccessor.for_block(block).to_arrow(), id_column, partial_masks
            )
        if BlockAccessor.for_block(block).num_rows() > 0:
            yield block


def plan_read_files_op_with_done_rows(
    op: ReadFiles,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
    *,
    load_checkpoint: Callable[[], GeneratedIdCheckpoint],
) -> MapOperator:
    """Plan ``ReadFiles`` followed by a step that drops the committed rows of
    partly done row groups.

    The step runs right after the read, so the generated ID column is still
    there, and it fuses into the read task. A limit pushed into the read
    counts rows before they are dropped, as with the ``id_column`` path's
    filter.
    """
    checkpoint_config = data_context.checkpoint_config
    assert checkpoint_config is not None
    read_op = plan_read_files_op(op, physical_children, data_context)
    id_column = checkpoint_config.id_column
    drop_op = MapOperator.create(
        MapTransformer(
            [
                BlockMapTransformFn(
                    partial(_drop_committed_rows_in_blocks, id_column=id_column)
                )
            ]
        ),
        read_op,
        data_context,
        name="DropCommittedRows",
    )
    drop_op.add_map_task_kwargs_fn(
        lambda: {_PARTIAL_MASKS_KWARG_NAME: load_checkpoint().partial_masks}
    )
    return drop_op

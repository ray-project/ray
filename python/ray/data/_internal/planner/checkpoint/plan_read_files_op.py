"""Checkpoint-aware planner for the V2 ``ReadFiles`` logical operator.

Mirrors :func:`plan_read_op_with_checkpoint_filter` (the V1 ``Read``
variant) so V2 uses the same wrapping ActorPool ``CheckpointFilter``
``MapOperator`` downstream of the read: same
``_CheckpointFilterFn`` / ``_get_checkpoint_map_transformer``, same
memory reservation formula, and ``supports_fusion=False`` so the
filter stays a distinct op.

Registered via ``Planner._get_plan_fns_for_checkpointing`` so it only
runs when ``DataContext.checkpoint_config`` is set *and* the logical
plan is a ``Write`` or ``StreamingSplit`` with a ``ReadFiles`` at the
leaf. V2's plain ``plan_read_files_op`` stays checkpoint-unaware; this
file is the only place V2 reads pick up a checkpoint filter.
"""
from typing import List, Optional

import pyarrow
import pyarrow.fs

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.logical.operators import ReadFiles
from ray.data._internal.planner.checkpoint.plan_read_op import (
    create_checkpoint_filter_op,
)
from ray.data._internal.planner.plan_read_files_op import plan_read_files_op
from ray.data.context import DataContext


def plan_read_files_op_with_checkpoint_filter(
    data_file_dir: Optional[str],
    data_file_filesystem: Optional["pyarrow.fs.FileSystem"],
    op: ReadFiles,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    """Wrap a V2 ``ReadFiles`` physical op with a ``CheckpointFilter``.

    Defers all wrapping behavior to V1's :func:`create_checkpoint_filter_op`
    so the not-found short-circuit, ``IdColumnCheckpointManager.load_checkpoint``
    invocation, actor-pool sizing, and ``supports_fusion=False`` placement stay
    in one place across the V1 and V2 read paths.
    """
    physical_read_op = plan_read_files_op(op, physical_children, data_context)
    return create_checkpoint_filter_op(
        physical_read_op, data_context, data_file_dir, data_file_filesystem
    )

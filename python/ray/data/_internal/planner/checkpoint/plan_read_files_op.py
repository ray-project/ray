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
import pyarrow.fs as fs

from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.operators import ReadFiles
from ray.data._internal.planner.checkpoint.plan_read_op import (
    CHECKPOINT_MEMORY_SAFETY_FACTOR,
    _get_checkpoint_map_transformer,
)
from ray.data._internal.planner.plan_read_files_op import plan_read_files_op
from ray.data.checkpoint.checkpoint_filter import IdColumnCheckpointManager
from ray.data.context import DataContext
from ray.data.datasource.path_util import _unwrap_protocol


def plan_read_files_op_with_checkpoint_filter(
    data_file_dir: Optional[str],
    data_file_filesystem: Optional["pyarrow.fs.FileSystem"],
    op: ReadFiles,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    """Wrap a V2 ``ReadFiles`` physical op with a ``CheckpointFilter``.

    Behavior parity with V1's ``plan_read_op_with_checkpoint_filter``:

    1. If the checkpoint directory doesn't exist, return the plain read
       op.
    2. Otherwise load committed IDs via
       :class:`IdColumnCheckpointManager.load_checkpoint` (including
       the pending-checkpoint cleanup step when ``data_file_dir`` is
       provided). If the load yields no IDs, return the plain read op.
    3. Wrap the read with an ``ActorPoolMapOperator`` running
       ``_CheckpointFilterFn``. Memory is reserved per-actor as
       ``max(checkpoint_actor_memory_bytes, ids_size * 1.5)`` and
       ``supports_fusion=False`` keeps the filter a distinct physical
       stage.
    """
    physical_read_op = plan_read_files_op(op, physical_children, data_context)

    checkpoint_config = data_context.checkpoint_config
    info = checkpoint_config.filesystem.get_file_info(
        _unwrap_protocol(checkpoint_config.checkpoint_path)
    )
    if info.type == fs.FileType.NotFound:
        return physical_read_op

    checkpoint_manager = IdColumnCheckpointManager(checkpoint_config=checkpoint_config)
    checkpointed_ids_ref, checkpointed_ids_size = checkpoint_manager.load_checkpoint(
        data_file_dir, data_file_filesystem
    )
    if not checkpointed_ids_ref:
        return physical_read_op

    map_transformer = _get_checkpoint_map_transformer(
        data_context, checkpointed_ids_ref
    )
    return MapOperator.create(
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

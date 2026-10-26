"""Checkpoint-aware planner for ``AbstractFrom`` logical operators.

Mirrors :func:`plan_read_op_with_checkpoint_filter` and
:func:`plan_read_files_op_with_checkpoint_filter` so datasets built from
in-memory data (``from_items``, ``from_pandas``, ``from_arrow``,
``from_numpy``, ``from_blocks``, ...) get the same wrapping ActorPool
``CheckpointFilter`` ``MapOperator`` downstream of their
``InputDataBuffer`` when a job resumes from a checkpoint.

Registered via ``Planner._get_plan_fns_for_checkpointing`` so it only
runs when ``DataContext.checkpoint_config`` is set *and* the logical
plan supports checkpointing. The plain ``plan_from_op`` stays
checkpoint-unaware.

The ``InputDataBuffer`` is built here rather than by calling
``planner.plan_from_op``: ``planner.py`` imports this package at load
time, so importing back from it would be circular.
"""
from typing import List, Optional

import pyarrow
import pyarrow.fs

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.input_data_buffer import (
    InputDataBuffer,
)
from ray.data._internal.logical.operators import AbstractFrom
from ray.data._internal.planner.checkpoint.plan_read_op import (
    create_checkpoint_filter_op,
)
from ray.data.context import DataContext


def plan_from_op_with_checkpoint_filter(
    data_file_dir: Optional[str],
    data_file_filesystem: Optional["pyarrow.fs.FileSystem"],
    op: AbstractFrom,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    """Wrap an ``AbstractFrom`` physical op with a ``CheckpointFilter``.

    Defers all wrapping behavior to :func:`create_checkpoint_filter_op`
    so the not-found short-circuit, ``IdColumnCheckpointManager.load_checkpoint``
    invocation, actor-pool sizing, and ``supports_fusion=False`` placement stay
    in one place across the read and in-memory source paths.
    """
    assert len(physical_children) == 0
    physical_from_op = InputDataBuffer(data_context, op.input_data)
    return create_checkpoint_filter_op(
        physical_from_op, data_context, data_file_dir, data_file_filesystem
    )

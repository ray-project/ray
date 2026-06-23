"""Physical planner for the V2 ``ReadFiles`` logical operator.

``ReadFiles`` consumes ``FileManifest`` blocks from an upstream
``ListFiles`` physical op. This planner wires one map transform —
``do_read`` — that calls ``scanner.create_reader().read(manifest)`` for
each incoming bucket.

V2 reads never rename columns at the read stage; column renaming is
always handled by a ``Project`` operator above ``ReadFiles``.

Listing, shuffling, and size-balanced bucketing previously lived here;
they've moved to :func:`plan_list_files_op` where they belong.

Checkpoint wrapping (when ``data_context.checkpoint_config`` is set) is
handled by the companion
:func:`ray.data._internal.planner.checkpoint.plan_read_files_op.plan_read_files_op_with_checkpoint_filter`,
registered via the planner's ``_get_plan_fns_for_checkpointing`` hook —
same dispatch shape V1 uses for ``plan_read_op_with_checkpoint_filter``.
"""

from __future__ import annotations

import logging
from typing import Iterable, List

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.scanners.file_scanner import FileScanner
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.logical.operators import ReadFiles
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data.block import Block
from ray.data.context import DataContext

logger = logging.getLogger(__name__)


def _resolve_read_files_output_block_target(data_context: DataContext) -> int:
    """Resolve the byte target the ``OutputBlockSizeOption`` coalesces to.

    Resolution order:

    1. ``data_context.parquet_reader_target_output_block_size_bytes`` —
       user-set knob, wins when not ``None``.
    2. ``data_context.target_min_block_size`` — the "middle path"
       default; produces output blocks of ~1 MiB unless overridden.
    3. ``data_context.target_max_block_size`` — last-resort fallback
       when block sizing is otherwise disabled; preserves prior V2
       behavior in that edge case.

    The returned value is the *target* the buffer coalesces toward — not
    the *max*. ``target_max_block_size`` continues to govern the
    safety-split behavior inside ``BlockOutputBuffer``.
    """
    knob = data_context.parquet_reader_target_output_block_size_bytes
    if knob is not None:
        return int(knob)
    if data_context.target_min_block_size is not None:
        return int(data_context.target_min_block_size)
    return int(data_context.target_max_block_size)


def plan_read_files_op(
    op: ReadFiles,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    """Convert a ``ReadFiles`` logical op into a reader ``MapOperator``.

    Expects exactly one physical child: the upstream ``ListFiles`` op,
    which produces balanced manifest blocks via its transform chain.
    """
    assert len(physical_children) == 1
    upstream = physical_children[0]

    # NOTE: Avoid capturing the whole ``op`` in closures — only field values.
    scanner = op.scanner
    block_udf = op.block_udf

    def do_read(blocks: Iterable[Block], _: TaskContext) -> Iterable[Block]:
        reader = scanner.create_reader()
        # File-level predicate pruning (partition predicates pushed down
        # onto the scanner) runs per incoming manifest block. Only
        # ``FileScanner`` subclasses expose ``prune_manifest``; the base
        # implementation is an identity no-op, and ``ArrowFileScanner``
        # overrides it to evaluate ``partition_predicate``.
        for block in blocks:
            manifest = FileManifest(block)
            if isinstance(scanner, FileScanner):
                manifest = scanner.prune_manifest(manifest)
            if len(manifest) == 0:
                continue
            for table in reader.read(manifest):
                if block_udf is not None:
                    table = block_udf(table)
                yield table

    output_block_target = _resolve_read_files_output_block_target(data_context)
    return MapOperator.create(
        MapTransformer(
            [
                BlockMapTransformFn(
                    do_read,
                    is_udf=False,
                    output_block_size_option=OutputBlockSizeOption.of(
                        target_max_block_size=output_block_target,
                    ),
                ),
            ]
        ),
        upstream,
        data_context,
        name=op.name,
        compute_strategy=op.compute,
        ray_remote_args=op.ray_remote_args,
    )

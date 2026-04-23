"""Physical planner for the V2 ``ReadFiles`` logical operator.

``ReadFiles`` now consumes ``FileManifest`` blocks from an upstream
``ListFiles`` physical op. This planner wires one map transform —
``do_read`` — that calls ``scanner.create_reader().read(manifest)`` for
each incoming bucket and applies any recorded column renames.

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
from typing import Dict, Iterable, List, Optional

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


def _apply_column_renames(block: Block, renames: Optional[Dict[str, str]]) -> Block:
    """Rename columns on the block according to ``renames``.

    Scanner-level projection pushdown only knows the original column names
    in the file schema. Any ``old → new`` rename recorded on ``ReadFiles``
    (via ``ProjectionPushdown._push_projection_into_read_op``) must be
    applied here, before the block hits the executor — otherwise
    downstream ops receive tables whose column names don't match the
    logical plan's schema.
    """
    if not renames:
        return block
    new_names = [renames.get(n, n) for n in block.schema.names]
    return block.rename_columns(new_names)


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
    renames = op.column_renames
    block_udf = op.block_udf

    def do_read(blocks: Iterable[Block], _: TaskContext) -> Iterable[Block]:
        reader = scanner.create_reader()
        # File-level predicate pruning (partition predicates pushed down
        # onto the scanner) runs per incoming manifest block. Only
        # ``FileScanner`` subclasses expose ``prune_manifest``; the base
        # implementation is an identity no-op, and ``ArrowFileScanner``
        # overrides it to evaluate ``partition_predicate``.
        is_file_scanner = isinstance(scanner, FileScanner)
        for block in blocks:
            manifest = FileManifest(block)
            if is_file_scanner:
                manifest = scanner.prune_manifest(manifest)
            if len(manifest) == 0:
                continue
            for table in reader.read(manifest):
                # Apply caller-supplied block transform before renames so
                # the UDF sees the original on-disk column names, matching
                # V1 ``ParquetDatasource`` semantics.
                if block_udf is not None:
                    table = block_udf(table)
                yield _apply_column_renames(table, renames)

    return MapOperator.create(
        MapTransformer(
            [
                BlockMapTransformFn(
                    do_read,
                    is_udf=False,
                    output_block_size_option=OutputBlockSizeOption.of(
                        target_max_block_size=data_context.target_max_block_size,
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

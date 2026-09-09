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
from ray.data._internal.datasource_v2.partitioners.round_robin_partitioner import (
    RoundRobinPartitioner,
)
from ray.data._internal.datasource_v2.scanners.file_scanner import FileScanner
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import (
    MapOperator,
    _split_blocks,
)
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.logical.operators import ReadFiles
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data.block import Block
from ray.data.context import DataContext

logger = logging.getLogger(__name__)


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

    read_transform = BlockMapTransformFn(
        do_read,
        is_udf=False,
        output_block_size_option=OutputBlockSizeOption.of(
            target_max_block_size=data_context.target_max_block_size,
        ),
    )

    def read_and_split(blocks: Iterable[Block], ctx: TaskContext) -> Iterable[Block]:
        for block in blocks:
            factor = FileManifest(block).output_split_factor
            # Shape first so a large file stays bounded, then split each output
            # block. Never coalesce the additional splits back together.
            outputs = read_transform([block], ctx)
            yield from _split_blocks(outputs, factor) if factor > 1 else outputs

    partitioner = op.input_dependencies[0].file_partitioner
    explicit_block_target = (
        isinstance(partitioner, RoundRobinPartitioner)
        and partitioner.requires_global_input
    )
    transform = (
        BlockMapTransformFn(read_and_split, disable_block_shaping=True)
        if explicit_block_target
        else read_transform
    )
    return MapOperator.create(
        MapTransformer([transform]),
        upstream,
        data_context,
        name=op.name,
        compute_strategy=op.compute,
        ray_remote_args=op.ray_remote_args,
        isolate_workers=data_context.isolate_read_workers,
        # As with V1's additional split factor, preserve the split boundary so
        # downstream tasks can consume the requested number of output blocks.
        supports_fusion=not explicit_block_target,
    )

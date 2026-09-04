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
import time
from typing import Iterable, List

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.scanners.file_scanner import FileScanner
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    CustomOpStatsReportFn,
    MapTransformer,
)
from ray.data._internal.logical.operators import ReadFiles
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data.block import Block, ReadFilesTaskStats
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

    def do_read(
        blocks: Iterable[Block],
        _: TaskContext,
        report_custom_op_stats: CustomOpStatsReportFn,
    ) -> Iterable[Block]:
        task_start_s = time.perf_counter()
        reader = scanner.create_reader()
        # Reader-level per-task aggregates (bytes/batches the decoder actually
        # produced, time spent inside its iterator, largest single table),
        # folded into per-task distributions by
        # ``OpRuntimeMetrics.on_task_finished``. The driver reads them off the
        # FINAL output block's ``TaskExecWorkerStats``, and whether a block is
        # emitted after this generator's last resume depends on how the
        # shaping buffer's flush happens to align with the reader's batch
        # sizes — so a single report at end-of-task is silently dropped on
        # some (reader-dependent!) shapes. Instead, report ONE stats object up
        # front and update it in place as batches flow: every block's
        # snapshot carries the totals so far, and the final block's carries
        # (at least) everything up to the last yielded table.
        task_stats = ReadFilesTaskStats()
        report_custom_op_stats(task_stats)
        decode_wall_s = 0.0
        decoded_bytes = decoded_batches = decoded_rows = peak_batch_bytes = 0
        manifests = 0
        trim_wall_s = 0.0
        yield_wall_s = 0.0
        first_table_wall_s = 0.0
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
            manifests += 1
            table_iter = iter(reader.read(manifest))
            # One-table lookahead: a block's stats snapshot is pickled when
            # the block leaves the task, and a table that completes a block
            # (buffer >= target, below the 1.5x slice limit -> emitted whole,
            # no remainder) is followed by NO flush block. Anything learned
            # only when the stream ends -- the reader's end-of-stream drain
            # (arrow-rs malloc_trim wall) -- therefore has to be folded in
            # BEFORE the last table is yielded, which means pulling the next
            # table first. Memory-neutral: the previous table stayed bound in
            # this frame during the next decode anyway.
            pending = None
            while True:
                start_s = time.perf_counter()
                try:
                    table = next(table_iter)
                except StopIteration:
                    # The reader's finalizer (incl. the eos trim) ran inside
                    # this next(); its wall is inside decode_wall_s.
                    decode_wall_s += time.perf_counter() - start_s
                    trim_wall_s += reader.pop_task_stats().get("trim_wall_s", 0.0)
                    task_stats._update(
                        decode_wall_s=decode_wall_s,
                        trim_wall_s=trim_wall_s,
                        yield_wall_s=yield_wall_s,
                    )
                    break
                decode_wall_s += time.perf_counter() - start_s
                if decoded_batches == 0:
                    first_table_wall_s = time.perf_counter() - task_start_s
                nbytes = table.nbytes
                decoded_bytes += nbytes
                decoded_batches += 1
                decoded_rows += table.num_rows
                if nbytes > peak_batch_bytes:
                    peak_batch_bytes = nbytes
                task_stats._update(
                    decode_wall_s=decode_wall_s,
                    decoded_bytes=decoded_bytes,
                    decoded_batches=decoded_batches,
                    decoded_rows=decoded_rows,
                    peak_batch_bytes=peak_batch_bytes,
                    manifests=manifests,
                    yield_wall_s=yield_wall_s,
                    first_table_wall_s=first_table_wall_s,
                )
                if pending is not None:
                    y0 = time.perf_counter()
                    yield pending
                    yield_wall_s += time.perf_counter() - y0
                pending = block_udf(table) if block_udf is not None else table
            if pending is not None:
                y0 = time.perf_counter()
                yield pending
                yield_wall_s += time.perf_counter() - y0
                task_stats._update(yield_wall_s=yield_wall_s)

    return MapOperator.create(
        MapTransformer(
            [
                BlockMapTransformFn(
                    do_read,
                    is_udf=False,
                    output_block_size_option=OutputBlockSizeOption.of(
                        target_max_block_size=data_context.target_max_block_size,
                    ),
                    should_report_custom_op_stats=True,
                ),
            ]
        ),
        upstream,
        data_context,
        name=op.name,
        compute_strategy=op.compute,
        ray_remote_args=op.ray_remote_args,
    )

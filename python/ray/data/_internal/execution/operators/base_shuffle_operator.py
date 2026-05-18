"""Generic shuffle Operator for Ray Data.

This module provides the infrastructure for map-reduce style operations:
scatter/gather hash shuffle, sort, random shuffle, and groupby aggregation.

The operator is parameterized by two callables:

- :data:`PartitionFn`: assigns rows in a block to output partitions.
- :data:`ReduceFn`: combines shards for one partition into output blocks.

Callers supply concrete implementations of these two functions; the operator
handles all infrastructure concerns:

* Pre-map merge with node-affinity batching (1 GB default threshold).
* Shard grouping: packs up to ``_MAX_RETURN_GROUPS`` shards per mapper return
  into a single ZSTD-compressed Arrow IPC stream, bounding object-store
  pressure from the M×N intermediate objects.
* Batched ``ray.get`` (256 refs at a time) in reducers to avoid memory spikes
  from a single large ``ray.get``.
* Streaming vs. blocking reduce: streaming flushes partition accumulators to
  ``reduce_fn`` once they reach ``DataContext.target_max_block_size``;
  blocking accumulates all shards first.  Output blocks pass through a
  :class:`BlockOutputBuffer` per partition to enforce the same target size.
* One concurrent reducer per live worker node to prevent object-store flooding.
* Optional ``runtime_env`` on map workers to isolate shuffle memory.
"""

import functools
import logging
import pickle
import time
import typing
from collections import defaultdict, deque
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import pyarrow as pa

import ray
from ray import ObjectRef
from ray._raylet import StreamingGeneratorStats
from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    MetadataOpTask,
    OpTask,
    TaskExecDriverStats,
    estimate_total_num_of_blocks,
)
from ray.data._internal.execution.operators.sub_progress import SubProgressBarMixin
from ray.data._internal.output_buffer import BlockOutputBuffer, OutputBlockSizeOption
from ray.data._internal.stats import OpRuntimeMetrics
from ray.data._internal.table_block import TableBlockAccessor
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadataWithSchema,
    BlockStats,
    BlockType,
    TaskExecWorkerStats,
    to_stats,
)
from ray.data.context import DataContext
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if typing.TYPE_CHECKING:
    from ray.data._internal.progress.base_progress import BaseProgressBar

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

# Takes a single, already-defragmented ``pa.Table`` block.
# Returns a dict mapping partition IDs to shard tables; empty partitions are
# omitted.  ``num_partitions`` and any other config are bound via closure.
PartitionFn = Callable[[pa.Table], Dict[int, pa.Table]]

# Takes ``(partition_id, accumulated_shard_tables)`` and yields output blocks.
#
# In *streaming* mode the operator calls this function multiple times per
# partition, each time with a partial list of shards whose total size has
# reached ``DataContext.target_max_block_size``.  Each call must produce a
# valid partial result from the given tables alone (concat is the canonical
# example).  Produced blocks are passed through a per-partition
# :class:`BlockOutputBuffer` that aggregates across calls and emits blocks
# shaped to ``target_max_block_size``.
#
# In *blocking* mode (``streaming_reduce=False``) the operator accumulates all
# shards for a partition before calling this function once with the full list.
ReduceFn = Callable[[int, List[pa.Table]], Iterable[Block]]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Cap return objects from each mapper to bound object-store pressure.
_MAX_RETURN_GROUPS = 200

# Number of ObjectRefs fetched per ray.get() call in reducers.
_REDUCE_BATCH_SIZE = 256


# ---------------------------------------------------------------------------
# Remote task: Map
# ---------------------------------------------------------------------------


@ray.remote
def _map_task(
    *blocks: Block,
    partition_fn: PartitionFn,
    num_partitions: int,
    shard_group_size: int = 1,
):
    """Map stage: partition one or more blocks and return grouped shards.

    Multiple input blocks may be passed when pre-map merge is enabled; they
    are partitioned individually (to avoid a large ``combine_chunks`` on the
    merged table) and their shards are accumulated before grouping.

    Must be called with ``num_returns = num_groups + 1`` where
    ``num_groups = ceil(num_partitions / shard_group_size)``.

    Args:
        *blocks: One or more input blocks.
        partition_fn: Callable ``(pa.Table) -> Dict[int, pa.Table]`` that
            assigns rows to output partitions.
        num_partitions: Total number of output partitions.
        shard_group_size: Number of partitions packed into each return object.

    Returns:
        Tuple of ``num_groups + 1`` values:

        - Index 0: ``(BlockMetadata, Dict)`` — aggregate input metadata and a
          per-partition ``(rows, bytes)`` size dict (keys are the partitions
          that received any rows from this mapper).
        - Index 1 … G: a ZSTD-compressed Arrow IPC stream for the partitions
          in that shard group, or ``None`` if the group is entirely empty.
    """
    import json as _json

    stats = BlockExecStats.builder()
    num_groups = (num_partitions + shard_group_size - 1) // shard_group_size

    total_rows = sum(b.num_rows for b in blocks)
    total_bytes = sum(b.nbytes for b in blocks)

    if total_rows == 0:
        input_meta = BlockAccessor.for_block(blocks[0]).get_metadata(
            block_exec_stats=stats.build(block_ser_time_s=0),
        )
        return (input_meta, {}), *([None] * num_groups)

    # Partition each block individually to avoid a full-table combine_chunks()
    # on the merged input.  Per-block combine_chunks() is cheap (~128 MB) and
    # lets the allocator reuse freed pages across iterations.
    partition_accumulators: Dict[int, List[pa.Table]] = {}

    for block in blocks:
        block = TableBlockAccessor.try_convert_block_type(
            block, block_type=BlockType.ARROW
        )
        if block.num_rows == 0:
            continue

        assert isinstance(block, pa.Table), f"Expected pa.Table, got {type(block)}"

        # Defragment chunked columns for faster partitioning.
        if any(col.num_chunks > 1 for col in block.columns):
            block = block.combine_chunks()

        block_partitions = partition_fn(block)

        for pid, shard in block_partitions.items():
            if shard.num_rows > 0:
                if pid not in partition_accumulators:
                    partition_accumulators[pid] = []
                partition_accumulators[pid].append(shard)

        del block, block_partitions

    # Build grouped output: compress one group at a time to bound peak memory.
    ipc_write_options = pa.ipc.IpcWriteOptions(compression=pa.Codec("zstd"))
    shard_sizes: Dict[int, Tuple[int, int]] = {}
    groups = [None] * num_groups

    # Bucket partition accumulators by group, then process one group at a time.
    group_pid_keys: Dict[int, List[int]] = {}
    for pid in partition_accumulators:
        group_idx = pid // shard_group_size
        if group_idx not in group_pid_keys:
            group_pid_keys[group_idx] = []
        group_pid_keys[group_idx].append(pid)

    for group_idx, pid_keys in group_pid_keys.items():
        sorted_pids = sorted(pid_keys)
        pid_tables: Dict[int, pa.Table] = {}
        for pid in sorted_pids:
            tables = partition_accumulators.pop(pid)
            merged = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
            pid_tables[pid] = merged
            shard_sizes[pid] = (merged.num_rows, merged.nbytes)

        # Write all partitions in this group as one compressed IPC stream.
        # Tag partition IDs in schema metadata so the reducer can route shards.
        first_table = pid_tables[sorted_pids[0]]
        # Copy: schema.metadata is shared across tables that share a schema.
        schema_meta = dict(first_table.schema.metadata or {})
        schema_meta[b"__pids__"] = _json.dumps(sorted_pids).encode()
        tagged_schema = first_table.schema.with_metadata(schema_meta)
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, tagged_schema, options=ipc_write_options)
        for pid in sorted_pids:
            table = pid_tables[pid]
            if table.num_columns > 0:
                table = table.combine_chunks()
            writer.write_batch(table.to_batches()[0])
        writer.close()
        groups[group_idx] = sink.getvalue()
        del pid_tables

    del group_pid_keys

    from dataclasses import replace as _dc_replace

    input_meta = BlockAccessor.for_block(blocks[0]).get_metadata(
        block_exec_stats=stats.build(block_ser_time_s=0),
    )
    input_meta = _dc_replace(input_meta, num_rows=total_rows, size_bytes=total_bytes)
    return (input_meta, shard_sizes), *groups


# ---------------------------------------------------------------------------
# Remote task: Reduce
# ---------------------------------------------------------------------------


def _read_ipc_group(
    group: pa.Buffer,
) -> List[Tuple[int, pa.Table]]:
    """Decompress one IPC group and return ``(partition_id, table)`` pairs.

    Strips the implementation-only ``__pids__`` metadata key so that downstream
    tables don't carry our shard-routing tag in their schema.
    """
    import json as _json

    reader = pa.ipc.open_stream(group)
    pids = _json.loads(reader.schema.metadata[b"__pids__"])
    meta = dict(reader.schema.metadata)
    meta.pop(b"__pids__", None)
    clean_schema = (
        reader.schema.with_metadata(meta) if meta else reader.schema.remove_metadata()
    )
    result = []
    for pid in pids:
        batch = reader.read_next_batch()
        if batch.num_rows > 0:
            result.append((pid, pa.Table.from_batches([batch], schema=clean_schema)))
    return result


@ray.remote
def _reduce_task(
    group_refs: List[ObjectRef],
    partition_ids: List[int],
    reduce_fn: ReduceFn,
    target_max_block_size: int,
    streaming: bool = True,
) -> Generator[Union[Block, bytes], None, None]:
    """Reduce stage: fetch shards and call ``reduce_fn`` for each partition.

    Both modes use batched ``ray.get`` (:data:`_REDUCE_BATCH_SIZE` refs at a
    time) to avoid memory spikes from a single large dereference.  Output
    blocks from ``reduce_fn`` are passed through a per-partition
    :class:`BlockOutputBuffer` so that each emitted block respects
    ``target_max_block_size``.

    Streaming mode (``streaming=True``):
        Calls ``reduce_fn(partition_id, accumulated_tables)`` whenever a
        partition's accumulator exceeds ``target_max_block_size``, then pushes
        the produced blocks through the output buffer.  Peak input accumulator
        per partition is bounded to ~``target_max_block_size``.  ``reduce_fn``
        must produce valid output from partial data.

    Blocking mode (``streaming=False``):
        Accumulates all shards for every partition before calling
        ``reduce_fn(partition_id, all_tables)`` once per partition.  Use this
        when ``reduce_fn`` requires the full partition (sort, aggregate).

    Args:
        group_refs: ObjectRefs to compressed IPC groups from all mappers.
        partition_ids: Partition IDs this reducer is responsible for.
        reduce_fn: User-supplied reduce callable.
        target_max_block_size: Target output block size (also the streaming
            flush threshold).  Comes from ``DataContext.target_max_block_size``.
        streaming: Whether to flush partitions incrementally (True) or
            accumulate all shards before reducing (False).

    Returns:
        Generator yielding output blocks shaped to ``target_max_block_size``,
        each followed by a serialized ``BlockMetadataWithSchema``.
    """
    start_time_s = time.perf_counter()

    if not group_refs or not partition_ids:
        return

    partition_ids_set = set(partition_ids)
    accum_tables: Dict[int, List[pa.Table]] = {pid: [] for pid in partition_ids}
    accum_bytes: Dict[int, int] = {pid: 0 for pid in partition_ids}
    # Lazy-allocated per-partition output buffer (created on first add).
    output_buffers: Dict[int, BlockOutputBuffer] = {}

    def _yield_with_stats(block: Block):
        """Yield (block, pickled metadata) following the streaming-gen protocol."""
        exec_stats_builder = BlockExecStats.builder()
        exec_stats_builder.finish()
        gen_stats: StreamingGeneratorStats = yield block
        exec_stats = exec_stats_builder.build(
            block_ser_time_s=(gen_stats.object_creation_dur_s if gen_stats else None),
        )
        yield pickle.dumps(
            BlockMetadataWithSchema.from_block(
                block,
                block_exec_stats=exec_stats,
                task_exec_stats=TaskExecWorkerStats(
                    task_wall_time_s=time.perf_counter() - start_time_s,
                ),
            )
        )

    def _reduce_into_buffer(pid: int, tables: List[pa.Table]):
        """Call reduce_fn, push outputs through buffer, yield ready blocks."""
        if pid not in output_buffers:
            output_buffers[pid] = BlockOutputBuffer(
                OutputBlockSizeOption(target_max_block_size=target_max_block_size)
            )
        buf = output_buffers[pid]
        for block in reduce_fn(pid, tables):
            buf.add_block(block)
            while buf.has_next():
                yield from _yield_with_stats(buf.next())

    for batch_start in range(0, len(group_refs), _REDUCE_BATCH_SIZE):
        batch = group_refs[batch_start : batch_start + _REDUCE_BATCH_SIZE]
        batch_groups = ray.get(batch)

        for group in batch_groups:
            if group is None:
                continue
            for pid, table in _read_ipc_group(group):
                if pid not in partition_ids_set:
                    continue
                accum_tables[pid].append(table)
                accum_bytes[pid] += table.nbytes

                if streaming and accum_bytes[pid] >= target_max_block_size:
                    tables = accum_tables[pid]
                    accum_tables[pid] = []
                    accum_bytes[pid] = 0
                    yield from _reduce_into_buffer(pid, tables)

        del batch_groups

    # Final flush: drain any remaining accumulated shards through reduce_fn.
    for pid in sorted(partition_ids):
        if accum_tables[pid]:
            yield from _reduce_into_buffer(pid, accum_tables[pid])

    # Finalize each touched buffer and drain remaining shaped blocks.
    for pid in sorted(output_buffers):
        buf = output_buffers[pid]
        buf.finalize()
        while buf.has_next():
            yield from _yield_with_stats(buf.next())


# ---------------------------------------------------------------------------
# Operator
# ---------------------------------------------------------------------------


class BaseShuffleOperator(PhysicalOperator, SubProgressBarMixin):
    """Generic map-reduce physical operator.

    Lifecycle
    ---------
    1. As upstream bundles arrive via :meth:`add_input`, blocks are optionally
       batched by node into a pre-map merge buffer.  When the buffer exceeds
       :attr:`_pre_map_merge_threshold`, a merged map task is submitted.
    2. Each map task calls :data:`PartitionFn` on its input blocks, groups
       shards into ZSTD-compressed IPC objects, and returns metadata + group
       refs.  The driver stores group refs in per-group buffers (no data copy).
    3. Once all map tasks complete, reduce tasks are launched.  Concurrency is
       limited to one reducer per live worker node.  Each reducer fetches its
       group refs, decompresses them, and calls :data:`ReduceFn` per partition.

    Parameters
    ----------
    input_op:
        Upstream physical operator.
    data_context:
        Runtime configuration.
    num_partitions:
        Total number of output partitions.
    partition_fn:
        Callable that assigns rows in a block to output partition IDs.
    reduce_fn:
        Callable that combines shard tables for one partition into blocks.
    streaming_reduce:
        If ``True`` (default), call ``reduce_fn`` incrementally as a
        partition's accumulator reaches ``data_context.target_max_block_size``.
        If ``False``, accumulate all shards before calling ``reduce_fn`` once
        — required for sort or aggregate.
    pre_map_merge_threshold:
        Byte threshold per node at which buffered blocks are merged into a
        single map task.  Set to 0 to disable pre-map merging.
    map_runtime_env:
        Optional ``runtime_env`` dict passed to each map task via
        ``@ray.remote(...).remote()``.  Use to isolate map workers into a
        dedicated pool.
    map_cpus:
        CPU request per map task.
    reduce_cpus:
        CPU request per reduce task.  If ``None``, auto-derived from partition
        sizes and cluster shape (1.0 for streaming, memory-proportional for
        blocking).
    name:
        Display name shown in progress bars and logs.
    """

    _DEFAULT_MAP_TASK_NUM_CPUS = 1.0
    _DEFAULT_REDUCE_TASK_NUM_CPUS = 1.0
    _DEFAULT_PRE_MAP_MERGE_THRESHOLD = 1024 * 1024 * 1024  # 1 GB

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        *,
        num_partitions: int,
        partition_fn: PartitionFn,
        reduce_fn: ReduceFn,
        streaming_reduce: bool = True,
        pre_map_merge_threshold: int = _DEFAULT_PRE_MAP_MERGE_THRESHOLD,
        map_runtime_env: Optional[Dict[str, Any]] = None,
        map_cpus: float = _DEFAULT_MAP_TASK_NUM_CPUS,
        reduce_cpus: Optional[float] = None,
        name: str = "Shuffle",
    ):
        super().__init__(
            name=name,
            input_dependencies=[input_op],
            data_context=data_context,
        )

        self._num_partitions: int = num_partitions
        self._partition_fn: PartitionFn = partition_fn
        self._reduce_fn: ReduceFn = reduce_fn
        self._streaming_reduce: bool = streaming_reduce

        # -- Shard grouping --------------------------------------------------
        # Cap at _MAX_RETURN_GROUPS return objects per mapper.
        self._shard_group_size: int = max(
            1,
            (num_partitions + _MAX_RETURN_GROUPS - 1) // _MAX_RETURN_GROUPS,
        )
        self._num_groups: int = (
            num_partitions + self._shard_group_size - 1
        ) // self._shard_group_size

        # -- Map task config -------------------------------------------------
        self._map_task_num_cpus: float = map_cpus
        self._map_runtime_env: Optional[Dict[str, Any]] = map_runtime_env

        # -- Map task tracking -----------------------------------------------
        self._next_map_task_idx: int = 0
        self._map_tasks: Dict[int, MetadataOpTask] = {}
        self._map_resource_usage = ExecutionResources.zero()

        # -- Per-partition stats (driver bookkeeping) ------------------------
        self._partition_row_counts: Dict[int, int] = defaultdict(int)
        self._partition_bytes: Dict[int, int] = defaultdict(int)

        # -- Per-group shard buffers -----------------------------------------
        # group_idx → list of ObjectRefs to IPC groups from each mapper
        self._group_buffers: Dict[int, List[ObjectRef]] = defaultdict(list)

        # -- Pre-map merge ---------------------------------------------------
        self._pre_map_merge_threshold: int = pre_map_merge_threshold
        # Per-node buffers: only co-located blocks are batched together to
        # avoid cross-node transfers that inflate raylet memory.
        self._merge_buffer_refs_by_node: Dict[str, List[ObjectRef]] = defaultdict(list)
        self._merge_buffer_bytes_by_node: Dict[str, int] = defaultdict(int)
        self._merge_buffer_bundles_by_node: Dict[str, List[RefBundle]] = defaultdict(
            list
        )

        # -- Reduce task config & tracking -----------------------------------
        self._reduce_task_num_cpus_override: Optional[float] = reduce_cpus
        self._reduce_task_num_cpus: float = self._DEFAULT_REDUCE_TASK_NUM_CPUS
        self._reduce_tasks: Dict[int, DataOpTask] = {}
        self._pending_reduce_group_ids: Set[int] = set(range(self._num_groups))

        # -- Output queue ----------------------------------------------------
        self._output_queue: deque = deque()

        # -- Timing ----------------------------------------------------------
        self._start_time: float = time.perf_counter()
        self._reduce_start_time: Optional[float] = None
        self._num_nodes: int = 1

        # -- Stats -----------------------------------------------------------
        self._total_input_rows: int = 0
        self._total_input_bytes: int = 0
        self._map_blocks_stats: List[BlockStats] = []
        self._output_blocks_stats: List[BlockStats] = []

        # -- Sub-progress bars -----------------------------------------------
        self._map_bar: Optional["BaseProgressBar"] = None
        self._map_metrics = OpRuntimeMetrics(self)
        self._reduce_bar: Optional["BaseProgressBar"] = None
        self._reduce_metrics = OpRuntimeMetrics(self)

    # -----------------------------------------------------------------------
    # Input handling (Map phase)
    # -----------------------------------------------------------------------

    def can_add_input(self) -> bool:
        return True

    def _add_input_inner(self, input_bundle: RefBundle, input_index: int) -> None:
        assert input_index == 0
        self._map_metrics.on_input_received(input_bundle)

        if self._pre_map_merge_threshold > 0:
            # Determine the primary node for this bundle's blocks.
            preferred_locs = input_bundle.get_preferred_object_locations()
            node_id = (
                max(preferred_locs, key=preferred_locs.get)
                if preferred_locs
                else "unknown"
            )

            for block_ref, block_metadata in zip(
                input_bundle.block_refs, input_bundle.metadata
            ):
                self._merge_buffer_refs_by_node[node_id].append(block_ref)
                self._merge_buffer_bytes_by_node[node_id] += (
                    block_metadata.size_bytes or 0
                )
            self._merge_buffer_bundles_by_node[node_id].append(input_bundle)

            if (
                self._merge_buffer_bytes_by_node[node_id]
                >= self._pre_map_merge_threshold
            ):
                self._flush_merge_buffer(node_id)
        else:
            for block_ref, block_metadata in zip(
                input_bundle.block_refs, input_bundle.metadata
            ):
                self._submit_map_task([block_ref], [input_bundle])

    def all_inputs_done(self) -> None:
        super().all_inputs_done()
        for node_id in list(self._merge_buffer_refs_by_node.keys()):
            self._flush_merge_buffer(node_id)

    def _flush_merge_buffer(self, node_id: str) -> None:
        """Drain one node's merge buffer and submit a merged map task."""
        block_refs = self._merge_buffer_refs_by_node.pop(node_id, [])
        if not block_refs:
            return
        bundles = self._merge_buffer_bundles_by_node.pop(node_id, [])
        estimated_bytes = self._merge_buffer_bytes_by_node.pop(node_id, 0)
        self._submit_map_task(
            block_refs,
            bundles,
            estimated_bytes=estimated_bytes,
            target_node_id=node_id if node_id != "unknown" else None,
        )

    def _submit_map_task(
        self,
        block_refs: List[ObjectRef],
        input_bundles: List[RefBundle],
        estimated_bytes: int = 0,
        target_node_id: Optional[str] = None,
    ) -> None:
        """Submit a map task for one or more input blocks.

        Args:
            block_refs: ObjectRefs of the input blocks.
            input_bundles: RefBundles owning the input blocks (kept alive until
                the task completes, then destroyed).
            estimated_bytes: Estimated total input size.  Used to request
                proportional memory for merged tasks.
            target_node_id: If set, pin the task to this node (soft) to avoid
                cross-node object transfers.
        """
        cur_task_idx = self._next_map_task_idx
        self._next_map_task_idx += 1

        task_resources: Dict[str, Any] = {"num_cpus": self._map_task_num_cpus}
        # For merged map tasks, request 2× input bytes: partition shards from
        # table.take() (~1×) plus hash arrays and serialisation overhead.
        if estimated_bytes > 0 and len(block_refs) > 1:
            task_resources["memory"] = estimated_bytes * 2

        if target_node_id is not None:
            task_resources["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                target_node_id, soft=True
            )

        remote_opts: Dict[str, Any] = {
            **task_resources,
            "num_returns": self._num_groups + 1,
        }
        if self._map_runtime_env is not None:
            remote_opts["runtime_env"] = self._map_runtime_env

        map_refs = _map_task.options(**remote_opts).remote(
            *block_refs,
            partition_fn=self._partition_fn,
            num_partitions=self._num_partitions,
            shard_group_size=self._shard_group_size,
        )
        metadata_ref = map_refs[0]
        group_refs = list(map_refs[1:])

        def _on_map_done(task_idx: int, g_refs: List[ObjectRef]) -> None:
            task = self._map_tasks.pop(task_idx)
            self._map_resource_usage = self._map_resource_usage.subtract(
                task.get_requested_resource_bundle()
            )

            input_meta, shard_sizes = ray.get(task.get_waitable(), timeout=60)

            for pid, (rows, nbytes) in shard_sizes.items():
                self._partition_row_counts[pid] += rows
                self._partition_bytes[pid] += nbytes

            # Append all group refs; reducer skips entries that resolve to None.
            for group_idx, ref in enumerate(g_refs):
                self._group_buffers[group_idx].append(ref)

            del g_refs

            for bundle in input_bundles:
                bundle.destroy_if_owned()

            self._total_input_rows += input_meta.num_rows or 0
            self._total_input_bytes += input_meta.size_bytes or 0
            self._map_blocks_stats.append(input_meta.to_stats())

            for bundle in input_bundles:
                self._map_metrics.on_output_taken(bundle)
            self._map_metrics.on_task_output_generated(
                cur_task_idx,
                RefBundle(
                    [(task.get_waitable(), input_meta)],
                    schema=None,
                    owns_blocks=False,
                ),
            )
            self._map_metrics.on_task_finished(
                cur_task_idx,
                None,
                task_exec_stats=None,
                task_exec_driver_stats=None,
            )

            if self._map_bar is not None:
                self._map_bar.update(increment=input_meta.num_rows or 0)

        task = MetadataOpTask(
            task_index=cur_task_idx,
            object_ref=metadata_ref,
            task_done_callback=functools.partial(
                _on_map_done, cur_task_idx, group_refs
            ),
            task_resource_bundle=ExecutionResources.from_resource_dict(task_resources),
        )
        self._map_tasks[cur_task_idx] = task
        self._map_resource_usage = self._map_resource_usage.add(
            task.get_requested_resource_bundle()
        )

        # Report all input blocks so average_num_inputs_per_task accounts for
        # pre-map merge batching (avoids inflating the progress bar denominator).
        all_blocks_meta = [
            (ref, meta)
            for bundle in input_bundles
            for ref, meta in zip(bundle.block_refs, bundle.metadata)
        ]
        if all_blocks_meta:
            self._map_metrics.on_task_submitted(
                cur_task_idx,
                RefBundle(all_blocks_meta, schema=None, owns_blocks=False),
                task_id=task.get_task_id(),
            )

        if self._map_bar is not None:
            _, _, num_rows = estimate_total_num_of_blocks(
                cur_task_idx + 1,
                self.upstream_op_num_outputs(),
                self._map_metrics,
                total_num_tasks=None,
            )
            self._map_bar.update(total=num_rows)

    # -----------------------------------------------------------------------
    # Output handling (Reduce phase)
    # -----------------------------------------------------------------------

    def has_next(self) -> bool:
        self._try_reduce()
        return len(self._output_queue) > 0

    def _get_next_inner(self) -> RefBundle:
        bundle: RefBundle = self._output_queue.popleft()
        self._reduce_metrics.on_output_dequeued(bundle)
        self._reduce_metrics.on_output_taken(bundle)
        self._output_blocks_stats.extend(to_stats(bundle.metadata))
        return bundle

    # -----------------------------------------------------------------------
    # Task tracking
    # -----------------------------------------------------------------------

    def get_active_tasks(self) -> List[OpTask]:
        return list(self._map_tasks.values()) + list(self._reduce_tasks.values())

    # -----------------------------------------------------------------------
    # Reduce scheduling
    # -----------------------------------------------------------------------

    def _is_map_done(self) -> bool:
        return self._inputs_complete and len(self._map_tasks) == 0

    def _is_all_reduce_submitted(self) -> bool:
        return len(self._pending_reduce_group_ids) == 0

    def _auto_derive_reduce_cpus(self) -> float:
        """Derive ``reduce_task_num_cpus`` from partition sizes and cluster shape.

        For streaming reduce, memory per partition is bounded by
        ``target_max_block_size`` (typically 128 MB), so 1 CPU maximises
        concurrency.  For blocking reduce, peak heap is proportional to the
        largest group's uncompressed bytes, so CPUs are scaled to bound
        co-located heap usage.
        """
        import math

        if self._streaming_reduce:
            return self._DEFAULT_REDUCE_TASK_NUM_CPUS

        if not self._partition_bytes:
            return self._DEFAULT_REDUCE_TASK_NUM_CPUS

        max_group_bytes = 0
        for group_idx in range(self._num_groups):
            start = group_idx * self._shard_group_size
            end = min(start + self._shard_group_size, self._num_partitions)
            group_bytes = sum(
                self._partition_bytes.get(pid, 0) for pid in range(start, end)
            )
            max_group_bytes = max(max_group_bytes, group_bytes)

        if max_group_bytes == 0:
            return self._DEFAULT_REDUCE_TASK_NUM_CPUS

        reducer_heap = max_group_bytes * 2  # decompressed input + output

        cluster_res = ray.cluster_resources()
        num_nodes = max(len(ray.nodes()), 1)
        node_cpus = max(1.0, cluster_res.get("CPU", 8) / num_nodes)
        node_memory = cluster_res.get("memory", 30e9) / num_nodes

        proportional_cpus = max(1.0, math.ceil(node_cpus * reducer_heap / node_memory))
        target_per_node = max(1, int(node_cpus / proportional_cpus))
        reduce_cpus = max(1.0, math.ceil(node_cpus / target_per_node))
        logger.info(
            f"Auto-derived reduce_cpus={reduce_cpus} "
            f"(max_group={max_group_bytes / 1e9:.1f}GB, "
            f"reducer_heap={reducer_heap / 1e9:.1f}GB, "
            f"node_memory={node_memory / 1e9:.1f}GB, "
            f"node_cpus={node_cpus})"
        )
        return float(reduce_cpus)

    def _log_partition_stats(self) -> None:
        counts = self._partition_row_counts
        if not counts:
            return
        row_counts = sorted(counts.values())
        n = len(row_counts)
        empty = self._num_partitions - n
        partition_bytes = sorted(self._partition_bytes[pid] for pid in counts)
        total_bytes = sum(partition_bytes)
        logger.info(
            f"Partition stats before reduce: "
            f"partitions={self._num_partitions} (non-empty={n}, empty={empty}), "
            f"total_rows={sum(row_counts):,}, total_bytes={total_bytes / 1e9:.1f}GB, "
            f"rows: min={row_counts[0]:,}, median={row_counts[n // 2]:,}, "
            f"max={row_counts[-1]:,}, "
            f"bytes: min={partition_bytes[0] / 1e6:.0f}MB, "
            f"median={partition_bytes[n // 2] / 1e6:.0f}MB, "
            f"max={partition_bytes[-1] / 1e6:.0f}MB, "
            f"groups={self._num_groups}, shard_group_size={self._shard_group_size}"
        )

    def _get_reduce_concurrency_limit(self) -> int:
        """Max concurrent reduce tasks allowed.

        Defaults to 1 per live worker node to prevent all reducers from
        launching simultaneously (which floods the object store before
        downstream can drain).  As each reducer finishes, the next wave is
        scheduled by :meth:`_try_reduce`.

        Override via ``DataContext`` config key
        ``map_reduce_max_concurrent_reducers``.
        """
        override = self._data_context.get_config(
            "map_reduce_max_concurrent_reducers", None
        )
        if override is not None:
            return int(override)
        return self._num_nodes

    def _try_reduce(self) -> None:
        """Launch reduce tasks once all map tasks have completed."""
        if self._is_all_reduce_submitted():
            return
        if not self._is_map_done():
            return

        if self._reduce_start_time is None:
            self._log_partition_stats()
            self._reduce_start_time = time.perf_counter()
            map_elapsed = self._reduce_start_time - self._start_time

            try:
                self._num_nodes = sum(
                    1
                    for n in ray.nodes()
                    if n.get("Alive", False)
                    and "node:__internal_head__" not in n.get("Resources", {})
                )
            except Exception:
                self._num_nodes = 1
            self._num_nodes = max(1, self._num_nodes)

            if self._reduce_task_num_cpus_override is not None:
                self._reduce_task_num_cpus = self._reduce_task_num_cpus_override
            else:
                self._reduce_task_num_cpus = self._auto_derive_reduce_cpus()

            logger.info(
                f"Reduce starting: map_elapsed={map_elapsed:.1f}s, "
                f"maps={self._next_map_task_idx}, "
                f"reduce_cpus={self._reduce_task_num_cpus}, "
                f"num_nodes={self._num_nodes}"
            )

        slots_available = self._get_reduce_concurrency_limit() - len(self._reduce_tasks)

        for group_id in list(self._pending_reduce_group_ids):
            if slots_available <= 0:
                break
            group_refs = self._group_buffers.get(group_id, [])

            group_start = group_id * self._shard_group_size
            group_end = min(group_start + self._shard_group_size, self._num_partitions)
            partition_ids = list(range(group_start, group_end))

            if not group_refs:
                self._pending_reduce_group_ids.discard(group_id)
                continue

            def _on_bundle_ready(gid: int, bundle: RefBundle) -> None:
                self._output_queue.append(bundle)
                self._reduce_metrics.on_output_queued(bundle)
                self._reduce_metrics.on_task_output_generated(
                    task_index=gid, output=bundle
                )
                _, num_outputs, num_rows = estimate_total_num_of_blocks(
                    gid + 1,
                    self.upstream_op_num_outputs(),
                    self._reduce_metrics,
                    total_num_tasks=self._num_groups,
                )
                self._estimated_num_output_bundles = num_outputs
                self._estimated_output_num_rows = num_rows
                if self._reduce_bar is not None:
                    self._reduce_bar.update(
                        increment=bundle.num_rows() or 0,
                        total=self.num_output_rows_total(),
                    )

            def _on_reduce_done(
                gid: int,
                exc: Optional[Exception],
                task_exec_stats: Optional[TaskExecWorkerStats],
                task_exec_driver_stats: Optional[TaskExecDriverStats],
            ) -> None:
                if gid in self._reduce_tasks:
                    self._reduce_tasks.pop(gid)
                    self._reduce_metrics.on_task_finished(
                        task_index=gid,
                        exception=exc,
                        task_exec_stats=task_exec_stats,
                        task_exec_driver_stats=task_exec_driver_stats,
                    )
                    if exc:
                        logger.error(
                            f"Reduce of group {gid} failed: {exc}", exc_info=exc
                        )

            estimated_bytes = sum(
                self._partition_bytes.get(pid, 0) for pid in partition_ids
            )
            cluster_res = ray.cluster_resources()
            num_nodes = max(len(ray.nodes()), 1)
            node_memory = cluster_res.get("memory", 30e9) / num_nodes
            reduce_resources: Dict[str, Any] = {"num_cpus": self._reduce_task_num_cpus}
            if estimated_bytes > 0:
                requested_memory = estimated_bytes * 2
                if requested_memory > node_memory:
                    requested_memory = 0
                reduce_resources["memory"] = requested_memory

            block_gen = _reduce_task.options(
                **reduce_resources,
                num_returns="streaming",
                scheduling_strategy="SPREAD",
            ).remote(
                group_refs,
                partition_ids=partition_ids,
                reduce_fn=self._reduce_fn,
                target_max_block_size=self.data_context.target_max_block_size,
                streaming=self._streaming_reduce,
            )

            data_task = DataOpTask(
                task_index=group_id,
                streaming_gen=block_gen,
                output_ready_callback=functools.partial(_on_bundle_ready, group_id),
                task_done_callback=functools.partial(_on_reduce_done, group_id),
                task_resource_bundle=ExecutionResources(
                    cpu=self._reduce_task_num_cpus,
                    memory=estimated_bytes,
                    object_store_memory=estimated_bytes,
                ),
                operator_name=self.name,
            )

            self._reduce_tasks[group_id] = data_task
            self._pending_reduce_group_ids.discard(group_id)
            slots_available -= 1

            # Release driver-side refs so intermediate objects can be reclaimed.
            self._group_buffers.pop(group_id, None)

            empty_bundle = RefBundle([], schema=None, owns_blocks=False)
            self._reduce_metrics.on_task_submitted(
                group_id, empty_bundle, task_id=data_task.get_task_id()
            )

    # -----------------------------------------------------------------------
    # Completion
    # -----------------------------------------------------------------------

    def has_execution_finished(self) -> bool:
        if not self._is_all_reduce_submitted():
            return False
        return super().has_execution_finished()

    def has_completed(self) -> bool:
        return self._is_all_reduce_submitted() and super().has_completed()

    # -----------------------------------------------------------------------
    # Shutdown
    # -----------------------------------------------------------------------

    def _do_shutdown(self, force: bool = False) -> None:
        end_time = time.perf_counter()
        total_elapsed = end_time - self._start_time
        reduce_elapsed = (
            end_time - self._reduce_start_time
            if self._reduce_start_time is not None
            else 0
        )
        map_elapsed = (
            self._reduce_start_time - self._start_time
            if self._reduce_start_time is not None
            else total_elapsed
        )
        logger.info(
            f"Shuffle timing: total={total_elapsed:.1f}s, "
            f"map={map_elapsed:.1f}s, reduce={reduce_elapsed:.1f}s"
        )
        super()._do_shutdown(force)
        self._map_tasks.clear()
        self._reduce_tasks.clear()
        self._group_buffers.clear()
        self._merge_buffer_refs_by_node.clear()
        self._merge_buffer_bundles_by_node.clear()
        self._merge_buffer_bytes_by_node.clear()

    # -----------------------------------------------------------------------
    # Stats / metrics
    # -----------------------------------------------------------------------

    def get_stats(self) -> Dict[str, List[BlockStats]]:
        return {
            f"{self._name}_map": self._map_blocks_stats,
            f"{self._name}_reduce": self._output_blocks_stats,
        }

    def _extra_metrics(self) -> Dict[str, Any]:
        return {
            f"{self._name}_map": self._map_metrics.as_dict(),
            f"{self._name}_reduce": self._reduce_metrics.as_dict(),
        }

    # -----------------------------------------------------------------------
    # Resource accounting
    # -----------------------------------------------------------------------

    def num_output_rows_total(self) -> Optional[int]:
        return self._total_input_rows if self._total_input_rows > 0 else None

    def current_logical_usage(self) -> ExecutionResources:
        reduce_cpu = len(self._reduce_tasks) * self._reduce_task_num_cpus
        return self._map_resource_usage.add(ExecutionResources(cpu=reduce_cpu))

    def incremental_resource_usage(self) -> ExecutionResources:
        return ExecutionResources(cpu=self._map_task_num_cpus)

    def min_scheduling_resources(self) -> ExecutionResources:
        return self.incremental_resource_usage()

    # -----------------------------------------------------------------------
    # Progress bar support
    # -----------------------------------------------------------------------

    def progress_str(self) -> str:
        maps_done = self._next_map_task_idx - len(self._map_tasks)
        reduces_submitted = self._num_groups - len(self._pending_reduce_group_ids)
        reduces_done = reduces_submitted - len(self._reduce_tasks)
        parts = [f"map: {maps_done}/{self._next_map_task_idx}"]
        total_merge_buf = sum(
            len(refs) for refs in self._merge_buffer_refs_by_node.values()
        )
        if total_merge_buf:
            parts.append(f"merge_buf: {total_merge_buf}")
        parts.append(f"reduce: {reduces_done}/{self._num_groups}")
        return ", ".join(parts)

    def get_sub_progress_bar_names(self) -> Optional[List[str]]:
        return ["Map", "Reduce"]

    def set_sub_progress_bar(self, name: str, pg: "BaseProgressBar") -> None:
        if name == "Map":
            self._map_bar = pg
        elif name == "Reduce":
            self._reduce_bar = pg

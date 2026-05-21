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


# Maps rows of a pa.Table block to output partition IDs.  num_partitions
# and any other config are bound via closure.  The operator filters out empty
# entries, so implementations don't need to.
PartitionFn = Callable[[pa.Table], Dict[int, pa.Table]]

# Takes (partition_id, accumulated_shard_tables) and yields output blocks.
# See :class:`BaseShuffleOperator` for streaming vs. blocking call semantics.
ReduceFn = Callable[[int, List[pa.Table]], Iterable[Block]]


# Cap return objects from each mapper to bound object-store pressure.
_MAX_RETURN_GROUPS = 200

# Number of ObjectRefs fetched per ray.get() call in reducers.
_REDUCE_BATCH_SIZE = 256


def _partition_blocks_to_shards(
    blocks: Tuple[Block, ...], partition_fn: PartitionFn
) -> Dict[int, List[pa.Table]]:
    """Run partition_fn on each block; collect non-empty shards by pid.

    Each block is partitioned independently so we never materialize a single
    concatenated table.  partition_fn is responsible for any defragmentation
    it needs (e.g. hash_partition calls try_combine_chunked_columns).
    """
    partition_accumulators: Dict[int, List[pa.Table]] = {}
    for block in blocks:
        block = TableBlockAccessor.try_convert_block_type(
            block, block_type=BlockType.ARROW
        )
        if block.num_rows == 0:
            continue
        assert isinstance(block, pa.Table), f"Expected pa.Table, got {type(block)}"
        if any(col.num_chunks > 1 for col in block.columns):
            block = block.combine_chunks()
        block_partitions = partition_fn(block)
        for pid, shard in block_partitions.items():
            if shard.num_rows > 0:
                partition_accumulators.setdefault(pid, []).append(shard)
        del block, block_partitions
    return partition_accumulators


def _encode_group_ipc(
    sorted_pids: List[int],
    pid_tables: Dict[int, pa.Table],
    ipc_write_options: pa.ipc.IpcWriteOptions,
) -> pa.Buffer:
    """ZSTD-encode one group as a single Arrow IPC stream."""
    import json as _json

    # Pre-materialize batches per pid so we can tag the schema with the
    # full [(pid, batch_count), ...] mapping before writing anything.
    pid_batches: List[Tuple[int, List[pa.RecordBatch]]] = []
    for pid in sorted_pids:
        table = pid_tables[pid]
        if table.num_columns > 0:
            table = table.combine_chunks()
        pid_batches.append((pid, table.to_batches()))

    # All-empty group: caller shouldn't pass us this, but be defensive.
    first_with_batches = next((bs for _, bs in pid_batches if bs), None)
    if first_with_batches is None:
        return pa.py_buffer(b"")

    pids_with_counts = [(pid, len(batches)) for pid, batches in pid_batches]
    # Copy: schema.metadata is shared across tables that share a schema, and
    # we don't want to mutate the upstream schema's metadata dict.
    schema_meta = dict(first_with_batches[0].schema.metadata or {})
    schema_meta[b"__pids__"] = _json.dumps(pids_with_counts).encode()
    tagged_schema = first_with_batches[0].schema.with_metadata(schema_meta)

    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, tagged_schema, options=ipc_write_options)
    for _, batches in pid_batches:
        for batch in batches:
            writer.write_batch(batch)
    writer.close()
    return sink.getvalue()


@ray.remote
def _shuffle_map_task(
    *blocks: Block,
    partition_fn: PartitionFn,
    num_partitions: int,
    shard_group_size: int = 1,
):
    """Map stage: partition one or more blocks and return grouped shards.

    Multiple input blocks may be passed when pre-map merge is enabled; they
    are partitioned individually (to avoid a large combine_chunks on the
    merged table) and their shards are accumulated before grouping.

    Must be called with num_returns = num_groups + 1 where
    num_groups = ceil(num_partitions / shard_group_size).

    Args:
        *blocks: One or more input blocks.
        partition_fn: Callable (pa.Table) -> Dict[int, pa.Table] that
            assigns rows to output partitions.
        num_partitions: Total number of output partitions.
        shard_group_size: Number of partitions packed into each return object.

    Returns:
        Tuple of num_groups + 1 values:

        - Index 0: (BlockMetadata, Dict) — aggregate input metadata and a
          per-partition (rows, bytes) size dict (keys are the partitions
          that received any rows from this mapper).
        - Index 1 … G: a ZSTD-compressed Arrow IPC stream for the partitions
          in that shard group, or None if the group is entirely empty.
    """
    from dataclasses import replace as _dc_replace

    stats = BlockExecStats.builder()
    num_groups = (num_partitions + shard_group_size - 1) // shard_group_size

    # Use BlockAccessor so we work for non-Arrow block types (pandas, numpy)
    # whose num_rows / nbytes attributes aren't named the same.
    accessors = [BlockAccessor.for_block(b) for b in blocks]
    total_rows = sum(a.num_rows() for a in accessors)
    total_bytes = sum((a.size_bytes() or 0) for a in accessors)

    # Empty-input fast path.
    if total_rows == 0:
        input_meta = BlockAccessor.for_block(blocks[0]).get_metadata(
            block_exec_stats=stats.build(block_ser_time_s=0),
        )
        return (input_meta, {}), *([None] * num_groups)

    # Step 1: partition each input block into (pid -> [shard_table, ...]).
    partition_accumulators = _partition_blocks_to_shards(blocks, partition_fn)

    # Step 2: bucket pids by their target group_idx.
    group_pid_keys: Dict[int, List[int]] = {}
    for pid in partition_accumulators:
        group_idx = pid // shard_group_size
        group_pid_keys.setdefault(group_idx, []).append(pid)

    # Step 3: merge per-pid shards and ZSTD-encode each group.
    ipc_write_options = pa.ipc.IpcWriteOptions(compression=pa.Codec("zstd"))
    shard_sizes: Dict[int, Tuple[int, int]] = {}
    groups = [None] * num_groups
    for group_idx, pid_keys in group_pid_keys.items():
        sorted_pids = sorted(pid_keys)
        pid_tables: Dict[int, pa.Table] = {}
        for pid in sorted_pids:
            tables = partition_accumulators.pop(pid)
            merged = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
            pid_tables[pid] = merged
            shard_sizes[pid] = (merged.num_rows, merged.nbytes)
        groups[group_idx] = _encode_group_ipc(
            sorted_pids, pid_tables, ipc_write_options
        )
        del pid_tables
    del group_pid_keys

    # Step 4: build aggregate input metadata and return.
    input_meta = BlockAccessor.for_block(blocks[0]).get_metadata(
        block_exec_stats=stats.build(block_ser_time_s=0),
    )
    input_meta = _dc_replace(input_meta, num_rows=total_rows, size_bytes=total_bytes)
    return (input_meta, shard_sizes), *groups


def _read_ipc_group(
    group: pa.Buffer,
) -> List[Tuple[int, pa.Table]]:
    """Decompress one IPC group and return (partition_id, table) pairs."""
    import json as _json

    reader = pa.ipc.open_stream(group)
    pids_with_counts = _json.loads(reader.schema.metadata[b"__pids__"])
    meta = dict(reader.schema.metadata)
    meta.pop(b"__pids__", None)
    clean_schema = (
        reader.schema.with_metadata(meta) if meta else reader.schema.remove_metadata()
    )
    result = []
    for pid, batch_count in pids_with_counts:
        batches = []
        for _ in range(batch_count):
            batch = reader.read_next_batch()
            if batch.num_rows > 0:
                batches.append(batch)
        if batches:
            result.append((pid, pa.Table.from_batches(batches, schema=clean_schema)))
    return result


@ray.remote
def _shuffle_reduce_task(
    group_refs: List[ObjectRef],
    partition_ids: List[int],
    reduce_fn: ReduceFn,
    target_max_block_size: Optional[int],
    streaming: bool = True,
) -> Generator[Union[Block, bytes], None, None]:
    """Reduce stage: fetch shards and call reduce_fn for each partition.

    Both modes use batched ray.get (_REDUCE_BATCH_SIZE refs at a
    time) to avoid memory spikes from a single large dereference.

    Streaming mode (streaming=True):
        Calls reduce_fn(partition_id, accumulated_tables) whenever a
        partition's accumulator exceeds target_max_block_size, then pushes
        the produced blocks through a per-partition :class:`BlockOutputBuffer`
        so each emitted block respects target_max_block_size.  Peak input
        accumulator per partition is bounded to ~target_max_block_size.
        reduce_fn must produce valid output from partial data.

    Blocking mode (streaming=False):
        Accumulates all shards for every partition before calling
        reduce_fn(partition_id, all_tables) once per partition.  Use this
        when reduce_fn requires the full partition (sort, aggregate).

    Args:
        group_refs: ObjectRefs to compressed IPC groups from all mappers.
        partition_ids: Partition IDs this reducer is responsible for.
        reduce_fn: User-supplied reduce callable.
        target_max_block_size: Target output block size (also the streaming
            flush threshold).  Comes from DataContext.target_max_block_size,
            or None when the operator was constructed with
            disallow_block_splitting=True — in that case the reducer skips
            output reshaping and the streaming-flush check (one block per
            partition, blocking-style).
        streaming: Whether to flush partitions incrementally (True) or
            accumulate all shards before reducing (False).

    Returns:
        Generator yielding output blocks, each followed by a serialized
        BlockMetadataWithSchema.
    """
    start_time_s = time.perf_counter()

    # Empty-input fast path.
    if not group_refs or not partition_ids:
        return

    # Per-partition state.
    # accum_tables / accum_bytes: input-side shard accumulators, used
    # to decide when to call reduce_fn in streaming mode.
    # output_buffers: output-side, one per touched partition.  Lazily
    # allocated on first reduce_fn call to avoid empty-block emission for
    # partitions this reducer received no rows for.
    partition_ids_set = set(partition_ids)
    accum_tables: Dict[int, List[pa.Table]] = {pid: [] for pid in partition_ids}
    accum_bytes: Dict[int, int] = {pid: 0 for pid in partition_ids}
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
                OutputBlockSizeOption.of(
                    target_max_block_size=target_max_block_size,
                )
            )
        buf = output_buffers[pid]
        for block in reduce_fn(pid, tables):
            buf.add_block(block)
            while buf.has_next():
                yield from _yield_with_stats(buf.next())

    # Step 1: fetch group refs in batches, decompress, route shards to their
    # partition accumulators.  In streaming mode, when a partition's
    # accumulator reaches target_max_block_size, flush it through
    # reduce_fn and yield any blocks the output buffer is ready to emit.
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

                if (
                    streaming
                    and target_max_block_size is not None
                    and accum_bytes[pid] >= target_max_block_size
                ):
                    tables = accum_tables[pid]
                    accum_tables[pid] = []
                    accum_bytes[pid] = 0
                    yield from _reduce_into_buffer(pid, tables)

        del batch_groups

    # Step 2: all input groups consumed.  Drain any remaining accumulated
    # shards through reduce_fn — this is the only reduce_fn call per
    # partition in blocking mode, and the tail-flush in streaming mode.
    for pid in sorted(partition_ids):
        if accum_tables[pid]:
            yield from _reduce_into_buffer(pid, accum_tables[pid])

    # Step 3: finalize each touched output buffer to flush any partial block
    # that hasn't reached target_max_block_size yet, then yield it.
    for pid in sorted(output_buffers):
        buf = output_buffers[pid]
        buf.finalize()
        while buf.has_next():
            yield from _yield_with_stats(buf.next())


class BaseShuffleOperator(PhysicalOperator, SubProgressBarMixin):
    """Generic map-reduce physical operator.

    Lifecycle
    ---------
    1. As upstream bundles arrive via :meth:`add_input`, blocks are optionally
       batched by node into a pre-map merge buffer.  When the buffer exceeds
       :attr:`_pre_map_merge_threshold`, a merged map task is submitted.
    2. Each map task calls PartitionFn on its input blocks, groups
       shards into ZSTD-compressed IPC objects, and returns metadata + group
       refs.  The driver stores group refs in per-group buffers (no data copy).
    3. Once all map tasks complete, reduce tasks are launched.  Concurrency is
       limited to one reducer per live worker node.  Each reducer fetches its
       group refs, decompresses them, and calls ReduceFn per partition.

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
        If True (default), call reduce_fn incrementally as a
        partition's accumulator reaches data_context.target_max_block_size.
        If False, accumulate all shards before calling reduce_fn once
        — required for sort or aggregate.  Implicitly forced to False when
        disallow_block_splitting=True.
    disallow_block_splitting:
        If True, output blocks from reduce_fn are emitted as-is without being
        reshaped to data_context.target_max_block_size.  Required for hash-
        shuffle semantics, where every row sharing a key must end up in the
        same output block (groupby, map_groups, repartition with keys).
        Forces blocking-mode reduce (no streaming-flush mid-partition).
        Defaults to False.
    pre_map_merge_threshold:
        Byte threshold per node at which buffered blocks are merged into a
        single map task.  Set to 0 to disable pre-map merging.
    map_runtime_env:
        Optional runtime_env dict passed to each map task via
        @ray.remote(...).remote().  Use to isolate map workers into a
        dedicated pool.
    map_cpus:
        CPU request per map task.
    reduce_cpus:
        CPU request per reduce task.  If None, auto-derived from partition
        sizes and cluster shape (1.0 for streaming, memory-proportional for
        blocking).
    name:
        Display name shown in progress bars and logs.
    """

    _DEFAULT_shuffle_map_task_NUM_CPUS = 1.0
    _DEFAULT_shuffle_reduce_task_NUM_CPUS = 1.0
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
        disallow_block_splitting: bool = False,
        pre_map_merge_threshold: int = _DEFAULT_PRE_MAP_MERGE_THRESHOLD,
        map_runtime_env: Optional[Dict[str, Any]] = None,
        map_cpus: float = _DEFAULT_shuffle_map_task_NUM_CPUS,
        reduce_cpus: Optional[float] = None,
        reduce_ray_remote_args_override: Optional[Dict[str, Any]] = None,
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
        self._disallow_block_splitting: bool = disallow_block_splitting
        # When block-splitting is disallowed, the reducer must see the entire
        # partition before emitting (matches legacy semantics; see the
        # disallow_block_splitting docstring).  Force blocking mode so we
        # never streaming-flush mid-partition.
        self._streaming_reduce: bool = streaming_reduce and not disallow_block_splitting

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
        self._shuffle_map_task_num_cpus: float = map_cpus
        self._map_runtime_env: Optional[Dict[str, Any]] = map_runtime_env

        # -- Map task tracking -----------------------------------------------
        self._next_shuffle_map_task_idx: int = 0
        self._shuffle_map_tasks: Dict[int, MetadataOpTask] = {}
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
        self._shuffle_reduce_task_num_cpus_override: Optional[float] = reduce_cpus
        self._shuffle_reduce_task_ray_remote_args_override: Dict[str, Any] = dict(
            reduce_ray_remote_args_override or {}
        )
        self._shuffle_reduce_task_num_cpus: float = (
            self._DEFAULT_shuffle_reduce_task_NUM_CPUS
        )
        self._shuffle_reduce_tasks: Dict[int, DataOpTask] = {}
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
                self._submit_shuffle_map_task(
                    [block_ref],
                    [input_bundle],
                    estimated_bytes=block_metadata.size_bytes or 0,
                )

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
        self._submit_shuffle_map_task(
            block_refs,
            bundles,
            estimated_bytes=estimated_bytes,
            target_node_id=node_id if node_id != "unknown" else None,
        )

    def _submit_shuffle_map_task(
        self,
        block_refs: List[ObjectRef],
        input_bundles: List[RefBundle],
        estimated_bytes: int = 0,
        target_node_id: Optional[str] = None,
    ) -> None:
        cur_task_idx = self._next_shuffle_map_task_idx
        self._next_shuffle_map_task_idx += 1

        task_resources: Dict[str, Any] = {"num_cpus": self._shuffle_map_task_num_cpus}
        if estimated_bytes > 0:
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

        map_refs = _shuffle_map_task.options(**remote_opts).remote(
            *block_refs,
            partition_fn=self._partition_fn,
            num_partitions=self._num_partitions,
            shard_group_size=self._shard_group_size,
        )
        metadata_ref = map_refs[0]
        group_refs = list(map_refs[1:])

        task = MetadataOpTask(
            task_index=cur_task_idx,
            object_ref=metadata_ref,
            task_done_callback=functools.partial(
                self._handle_map_done, cur_task_idx, group_refs, input_bundles
            ),
            task_resource_bundle=ExecutionResources.from_resource_dict(task_resources),
        )
        self._shuffle_map_tasks[cur_task_idx] = task
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

    def _handle_map_done(
        self,
        task_idx: int,
        group_refs: List[ObjectRef],
        input_bundles: List[RefBundle],
    ) -> None:
        task = self._shuffle_map_tasks.pop(task_idx)
        self._map_resource_usage = self._map_resource_usage.subtract(
            task.get_requested_resource_bundle()
        )

        input_meta, shard_sizes = ray.get(task.get_waitable(), timeout=60)

        for pid, (rows, nbytes) in shard_sizes.items():
            self._partition_row_counts[pid] += rows
            self._partition_bytes[pid] += nbytes

        # Append all group refs; reducer skips entries that resolve to None.
        for group_idx, ref in enumerate(group_refs):
            self._group_buffers[group_idx].append(ref)

        for bundle in input_bundles:
            bundle.destroy_if_owned()

        self._total_input_rows += input_meta.num_rows or 0
        self._total_input_bytes += input_meta.size_bytes or 0
        self._map_blocks_stats.append(input_meta.to_stats())

        for bundle in input_bundles:
            self._map_metrics.on_output_taken(bundle)
        self._map_metrics.on_task_output_generated(
            task_idx,
            RefBundle(
                [(task.get_waitable(), input_meta)],
                schema=None,
                owns_blocks=False,
            ),
        )
        self._map_metrics.on_task_finished(
            task_idx,
            None,
            task_exec_stats=None,
            task_exec_driver_stats=None,
        )

        if self._map_bar is not None:
            self._map_bar.update(increment=input_meta.num_rows or 0)

    def has_next(self) -> bool:
        self._try_reduce()
        return len(self._output_queue) > 0

    def _get_next_inner(self) -> RefBundle:
        bundle: RefBundle = self._output_queue.popleft()
        self._reduce_metrics.on_output_dequeued(bundle)
        self._reduce_metrics.on_output_taken(bundle)
        self._output_blocks_stats.extend(to_stats(bundle.metadata))
        return bundle

    def get_active_tasks(self) -> List[OpTask]:
        return list(self._shuffle_map_tasks.values()) + list(
            self._shuffle_reduce_tasks.values()
        )

    def _is_map_done(self) -> bool:
        return self._inputs_complete and len(self._shuffle_map_tasks) == 0

    def _is_all_reduce_submitted(self) -> bool:
        return len(self._pending_reduce_group_ids) == 0

    def _auto_derive_reduce_cpus(self) -> float:
        """Derive shuffle_reduce_task_num_cpus from partition sizes and cluster shape.

        For streaming reduce, memory per partition is bounded by
        target_max_block_size (typically 128 MB), so 1 CPU maximises
        concurrency.  For blocking reduce, peak heap is proportional to the
        largest group's uncompressed bytes, so CPUs are scaled to bound
        co-located heap usage.
        """
        import math

        if self._streaming_reduce:
            return self._DEFAULT_shuffle_reduce_task_NUM_CPUS

        if not self._partition_bytes:
            return self._DEFAULT_shuffle_reduce_task_NUM_CPUS

        max_group_bytes = 0
        for group_idx in range(self._num_groups):
            start = group_idx * self._shard_group_size
            end = min(start + self._shard_group_size, self._num_partitions)
            group_bytes = sum(
                self._partition_bytes.get(pid, 0) for pid in range(start, end)
            )
            max_group_bytes = max(max_group_bytes, group_bytes)

        if max_group_bytes == 0:
            return self._DEFAULT_shuffle_reduce_task_NUM_CPUS

        reducer_heap = max_group_bytes * 2  # decompressed input + output

        cluster_res = ray.cluster_resources()
        num_nodes = max(len(ray.nodes()), 1)
        node_cpus = max(1.0, cluster_res.get("CPU", 8) / num_nodes)
        node_memory = cluster_res.get("memory", 30e9) / num_nodes

        proportional_cpus = max(1.0, math.ceil(node_cpus * reducer_heap / node_memory))
        target_per_node = max(1, int(node_cpus / proportional_cpus))
        reduce_cpus = max(1.0, math.ceil(node_cpus / target_per_node))
        return float(reduce_cpus)

    def _get_reduce_concurrency_limit(self) -> int:
        """Max concurrent reduce tasks allowed.

        Defaults to 1 per live worker node to prevent all reducers from
        launching simultaneously (which floods the object store before
        downstream can drain).  As each reducer finishes, the next wave is
        scheduled by :meth:`_try_reduce`.

        Override via DataContext config key
        map_reduce_max_concurrent_reducers.
        """
        override = self._data_context.get_config(
            "map_reduce_max_concurrent_reducers", None
        )
        if override is not None:
            return int(override)
        return self._num_nodes

    def _try_reduce(self) -> None:
        if self._is_all_reduce_submitted():
            return
        if not self._is_map_done():
            return

        if self._reduce_start_time is None:
            self._init_reduce_phase()

        slots_available = self._get_reduce_concurrency_limit() - len(
            self._shuffle_reduce_tasks
        )

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

            reduce_resources, estimated_bytes = self._compute_reduce_resources(
                partition_ids
            )

            reduce_options = {
                **reduce_resources,
                "scheduling_strategy": "SPREAD",
                **self._shuffle_reduce_task_ray_remote_args_override,
                "num_returns": "streaming",
            }

            block_gen = _shuffle_reduce_task.options(**reduce_options).remote(
                group_refs,
                partition_ids=partition_ids,
                reduce_fn=self._reduce_fn,
                # None tells the reducer to emit blocks as-is (no
                # BlockOutputBuffer reshaping, no streaming flush).
                target_max_block_size=(
                    None
                    if self._disallow_block_splitting
                    else self.data_context.target_max_block_size
                ),
                streaming=self._streaming_reduce,
            )

            data_task = DataOpTask(
                task_index=group_id,
                streaming_gen=block_gen,
                output_ready_callback=functools.partial(
                    self._handle_reduce_output_ready, group_id
                ),
                task_done_callback=functools.partial(
                    self._handle_reduce_done, group_id
                ),
                task_resource_bundle=ExecutionResources(
                    cpu=reduce_options.get(
                        "num_cpus", self._shuffle_reduce_task_num_cpus
                    ),
                    memory=estimated_bytes,
                    object_store_memory=estimated_bytes,
                ),
                operator_name=self.name,
            )

            self._shuffle_reduce_tasks[group_id] = data_task
            self._pending_reduce_group_ids.discard(group_id)
            slots_available -= 1

            # Release driver-side refs so intermediate objects can be reclaimed.
            self._group_buffers.pop(group_id, None)

            empty_bundle = RefBundle([], schema=None, owns_blocks=False)
            self._reduce_metrics.on_task_submitted(
                group_id, empty_bundle, task_id=data_task.get_task_id()
            )

    def _init_reduce_phase(self) -> None:
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

        if self._shuffle_reduce_task_num_cpus_override is not None:
            self._shuffle_reduce_task_num_cpus = (
                self._shuffle_reduce_task_num_cpus_override
            )
        else:
            self._shuffle_reduce_task_num_cpus = self._auto_derive_reduce_cpus()

        logger.info(
            f"Reduce starting: map_elapsed={map_elapsed:.1f}s, "
            f"maps={self._next_shuffle_map_task_idx}, "
            f"reduce_cpus={self._shuffle_reduce_task_num_cpus}, "
            f"num_nodes={self._num_nodes}"
        )

    def _compute_reduce_resources(
        self, partition_ids: List[int]
    ) -> Tuple[Dict[str, Any], int]:

        estimated_bytes = sum(
            self._partition_bytes.get(pid, 0) for pid in partition_ids
        )
        cluster_res = ray.cluster_resources()
        num_nodes = max(len(ray.nodes()), 1)
        node_memory = cluster_res.get("memory", 30e9) / num_nodes
        reduce_resources: Dict[str, Any] = {
            "num_cpus": self._shuffle_reduce_task_num_cpus
        }
        if estimated_bytes > 0:
            # Cap at a single node's memory so the scheduler still has a
            # meaningful hint (it'll pick the most-free node).  If we asked
            # for more than any node has, Ray would queue forever — but
            # falling back to 0 disables memory-aware placement entirely,
            # which is worse: the reducer can land on a loaded node and OOM.
            reduce_resources["memory"] = min(estimated_bytes * 2, node_memory)
        return reduce_resources, estimated_bytes

    def _handle_reduce_output_ready(self, group_id: int, bundle: RefBundle) -> None:
        self._output_queue.append(bundle)
        self._reduce_metrics.on_output_queued(bundle)
        self._reduce_metrics.on_task_output_generated(
            task_index=group_id, output=bundle
        )
        _, num_outputs, num_rows = estimate_total_num_of_blocks(
            group_id + 1,
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

    def _handle_reduce_done(
        self,
        group_id: int,
        exc: Optional[Exception],
        task_exec_stats: Optional[TaskExecWorkerStats],
        task_exec_driver_stats: Optional[TaskExecDriverStats],
    ) -> None:
        """Callback when a reduce task finishes (with or without exception)."""
        if group_id not in self._shuffle_reduce_tasks:
            return
        self._shuffle_reduce_tasks.pop(group_id)
        self._reduce_metrics.on_task_finished(
            task_index=group_id,
            exception=exc,
            task_exec_stats=task_exec_stats,
            task_exec_driver_stats=task_exec_driver_stats,
        )
        if exc:
            logger.error(f"Reduce of group {group_id} failed: {exc}", exc_info=exc)

    def has_execution_finished(self) -> bool:
        if not self._is_all_reduce_submitted():
            return False
        return super().has_execution_finished()

    def has_completed(self) -> bool:
        return self._is_all_reduce_submitted() and super().has_completed()

    def _do_shutdown(self, force: bool = False) -> None:
        super()._do_shutdown(force)
        self._shuffle_map_tasks.clear()
        self._shuffle_reduce_tasks.clear()
        self._group_buffers.clear()
        self._merge_buffer_refs_by_node.clear()
        self._merge_buffer_bundles_by_node.clear()
        self._merge_buffer_bytes_by_node.clear()

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

    def num_output_rows_total(self) -> Optional[int]:
        return self._total_input_rows if self._total_input_rows > 0 else None

    def current_logical_usage(self) -> ExecutionResources:
        usage = self._map_resource_usage
        for task in self._shuffle_reduce_tasks.values():
            bundle = task.get_requested_resource_bundle()
            usage = usage.add(ExecutionResources(cpu=bundle.cpu, memory=bundle.memory))
        return usage

    def incremental_resource_usage(self) -> ExecutionResources:
        return ExecutionResources(cpu=self._shuffle_map_task_num_cpus)

    def min_scheduling_resources(self) -> ExecutionResources:
        return self.incremental_resource_usage()

    def progress_str(self) -> str:
        maps_done = self._next_shuffle_map_task_idx - len(self._shuffle_map_tasks)
        reduces_submitted = self._num_groups - len(self._pending_reduce_group_ids)
        reduces_done = reduces_submitted - len(self._shuffle_reduce_tasks)
        parts = [f"map: {maps_done}/{self._next_shuffle_map_task_idx}"]
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

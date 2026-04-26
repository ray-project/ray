"""Actorless Hash Shuffle Operator.

This module implements a hash shuffle using only stateless Ray tasks (no actors).
The shuffle has up to three stages:

1. **Map & Scatter**: Each mapper task hash-partitions an input block into M
   partitions and returns each shard as a separate task output.
   The driver receives one ``ObjectRef`` per partition.

2. **Driver Buffering**: The driver collects ObjectRefs into per-partition buffer
   lists. This is pure bookkeeping with no data movement.

3. **Reduce**: After all map tasks complete, the driver launches M reducer tasks.
   Each reducer dereferences all shard refs for its partition, concatenates them,
   and yields output blocks via a streaming generator.
"""

import functools
import logging
import time
import typing
from collections import defaultdict, deque
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
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
from ray.data._internal.arrow_block import ArrowBlockBuilder
from ray.data._internal.arrow_ops.transform_pyarrow import hash_partition
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

BlockTransformer = Callable[[Block], Block]


# Isolate shuffle map workers into a dedicated worker pool so that
# ReadParquet/Project tasks don't run on the same workers. Without this,
# shared memory pages from object store accesses (mmap'd during
# combine_chunks) accumulate across task types and inflate worker RSS.
_SHUFFLE_MAP_RUNTIME_ENV = {"env_vars": {"RAY_DATA_SHUFFLE_MAP_WORKER": "1"}}


@ray.remote
def _shuffle_map(
    *blocks: Block,
    key_columns: List[str],
    num_partitions: int,
    shard_group_size: int = 1,
    block_transformer: Optional[BlockTransformer] = None,
):
    """Map stage: hash-partition one or more blocks and return grouped shards.

    When pre-map merge is enabled, multiple input blocks are passed and
    concatenated before hash-partitioning, reducing the number of map tasks
    and thus the M×N intermediate object count.

    Must be called with ``num_returns=num_groups + 1`` where
    ``num_groups = ceil(num_partitions / shard_group_size)``.  Returns
    ``num_groups + 1`` objects:

    - Index 0: ``(BlockMetadata, List[int], Dict)`` — input block metadata,
      list of non-empty partition IDs, and per-partition shard sizes.
    - Index 1..G: a dict ``{partition_id: pa.Table}`` for the partitions in
      that group, or ``None`` if all partitions in the group are empty.

    Args:
        *blocks: One or more input blocks to shuffle. Multiple blocks are
            concatenated before partitioning (pre-map merge).
        key_columns: Columns used for hash-partitioning.
        num_partitions: Total number of output partitions (M).
        shard_group_size: Number of partitions packed into each return object.
        block_transformer: Optional transform applied before partitioning.

    Returns:
        A tuple of ``(BlockMetadata, List[int], Dict)`` and the grouped
        shard dicts.
    """
    import time as _time

    stats = BlockExecStats.builder()

    t0 = _time.perf_counter()

    num_groups = (num_partitions + shard_group_size - 1) // shard_group_size

    # Compute total input metadata from all blocks.
    total_rows = sum(b.num_rows for b in blocks)
    total_bytes = sum(b.nbytes for b in blocks)

    if total_rows == 0:
        input_block_metadata = BlockAccessor.for_block(blocks[0]).get_metadata(
            block_exec_stats=stats.build(block_ser_time_s=0),
        )
        return (input_block_metadata, [], {}), *([None] * num_groups)

    t1 = _time.perf_counter()

    # Hash-partition each block individually to keep peak memory at ~2x one
    # block instead of ~2x all blocks combined. Accumulate per-partition
    # shards as lists of tables, then concatenate at the end.
    partition_accumulators: Dict[int, List[pa.Table]] = {}

    for block in blocks:
        if block_transformer is not None:
            block = block_transformer(block)

        block = TableBlockAccessor.try_convert_block_type(
            block, block_type=BlockType.ARROW
        )

        if block.num_rows == 0:
            continue

        assert isinstance(block, pa.Table), f"Expected pa.Table, got {type(block)}"

        # Defragment chunked columns for faster hash-partitioning.
        if any(col.num_chunks > 1 for col in block.columns):
            block = block.combine_chunks()

        block_partitions = hash_partition(
            block, hash_cols=key_columns, num_partitions=num_partitions
        )

        for pid_key, shard in block_partitions.items():
            if shard.num_rows > 0:
                if pid_key not in partition_accumulators:
                    partition_accumulators[pid_key] = []
                partition_accumulators[pid_key].append(shard)

        # Release references to allow GC of this block's intermediate data.
        del block, block_partitions

    # Build grouped output: concatenate accumulated shards per partition.
    # Serialize each shard as LZ4-compressed Arrow IPC to reduce bytes
    # transferred over the network during the reduce fetch phase.
    ipc_write_options = pa.ipc.IpcWriteOptions(compression=pa.Codec("zstd"))
    groups = [None] * num_groups
    non_empty_pids = []
    shard_sizes = {}
    total_compressed = 0
    total_uncompressed = 0
    for pid_key, tables in partition_accumulators.items():
        merged = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        group_idx = pid_key // shard_group_size
        if groups[group_idx] is None:
            groups[group_idx] = {}
        # Serialize to compressed IPC bytes.
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, merged.schema, options=ipc_write_options)
        writer.write_table(merged)
        writer.close()
        compressed_buf = sink.getvalue()
        total_compressed += len(compressed_buf)
        total_uncompressed += merged.nbytes
        groups[group_idx][pid_key] = compressed_buf
        non_empty_pids.append(pid_key)
        shard_sizes[pid_key] = (merged.num_rows, merged.nbytes)
    if total_uncompressed > 0:
        logger.info(
            f"_shuffle_map compression: "
            f"uncompressed={total_uncompressed / 1e6:.0f}MB, "
            f"compressed={total_compressed / 1e6:.0f}MB, "
            f"ratio={total_uncompressed / total_compressed:.1f}x"
        )

    # Build input metadata with totals across all input blocks.
    from dataclasses import replace as _dc_replace

    input_block_metadata = BlockAccessor.for_block(blocks[0]).get_metadata(
        block_exec_stats=stats.build(block_ser_time_s=0),
    )
    input_block_metadata = _dc_replace(
        input_block_metadata,
        num_rows=total_rows,
        size_bytes=total_bytes,
    )

    t3 = _time.perf_counter()
    t4 = t3  # No separate build_shards step

    import os as _os

    def _mem_breakdown():
        pid = _os.getpid()
        info = {}
        try:
            with open(f"/proc/{pid}/smaps_rollup") as f:
                for line in f:
                    parts = line.split()
                    if len(parts) >= 2:
                        try:
                            info[parts[0].rstrip(":")] = int(parts[1]) / 1024
                        except ValueError:
                            continue
        except Exception:
            pass
        if not info:
            try:
                with open(f"/proc/{pid}/statm") as f:
                    info["Rss"] = (
                        int(f.read().split()[1]) * _os.sysconf("SC_PAGE_SIZE") / 1e6
                    )
            except Exception:
                info["Rss"] = -1
        return info

    sm = _mem_breakdown()
    # USS = Private_Clean + Private_Dirty (pages unique to this process)
    uss = sm.get("Private_Clean", 0) + sm.get("Private_Dirty", 0)
    rss = sm.get("Rss", -1)
    shared = sm.get("Shared_Clean", 0) + sm.get("Shared_Dirty", 0)
    anon = sm.get("Anonymous", 0)

    logger.info(
        f"_shuffle_map memory: rows={total_rows}, blocks={len(blocks)}, "
        f"prep={t1-t0:.3f}s, partition+merge={t3-t1:.3f}s, "
        f"total={t4-t0:.3f}s, "
        f"uss={uss:.0f}MB, rss={rss:.0f}MB, "
        f"shared={shared:.0f}MB, anon(heap)={anon:.0f}MB"
    )

    return (input_block_metadata, non_empty_pids, shard_sizes), *groups


@ray.remote
def _shuffle_reduce(
    group_refs: List[ObjectRef],
    partition_ids: List[int],
    target_max_block_size: Optional[int] = None,
    should_sort: bool = False,
    key_columns: Optional[List[str]] = None,
) -> Generator[Union[Block, BlockMetadataWithSchema], None, None]:
    """Reduce stage: concatenate shards for a group of partitions.

    Each group_ref resolves to a dict ``{partition_id: pa.Table}`` containing
    shards from one mapper.  This reducer unpacks all ``partition_ids`` from
    each group, concatenates per-partition, and yields output blocks.

    Args:
        group_refs: Object references to grouped shard dicts from all mappers.
        partition_ids: The partition IDs this reducer is responsible for.
        target_max_block_size: If set, output blocks are reshaped to this
            target size.
        should_sort: Whether to sort the output by ``key_columns``.
        key_columns: Columns to sort by (required when ``should_sort=True``).
    """
    import time as _time

    start_time_s = _time.perf_counter()

    if not group_refs or not partition_ids:
        return

    t0 = _time.perf_counter()

    # Per-ref fetch instrumentation: log individual fetch times to
    # distinguish plasma-resident fetches (~1 GB/s) from de-spilled
    # fetches (~0.05-0.1 GB/s).  Bimodal distribution = spill is the
    # bottleneck, not network bandwidth.
    groups = []
    fetch_times = []
    fetch_bytes = []
    for i, ref in enumerate(group_refs):
        t_ref_start = _time.perf_counter()
        group = ray.get(ref)
        t_ref_end = _time.perf_counter()
        elapsed = t_ref_end - t_ref_start
        nbytes = 0
        if group is not None:
            nbytes = sum(len(buf) for buf in group.values())
        fetch_times.append(elapsed)
        fetch_bytes.append(nbytes)
        groups.append(group)

    t_fetch = _time.perf_counter()

    # Log per-fetch throughput distribution.
    if fetch_times:
        throughputs = [
            (fb / ft / 1e9 if ft > 0 else float("inf"))
            for ft, fb in zip(fetch_times, fetch_bytes)
        ]
        throughputs_sorted = sorted(throughputs)
        n = len(throughputs_sorted)
        slow_count = sum(1 for tp in throughputs if tp < 0.3)  # < 300 MB/s
        logger.info(
            f"_shuffle_reduce fetch profile: refs={n}, "
            f"total_fetch={t_fetch - t0:.2f}s, "
            f"throughput(GB/s) min={throughputs_sorted[0]:.3f}, "
            f"p50={throughputs_sorted[n // 2]:.3f}, "
            f"p90={throughputs_sorted[int(n * 0.9)]:.3f}, "
            f"max={throughputs_sorted[-1]:.3f}, "
            f"slow_fetches(<300MB/s)={slow_count}/{n}"
        )

    # Accumulate shards per partition from all mapper groups.
    # Shards are LZ4-compressed Arrow IPC buffers — decompress here.
    partition_builders: Dict[int, ArrowBlockBuilder] = {
        pid: ArrowBlockBuilder() for pid in partition_ids
    }

    total_shards = 0
    total_shard_bytes = 0
    total_compressed_bytes = 0
    for group in groups:
        if group is not None:
            for pid in partition_ids:
                if pid in group:
                    compressed_buf = group[pid]
                    total_compressed_bytes += len(compressed_buf)
                    reader = pa.ipc.open_stream(compressed_buf)
                    table = reader.read_all()
                    partition_builders[pid].add_block(table)
                    total_shards += 1
                    total_shard_bytes += table.nbytes
    del groups
    t_decompress = _time.perf_counter()

    t_accumulate = _time.perf_counter()

    # Yield each partition as a separate output block.
    for pid in sorted(partition_ids):
        exec_stats_builder = BlockExecStats.builder()
        result = partition_builders[pid].build()

        if result.num_rows == 0:
            continue

        if should_sort and key_columns:
            result = result.sort_by([(k, "ascending") for k in key_columns])

        if target_max_block_size is not None:
            output_buffer = BlockOutputBuffer(
                output_block_size_option=OutputBlockSizeOption(
                    target_max_block_size=target_max_block_size
                )
            )
            output_buffer.add_block(result)
            output_buffer.finalize()

            while output_buffer.has_next():
                out_block = output_buffer.next()
                exec_stats_builder.finish()

                gen_stats: StreamingGeneratorStats = yield out_block

                exec_stats = exec_stats_builder.build(
                    block_ser_time_s=(
                        gen_stats.object_creation_dur_s if gen_stats else None
                    ),
                )
                exec_stats_builder = BlockExecStats.builder()

                yield BlockMetadataWithSchema.from_block(
                    out_block,
                    block_exec_stats=exec_stats,
                    task_exec_stats=TaskExecWorkerStats(
                        task_wall_time_s=time.perf_counter() - start_time_s,
                    ),
                )
        else:
            exec_stats_builder.finish()

            gen_stats: StreamingGeneratorStats = yield result

            exec_stats = exec_stats_builder.build(
                block_ser_time_s=(
                    gen_stats.object_creation_dur_s if gen_stats else None
                ),
            )

            yield BlockMetadataWithSchema.from_block(
                result,
                block_exec_stats=exec_stats,
                task_exec_stats=TaskExecWorkerStats(
                    task_wall_time_s=time.perf_counter() - start_time_s,
                ),
            )

    t_yield = _time.perf_counter()

    logger.info(
        f"_shuffle_reduce: partitions={len(partition_ids)}, "
        f"groups={len(group_refs)}, shards={total_shards}, "
        f"shard_bytes={total_shard_bytes / 1e6:.1f}MB, "
        f"compressed_bytes={total_compressed_bytes / 1e6:.1f}MB, "
        f"ratio={total_shard_bytes / max(total_compressed_bytes, 1):.1f}x, "
        f"fetch={t_fetch - t0:.3f}s, "
        f"decompress={t_decompress - t_fetch:.3f}s, "
        f"accumulate={t_accumulate - t_decompress:.3f}s, "
        f"build+yield={t_yield - t_accumulate:.3f}s, "
        f"total={t_yield - t0:.3f}s"
    )


# ---------------------------------------------------------------------------
# Operator
# ---------------------------------------------------------------------------


class ActorlessHashShuffleOperator(PhysicalOperator, SubProgressBarMixin):
    """Physical operator for actorless hash shuffle.

    Lifecycle
    ---------
    1. As upstream bundles arrive via :meth:`add_input`, a stateless map task is
       submitted per block.  Each map task hash-partitions the block and returns
       each shard as a separate task output.
    2. The driver accumulates shard ``ObjectRef``s per partition (pure
       bookkeeping).
    3. Once all map tasks are complete, reduce tasks are launched.  Each reduce
       task fetches all shards for one partition, concatenates them, and yields
       output blocks.
    """

    _DEFAULT_MAP_TASK_NUM_CPUS = 1.0
    _DEFAULT_REDUCE_TASK_NUM_CPUS = 1.0
    _DEFAULT_COMPACTION_STRATEGY = "pre_map_merge"  # "none" | "pre_map_merge"
    _DEFAULT_PRE_MAP_MERGE_THRESHOLD = 0.5 * 1024 * 1024 * 1024  # 0.5 GB

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        *,
        key_columns: Tuple[str, ...],
        num_partitions: Optional[int] = None,
        should_sort: bool = False,
    ):
        input_logical_op = input_op._logical_operators[0]
        estimated_input_blocks = input_logical_op.estimated_num_outputs()

        target_num_partitions: int = (
            num_partitions
            or estimated_input_blocks
            or data_context.default_hash_shuffle_parallelism
        )

        name = (
            f"ActorlessShuffle(keys={key_columns}, "
            f"partitions={target_num_partitions})"
        )

        super().__init__(
            name=name,
            input_dependencies=[input_op],
            data_context=data_context,
        )

        self._key_columns: List[str] = list(key_columns)
        self._num_partitions: int = target_num_partitions
        self._should_sort: bool = should_sort

        # -- Shard grouping ---------------------------------------------------
        # Cap at ~200 return objects per mapper to reduce object store pressure.
        _MAX_RETURN_GROUPS = 200
        self._shard_group_size: int = max(
            1, (target_num_partitions + _MAX_RETURN_GROUPS - 1) // _MAX_RETURN_GROUPS
        )
        self._num_groups: int = (
            target_num_partitions + self._shard_group_size - 1
        ) // self._shard_group_size

        # -- Map task tracking ------------------------------------------------
        self._next_map_task_idx: int = 0
        self._map_tasks: Dict[int, MetadataOpTask] = {}
        self._map_resource_usage = ExecutionResources.zero()

        # -- Per-partition stats (driver bookkeeping) -------------------------
        self._partition_row_counts: Dict[int, int] = defaultdict(int)
        self._partition_bytes: Dict[int, int] = defaultdict(int)
        # -- Per-group shard buffers ------------------------------------------
        # group_idx → list of ObjectRefs to group dicts from each mapper
        self._group_buffers: Dict[int, List[ObjectRef]] = defaultdict(list)

        # -- Compaction strategy -----------------------------------------------
        self._compaction_strategy: str = data_context.get_config(
            "actorless_shuffle_compaction_strategy",
            self._DEFAULT_COMPACTION_STRATEGY,
        )

        # -- Pre-map merge buffer ---------------------------------------------
        self._pre_map_merge_threshold: int = data_context.get_config(
            "actorless_shuffle_pre_map_merge_threshold",
            self._DEFAULT_PRE_MAP_MERGE_THRESHOLD,
        )
        # Per-node merge buffers: only co-located blocks are batched together
        # to avoid cross-node object transfers that bloat raylet memory.
        self._merge_buffer_refs_by_node: Dict[str, List[ObjectRef]] = defaultdict(list)
        self._merge_buffer_bytes_by_node: Dict[str, int] = defaultdict(int)
        self._merge_buffer_bundles_by_node: Dict[str, List[RefBundle]] = defaultdict(
            list
        )

        # -- Reduce task tracking ---------------------------------------------
        self._reduce_tasks: Dict[int, DataOpTask] = {}
        self._pending_reduce_group_ids: Set[int] = set(range(self._num_groups))

        # -- Reduce per-task CPU reservation ----------------------------------
        # Reserving >1 CPU per reducer limits how many reducers Ray schedules
        # on the same node, reducing plasma pressure and network contention.
        # This is the sole mechanism for controlling reducer concurrency —
        # Ray's scheduler enforces per-node limits via the CPU budget.
        self._reduce_task_num_cpus: float = data_context.get_config(
            "actorless_shuffle_reduce_task_num_cpus",
            self._DEFAULT_REDUCE_TASK_NUM_CPUS,
        )

        # -- Output queue -----------------------------------------------------
        self._output_queue: deque = deque()

        # -- Timing -----------------------------------------------------------
        self._start_time: float = time.perf_counter()
        self._reduce_start_time: Optional[float] = None

        # -- Stats ------------------------------------------------------------
        self._total_input_rows: int = 0
        self._total_input_bytes: int = 0
        self._shuffled_blocks_stats: List[BlockStats] = []
        self._output_blocks_stats: List[BlockStats] = []

        # -- Sub-progress bars ------------------------------------------------
        self._shuffle_bar: Optional["BaseProgressBar"] = None
        self._shuffle_metrics = OpRuntimeMetrics(self)
        self._reduce_bar: Optional["BaseProgressBar"] = None
        self._reduce_metrics = OpRuntimeMetrics(self)

    # -- Input handling (Map phase) -------------------------------------------

    def can_add_input(self) -> bool:
        # TODO: too many map tasks might compete with upstream operators,
        # we should add a throttle here.
        return True

    def _add_input_inner(self, input_bundle: RefBundle, input_index: int) -> None:
        assert input_index == 0

        self._shuffle_metrics.on_input_received(input_bundle)

        if self._compaction_strategy == "pre_map_merge":
            # Determine the primary node for this bundle's blocks.
            preferred_locs = input_bundle.get_preferred_object_locations()
            if preferred_locs:
                node_id = max(preferred_locs, key=preferred_locs.get)
            else:
                node_id = "unknown"

            # Buffer blocks per-node so merged map tasks only use co-located
            # blocks, avoiding cross-node transfers that bloat raylet memory.
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
            # Default: submit one map task per block immediately.
            for block_ref, block_metadata in zip(
                input_bundle.block_refs, input_bundle.metadata
            ):
                self._submit_map_task([block_ref], [input_bundle])

    def all_inputs_done(self) -> None:
        super().all_inputs_done()
        # Flush any remaining blocks in all per-node merge buffers.
        if self._compaction_strategy == "pre_map_merge":
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
            input_bundles: The RefBundles that own the input blocks (kept alive
                until the map task completes, then destroyed).
            estimated_bytes: Estimated total input size in bytes. Used to
                request memory for merged map tasks to prevent OOM.
            target_node_id: If set, schedule the task on this node using
                NodeAffinitySchedulingStrategy(soft=True) to avoid cross-node
                object transfers.
        """
        cur_task_idx = self._next_map_task_idx
        self._next_map_task_idx += 1

        shuffle_task_resources = {
            "num_cpus": self._DEFAULT_MAP_TASK_NUM_CPUS,
        }
        # For merged map tasks, request memory proportional to input size.
        # With zero-copy concat and skipped combine_chunks, the main heap
        # allocation is the partition shards from table.take() (~1x input).
        # Request 2x to account for hash arrays and serialization overhead.
        if estimated_bytes > 0 and len(block_refs) > 1:
            shuffle_task_resources["memory"] = estimated_bytes * 2

        if target_node_id is not None:
            shuffle_task_resources[
                "scheduling_strategy"
            ] = NodeAffinitySchedulingStrategy(target_node_id, soft=True)

        map_refs = _shuffle_map.options(
            **shuffle_task_resources,
            num_returns=self._num_groups + 1,
            runtime_env=_SHUFFLE_MAP_RUNTIME_ENV,
        ).remote(
            *block_refs,
            key_columns=self._key_columns,
            num_partitions=self._num_partitions,
            shard_group_size=self._shard_group_size,
        )
        metadata_ref = map_refs[0]
        group_refs = map_refs[1:]

        def _on_map_done(task_idx: int, group_refs: List[ObjectRef]):
            task = self._map_tasks.pop(task_idx)
            self._map_resource_usage = self._map_resource_usage.subtract(
                task.get_requested_resource_bundle()
            )

            input_block_metadata, non_empty_pids, shard_sizes = ray.get(
                task.get_waitable(), timeout=60
            )

            # Track which groups have data from this mapper.
            non_empty_group_idxs = set()
            for pid in non_empty_pids:
                group_idx = pid // self._shard_group_size
                non_empty_group_idxs.add(group_idx)
                rows, nbytes = shard_sizes.get(pid, (0, 0))
                self._partition_row_counts[pid] += rows
                self._partition_bytes[pid] += nbytes

            for group_idx in non_empty_group_idxs:
                self._group_buffers[group_idx].append(group_refs[group_idx])

            # Drop local list so empty-group refs (with no other
            # references) are reclaimed by Ray's reference counting.
            del group_refs

            # Free the input blocks now that the map task has consumed them.
            for bundle in input_bundles:
                bundle.destroy_if_owned()

            self._total_input_rows += input_block_metadata.num_rows or 0
            self._total_input_bytes += input_block_metadata.size_bytes or 0
            self._shuffled_blocks_stats.append(input_block_metadata.to_stats())

            # Update shuffle metrics.
            for bundle in input_bundles:
                self._shuffle_metrics.on_output_taken(bundle)
            self._shuffle_metrics.on_task_output_generated(
                cur_task_idx,
                RefBundle(
                    [(task.get_waitable(), input_block_metadata)],
                    schema=None,
                    owns_blocks=False,
                ),
            )
            self._shuffle_metrics.on_task_finished(
                cur_task_idx,
                None,
                task_exec_stats=None,
                task_exec_driver_stats=None,
            )

            if self._shuffle_bar is not None:
                self._shuffle_bar.update(increment=input_block_metadata.num_rows or 0)

        task = MetadataOpTask(
            task_index=cur_task_idx,
            object_ref=metadata_ref,
            task_done_callback=functools.partial(
                _on_map_done, cur_task_idx, group_refs
            ),
            task_resource_bundle=ExecutionResources.from_resource_dict(
                shuffle_task_resources
            ),
        )
        self._map_tasks[cur_task_idx] = task
        self._map_resource_usage = self._map_resource_usage.add(
            task.get_requested_resource_bundle()
        )

        # Update shuffle metrics — use metadata from the first input bundle.
        first_meta = input_bundles[0].metadata[0] if input_bundles else None
        if first_meta is not None:
            self._shuffle_metrics.on_task_submitted(
                cur_task_idx,
                RefBundle(
                    [(block_refs[0], first_meta)], schema=None, owns_blocks=False
                ),
                task_id=task.get_task_id(),
            )

        if self._shuffle_bar is not None:
            _, _, num_rows = estimate_total_num_of_blocks(
                cur_task_idx + 1,
                self.upstream_op_num_outputs(),
                self._shuffle_metrics,
                total_num_tasks=None,
            )
            self._shuffle_bar.update(total=num_rows)

    # -- Output handling (Reduce phase) ---------------------------------------

    def has_next(self) -> bool:
        self._try_reduce()
        return len(self._output_queue) > 0

    def _get_next_inner(self) -> RefBundle:
        bundle: RefBundle = self._output_queue.popleft()

        self._reduce_metrics.on_output_dequeued(bundle)
        self._reduce_metrics.on_output_taken(bundle)
        self._output_blocks_stats.extend(to_stats(bundle.metadata))

        return bundle

    # -- Task tracking --------------------------------------------------------

    def get_active_tasks(self) -> List[OpTask]:
        map_tasks: List[OpTask] = list(self._map_tasks.values())
        reduce_tasks: List[OpTask] = list(self._reduce_tasks.values())
        return map_tasks + reduce_tasks

    # -- Reduce scheduling ----------------------------------------------------

    def _is_map_done(self) -> bool:
        return self._inputs_complete and len(self._map_tasks) == 0

    def _is_ready_for_reduce(self) -> bool:
        return self._is_map_done()

    def _is_all_reduce_submitted(self) -> bool:
        return len(self._pending_reduce_group_ids) == 0

    def _estimate_partition_bytes(self, partition_id: int) -> int:
        """Return the actual in-memory size of a partition's shards."""
        nbytes = self._partition_bytes.get(partition_id, 0)
        return nbytes

    def _log_partition_stats(self):
        """Log partition size distribution before reduce."""
        counts = self._partition_row_counts
        if not counts:
            return
        row_counts = sorted(counts.values())
        total = sum(row_counts)
        n = len(row_counts)
        empty = self._num_partitions - n
        partition_bytes = sorted(self._partition_bytes[pid] for pid in counts)
        total_bytes = sum(partition_bytes)
        logger.info(
            f"Partition stats before reduce: "
            f"partitions={self._num_partitions} (non-empty={n}, empty={empty}), "
            f"total_rows={total:,}, total_bytes={total_bytes / 1e9:.1f}GB, "
            f"rows: min={row_counts[0]:,}, median={row_counts[n // 2]:,}, max={row_counts[-1]:,}, "
            f"partition_size: min={partition_bytes[0] / 1e6:.0f}MB, "
            f"median={partition_bytes[n // 2] / 1e6:.0f}MB, "
            f"max={partition_bytes[-1] / 1e6:.0f}MB, "
            f"groups={self._num_groups}, shard_group_size={self._shard_group_size}, "
            f"refs_per_group: min={min(len(v) for v in self._group_buffers.values()) if self._group_buffers else 0}, "
            f"max={max(len(v) for v in self._group_buffers.values()) if self._group_buffers else 0}"
        )

    def _try_reduce(self):
        """Launch reduce tasks once all map tasks have been submitted."""
        if self._is_all_reduce_submitted():
            return

        if not self._is_ready_for_reduce():
            return

        if self._reduce_start_time is None:
            self._log_partition_stats()
            self._reduce_start_time = time.perf_counter()
            map_elapsed = self._reduce_start_time - self._start_time
            maps_done = self._next_map_task_idx - len(self._map_tasks)
            maps_total = self._next_map_task_idx
            logger.info(
                f"Reduce starting: map_elapsed={map_elapsed:.1f}s, "
                f"maps_completed={maps_done}/{maps_total}, "
                f"maps_still_running={len(self._map_tasks)}, "
                f"reduce_task_num_cpus={self._reduce_task_num_cpus}"
            )

        target_max_block_size = self._data_context.target_max_block_size

        for group_id in list(self._pending_reduce_group_ids):
            group_refs = self._group_buffers.get(group_id, [])

            # Compute which partition IDs belong to this group.
            group_start = group_id * self._shard_group_size
            group_end = min(group_start + self._shard_group_size, self._num_partitions)
            partition_ids = list(range(group_start, group_end))

            # Skip empty groups – no data to reduce.
            if not group_refs:
                self._pending_reduce_group_ids.discard(group_id)
                continue

            def _on_bundle_ready(gid: int, bundle: RefBundle):
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
            ):
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
                            f"Reduce of group {gid} failed: {exc}",
                            exc_info=exc,
                        )

            # Estimate memory for all partitions in this group.
            estimated_bytes = sum(
                self._estimate_partition_bytes(pid) for pid in partition_ids
            )
            reduce_resources = {"num_cpus": self._reduce_task_num_cpus}
            if estimated_bytes > 0:
                reduce_resources["memory"] = estimated_bytes

            block_gen = _shuffle_reduce.options(
                **reduce_resources,
                num_returns="streaming",
                scheduling_strategy="SPREAD",
            ).remote(
                group_refs,
                partition_ids=partition_ids,
                target_max_block_size=target_max_block_size,
                should_sort=self._should_sort,
                key_columns=self._key_columns if self._should_sort else None,
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

            # Release driver-side refs to intermediate group objects now that
            # the reduce task has been submitted.
            self._group_buffers.pop(group_id, None)

            # Update reduce metrics.
            empty_bundle = RefBundle([], schema=None, owns_blocks=False)
            self._reduce_metrics.on_task_submitted(
                group_id, empty_bundle, task_id=data_task.get_task_id()
            )

    # -- Completion -----------------------------------------------------------

    def has_execution_finished(self) -> bool:
        if not self._is_all_reduce_submitted():
            return False
        return super().has_execution_finished()

    def has_completed(self) -> bool:
        return self._is_all_reduce_submitted() and super().has_completed()

    # -- Shutdown -------------------------------------------------------------

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
            f"map_stage={map_elapsed:.1f}s, "
            f"reduce_stage={reduce_elapsed:.1f}s "
            f"(overlap={max(0, map_elapsed + reduce_elapsed - total_elapsed):.1f}s)"
        )
        super()._do_shutdown(force)
        self._map_tasks.clear()
        self._reduce_tasks.clear()
        self._group_buffers.clear()
        self._merge_buffer_refs_by_node.clear()
        self._merge_buffer_bundles_by_node.clear()
        self._merge_buffer_bytes_by_node.clear()

    # -- Stats / metrics ------------------------------------------------------

    def get_stats(self) -> Dict[str, List[BlockStats]]:
        return {
            f"{self._name}_shuffle": self._shuffled_blocks_stats,
            f"{self._name}_reduce": self._output_blocks_stats,
        }

    def _extra_metrics(self) -> Dict[str, Any]:
        return {
            f"{self._name}_shuffle": self._shuffle_metrics.as_dict(),
            f"{self._name}_reduce": self._reduce_metrics.as_dict(),
        }

    # -- Resource accounting --------------------------------------------------

    def current_logical_usage(self) -> ExecutionResources:
        return self._map_resource_usage

    def incremental_resource_usage(self) -> ExecutionResources:
        return ExecutionResources(cpu=self._DEFAULT_MAP_TASK_NUM_CPUS)

    def min_scheduling_resources(self) -> ExecutionResources:
        return self.incremental_resource_usage()

    # -- Progress bar support -------------------------------------------------

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
        return ["Shuffle", "Reduce"]

    def set_sub_progress_bar(self, name: str, pg: "BaseProgressBar"):
        if name == "Shuffle":
            self._shuffle_bar = pg
        elif name == "Reduce":
            self._reduce_bar = pg

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
    block_transformer: Optional[BlockTransformer] = None,
):
    """Map stage: hash-partition one or more blocks and return shards.

    When pre-map merge is enabled, multiple input blocks are passed and
    concatenated before hash-partitioning, reducing the number of map tasks
    and thus the M×N intermediate object count.

    Must be called with ``num_returns=num_partitions + 1``. Returns
    ``num_partitions + 1`` objects:

    - Index 0: ``(BlockMetadata, List[int], Dict)`` — input block metadata,
      list of non-empty partition IDs, and per-partition shard sizes.
    - Index 1..N: the partition shard (``pa.Table``) or ``None`` for empty
      partitions.

    Args:
        *blocks: One or more input blocks to shuffle. Multiple blocks are
            concatenated before partitioning (pre-map merge).
        key_columns: Columns used for hash-partitioning.
        num_partitions: Total number of output partitions (M).
        block_transformer: Optional transform applied before partitioning.

    Returns:
        A tuple of ``(BlockMetadata, List[int], Dict)`` and the partition
        shards (``pa.Table``) or ``None`` for empty partitions.
    """
    import time as _time

    stats = BlockExecStats.builder()

    t0 = _time.perf_counter()

    # Concatenate multiple blocks when pre-map merge is active.
    # Use pa.concat_tables for zero-copy: the result references the input
    # blocks' buffers in shared memory instead of copying ~1 GB to heap.
    if len(blocks) == 1:
        block = blocks[0]
    else:
        block = pa.concat_tables(blocks)

    if block_transformer is not None:
        block = block_transformer(block)

    block = TableBlockAccessor.try_convert_block_type(block, block_type=BlockType.ARROW)

    input_block_metadata = BlockAccessor.for_block(block).get_metadata(
        block_exec_stats=stats.build(block_ser_time_s=0),
    )

    if block.num_rows == 0:
        return (input_block_metadata, [], {}), *([None] * num_partitions)  # noqa: E501

    assert isinstance(block, pa.Table), f"Expected pa.Table, got {type(block)}"

    t1 = _time.perf_counter()

    # Defragment chunked tables before hash-partitioning. Blocks from
    # parquet reads or pre-map merge concatenation can have multiple chunks
    # per column (e.g., 8 chunks from parquet row groups). The downstream
    # table.take() calls (one per partition) are ~8x slower on chunked
    # mmap'd data due to page faults.
    if any(col.num_chunks > 1 for col in block.columns):
        block = block.combine_chunks()

    t2 = _time.perf_counter()

    block_partitions = hash_partition(
        block, hash_cols=key_columns, num_partitions=num_partitions
    )

    t3 = _time.perf_counter()

    shards = [None] * num_partitions
    non_empty_pids = []
    shard_sizes = {}
    for pid_key, shard in block_partitions.items():
        if shard.num_rows > 0:
            shards[pid_key] = shard
            non_empty_pids.append(pid_key)
            shard_sizes[pid_key] = (shard.num_rows, shard.nbytes)

    t4 = _time.perf_counter()

    logger.info(
        f"_shuffle_map profiling: rows={block.num_rows}, "
        f"prep={t1-t0:.3f}s, combine_chunks={t2-t1:.3f}s, "
        f"hash_partition={t3-t2:.3f}s, build_shards={t4-t3:.3f}s, "
        f"total={t4-t0:.3f}s"
    )

    return (input_block_metadata, non_empty_pids, shard_sizes), *shards


@ray.remote
def _shuffle_reduce(
    block_refs: List[ObjectRef],
    target_max_block_size: Optional[int] = None,
    should_sort: bool = False,
    key_columns: Optional[List[str]] = None,
) -> Generator[Union[Block, BlockMetadataWithSchema], None, None]:
    """Reduce stage: concatenate all shards for a single partition.

    This is a streaming generator that yields ``(block, metadata)`` pairs,
    compatible with :class:`DataOpTask`.

    Block refs are resolved incrementally via ``ray.get()`` one at a time so
    that only one input shard needs to be in memory at any moment.  Each shard
    is freed immediately after concatenation, reducing peak object-store
    pressure and avoiding unnecessary spilling.

    Args:
        block_refs: Object references to the partition shards.
        target_max_block_size: If set, output blocks are reshaped to this
            target size.
        should_sort: Whether to sort the output by ``key_columns``.
        key_columns: Columns to sort by (required when ``should_sort=True``).
    """
    start_time_s = time.perf_counter()
    exec_stats_builder = BlockExecStats.builder()

    if not block_refs:
        return

    # Concatenate shards incrementally – resolve one ref at a time to keep
    # peak memory low and allow earlier object freeing.
    builder = ArrowBlockBuilder()
    for ref in block_refs:
        block = ray.get(ref)
        builder.add_block(block)
        del block
    result = builder.build()

    if result.num_rows == 0:
        return

    # Optionally sort.
    if should_sort and key_columns and result.num_rows > 0:
        result = result.sort_by([(k, "ascending") for k in key_columns])

    # Yield output blocks (optionally reshaped to target size).
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
            block_ser_time_s=(gen_stats.object_creation_dur_s if gen_stats else None),
        )

        yield BlockMetadataWithSchema.from_block(
            result,
            block_exec_stats=exec_stats,
            task_exec_stats=TaskExecWorkerStats(
                task_wall_time_s=time.perf_counter() - start_time_s,
            ),
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
    _DEFAULT_COMPACTION_STRATEGY = "none"  # "none" | "pre_map_merge"
    _DEFAULT_PRE_MAP_MERGE_THRESHOLD = 1024 * 1024 * 1024  # 1 GB
    _DEFAULT_PRE_MAP_MERGE_NUM_PARTITIONS_THRESHOLD = 1000

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

        # -- Map task tracking ------------------------------------------------
        self._next_map_task_idx: int = 0
        self._map_tasks: Dict[int, MetadataOpTask] = {}
        self._map_resource_usage = ExecutionResources.zero()

        # -- Per-partition shard buffers (driver bookkeeping) -----------------
        self._partition_row_counts: Dict[int, int] = defaultdict(int)
        self._partition_bytes: Dict[int, int] = defaultdict(int)
        self._partition_buffers: Dict[int, List[ObjectRef]] = defaultdict(list)

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
        self._pre_map_merge_num_partitions_threshold: int = data_context.get_config(
            "actorless_shuffle_pre_map_merge_num_partitions_threshold",
            self._DEFAULT_PRE_MAP_MERGE_NUM_PARTITIONS_THRESHOLD,
        )
        # Auto-disable pre-map merge when num_partitions is too low — the
        # merge overhead outweighs the reduction in intermediate objects.
        # TODO: use input_logical_op.infer_metadata().size_bytes to also
        # factor in estimated data size (M×N) for smarter auto-selection.
        if (
            self._compaction_strategy == "pre_map_merge"
            and self._num_partitions < self._pre_map_merge_num_partitions_threshold
        ):
            logger.info(
                f"Pre-map merge: disabled because num_partitions="
                f"{self._num_partitions} < threshold="
                f"{self._pre_map_merge_num_partitions_threshold}"
            )
            self._compaction_strategy = "none"
        self._merge_buffer_refs: List[ObjectRef] = []
        self._merge_buffer_bytes: int = 0
        self._merge_buffer_bundles: List[RefBundle] = []

        # -- Reduce task tracking ---------------------------------------------
        self._reduce_tasks: Dict[int, DataOpTask] = {}
        self._pending_reduce_partition_ids: Set[int] = set(range(target_num_partitions))

        # -- Output queue -----------------------------------------------------
        self._output_queue: deque = deque()

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
            # Buffer blocks and flush when accumulated size reaches threshold.
            for block_ref, block_metadata in zip(
                input_bundle.block_refs, input_bundle.metadata
            ):
                self._merge_buffer_refs.append(block_ref)
                self._merge_buffer_bytes += block_metadata.size_bytes or 0
            self._merge_buffer_bundles.append(input_bundle)

            if self._merge_buffer_bytes >= self._pre_map_merge_threshold:
                self._flush_merge_buffer()
        else:
            # Default: submit one map task per block immediately.
            for block_ref, block_metadata in zip(
                input_bundle.block_refs, input_bundle.metadata
            ):
                self._submit_map_task([block_ref], [input_bundle])

    def all_inputs_done(self) -> None:
        super().all_inputs_done()
        # Flush any remaining blocks in the pre-map merge buffer.
        if self._compaction_strategy == "pre_map_merge":
            self._flush_merge_buffer()

    def _flush_merge_buffer(self) -> None:
        """Drain the pre-map merge buffer and submit a single merged map task."""
        if not self._merge_buffer_refs:
            return
        block_refs = self._merge_buffer_refs
        bundles = self._merge_buffer_bundles
        estimated_bytes = self._merge_buffer_bytes
        self._merge_buffer_refs = []
        self._merge_buffer_bytes = 0
        self._merge_buffer_bundles = []

        logger.info(
            f"Pre-map merge: flushing {len(block_refs)} blocks "
            f"({estimated_bytes / 1e6:.0f} MB) into one map task"
        )
        self._submit_map_task(block_refs, bundles, estimated_bytes=estimated_bytes)

    def _submit_map_task(
        self,
        block_refs: List[ObjectRef],
        input_bundles: List[RefBundle],
        estimated_bytes: int = 0,
    ) -> None:
        """Submit a map task for one or more input blocks.

        Args:
            block_refs: ObjectRefs of the input blocks.
            input_bundles: The RefBundles that own the input blocks (kept alive
                until the map task completes, then destroyed).
            estimated_bytes: Estimated total input size in bytes. Used to
                request memory for merged map tasks to prevent OOM.
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

        map_refs = _shuffle_map.options(
            **shuffle_task_resources,
            num_returns=self._num_partitions + 1,
            runtime_env=_SHUFFLE_MAP_RUNTIME_ENV,
        ).remote(
            *block_refs,
            key_columns=self._key_columns,
            num_partitions=self._num_partitions,
        )
        metadata_ref = map_refs[0]
        shard_refs = map_refs[1:]

        def _on_map_done(task_idx: int, shard_refs: List[ObjectRef]):
            task = self._map_tasks.pop(task_idx)
            self._map_resource_usage = self._map_resource_usage.subtract(
                task.get_requested_resource_bundle()
            )

            input_block_metadata, non_empty_pids, shard_sizes = ray.get(
                task.get_waitable(), timeout=60
            )

            # Only add refs for non-empty partitions.
            for pid in non_empty_pids:
                self._partition_buffers[pid].append(shard_refs[pid])
                rows, nbytes = shard_sizes.get(pid, (0, 0))
                self._partition_row_counts[pid] += rows
                self._partition_bytes[pid] += nbytes

            # Drop local list so empty-partition refs (with no other
            # references) are reclaimed by Ray's reference counting.
            del shard_refs

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
                _on_map_done, cur_task_idx, shard_refs
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
        return len(self._pending_reduce_partition_ids) == 0

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
            f"refs_per_partition: min={min(len(self._partition_buffers[pid]) for pid in counts)}, "
            f"max={max(len(self._partition_buffers[pid]) for pid in counts)}"
        )

    def _try_reduce(self):
        """Launch reduce tasks once all map tasks have completed."""
        if self._is_all_reduce_submitted():
            return

        if not self._is_ready_for_reduce():
            return

        self._log_partition_stats()

        target_max_block_size = self._data_context.target_max_block_size

        for partition_id in list(self._pending_reduce_partition_ids):
            shard_refs = self._partition_buffers.get(partition_id, [])

            # Skip empty partitions – no data to reduce.
            if not shard_refs:
                self._pending_reduce_partition_ids.discard(partition_id)
                continue

            def _on_bundle_ready(pid: int, bundle: RefBundle):
                self._output_queue.append(bundle)
                self._reduce_metrics.on_output_queued(bundle)
                self._reduce_metrics.on_task_output_generated(
                    task_index=pid, output=bundle
                )
                _, num_outputs, num_rows = estimate_total_num_of_blocks(
                    pid + 1,
                    self.upstream_op_num_outputs(),
                    self._reduce_metrics,
                    total_num_tasks=self._num_partitions,
                )
                self._estimated_num_output_bundles = num_outputs
                self._estimated_output_num_rows = num_rows

                if self._reduce_bar is not None:
                    self._reduce_bar.update(
                        increment=bundle.num_rows() or 0,
                        total=self.num_output_rows_total(),
                    )

            def _on_reduce_done(
                pid: int,
                exc: Optional[Exception],
                task_exec_stats: Optional[TaskExecWorkerStats],
                task_exec_driver_stats: Optional[TaskExecDriverStats],
            ):
                if pid in self._reduce_tasks:
                    self._reduce_tasks.pop(pid)
                    self._reduce_metrics.on_task_finished(
                        task_index=pid,
                        exception=exc,
                        task_exec_stats=task_exec_stats,
                        task_exec_driver_stats=task_exec_driver_stats,
                    )
                    if exc:
                        logger.error(
                            f"Reduce of partition {pid} failed: {exc}",
                            exc_info=exc,
                        )

            estimated_bytes = self._estimate_partition_bytes(partition_id)
            reduce_resources = {"num_cpus": 1}
            if estimated_bytes > 0:
                reduce_resources["memory"] = estimated_bytes

            block_gen = _shuffle_reduce.options(
                **reduce_resources,
                num_returns="streaming",
            ).remote(
                shard_refs,
                target_max_block_size=target_max_block_size,
                should_sort=self._should_sort,
                key_columns=self._key_columns if self._should_sort else None,
            )

            data_task = DataOpTask(
                task_index=partition_id,
                streaming_gen=block_gen,
                output_ready_callback=functools.partial(_on_bundle_ready, partition_id),
                task_done_callback=functools.partial(_on_reduce_done, partition_id),
                task_resource_bundle=ExecutionResources(
                    cpu=1,
                    memory=estimated_bytes,
                    object_store_memory=estimated_bytes,
                ),
                operator_name=self.name,
            )

            self._reduce_tasks[partition_id] = data_task
            self._pending_reduce_partition_ids.discard(partition_id)

            # Release driver-side refs to intermediate shards now that the
            # reduce task has been submitted (it pins them via its args).
            # This lets objects be freed as reduce tasks complete, instead of
            # holding all shards until _do_shutdown.
            self._partition_buffers.pop(partition_id, None)

            # Update reduce metrics.
            empty_bundle = RefBundle([], schema=None, owns_blocks=False)
            self._reduce_metrics.on_task_submitted(
                partition_id, empty_bundle, task_id=data_task.get_task_id()
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
        super()._do_shutdown(force)
        self._map_tasks.clear()
        self._reduce_tasks.clear()
        self._partition_buffers.clear()
        self._merge_buffer_refs.clear()
        self._merge_buffer_bundles.clear()
        self._merge_buffer_bytes = 0

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
        reduces_submitted = self._num_partitions - len(
            self._pending_reduce_partition_ids
        )
        reduces_done = reduces_submitted - len(self._reduce_tasks)
        parts = [f"map: {maps_done}/{self._next_map_task_idx}"]
        if self._merge_buffer_refs:
            parts.append(f"merge_buf: {len(self._merge_buffer_refs)}")
        parts.append(f"reduce: {reduces_done}/{self._num_partitions}")
        return ", ".join(parts)

    def get_sub_progress_bar_names(self) -> Optional[List[str]]:
        return ["Shuffle", "Reduce"]

    def set_sub_progress_bar(self, name: str, pg: "BaseProgressBar"):
        if name == "Shuffle":
            self._shuffle_bar = pg
        elif name == "Reduce":
            self._reduce_bar = pg

"""Actorless Hash Shuffle Operator.

This module implements a hash shuffle using only stateless Ray tasks (no actors).
The shuffle has up to four stages:

1. **Map & Scatter**: Each mapper task hash-partitions an input block into M
   partitions and returns each shard as a separate task output.
   The driver receives one ``ObjectRef`` per partition.

2. **Driver Buffering**: The driver collects ObjectRefs into per-partition buffer
   lists. This is pure bookkeeping with no data movement.

3. **Compact**: When a partition's uncompacted buffer reaches a
   threshold, a stateless compactor task merges the small shards into one larger
   block, reducing object-store pressure. Runs concurrently with map tasks.

4. **Reduce**: After all map and compact tasks complete, the driver launches M
   reducer tasks. Each reducer dereferences all shard refs for its partition,
   concatenates them, and yields output blocks via a streaming generator.
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


@ray.remote
def _shuffle_map(
    block: Block,
    key_columns: List[str],
    num_partitions: int,
    block_transformer: Optional[BlockTransformer] = None,
):
    """Map stage: hash-partition a block and return shards as task outputs.

    Must be called with ``num_returns=num_partitions + 1``. Returns
    ``num_partitions + 1`` objects:

    - Index 0: ``(BlockMetadata, List[int])`` — input block metadata and the
      list of non-empty partition IDs.
    - Index 1..N: the partition shard (``pa.Table``) or ``None`` for empty
      partitions.

    Args:
        block: Input block to shuffle.
        key_columns: Columns used for hash-partitioning.
        num_partitions: Total number of output partitions (M).
        block_transformer: Optional transform applied before partitioning.

    Returns:
        A tuple of ``(BlockMetadata, List[int])`` and the partition shards (``pa.Table``) or ``None`` for empty partitions.
    """
    stats = BlockExecStats.builder()

    if block_transformer is not None:
        block = block_transformer(block)

    block = TableBlockAccessor.try_convert_block_type(block, block_type=BlockType.ARROW)

    input_block_metadata = BlockAccessor.for_block(block).get_metadata(
        block_exec_stats=stats.build(block_ser_time_s=0),
    )

    if block.num_rows == 0:
        return (input_block_metadata, []), *([None] * num_partitions)

    assert isinstance(block, pa.Table), f"Expected pa.Table, got {type(block)}"

    block_partitions = hash_partition(
        block, hash_cols=key_columns, num_partitions=num_partitions
    )

    shards = [None] * num_partitions
    non_empty_pids = []
    for pid, shard in block_partitions.items():
        if shard.num_rows > 0:
            shards[pid] = shard
            non_empty_pids.append(pid)

    return (input_block_metadata, non_empty_pids), *shards


@ray.remote
def _shuffle_compact(
    *blocks: Block,
) -> Block:
    """Compact multiple small shards into a single larger block.

    Args:
        *blocks: Partition shards to concatenate.

    Returns:
        The concatenated block.
    """
    builder = ArrowBlockBuilder()
    for block in blocks:
        builder.add_block(block)
    return builder.build()


@ray.remote
def _shuffle_reduce(
    *blocks: Block,
    target_max_block_size: Optional[int] = None,
    should_sort: bool = False,
    key_columns: Optional[List[str]] = None,
) -> Generator[Union[Block, BlockMetadataWithSchema], None, None]:
    """Reduce stage: concatenate all shards for a single partition.

    This is a streaming generator that yields ``(block, metadata)`` pairs,
    compatible with :class:`DataOpTask`.

    Args:
        *blocks: Partition shards.
        target_max_block_size: If set, output blocks are reshaped to this
            target size.
        should_sort: Whether to sort the output by ``key_columns``.
        key_columns: Columns to sort by (required when ``should_sort=True``).
    """
    start_time_s = time.perf_counter()
    exec_stats_builder = BlockExecStats.builder()

    if not blocks:
        return

    # Concatenate into a single block.
    builder = ArrowBlockBuilder()
    for block in blocks:
        builder.add_block(block)
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
            exec_stats = exec_stats_builder.build()
            exec_stats_builder = BlockExecStats.builder()

            gen_stats: StreamingGeneratorStats = yield out_block
            if gen_stats:
                exec_stats.block_ser_time_s = gen_stats.object_creation_dur_s

            yield BlockMetadataWithSchema.from_block(
                out_block,
                block_exec_stats=exec_stats,
                task_exec_stats=TaskExecWorkerStats(
                    task_wall_time_s=time.perf_counter() - start_time_s,
                ),
            )
    else:
        exec_stats = exec_stats_builder.build()

        gen_stats: StreamingGeneratorStats = yield result
        if gen_stats:
            exec_stats.block_ser_time_s = gen_stats.object_creation_dur_s

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
       bookkeeping).  When a partition's buffer reaches the compaction threshold,
       a compactor task merges the small shards into one larger block.
    3. Once all map and compact tasks are complete, reduce tasks are launched.
       Each reduce task fetches all shards for one partition, concatenates them,
       and yields output blocks.
    """

    _DEFAULT_MAP_TASK_NUM_CPUS = 1.0
    _DEFAULT_COMPACTION_THRESHOLD = 10

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
        self._partition_buffers: Dict[int, List[ObjectRef]] = defaultdict(list)
        self._compacted_partition_buffers: Dict[int, List[ObjectRef]] = defaultdict(
            list
        )

        # -- Compaction tracking -----------------------------------------------
        self._compaction_threshold: int = data_context.get_config(
            "actorless_shuffle_compaction_threshold",
            self._DEFAULT_COMPACTION_THRESHOLD,
        )
        self._next_compact_task_idx: int = 0
        self._compact_tasks: Dict[int, MetadataOpTask] = {}
        self._compact_resource_usage = ExecutionResources.zero()

        # -- Reduce task tracking ---------------------------------------------
        self._reduce_tasks: Dict[int, DataOpTask] = {}
        self._pending_reduce_partition_ids: Set[int] = set(range(target_num_partitions))

        # -- Output queue -----------------------------------------------------
        self._output_queue: deque = deque()

        # -- Stats ------------------------------------------------------------
        self._shuffled_blocks_stats: List[BlockStats] = []
        self._output_blocks_stats: List[BlockStats] = []

        # -- Sub-progress bars ------------------------------------------------
        self._shuffle_bar: Optional["BaseProgressBar"] = None
        self._shuffle_metrics = OpRuntimeMetrics(self)
        self._reduce_bar: Optional["BaseProgressBar"] = None
        self._reduce_metrics = OpRuntimeMetrics(self)

    # -- Input handling (Map phase) -------------------------------------------

    def _add_input_inner(self, input_bundle: RefBundle, input_index: int) -> None:
        assert input_index == 0

        self._shuffle_metrics.on_input_received(input_bundle)

        for block_ref, block_metadata in zip(
            input_bundle.block_refs, input_bundle.metadata
        ):
            cur_task_idx = self._next_map_task_idx
            self._next_map_task_idx += 1

            shuffle_task_resources = {
                "num_cpus": self._DEFAULT_MAP_TASK_NUM_CPUS,
            }

            map_refs = _shuffle_map.options(
                **shuffle_task_resources,
                num_returns=self._num_partitions + 1,
            ).remote(
                block_ref,
                self._key_columns,
                self._num_partitions,
            )
            metadata_ref = map_refs[0]
            shard_refs = map_refs[1:]

            def _on_map_done(task_idx: int, shard_refs: List[ObjectRef]):
                task = self._map_tasks.pop(task_idx)
                self._map_resource_usage = self._map_resource_usage.subtract(
                    task.get_requested_resource_bundle()
                )

                input_block_metadata, non_empty_pids = ray.get(
                    task.get_waitable(), timeout=60
                )

                # Only add refs for non-empty partitions.
                for pid in non_empty_pids:
                    self._partition_buffers[pid].append(shard_refs[pid])
                    self._maybe_compact_partition(pid)

                # Drop local list so empty-partition refs (with no other
                # references) are reclaimed by Ray's reference counting.
                del shard_refs

                self._shuffled_blocks_stats.append(input_block_metadata.to_stats())

                # Update shuffle metrics.
                self._shuffle_metrics.on_output_taken(input_bundle)
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
                    self._shuffle_bar.update(
                        increment=input_block_metadata.num_rows or 0
                    )

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

            # Update shuffle metrics.
            self._shuffle_metrics.on_task_submitted(
                cur_task_idx,
                RefBundle(
                    [(block_ref, block_metadata)], schema=None, owns_blocks=False
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

    # -- Compaction -----------------------------------------------------------

    def _maybe_compact_partition(self, partition_id: int) -> None:
        """Launch a compaction task if the partition buffer exceeds the threshold."""
        if self._compaction_threshold <= 0:
            return
        if len(self._partition_buffers[partition_id]) < self._compaction_threshold:
            return
        # Drain uncompacted refs and launch a compact task.
        shard_refs = self._partition_buffers[partition_id]
        self._partition_buffers[partition_id] = []

        compact_task_idx = self._next_compact_task_idx
        self._next_compact_task_idx += 1

        compact_resources = {"num_cpus": 1}

        compact_ref = _shuffle_compact.options(
            **compact_resources,
            num_returns=1,
        ).remote(*shard_refs)

        self._compacted_partition_buffers[partition_id].append(compact_ref)

        def _on_compact_done(task_idx: int):
            done_task = self._compact_tasks.pop(task_idx)
            self._compact_resource_usage = self._compact_resource_usage.subtract(
                done_task.get_requested_resource_bundle()
            )

        task = MetadataOpTask(
            task_index=compact_task_idx,
            object_ref=compact_ref,
            task_done_callback=functools.partial(_on_compact_done, compact_task_idx),
            task_resource_bundle=ExecutionResources.from_resource_dict(
                compact_resources
            ),
        )
        self._compact_tasks[compact_task_idx] = task
        self._compact_resource_usage = self._compact_resource_usage.add(
            task.get_requested_resource_bundle()
        )

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
        compact_tasks: List[OpTask] = list(self._compact_tasks.values())
        reduce_tasks: List[OpTask] = list(self._reduce_tasks.values())
        return map_tasks + compact_tasks + reduce_tasks

    # -- Reduce scheduling ----------------------------------------------------

    def _is_map_done(self) -> bool:
        return self._inputs_complete and len(self._map_tasks) == 0

    def _is_ready_for_reduce(self) -> bool:
        return self._is_map_done() and len(self._compact_tasks) == 0

    def _is_all_reduce_submitted(self) -> bool:
        return len(self._pending_reduce_partition_ids) == 0

    def _try_reduce(self):
        """Launch reduce tasks once all map and compact tasks have completed."""
        if self._is_all_reduce_submitted():
            return

        if not self._is_ready_for_reduce():
            return

        target_max_block_size = self._data_context.target_max_block_size

        for partition_id in list(self._pending_reduce_partition_ids):
            # Compacted (larger) refs first, then any remaining uncompacted tail.
            shard_refs = self._compacted_partition_buffers.get(
                partition_id, []
            ) + self._partition_buffers.get(partition_id, [])

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

            block_gen = _shuffle_reduce.options(
                num_cpus=1,
                num_returns="streaming",
            ).remote(
                *shard_refs,
                target_max_block_size=target_max_block_size,
                should_sort=self._should_sort,
                key_columns=self._key_columns if self._should_sort else None,
            )

            data_task = DataOpTask(
                task_index=partition_id,
                streaming_gen=block_gen,
                output_ready_callback=functools.partial(_on_bundle_ready, partition_id),
                task_done_callback=functools.partial(_on_reduce_done, partition_id),
                task_resource_bundle=ExecutionResources(cpu=1),
                operator_name=self.name,
            )

            self._reduce_tasks[partition_id] = data_task
            self._pending_reduce_partition_ids.discard(partition_id)

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
        self._compact_tasks.clear()
        self._reduce_tasks.clear()
        self._partition_buffers.clear()
        self._compacted_partition_buffers.clear()

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
        return self._map_resource_usage.add(self._compact_resource_usage)

    def incremental_resource_usage(self) -> ExecutionResources:
        return ExecutionResources(cpu=self._DEFAULT_MAP_TASK_NUM_CPUS)

    def min_scheduling_resources(self) -> ExecutionResources:
        return self.incremental_resource_usage()

    # -- Progress bar support -------------------------------------------------

    def progress_str(self) -> str:
        maps_done = self._next_map_task_idx - len(self._map_tasks)
        compacts_done = self._next_compact_task_idx - len(self._compact_tasks)
        reduces_submitted = self._num_partitions - len(
            self._pending_reduce_partition_ids
        )
        reduces_done = reduces_submitted - len(self._reduce_tasks)
        parts = [f"map: {maps_done}/{self._next_map_task_idx}"]
        if self._next_compact_task_idx > 0:
            parts.append(f"compact: {compacts_done}/{self._next_compact_task_idx}")
        parts.append(f"reduce: {reduces_done}/{self._num_partitions}")
        return ", ".join(parts)

    def get_sub_progress_bar_names(self) -> Optional[List[str]]:
        return ["Shuffle", "Reduce"]

    def set_sub_progress_bar(self, name: str, pg: "BaseProgressBar"):
        if name == "Shuffle":
            self._shuffle_bar = pg
        elif name == "Reduce":
            self._reduce_bar = pg

import dataclasses
import functools
import logging
import typing
from collections import defaultdict
from typing import Any, Dict, List, Optional

import ray
from ray import ObjectRef
from ray.data._internal.execution.bundle_queue import (
    BaseBundleQueue,
    FIFOBundleQueue,
)
from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    MetadataOpTask,
    OpTask,
    estimate_total_num_of_blocks,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
)
from ray.data._internal.execution.operators.shuffle_operators._shuffle_tasks import (
    PartitionFn,
    _shuffle_map_task,
)
from ray.data._internal.execution.operators.sub_progress import SubProgressBarMixin
from ray.data.block import BlockMetadata, BlockStats
from ray.data.context import DataContext
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if typing.TYPE_CHECKING:
    from ray.data._internal.progress.base_progress import BaseProgressBar

logger = logging.getLogger(__name__)


# Sentinel prefix used to carry partition_id on a block's
# BlockMetadata.input_files.  The downstream ShuffleReduceOp parses this
# out of each input bundle to recover which partition it represents.
_PARTITION_ID_SENTINEL = "__partition__"


def make_partition_sentinel(partition_id: int) -> List[str]:
    """Build the `BlockMetadata.input_files` list used to mark a block
    with its partition_id.  Kept as a module-level helper so the reduce op
    can use the same encoding without depending on map-op internals."""
    return [f"{_PARTITION_ID_SENTINEL}{partition_id}"]


def extract_partition_id(bundle: RefBundle) -> int:
    """Recover the partition_id stamped onto an upstream bundle by
    `make_partition_sentinel`.  Raises if no sentinel is found."""
    for _, meta in bundle.blocks:
        files = meta.input_files
        if not files:
            continue
        for f in files:
            if f.startswith(_PARTITION_ID_SENTINEL):
                return int(f[len(_PARTITION_ID_SENTINEL) :])
    raise ValueError(
        "ShuffleMapOp bundle is missing a partition_id sentinel in "
        "BlockMetadata.input_files; this should never happen in the planner-"
        "wired ShuffleMapOp → ShuffleReduceOp pipeline."
    )


class ShuffleMapOp(InternalQueueOperatorMixin, PhysicalOperator, SubProgressBarMixin):
    """Map phase of a shuffle.

    Each map task partitions its inputs into `num_partitions` shards.  As
    map tasks complete, each shard is pushed into a per-partition staging
    `FIFOBundleQueue`.  When upstream signals `all_inputs_done`
    and no map tasks remain, the operator drains each staging queue, merges
    its shards into a single `RefBundle` (one block per mapper), and
    pushes those N bundles into `_output_queue`.  Empty partitions are
    skipped — the reducer never sees them.

    The staging queues are exposed via `InternalQueueOperatorMixin`'s
    `_output_queues` so the pre-barrier shards show up in the
    `Queued blocks (X)` column of the progress bar — without that, the
    bytes vanish from the user-visible queue summary between
    `_handle_map_done` and `_maybe_emit_partition_bundles`.

    This 1-bundle-per-partition output contract lets the downstream
    `ShuffleReduceOp` behave as a regular MapOperator-style consumer
    (1 input bundle → 1 reducer task) and benefit from the framework's
    standard backpressure / per-op resource budgeting.

    Args:
        input_op: Upstream physical operator.
        data_context: Runtime configuration.
        num_partitions: Total number of output partitions.  Each map task
            returns `num_partitions + 1` objects: the metadata bundle
            plus one ZSTD-compressed Arrow IPC stream per partition (or
            `None` for partitions that received no rows from this
            mapper).
        partition_fn: Function mapping a pa.Table to Dict[int, pa.Table].
        pre_map_merge_threshold: Byte threshold per node at which buffered
            blocks are merged into a single map task.  Set to 0 to disable.
        map_runtime_env: Optional runtime_env for map tasks; useful to
            isolate map workers from other ops.
        map_cpus: CPU request per map task.
        name: Display name shown in progress bars and logs.
    """

    _DEFAULT_SHUFFLE_MAP_TASK_NUM_CPUS = 1.0
    _DEFAULT_PRE_MAP_MERGE_THRESHOLD = 1024 * 1024 * 1024  # 1 GB

    # Map outputs are pipeline-internal shuffle shards: they will be
    # consumed by the immediately-downstream ShuffleReduceOp and spill
    # cleanly to disk if plasma fills up.  Excluding them from the
    # framework's plasma budget prevents downstream ops (Reduce, Write)
    # from being starved by transient shuffle intermediates — matching
    # the actor-pool shuffle's behavior where shards live in process
    # memory and never appear in plasma accounting.
    exclude_from_plasma_accounting: bool = True

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        *,
        num_partitions: int,
        partition_fn: PartitionFn,
        pre_map_merge_threshold: int = _DEFAULT_PRE_MAP_MERGE_THRESHOLD,
        map_runtime_env: Optional[Dict[str, Any]] = None,
        map_cpus: float = _DEFAULT_SHUFFLE_MAP_TASK_NUM_CPUS,
        name: str = "ShuffleMap",
    ):
        super().__init__(
            name=name,
            input_dependencies=[input_op],
            data_context=data_context,
        )

        self._num_partitions: int = num_partitions
        self._partition_fn: PartitionFn = partition_fn

        # -- Map task config -------------------------------------------------
        self._shuffle_map_task_num_cpus: float = map_cpus
        self._map_runtime_env: Optional[Dict[str, Any]] = map_runtime_env

        # -- Pre-map merge ---------------------------------------------------
        self._pre_map_merge_threshold: int = pre_map_merge_threshold
        self._merge_buffer_refs_by_node: Dict[str, List[ObjectRef]] = defaultdict(list)
        self._merge_buffer_bytes_by_node: Dict[str, int] = defaultdict(int)
        self._merge_buffer_bundles_by_node: Dict[str, List[RefBundle]] = defaultdict(
            list
        )

        # -- Map task tracking -----------------------------------------------
        self._next_shuffle_map_task_idx: int = 0
        self._shuffle_map_tasks: Dict[int, MetadataOpTask] = {}
        self._map_resource_usage = ExecutionResources.zero()

        # -- Per-partition staging queues ------------------------------------
        # Each completed map task pushes one single-block bundle per
        # non-empty partition into the corresponding queue.  At barrier
        # release we drain → merge → push to `_output_queue`.  Exposed
        # via `_output_queues` for the `Queued blocks (X)` progress
        # column.
        self._partition_staging: Dict[int, FIFOBundleQueue] = {
            pid: FIFOBundleQueue() for pid in range(num_partitions)
        }

        # -- Per-partition byte totals (driver bookkeeping) ------------------
        # Mirrors what's currently in `_partition_staging` but lives
        # past the barrier drain so downstream ShuffleReduceOp can size
        # its reducer memory hint per-partition.
        self._partition_bytes: Dict[int, int] = defaultdict(int)

        # -- Output queue (post-barrier merged partition-bundles) ------------
        self._output_queue: FIFOBundleQueue = FIFOBundleQueue()
        self._partition_bundles_emitted: bool = False

        # -- Stats -----------------------------------------------------------
        self._total_input_rows: int = 0
        self._total_input_bytes: int = 0
        self._map_blocks_stats: List[BlockStats] = []

        # -- Sub-progress bars -----------------------------------------------
        self._map_bar: Optional["BaseProgressBar"] = None

    # -----------------------------------------------------------------------
    # InternalQueueOperatorMixin
    # -----------------------------------------------------------------------

    @property
    def _input_queues(self) -> List[BaseBundleQueue]:
        # The pre-map merge buffer holds raw ObjectRef + sizes (not
        # RefBundles) so it can't easily be exposed as a BaseBundleQueue.
        # In practice it only holds up to ~1 GB per node before flushing
        # into a map task, which is small relative to the total shuffle
        # working set, so leaving it unaccounted here is an acceptable
        # trade-off.  The framework picks up the upstream bundles via the
        # standard input queue path.
        return []

    @property
    def _output_queues(self) -> List[BaseBundleQueue]:
        # Pre-barrier staging + post-barrier final emit queue.  Surfaced
        # to `OpState.total_enqueued_output_blocks_bytes` so the
        # `Queued blocks (X)` progress column reflects what we've
        # produced but not yet handed downstream.
        return list(self._partition_staging.values()) + [self._output_queue]

    # -----------------------------------------------------------------------
    # Input handling
    # -----------------------------------------------------------------------

    def _add_input_inner(self, input_bundle: RefBundle, input_index: int) -> None:
        assert input_index == 0

        if not input_bundle.block_refs:
            input_bundle.destroy_if_owned()
            return

        if self._pre_map_merge_threshold > 0:
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
            self._submit_shuffle_map_task(
                list(input_bundle.block_refs),
                [input_bundle],
                estimated_bytes=sum((m.size_bytes or 0) for m in input_bundle.metadata),
            )

    def all_inputs_done(self) -> None:
        super().all_inputs_done()
        for node_id in list(self._merge_buffer_refs_by_node.keys()):
            self._flush_merge_buffer(node_id)
        # All input done; if all map tasks happened to finish before this
        # fired (e.g. very small inputs), the barrier hasn't been released
        # yet — emit now.
        self._maybe_emit_partition_bundles()

    def _flush_merge_buffer(self, node_id: str) -> None:
        block_refs = self._merge_buffer_refs_by_node.pop(node_id, [])
        bundles = self._merge_buffer_bundles_by_node.pop(node_id, [])
        estimated_bytes = self._merge_buffer_bytes_by_node.pop(node_id, 0)
        if not block_refs:
            for bundle in bundles:
                bundle.destroy_if_owned()
            return
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

        resources: Dict[str, Any] = {"num_cpus": self._shuffle_map_task_num_cpus}
        if estimated_bytes > 0:
            resources["memory"] = estimated_bytes * 2

        ray_options: Dict[str, Any] = {
            **resources,
            "num_returns": self._num_partitions + 1,
        }
        if target_node_id is not None:
            ray_options["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                target_node_id, soft=True
            )
        if self._map_runtime_env is not None:
            ray_options["runtime_env"] = self._map_runtime_env

        map_refs = _shuffle_map_task.options(**ray_options).remote(
            *block_refs,
            partition_fn=self._partition_fn,
            num_partitions=self._num_partitions,
        )
        metadata_ref = map_refs[0]
        partition_refs = list(map_refs[1:])

        task = MetadataOpTask(
            task_index=cur_task_idx,
            object_ref=metadata_ref,
            task_done_callback=functools.partial(
                self._handle_map_done, cur_task_idx, partition_refs, input_bundles
            ),
            task_resource_bundle=ExecutionResources.from_resource_dict(resources),
        )
        self._shuffle_map_tasks[cur_task_idx] = task
        self._map_resource_usage = self._map_resource_usage.add(
            task.get_requested_resource_bundle()
        )

        # `block_refs` is non-empty (callers guarantee it; empty input
        # bundles short-circuit in `_add_input_inner`, and
        # `_flush_merge_buffer` returns early for empty buffers), so
        # `input_bundles` here also has at least one block.
        all_blocks_meta = [
            (ref, meta)
            for bundle in input_bundles
            for ref, meta in zip(bundle.block_refs, bundle.metadata)
        ]
        self._metrics.on_task_submitted(
            cur_task_idx,
            RefBundle(all_blocks_meta, schema=None, owns_blocks=False),
            task_id=task.get_task_id(),
        )

        if self._map_bar is not None:
            _, _, num_rows = estimate_total_num_of_blocks(
                cur_task_idx + 1,
                self.upstream_op_num_outputs(),
                self._metrics,
                total_num_tasks=None,
            )
            self._map_bar.update(total=num_rows)

    def _handle_map_done(
        self,
        task_idx: int,
        partition_refs: List[ObjectRef],
        input_bundles: List[RefBundle],
    ) -> None:
        task = self._shuffle_map_tasks.pop(task_idx)
        self._map_resource_usage = self._map_resource_usage.subtract(
            task.get_requested_resource_bundle()
        )

        # `task_done_callback` fires only after the metadata ref is ready,
        # so this is just local deserialization.
        input_meta, shard_sizes = ray.get(task.get_waitable())

        # Push each non-empty shard into its partition's staging queue.
        # Mappers that produced no rows for a given partition are absent
        # from `shard_sizes` and their ref in `partition_refs` is None.
        for pid, (rows, nbytes) in shard_sizes.items():
            ref = partition_refs[pid]
            if ref is None:
                continue
            shard_meta = BlockMetadata(
                num_rows=rows,
                size_bytes=nbytes,
                exec_stats=None,
                input_files=None,
            )
            shard_bundle = RefBundle([(ref, shard_meta)], schema=None, owns_blocks=True)
            self._partition_staging[pid].add(shard_bundle)
            self._partition_bytes[pid] += nbytes

        for bundle in input_bundles:
            bundle.destroy_if_owned()

        self._total_input_rows += input_meta.num_rows or 0
        self._total_input_bytes += input_meta.size_bytes or 0
        self._map_blocks_stats.append(input_meta.to_stats())

        self._metrics.on_task_finished(
            task_idx,
            None,
            task_exec_stats=None,
            task_exec_driver_stats=None,
        )

        if self._map_bar is not None:
            self._map_bar.update(increment=input_meta.num_rows or 0)

        # Barrier: emit partition-bundles once all map tasks are done AND
        # upstream has signalled no more inputs are coming.
        self._maybe_emit_partition_bundles()

    def _maybe_emit_partition_bundles(self) -> None:
        """Drain each non-empty staging queue into one output bundle.

        Fires exactly once, when (a) no map tasks are pending, (b) no
        merge buffers are waiting to be flushed, and (c) upstream has
        signalled `all_inputs_done`.  Empty partitions are skipped —
        the reducer never sees them, so it never schedules a no-op task.
        """
        if self._partition_bundles_emitted:
            return
        if self._shuffle_map_tasks or self._merge_buffer_refs_by_node:
            return
        if not self._inputs_complete:
            return

        self._partition_bundles_emitted = True

        for pid in range(self._num_partitions):
            staging = self._partition_staging[pid]
            if not staging.has_next():
                continue
            shards: List[RefBundle] = []
            while staging.has_next():
                shards.append(staging.get_next())

            merged = RefBundle.merge_ref_bundles(shards)
            # Stamp the partition_id sentinel onto the merged bundle's
            # first block so the downstream reducer can recover the
            # partition this bundle represents.
            stamped_blocks = []
            for i, (ref, meta) in enumerate(merged.blocks):
                if i == 0:
                    meta = dataclasses.replace(
                        meta, input_files=make_partition_sentinel(pid)
                    )
                stamped_blocks.append((ref, meta))
            stamped = RefBundle(
                stamped_blocks,
                schema=merged.schema,
                owns_blocks=merged.owns_blocks,
            )
            self._output_queue.add(stamped)
            self._metrics.on_output_queued(stamped)

    # -----------------------------------------------------------------------
    # Output handling
    # -----------------------------------------------------------------------

    def has_next(self) -> bool:
        return self._output_queue.has_next()

    def _get_next_inner(self) -> RefBundle:
        bundle: RefBundle = self._output_queue.get_next()
        self._metrics.on_output_dequeued(bundle)
        return bundle

    # -----------------------------------------------------------------------
    # Public interface for downstream ShuffleReduceOp
    # -----------------------------------------------------------------------

    def get_partition_bytes(self) -> Dict[int, int]:
        """Per-partition uncompressed byte count.  Used by ShuffleReduceOp
        to size each reducer's memory hint."""
        return dict(self._partition_bytes)

    # -----------------------------------------------------------------------
    # Task tracking
    # -----------------------------------------------------------------------

    def get_active_tasks(self) -> List[OpTask]:
        return list(self._shuffle_map_tasks.values())

    def has_execution_finished(self) -> bool:
        if (
            self._shuffle_map_tasks
            or self._merge_buffer_refs_by_node
            or not self._partition_bundles_emitted
            or self._output_queue.has_next()
        ):
            return False
        return super().has_execution_finished()

    def has_completed(self) -> bool:
        return (
            not self._shuffle_map_tasks
            and not self._merge_buffer_refs_by_node
            and self._partition_bundles_emitted
            and not self._output_queue.has_next()
            and super().has_completed()
        )

    # -----------------------------------------------------------------------
    # Shutdown
    # -----------------------------------------------------------------------

    def _do_shutdown(self, force: bool = False) -> None:
        super()._do_shutdown(force)
        self._shuffle_map_tasks.clear()
        self._merge_buffer_refs_by_node.clear()
        self._merge_buffer_bundles_by_node.clear()
        self._merge_buffer_bytes_by_node.clear()
        for queue in self._partition_staging.values():
            queue.clear()
        self._output_queue.clear()

    # -----------------------------------------------------------------------
    # Stats / metrics
    # -----------------------------------------------------------------------

    def get_stats(self) -> Dict[str, List[BlockStats]]:
        return {self._name: self._map_blocks_stats}

    # -----------------------------------------------------------------------
    # Resource accounting
    # -----------------------------------------------------------------------

    def num_output_rows_total(self) -> Optional[int]:
        # Shuffle preserves row count: total output rows = total input rows.
        # Returns None until at least one map task has finished so we have
        # a meaningful count to report.
        return self._total_input_rows if self._total_input_rows > 0 else None

    def current_logical_usage(self) -> ExecutionResources:
        return ExecutionResources(
            cpu=self._map_resource_usage.cpu,
            memory=self._map_resource_usage.memory,
        )

    def incremental_resource_usage(self) -> ExecutionResources:
        # Memory ask should match what `_submit_shuffle_map_task` declares
        # to Ray Core (`estimated_bytes * 2`).  We don't know the next
        # input's size up front, so use the running average across
        # already-submitted tasks; 0 until the first task is submitted.
        avg_input = self._metrics.average_bytes_inputs_per_task
        memory = int(avg_input * 2) if avg_input else 0
        return ExecutionResources(
            cpu=self._shuffle_map_task_num_cpus,
            memory=memory,
        )

    def min_scheduling_resources(self) -> ExecutionResources:
        return self.incremental_resource_usage()

    # -----------------------------------------------------------------------
    # Progress bar support
    # -----------------------------------------------------------------------

    def progress_str(self) -> str:
        maps_done = self._next_shuffle_map_task_idx - len(self._shuffle_map_tasks)
        parts = [f"map: {maps_done}/{self._next_shuffle_map_task_idx}"]
        total_merge_buf = sum(
            len(refs) for refs in self._merge_buffer_refs_by_node.values()
        )
        if total_merge_buf:
            parts.append(f"merge_buf: {total_merge_buf}")
        return ", ".join(parts)

    def get_sub_progress_bar_names(self) -> Optional[List[str]]:
        return ["Map"]

    def set_sub_progress_bar(self, name: str, pg: "BaseProgressBar") -> None:
        if name == "Map":
            self._map_bar = pg

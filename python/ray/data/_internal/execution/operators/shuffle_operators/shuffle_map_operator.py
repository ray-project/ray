import functools
import logging
import typing
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional

import ray
from ray import ObjectRef
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
from ray.data._internal.execution.operators.shuffle_operators._shuffle_tasks import (
    PartitionFn,
    _shuffle_map_task,
)
from ray.data._internal.execution.operators.sub_progress import SubProgressBarMixin
from ray.data._internal.stats import OpRuntimeMetrics
from ray.data.block import BlockMetadata, BlockStats
from ray.data.context import DataContext
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if typing.TYPE_CHECKING:
    from ray.data._internal.progress.base_progress import BaseProgressBar

logger = logging.getLogger(__name__)


class ShuffleMapOp(PhysicalOperator, SubProgressBarMixin):
    """Map phase of a shuffle.

    Args:
        input_op: Upstream physical operator.
        data_context: Runtime configuration.
        num_partitions: Total number of output partitions (full shuffle width).
        shard_group_size: Number of partitions packed into each return object
            from a map task.  Computed by the planner via
            :func:`_shuffle_tasks.compute_shard_group_size` to cap M × N.
        num_groups: Number of groups each map task returns
            (ceil(num_partitions / shard_group_size)).
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

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        *,
        num_partitions: int,
        shard_group_size: int,
        num_groups: int,
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
        self._shard_group_size: int = shard_group_size
        self._num_groups: int = num_groups
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

        # -- Per-partition stats (driver bookkeeping) ------------------------
        # Exposed to downstream ShuffleReduceOp via get_partition_bytes() so
        # it can size reducer memory hints.
        self._partition_row_counts: Dict[int, int] = defaultdict(int)
        self._partition_bytes: Dict[int, int] = defaultdict(int)

        # -- Output queue (one bundle per completed map task, containing G
        #    group refs as blocks ordered by group_idx) -----------------------
        self._output_queue: deque = deque()

        # -- Stats -----------------------------------------------------------
        self._total_input_rows: int = 0
        self._total_input_bytes: int = 0
        self._map_blocks_stats: List[BlockStats] = []

        # -- Sub-progress bars -----------------------------------------------
        self._map_bar: Optional["BaseProgressBar"] = None
        self._map_metrics = OpRuntimeMetrics(self)

    # -----------------------------------------------------------------------
    # Input handling
    # -----------------------------------------------------------------------

    def can_add_input(self) -> bool:
        return True

    def _add_input_inner(self, input_bundle: RefBundle, input_index: int) -> None:
        assert input_index == 0
        self._map_metrics.on_input_received(input_bundle)

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

        # Split into two dicts: resources is the numeric resource ask used
        # for both Ray Core scheduling and our own ExecutionResources
        # accounting; ray_options is the full @ray.remote.options(...)
        # bag which adds scheduling_strategy / runtime_env / num_returns.
        resources: Dict[str, Any] = {"num_cpus": self._shuffle_map_task_num_cpus}
        if estimated_bytes > 0:
            resources["memory"] = estimated_bytes * 2

        ray_options: Dict[str, Any] = {
            **resources,
            "num_returns": self._num_groups + 1,
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
            task_resource_bundle=ExecutionResources.from_resource_dict(resources),
        )
        self._shuffle_map_tasks[cur_task_idx] = task
        self._map_resource_usage = self._map_resource_usage.add(
            task.get_requested_resource_bundle()
        )

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

        # task_done_callback fires only after the ObjectRef is ready, so
        # this ray.get is just local deserialization.
        input_meta, shard_sizes = ray.get(task.get_waitable())

        for pid, (rows, nbytes) in shard_sizes.items():
            self._partition_row_counts[pid] += rows
            self._partition_bytes[pid] += nbytes

        for bundle in input_bundles:
            bundle.destroy_if_owned()

        self._total_input_rows += input_meta.num_rows or 0
        self._total_input_bytes += input_meta.size_bytes or 0
        self._map_blocks_stats.append(input_meta.to_stats())

        # Build the output bundle for this task: G blocks in group_idx order.
        # Block i corresponds to group_idx i; the downstream ShuffleReduceOp
        # gathers by block position.  We compute per-group (rows, bytes) by
        # summing the pid sizes for pids that fall in each group.
        group_sizes: List[List[int]] = [[0, 0] for _ in range(self._num_groups)]
        for pid, (rows, nbytes) in shard_sizes.items():
            g = pid // self._shard_group_size
            group_sizes[g][0] += rows
            group_sizes[g][1] += nbytes

        blocks_with_meta = [
            (
                group_refs[g],
                BlockMetadata(
                    num_rows=group_sizes[g][0],
                    size_bytes=group_sizes[g][1],
                    exec_stats=None,
                    input_files=None,
                ),
            )
            for g in range(self._num_groups)
        ]
        output_bundle = RefBundle(blocks_with_meta, schema=None, owns_blocks=True)
        self._output_queue.append(output_bundle)

        for bundle in input_bundles:
            self._map_metrics.on_output_taken(bundle)
        # NOTE: we don't call on_task_output_generated here.  That metric
        # path asserts exec_stats.block_ser_time_s is not None on every
        # output block, and shuffle has no per-output-block timing (one map
        # task produces G blocks together).  The metrics it populates
        # (num_task_outputs_generated, block-size histograms, etc.) are
        # observability only — execution correctness and resource accounting
        # don't depend on them.  Plasma usage for these outputs is tracked
        # via the downstream op's input-queue instead.
        self._map_metrics.on_output_queued(output_bundle)
        self._map_metrics.on_task_finished(
            task_idx,
            None,
            task_exec_stats=None,
            task_exec_driver_stats=None,
        )

        if self._map_bar is not None:
            self._map_bar.update(increment=input_meta.num_rows or 0)

    # -----------------------------------------------------------------------
    # Output handling
    # -----------------------------------------------------------------------

    def has_next(self) -> bool:
        return len(self._output_queue) > 0

    def _get_next_inner(self) -> RefBundle:
        bundle: RefBundle = self._output_queue.popleft()
        self._map_metrics.on_output_dequeued(bundle)
        return bundle

    # -----------------------------------------------------------------------
    # Public interface for downstream ShuffleReduceOp
    # -----------------------------------------------------------------------

    def get_partition_bytes(self) -> Dict[int, int]:
        """Per-partition uncompressed byte count.  Used by ShuffleReduceOp
        to size each reducer's memory hint."""
        return dict(self._partition_bytes)

    def get_partition_row_counts(self) -> Dict[int, int]:
        """Per-partition row count.  Used for logging / diagnostics."""
        return dict(self._partition_row_counts)

    # -----------------------------------------------------------------------
    # Task tracking
    # -----------------------------------------------------------------------

    def get_active_tasks(self) -> List[OpTask]:
        return list(self._shuffle_map_tasks.values())

    def has_execution_finished(self) -> bool:
        if self._shuffle_map_tasks or self._merge_buffer_refs_by_node:
            return False
        return super().has_execution_finished()

    def has_completed(self) -> bool:
        return (
            not self._shuffle_map_tasks
            and not self._merge_buffer_refs_by_node
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

    # -----------------------------------------------------------------------
    # Stats / metrics
    # -----------------------------------------------------------------------

    def get_stats(self) -> Dict[str, List[BlockStats]]:
        return {self._name: self._map_blocks_stats}

    def _extra_metrics(self) -> Dict[str, Any]:
        return {self._name: self._map_metrics.as_dict()}

    # -----------------------------------------------------------------------
    # Resource accounting
    # -----------------------------------------------------------------------

    def num_output_rows_total(self) -> Optional[int]:
        return self._total_input_rows if self._total_input_rows > 0 else None

    def current_logical_usage(self) -> ExecutionResources:
        # NOTE: object_store_memory is intentionally excluded.
        # ResourceManager.update_usages asserts
        # current_logical_usage().object_store_memory == 0 and then
        # overwrites it with its own estimate from the op's input/output
        # queue sizes — anything we put here would be discarded.
        return ExecutionResources(
            cpu=self._map_resource_usage.cpu,
            memory=self._map_resource_usage.memory,
        )

    def incremental_resource_usage(self) -> ExecutionResources:
        return ExecutionResources(cpu=self._shuffle_map_task_num_cpus)

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

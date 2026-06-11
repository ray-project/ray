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
    BlockEntry,
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    MetadataOpTask,
    ObjectStoreUsage,
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


_PARTITION_ID_SENTINEL = "__partition__"


def make_partition_sentinel(partition_id: int) -> List[str]:
    return [f"{_PARTITION_ID_SENTINEL}{partition_id}"]


def extract_partition_id(bundle: RefBundle) -> int:
    """Recover the partition_id stamped onto an upstream bundle."""
    for entry in bundle.blocks:
        files = entry.metadata.input_files
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
    """Map phase of a shuffle: partition inputs and group shards by partition.

    Each map task splits its input into num_partitions shards.  Shards land in a
    per-partition staging queue as tasks finish.  Once upstream is done and no
    map tasks remain, each staging queue is drained and merged into a single
    block per mapper.  That yields one output bundle per partition;

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
        self._partition_staging: Dict[int, FIFOBundleQueue] = {
            partition_id: FIFOBundleQueue() for partition_id in range(num_partitions)
        }

        # -- Per-partition total bytes ---------------------------------------
        self._partition_bytes: Dict[int, int] = defaultdict(int)

        # -- Output queue  ---------------------------------------------------
        self._output_queue: FIFOBundleQueue = FIFOBundleQueue()
        self._partition_bundles_emitted: bool = False

        # -- Stats -----------------------------------------------------------
        self._total_input_rows: int = 0
        self._total_input_bytes: int = 0
        self._map_blocks_stats: List[BlockStats] = []

        # -- Sub-progress bars -----------------------------------------------
        self._map_bar: Optional["BaseProgressBar"] = None

    @property
    def _input_queues(self) -> List[BaseBundleQueue]:
        return []

    @property
    def _output_queues(self) -> List[BaseBundleQueue]:
        return list(self._partition_staging.values()) + [self._output_queue]

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

        all_blocks_meta = [
            BlockEntry(ref=ref, metadata=meta)
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

        for partition_id, (rows, nbytes) in shard_sizes.items():
            ref = partition_refs[partition_id]
            if ref is None:
                continue
            shard_meta = BlockMetadata(
                num_rows=rows,
                size_bytes=nbytes,
                exec_stats=None,
                input_files=None,
            )
            shard_bundle = RefBundle(
                [BlockEntry(ref=ref, metadata=shard_meta)],
                schema=None,
                owns_blocks=True,
            )
            self._partition_staging[partition_id].add(shard_bundle)
            self._partition_bytes[partition_id] += nbytes

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

        self._maybe_emit_partition_bundles()

    def _maybe_emit_partition_bundles(self) -> None:
        """Drain each non-empty staging queue into one output bundle."""
        if self._partition_bundles_emitted:
            return
        if self._shuffle_map_tasks or self._merge_buffer_refs_by_node:
            return
        if not self._inputs_complete:
            return

        self._partition_bundles_emitted = True

        for partition_id in range(self._num_partitions):
            staging = self._partition_staging[partition_id]
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
            for i, entry in enumerate(merged.blocks):
                meta = entry.metadata
                if i == 0:
                    meta = dataclasses.replace(
                        meta, input_files=make_partition_sentinel(partition_id)
                    )
                stamped_blocks.append(BlockEntry(ref=entry.ref, metadata=meta))
            stamped = RefBundle(
                stamped_blocks,
                schema=merged.schema,
                owns_blocks=merged.owns_blocks,
            )
            self._output_queue.add(stamped)
            self._metrics.on_output_queued(stamped)

    def has_next(self) -> bool:
        return self._output_queue.has_next()

    def _get_next_inner(self) -> RefBundle:
        bundle: RefBundle = self._output_queue.get_next()
        self._metrics.on_output_dequeued(bundle)
        return bundle

    def get_partition_bytes(self) -> Dict[int, int]:
        return dict(self._partition_bytes)

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

    def _do_shutdown(self, force: bool = False) -> None:
        super()._do_shutdown(force)
        self._shuffle_map_tasks.clear()
        self._merge_buffer_refs_by_node.clear()
        self._merge_buffer_bundles_by_node.clear()
        self._merge_buffer_bytes_by_node.clear()
        for queue in self._partition_staging.values():
            queue.clear()
        self._output_queue.clear()

    def get_stats(self) -> Dict[str, List[BlockStats]]:
        return {self._name: self._map_blocks_stats}

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

    def estimate_object_store_usage(self, state) -> ObjectStoreUsage:
        return ObjectStoreUsage(internal=0, outputs=0)

    def incremental_resource_usage(self) -> ExecutionResources:
        avg_input = self._metrics.average_bytes_inputs_per_task
        memory = int(avg_input * 2) if avg_input else 0
        return ExecutionResources(
            cpu=self._shuffle_map_task_num_cpus,
            memory=memory,
        )

    def min_scheduling_resources(self) -> ExecutionResources:
        return self.incremental_resource_usage()

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

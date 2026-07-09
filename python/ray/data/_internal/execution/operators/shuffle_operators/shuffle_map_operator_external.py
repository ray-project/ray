"""ExternalHashShuffleMapOp — map phase of the external-shuffle variant.

Drives one ``external_hash_shuffle_map_task`` per input group and, once all mappers finish,
emits N ``RefBundle`` wrappers to the output queue — one per partition_id,
each carrying the SAME shared plasma object (the list of handle refs)
and a distinct ``__partition__<pid>`` sentinel. Wire protocol and task
body live in ``hash_shuffle_external.py``.
"""

import functools
import logging
import os
import secrets
import tempfile
import time
import typing
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import ray
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
from ray.data._internal.execution.operators.hash_shuffle_external import (
    PartitionFn,
    ShuffleCompression,
    external_hash_shuffle_map_task,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
    make_partition_sentinel,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    SHUFFLE_PEAK_MEMORY_MULTIPLIER,
)
from ray.data._internal.execution.operators.sub_progress import SubProgressBarMixin
from ray.data.block import BlockExecStats, BlockMetadata, BlockStats
from ray.data.context import DataContext
from ray.types import ObjectRef
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if typing.TYPE_CHECKING:
    from ray.data._internal.progress.base_progress import BaseProgressBar

logger = logging.getLogger(__name__)


_MAPPER_ID_SENTINEL = "__external_mapper__"


def _make_mapper_sentinel(mapper_id: int) -> List[str]:
    return [f"{_MAPPER_ID_SENTINEL}{mapper_id}"]


class ExternalHashShuffleMapOp(InternalQueueOperatorMixin, PhysicalOperator, SubProgressBarMixin):
    """External-shuffle map operator. See module docstring."""

    _DEFAULT_SHUFFLE_MAP_TASK_NUM_CPUS = 1.0
    _DEFAULT_PRE_MAP_MERGE_THRESHOLD = 1024 * 1024 * 1024  # 1 GB
    # Default = unbounded partition pool: accumulate every partition fully
    # and encode it once at end-of-task (matching v2, which has no pool and
    # bounds memory via the ``memory`` resource request instead). Opt in
    # to a real pool by passing ``pool_budget_bytes=``.
    _UNBOUNDED_POOL_BYTES = 1 << 62

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
        name: str = "ExternalHashShuffleMap",
        # -- External-shuffle-specific below --
        pool_budget_bytes: Optional[int] = None,
        fsync_on_close: bool = True,
        base_dir: Optional[str] = None,
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

        # -- Output queue  ---------------------------------------------------
        # Populated at ``all_inputs_done`` with N partition-wrapper bundles
        # (one per partition_id), each sharing ``_shared_handles_ref`` plus
        # a distinct partition_id sentinel in metadata.
        self._output_queue: FIFOBundleQueue = FIFOBundleQueue()
        self._partition_bundles_emitted: bool = False

        # -- Stats -----------------------------------------------------------
        self._total_input_rows: int = 0
        self._total_input_bytes: int = 0
        self._map_blocks_stats: List[BlockStats] = []
        # Per-partition decoded (pa.Table.nbytes, pre-compression) byte total,
        # summed across all completed mappers. Consumed by ExternalHashShuffleReduceOp
        # via ``get_partition_bytes`` — mirrors v2's ``_partition_bytes``.
        self._partition_bytes: Dict[int, int] = defaultdict(int)

        # -- Sub-progress bars -----------------------------------------------
        self._map_bar: Optional["BaseProgressBar"] = None

        # =====================================================================
        # External-shuffle-specific state below.
        # =====================================================================

        # -- External-shuffle config knobs -----------------------------------
        # Fixed override; ``None`` ⇒ ``_UNBOUNDED_POOL_BYTES``.
        self._pool_budget_override: Optional[int] = pool_budget_bytes
        self._fsync_on_close: bool = fsync_on_close

        # -- Per-shuffle identity & on-disk staging --------------------------
        # ``base_dir`` is a directory-name template — each node mkdirs the
        # same path on its OWN local FS. Driver owns nothing on remote disks.
        # Cleanup layers, precedence:
        #   1. Driver ``_do_shutdown`` → ``mgr.__ray_terminate__.remote()``.
        #   2. atexit hook on ShuffleManager (graceful Python shutdown).
        #   3. Actor crash + ``max_restarts=-1`` same-node respawn (files
        #      stay on disk for the respawn to pick up).
        #   4. OS tmpwatch: base_dir sits under ``$TMPDIR``.
        self._token: str = secrets.token_hex(16)
        self._shuffle_id: str = secrets.token_hex(8)
        # Default name is derived from ``shuffle_id``; driver deliberately
        # does NOT mkdir — mappers create their own local copy on the node
        # they run on. Caller-supplied ``base_dir`` is treated as scratch.
        self._base_dir: str = base_dir or os.path.join(
            tempfile.gettempdir(), f"ray_shuffle_external_{self._shuffle_id}"
        )

        # -- Partition-wrapper emission state --------------------------------
        # Handles returned by completed mappers; drained into
        # ``_shared_handles_ref`` at wrapper-emit time.
        self._completed_handle_refs: List[ObjectRef] = []
        # Single plasma object holding the handle-ref list. Every partition
        # wrapper points at this one ref (1 nested-ref serialization per
        # reducer dispatch, not M).
        self._shared_handles_ref: Optional[ObjectRef] = None

    @property
    def _input_queues(self) -> List[BaseBundleQueue]:
        return []

    @property
    def _output_queues(self) -> List[BaseBundleQueue]:
        return [self._output_queue]

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert input_index == 0
        if not refs.block_refs:
            refs.destroy_if_owned()
            return

        if self._pre_map_merge_threshold > 0:
            preferred_locs = refs.get_preferred_object_locations()
            node_id = (
                max(preferred_locs, key=lambda n: preferred_locs[n])
                if preferred_locs
                else "unknown"
            )
            for block_ref, block_metadata in zip(refs.block_refs, refs.metadata):
                self._merge_buffer_refs_by_node[node_id].append(block_ref)
                self._merge_buffer_bytes_by_node[node_id] += (
                    block_metadata.size_bytes or 0
                )
            self._merge_buffer_bundles_by_node[node_id].append(refs)

            if (
                self._merge_buffer_bytes_by_node[node_id]
                >= self._pre_map_merge_threshold
            ):
                self._flush_merge_buffer(node_id)
        else:
            self._submit_shuffle_map_task(
                list(refs.block_refs),
                [refs],
                estimated_bytes=sum((m.size_bytes or 0) for m in refs.metadata),
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

        pool_budget_bytes = (
            self._pool_budget_override
            if self._pool_budget_override is not None
            else self._UNBOUNDED_POOL_BYTES
        )

        resources: Dict[str, Any] = {"num_cpus": self._shuffle_map_task_num_cpus}
        if estimated_bytes > 0:
            resources["memory"] = estimated_bytes * SHUFFLE_PEAK_MEMORY_MULTIPLIER

        ray_options: Dict[str, Any] = {**resources}
        if target_node_id is not None:
            ray_options["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                target_node_id, soft=True
            )
        if self._map_runtime_env is not None:
            ray_options["runtime_env"] = self._map_runtime_env

        # The task body wants Optional[Literal["lz4", "zstd"]]; data_context
        # stores the raw string ("none" | "lz4" | "zstd"). Translate here.
        raw_compression = (self.data_context.hash_shuffle_compression or "none").lower()
        compression: ShuffleCompression = (
            None if raw_compression == "none" else raw_compression
        )

        handle_ref = external_hash_shuffle_map_task.options(**ray_options).remote(
            *block_refs,
            partition_fn=self._partition_fn,
            num_partitions=self._num_partitions,
            out_dir=self._base_dir,
            map_id=cur_task_idx,
            shuffle_id=self._shuffle_id,
            token=self._token,
            map_op_name=self.name,
            pool_budget_bytes=pool_budget_bytes,
            compression=compression,
            fsync_on_close=self._fsync_on_close,
        )

        task = MetadataOpTask(
            task_index=cur_task_idx,
            object_ref=handle_ref,
            task_done_callback=functools.partial(
                self._handle_map_done, cur_task_idx, handle_ref, input_bundles
            ),
            task_resource_bundle=ExecutionResources.from_resource_dict(resources),
        )
        self._shuffle_map_tasks[cur_task_idx] = task
        requested = task.get_requested_resource_bundle()
        assert requested is not None
        self._map_resource_usage = self._map_resource_usage.add(requested)

        all_blocks_meta = tuple(
            BlockEntry(ref=ref, metadata=meta)
            for bundle in input_bundles
            for ref, meta in zip(bundle.block_refs, bundle.metadata)
        )
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
        handle_ref: "ObjectRef",
        input_bundles: List[RefBundle],
    ) -> None:
        task = self._shuffle_map_tasks.pop(task_idx)
        requested = task.get_requested_resource_bundle()
        assert requested is not None
        self._map_resource_usage = self._map_resource_usage.subtract(requested)

        # Roll up input stats from the bundles now — the task return
        # doesn't carry them (v2 gets them from ``ray.get`` below), so
        # compute before ``destroy_if_owned`` drops the metadata.
        input_rows = sum(
            m.num_rows or 0 for bundle in input_bundles for m in bundle.metadata
        )
        input_bytes = sum(
            m.size_bytes or 0 for bundle in input_bundles for m in bundle.metadata
        )

        # `task_done_callback` fires only after the handle ref is ready,
        # so this is just local deserialization.
        handle = ray.get(handle_ref)
        for pid, nbytes in (handle.get("decoded_bytes") or {}).items():
            self._partition_bytes[pid] += nbytes

        # Synthetic per-mapper output bundle for metric bookkeeping only
        # — not pushed to the output queue (downstream sees the N
        # partition wrappers built at all_inputs_done).
        exec_stats = BlockExecStats.builder().build(block_ser_time_s=0.0)
        out_meta = BlockMetadata(
            num_rows=0,
            size_bytes=0,
            exec_stats=exec_stats,
            input_files=_make_mapper_sentinel(task_idx),
        )
        out_bundle = RefBundle(
            (BlockEntry(ref=handle_ref, metadata=out_meta),),
            schema=None,
            owns_blocks=False,
        )
        self._completed_handle_refs.append(handle_ref)

        for bundle in input_bundles:
            bundle.destroy_if_owned()

        self._total_input_rows += input_rows
        self._total_input_bytes += input_bytes
        input_meta = BlockMetadata(
            num_rows=input_rows,
            size_bytes=input_bytes,
            exec_stats=None,
            input_files=None,
        )
        self._map_blocks_stats.append(input_meta.to_stats())

        # Order: on_task_output_generated needs the task still in
        # _running_tasks; on_task_finished pops it.
        self._metrics.on_task_output_generated(task_index=task_idx, output=out_bundle)
        self._metrics.on_task_finished(
            task_idx,
            None,
            task_exec_stats=None,
            task_exec_driver_stats=None,
        )

        if self._map_bar is not None:
            self._map_bar.update(increment=input_rows)

        self._maybe_emit_partition_bundles()

    def _maybe_emit_partition_bundles(self) -> None:
        """Emit one wrapper bundle per partition into ``_output_queue``.

        All N wrappers share the same ``_shared_handles_ref`` and differ
        only by the stamped ``__partition__<pid>`` sentinel.
        """
        if self._partition_bundles_emitted:
            return
        if self._shuffle_map_tasks or self._merge_buffer_refs_by_node:
            return
        if not self._inputs_complete:
            return
        self._partition_bundles_emitted = True

        if not self._completed_handle_refs:
            return

        # One plasma object shared across all N wrappers.
        self._shared_handles_ref = ray.put(self._completed_handle_refs)

        partition_bytes = self.get_partition_bytes()
        for partition_id in range(self._num_partitions):
            size_bytes = partition_bytes.get(partition_id, 0)
            exec_stats = BlockExecStats.builder().build(block_ser_time_s=0.0)
            wrapper_meta = BlockMetadata(
                num_rows=0,
                size_bytes=size_bytes,
                exec_stats=exec_stats,
                input_files=list(make_partition_sentinel(partition_id)),
            )
            wrapper = RefBundle(
                (BlockEntry(ref=self._shared_handles_ref, metadata=wrapper_meta),),
                schema=None,
                owns_blocks=False,
            )
            self._output_queue.add(wrapper)
            self._metrics.on_output_queued(wrapper)

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
            or self._output_queue.has_next()
        ):
            return False
        # Between "last mapper task finished" and
        # ``_maybe_emit_partition_bundles`` firing, both queues can be
        # transiently empty. Don't declare finished before emission has
        # actually happened.
        if self._inputs_complete and not self._partition_bundles_emitted:
            return False
        return super().has_execution_finished()

    def has_completed(self) -> bool:
        return (
            not self._shuffle_map_tasks
            and not self._merge_buffer_refs_by_node
            and not self._output_queue.has_next()
            and (not self._inputs_complete or self._partition_bundles_emitted)
            and super().has_completed()
        )

    def _do_shutdown(self, force: bool = False) -> None:
        super()._do_shutdown(force)
        self._shuffle_map_tasks.clear()
        for bundles in self._merge_buffer_bundles_by_node.values():
            for bundle in bundles:
                bundle.destroy_if_owned()
        self._merge_buffer_refs_by_node.clear()
        self._merge_buffer_bundles_by_node.clear()
        self._merge_buffer_bytes_by_node.clear()
        self._output_queue.clear()
        # External-shuffle-specific: detached ShuffleManagers don't die via ref-count.
        # Terminate them gracefully (__ray_shutdown__ rmtrees base_dir +
        # stops server) before dropping their handle refs.
        self._kill_managers_from_completed_handles()
        self._completed_handle_refs.clear()
        self._shared_handles_ref = None

    def _kill_managers_from_completed_handles(self) -> None:
        """Terminate each unique ShuffleManager referenced by completed
        handles. Graceful first (``__ray_terminate__`` → ``__ray_shutdown__``
        → rmtree base_dir), fire in parallel; bounded ``ray.wait`` +
        ``ray.kill`` fallback so a dead-node hang can't stall shutdown."""
        from ray.data._internal.execution.operators.hash_shuffle_external import (
            _lookup_manager,
        )

        seen: set = set()
        mgrs = []
        for ref in self._completed_handle_refs:
            try:
                handle = ray.get(ref)
            except Exception:
                continue
            if not isinstance(handle, dict):
                continue
            shuffle_id = handle.get("shuffle_id")
            node_id = handle.get("node_id")
            if not shuffle_id or not node_id:
                continue
            key = (shuffle_id, node_id)
            if key in seen:
                continue
            seen.add(key)
            try:
                mgrs.append(_lookup_manager(shuffle_id, node_id))
            except Exception:
                # Actor name never registered / already GC'd — nothing to kill.
                continue

        # All ``.remote()`` calls fire before we wait → wall time = slowest
        # shutdown, not the sum.
        term_refs_by_mgr: List[Tuple[Any, Any]] = []
        for mgr in mgrs:
            try:
                term_refs_by_mgr.append((mgr, mgr.__ray_terminate__.remote()))
            except Exception:
                pass

        _GRACEFUL_TERM_TIMEOUT_S = 30.0
        pending = list(term_refs_by_mgr)
        deadline = time.monotonic() + _GRACEFUL_TERM_TIMEOUT_S
        while pending and time.monotonic() < deadline:
            refs = [r for _, r in pending]
            ready, _ = ray.wait(
                refs,
                num_returns=len(refs),
                timeout=max(0.1, deadline - time.monotonic()),
            )
            if not ready:
                break
            ready_set = set(ready)
            for r in ready:
                try:
                    ray.get(r)
                except Exception:
                    # Already-terminated / actor dead = success for us.
                    pass
            pending = [(m, r) for m, r in pending if r not in ready_set]

        # Timed-out (dead node) or otherwise stuck: hard kill so shutdown
        # progresses. Skips ``__ray_shutdown__`` — base_dir may leak on disk
        # for this manager (OS tmpwatch cleans it up eventually).
        for mgr, _ in pending:
            try:
                ray.kill(mgr, no_restart=True)
            except Exception:
                pass

    def get_stats(self) -> Dict[str, List[BlockStats]]:
        return {self._name: self._map_blocks_stats}

    def num_output_rows_total(self) -> Optional[int]:
        return self._total_input_rows if self._total_input_rows > 0 else None

    def current_logical_usage(self) -> ExecutionResources:
        return ExecutionResources(
            cpu=self._map_resource_usage.cpu,
            memory=self._map_resource_usage.memory,
        )

    def estimate_object_store_usage(self, state) -> ObjectStoreUsage:
        # Bulk data lives on disk. Handles are ~KB-scale — negligible.
        return ObjectStoreUsage(internal=0, outputs=0)

    def incremental_resource_usage(self) -> ExecutionResources:
        avg_input = self._metrics.average_bytes_inputs_per_task
        memory = int(avg_input * SHUFFLE_PEAK_MEMORY_MULTIPLIER) if avg_input else 0
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

    # External-shuffle-specific accessors consumed by ExternalHashShuffleReduceOp.
    @property
    def num_partitions(self) -> int:
        return self._num_partitions

    @property
    def base_dir(self) -> str:
        return self._base_dir

    @property
    def token(self) -> str:
        return self._token

    @property
    def shuffle_id(self) -> str:
        return self._shuffle_id

import functools
import logging
import typing
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Set, Tuple

import ray
from ray import ObjectRef
from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    OpTask,
    TaskExecDriverStats,
    estimate_total_num_of_blocks,
)
from ray.data._internal.execution.operators.shuffle_operators._shuffle_tasks import (
    ReduceFn,
    _shuffle_reduce_task,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
    ShuffleMapOp,
)
from ray.data._internal.execution.operators.sub_progress import SubProgressBarMixin
from ray.data._internal.stats import OpRuntimeMetrics
from ray.data.block import BlockStats, TaskExecWorkerStats, to_stats
from ray.data.context import DataContext

if typing.TYPE_CHECKING:
    from ray.data._internal.progress.base_progress import BaseProgressBar

logger = logging.getLogger(__name__)


class ShuffleReduceOp(PhysicalOperator, SubProgressBarMixin):
    """Reduce phase of a shuffle.

    Args:
        input_op: Upstream :class:`ShuffleMapOp`.
        data_context: Runtime configuration.
        num_partitions: Total number of output partitions.  Must match the
            value used by the paired :class:`ShuffleMapOp` — one reducer
            task per partition.
        reduce_fn: Function called once per partition (in blocking mode) or
            incrementally (in streaming mode) to combine input shards into
            output blocks.
        streaming_reduce: If True, ``reduce_fn`` is called whenever a
            partition's accumulator reaches ``target_max_block_size`` (for
            partial-result-friendly reductions like plain concat).  If False,
            wait for all shards before calling once (required for sort or
            stateful aggregation).  Forced to False when
            ``disallow_block_splitting=True``.
        disallow_block_splitting: If True, output blocks are emitted as-is
            without being reshaped to ``target_max_block_size`` — required
            for hash-shuffle's "partition = block" contract.
        reduce_cpus: CPU request per reduce task.  Defaults to 1.  With
            concurrency capped at one reducer per node, asking for more CPUs
            blocks downstream from co-locating; only raise it for CPU-bound
            reduce_fns.
        name: Display name shown in progress bars and logs.
    """

    _DEFAULT_SHUFFLE_REDUCE_TASK_NUM_CPUS = 1.0

    def __init__(
        self,
        input_op: ShuffleMapOp,
        data_context: DataContext,
        *,
        num_partitions: int,
        reduce_fn: ReduceFn,
        streaming_reduce: bool = True,
        disallow_block_splitting: bool = False,
        reduce_cpus: Optional[float] = None,
        name: str = "ShuffleReduce",
    ):
        super().__init__(
            name=name,
            input_dependencies=[input_op],
            data_context=data_context,
        )

        self._num_partitions: int = num_partitions
        self._reduce_fn: ReduceFn = reduce_fn
        self._disallow_block_splitting: bool = disallow_block_splitting
        # Block-splitting disallowed → reducer must see entire partition
        # before emitting, so force blocking mode regardless of the flag.
        self._streaming_reduce: bool = streaming_reduce and not disallow_block_splitting

        # -- Reduce task config & tracking -----------------------------------
        self._shuffle_reduce_task_num_cpus_override: Optional[float] = reduce_cpus
        self._shuffle_reduce_task_num_cpus: float = (
            self._DEFAULT_SHUFFLE_REDUCE_TASK_NUM_CPUS
        )
        self._shuffle_reduce_tasks: Dict[int, DataOpTask] = {}
        self._pending_partition_ids: Set[int] = set(range(self._num_partitions))

        # -- Per-partition shard buffers (populated by _add_input_inner) -----
        # partition_id → list of ObjectRefs to compressed IPC shards from
        # each mapper (one ref per mapper; may be ``None`` if that mapper
        # produced no rows for this partition).
        self._partition_buffers: Dict[int, List[ObjectRef]] = defaultdict(list)

        # -- Per-partition stats (snapshot from upstream on barrier) ---------
        self._partition_bytes: Dict[int, int] = {}

        # -- Output queue ----------------------------------------------------
        self._output_queue: deque = deque()

        # -- Reduce-phase init flag + cluster shape (set in _init_reduce_phase) --
        self._reduce_phase_initialized: bool = False
        self._num_nodes: Optional[int] = None

        # -- Stats -----------------------------------------------------------
        self._output_blocks_stats: List[BlockStats] = []

        # -- Sub-progress bars -----------------------------------------------
        self._reduce_bar: Optional["BaseProgressBar"] = None
        self._reduce_metrics = OpRuntimeMetrics(self)

    # -----------------------------------------------------------------------
    # Input handling: bucket bundles by block-position into partition buffers
    # -----------------------------------------------------------------------

    def can_add_input(self) -> bool:
        return True

    def _add_input_inner(self, input_bundle: RefBundle, input_index: int) -> None:
        """Each upstream bundle carries N blocks in partition-id order;
        accumulate ``blocks[pid]`` into ``_partition_buffers[pid]``."""
        assert input_index == 0
        self._reduce_metrics.on_input_received(input_bundle)

        # Inter-op contract: ShuffleMapOp emits exactly num_partitions blocks
        # per bundle (one IPC stream per output block, in partition-id order).
        # A violation means silent data corruption downstream, so this is a
        # ``ValueError`` rather than an ``assert`` (which gets stripped with
        # ``python -O``).
        if len(input_bundle.block_refs) != self._num_partitions:
            raise ValueError(
                f"ShuffleReduceOp expected {self._num_partitions} blocks per "
                f"input bundle from ShuffleMapOp, got "
                f"{len(input_bundle.block_refs)}"
            )
        for pid, block_ref in enumerate(input_bundle.block_refs):
            self._partition_buffers[pid].append(block_ref)

    def all_inputs_done(self) -> None:
        super().all_inputs_done()
        # Snapshot partition stats from upstream so we can size reducer
        # memory.  Upstream is guaranteed complete by the executor before
        # this is called.  The planner pairs us with a ShuffleMapOp upstream
        # (see ``_plan_hash_shuffle_repartition``); other upstreams are a
        # programming error.
        upstream = self.input_dependencies[0]
        assert isinstance(upstream, ShuffleMapOp), (
            f"ShuffleReduceOp expects a ShuffleMapOp upstream, got "
            f"{type(upstream).__name__}"
        )
        self._partition_bytes = upstream.get_partition_bytes()

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
        return list(self._shuffle_reduce_tasks.values())

    def _is_all_reduce_submitted(self) -> bool:
        return len(self._pending_partition_ids) == 0

    # -----------------------------------------------------------------------
    # Reduce scheduling
    # -----------------------------------------------------------------------

    def _get_reduce_concurrency_limit(self) -> int:
        """Max concurrent reduce tasks allowed.

        Defaults to 1 per live worker node, distributed via SPREAD.  Override
        via ``DataContext`` config ``map_reduce_max_concurrent_reducers``.
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
        # Barrier: cannot launch reducers until all upstream bundles are in.
        if not self._inputs_complete:
            return

        if not self._reduce_phase_initialized:
            self._init_reduce_phase()

        slots_available = self._get_reduce_concurrency_limit() - len(
            self._shuffle_reduce_tasks
        )

        for partition_id in list(self._pending_partition_ids):
            if slots_available <= 0:
                break

            if self._partition_bytes.get(partition_id, 0) == 0:
                self._pending_partition_ids.discard(partition_id)
                self._partition_buffers.pop(partition_id, None)
                continue

            shard_refs = self._partition_buffers.get(partition_id, [])

            if not shard_refs:
                self._pending_partition_ids.discard(partition_id)
                continue

            reduce_resources, estimated_bytes = self._compute_reduce_resources(
                partition_id
            )

            reduce_options = {
                **reduce_resources,
                "scheduling_strategy": "SPREAD",
                "num_returns": "streaming",
            }

            block_gen = _shuffle_reduce_task.options(**reduce_options).remote(
                shard_refs,
                partition_id=partition_id,
                reduce_fn=self._reduce_fn,
                target_max_block_size=(
                    None
                    if self._disallow_block_splitting
                    else self.data_context.target_max_block_size
                ),
                streaming=self._streaming_reduce,
            )

            data_task = DataOpTask(
                task_index=partition_id,
                streaming_gen=block_gen,
                output_ready_callback=functools.partial(
                    self._handle_reduce_output_ready, partition_id
                ),
                task_done_callback=functools.partial(
                    self._handle_reduce_done, partition_id
                ),
                # Mirror the actual Ray task resource ask so the executor's
                # bookkeeping matches what Ray reserved.  ``object_store_memory``
                # is omitted — the resource manager computes per-op plasma
                # usage from its own input/output queue estimate.
                task_resource_bundle=ExecutionResources.from_resource_dict(
                    reduce_resources
                ),
                operator_name=self.name,
            )

            self._shuffle_reduce_tasks[partition_id] = data_task
            self._pending_partition_ids.discard(partition_id)
            slots_available -= 1

            # Release driver-side refs so intermediate objects can be reclaimed.
            self._partition_buffers.pop(partition_id, None)

            empty_bundle = RefBundle([], schema=None, owns_blocks=False)
            self._reduce_metrics.on_task_submitted(
                partition_id, empty_bundle, task_id=data_task.get_task_id()
            )

    def _log_partition_stats(self) -> None:
        """Log per-partition row/byte distribution before reduce starts."""
        if not self._partition_bytes:
            return
        counts = self._partition_bytes
        n = len(counts)
        empty = self._num_partitions - n
        partition_bytes = sorted(counts.values())
        total_bytes = sum(partition_bytes)
        logger.info(
            f"Partition stats before reduce: "
            f"partitions={self._num_partitions} (non-empty={n}, empty={empty}), "
            f"total_bytes={total_bytes / 1e9:.1f}GB, "
            f"bytes: min={partition_bytes[0] / 1e6:.0f}MB, "
            f"median={partition_bytes[n // 2] / 1e6:.0f}MB, "
            f"max={partition_bytes[-1] / 1e6:.0f}MB"
        )

    def _init_reduce_phase(self) -> None:
        """One-shot setup before the first reducer launches: snapshot cluster
        shape, resolve per-task CPU ask, log partition stats."""
        self._log_partition_stats()

        try:
            self._num_nodes = max(
                1,
                sum(
                    1
                    for n in ray.nodes()
                    if n.get("Alive", False)
                    and "node:__internal_head__" not in n.get("Resources", {})
                ),
            )
        except Exception:
            self._num_nodes = 1

        # With concurrency capped at one reducer per node, there's no benefit
        # to reserving more than one CPU per reducer — doing so just blocks
        # downstream ops from co-locating.  Users with CPU-bound reduce_fns
        # can opt up via the override.
        self._shuffle_reduce_task_num_cpus = (
            self._shuffle_reduce_task_num_cpus_override
            if self._shuffle_reduce_task_num_cpus_override is not None
            else self._DEFAULT_SHUFFLE_REDUCE_TASK_NUM_CPUS
        )

        logger.info(
            f"Reduce starting: "
            f"reduce_cpus={self._shuffle_reduce_task_num_cpus}, "
            f"num_nodes={self._num_nodes}"
        )

        self._reduce_phase_initialized = True

    def _compute_reduce_resources(
        self, partition_id: int
    ) -> Tuple[Dict[str, Any], int]:
        """Build the resource ask for one reduce task.

        Returns ``(resources_dict, estimated_bytes)``:

        - ``resources_dict``: ``num_cpus`` and a ``memory`` hint sized to
          ``2 × estimated_bytes`` — covers decompressed input + concat copy.
          The memory ask is capped at a single node's memory so the scheduler
          still has a meaningful placement hint.
        - ``estimated_bytes``: uncompressed bytes for this partition.
        """
        estimated_bytes = self._partition_bytes.get(partition_id, 0)
        # ``_init_reduce_phase`` runs before any reducer launch and caches
        # ``_num_nodes`` after filtering dead + head-only nodes; reuse it.
        assert (
            self._num_nodes is not None
        ), "_compute_reduce_resources called before _init_reduce_phase"
        node_memory = ray.cluster_resources().get("memory", 30e9) / self._num_nodes
        reduce_resources: Dict[str, Any] = {
            "num_cpus": self._shuffle_reduce_task_num_cpus
        }
        if estimated_bytes > 0:
            reduce_resources["memory"] = min(estimated_bytes * 2, node_memory)
        return reduce_resources, estimated_bytes

    def _handle_reduce_output_ready(self, partition_id: int, bundle: RefBundle) -> None:
        self._output_queue.append(bundle)
        self._reduce_metrics.on_output_queued(bundle)
        self._reduce_metrics.on_task_output_generated(
            task_index=partition_id, output=bundle
        )
        _, num_outputs, num_rows = estimate_total_num_of_blocks(
            partition_id + 1,
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

    def _handle_reduce_done(
        self,
        partition_id: int,
        exc: Optional[Exception],
        task_exec_stats: Optional[TaskExecWorkerStats],
        task_exec_driver_stats: Optional[TaskExecDriverStats],
    ) -> None:
        """Callback when a reduce task finishes (with or without exception)."""
        if partition_id not in self._shuffle_reduce_tasks:
            return
        self._shuffle_reduce_tasks.pop(partition_id)
        self._reduce_metrics.on_task_finished(
            task_index=partition_id,
            exception=exc,
            task_exec_stats=task_exec_stats,
            task_exec_driver_stats=task_exec_driver_stats,
        )
        if exc:
            logger.error(
                f"Reduce of partition {partition_id} failed: {exc}", exc_info=exc
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
        super()._do_shutdown(force)
        self._shuffle_reduce_tasks.clear()
        self._partition_buffers.clear()

    # -----------------------------------------------------------------------
    # Stats / metrics
    # -----------------------------------------------------------------------

    def get_stats(self) -> Dict[str, List[BlockStats]]:
        return {self._name: self._output_blocks_stats}

    def _extra_metrics(self) -> Dict[str, Any]:
        return {self._name: self._reduce_metrics.as_dict()}

    # -----------------------------------------------------------------------
    # Resource accounting
    # -----------------------------------------------------------------------

    def num_output_rows_total(self) -> Optional[int]:
        # Output row count matches upstream input row count.  The planner
        # guarantees a ShuffleMapOp upstream — see ``_add_input_inner``.
        upstream = self.input_dependencies[0]
        assert isinstance(upstream, ShuffleMapOp)
        return upstream.num_output_rows_total()

    def current_logical_usage(self) -> ExecutionResources:
        # NOTE: object_store_memory excluded; framework computes it itself.
        usage = ExecutionResources.zero()
        for task in self._shuffle_reduce_tasks.values():
            bundle = task.get_requested_resource_bundle()
            usage = usage.add(ExecutionResources(cpu=bundle.cpu, memory=bundle.memory))
        return usage

    def incremental_resource_usage(self) -> ExecutionResources:
        return ExecutionResources(cpu=self._shuffle_reduce_task_num_cpus)

    def min_scheduling_resources(self) -> ExecutionResources:
        return self.incremental_resource_usage()

    # -----------------------------------------------------------------------
    # Progress bar support
    # -----------------------------------------------------------------------

    def progress_str(self) -> str:
        reduces_submitted = self._num_partitions - len(self._pending_partition_ids)
        reduces_done = reduces_submitted - len(self._shuffle_reduce_tasks)
        return f"reduce: {reduces_done}/{self._num_partitions}"

    def get_sub_progress_bar_names(self) -> Optional[List[str]]:
        return ["Reduce"]

    def set_sub_progress_bar(self, name: str, pg: "BaseProgressBar") -> None:
        if name == "Reduce":
            self._reduce_bar = pg

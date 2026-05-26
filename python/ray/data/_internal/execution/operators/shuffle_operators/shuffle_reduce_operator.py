import functools
import logging
import typing
from collections import deque
from typing import Any, Dict, List, Optional

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
    extract_partition_id,
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

    Each input bundle from `ShuffleMapOp` carries the M shards for
    exactly one partition (with the partition_id stamped into the first
    block's `BlockMetadata.input_files`).  This op submits one reducer
    task per input bundle — a 1:1 input-to-task shape that lets the
    framework's standard backpressure policies (ResourceBudget /
    OutputBackpressure) throttle reducer submission, the same way they
    throttle a regular MapOperator like `ReadParquet`.

    Per-task CPU and memory are declared via `incremental_resource_usage`;
    the framework's `ReservationOpResourceAllocator` uses that to size
    the reducer's share of the cluster budget and `can_add_input` gates
    each new task accordingly.  We do not override
    `min_max_resource_requirements`, so reducer concurrency falls out of
    the framework's overall budget arithmetic rather than a per-op cap.

    Args:
        input_op: Upstream `ShuffleMapOp`.
        data_context: Runtime configuration.
        num_partitions: Total number of output partitions.  Must match the
            value used by the paired `ShuffleMapOp`.  Empty
            partitions are skipped on the map side and produce no reducer
            task here.
        reduce_fn: Function called once per partition (in blocking mode)
            or incrementally (in streaming mode) to combine input shards
            into output blocks.
        streaming_reduce: If True, `reduce_fn` is called whenever a
            partition's accumulator reaches `target_max_block_size`.  If
            False, wait for all shards before calling once (required for
            sort or stateful aggregation).  Forced to False when
            `disallow_block_splitting=True`.
        disallow_block_splitting: If True, output blocks are emitted as-is
            without being reshaped to `target_max_block_size` — required
            for hash-shuffle's "partition = block" contract.
        reduce_cpus: CPU request per reduce task.  Defaults to 1.
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
        self._shuffle_reduce_task_num_cpus: float = (
            reduce_cpus
            if reduce_cpus is not None
            else self._DEFAULT_SHUFFLE_REDUCE_TASK_NUM_CPUS
        )
        self._shuffle_reduce_tasks: Dict[int, DataOpTask] = {}
        self._num_reduce_tasks_submitted: int = 0

        # -- Output queue ----------------------------------------------------
        self._output_queue: deque = deque()

        # -- Stats -----------------------------------------------------------
        self._output_blocks_stats: List[BlockStats] = []

        # -- Sub-progress bars -----------------------------------------------
        self._reduce_bar: Optional["BaseProgressBar"] = None
        self._reduce_metrics = OpRuntimeMetrics(self)

    # -----------------------------------------------------------------------
    # Input handling: one bundle → one reducer task
    # -----------------------------------------------------------------------

    def _add_input_inner(self, input_bundle: RefBundle, input_index: int) -> None:
        """Submit one reducer task for this partition-bundle.

        Each upstream bundle is a single partition's shards (M blocks from
        M mappers).  The partition_id is encoded in the first block's
        `input_files`.  This is the framework-gated entry point — the
        executor only calls it when all configured backpressure policies
        say the op can accept another input.
        """
        assert input_index == 0
        self._reduce_metrics.on_input_received(input_bundle)

        if not input_bundle.block_refs:
            # Defensive: ShuffleMapOp skips empty partitions, but a future
            # regression here would silently launch a no-op reducer.
            input_bundle.destroy_if_owned()
            return

        partition_id = extract_partition_id(input_bundle)
        shard_refs = list(input_bundle.block_refs)
        # Sum of per-shard `size_bytes`, which ShuffleMapOp set to the
        # uncompressed Arrow `nbytes` at map time.  This is the total
        # uncompressed size of this partition — the relevant number for
        # sizing the reducer's heap, since the reducer decompresses
        # every shard before reducing.
        estimated_bytes = sum((m.size_bytes or 0) for m in input_bundle.metadata)

        # Per-task ask: 1 CPU + `2 × estimated_bytes` memory.  The
        # 2× covers peak USS (decompressed accumulator + transient
        # concat copy + small overhead share).  Relies on
        # `@ray.remote(max_calls=1)` on `_shuffle_reduce_task` to
        # keep the worker heap baseline clean between consecutive tasks.
        reduce_resources: Dict[str, Any] = {
            "num_cpus": self._shuffle_reduce_task_num_cpus,
        }
        if estimated_bytes > 0:
            reduce_resources["memory"] = int(estimated_bytes * 2)
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
                self._handle_reduce_done, partition_id, input_bundle
            ),
            task_resource_bundle=ExecutionResources.from_resource_dict(
                reduce_resources
            ),
            operator_name=self.name,
        )

        assert partition_id not in self._shuffle_reduce_tasks, (
            f"partition_id {partition_id} already has an in-flight reducer "
            f"task; ShuffleMapOp must emit at most one bundle per partition"
        )
        self._shuffle_reduce_tasks[partition_id] = data_task
        self._num_reduce_tasks_submitted += 1
        self._reduce_metrics.on_task_submitted(
            partition_id, input_bundle, task_id=data_task.get_task_id()
        )

    # -----------------------------------------------------------------------
    # Output handling
    # -----------------------------------------------------------------------

    def has_next(self) -> bool:
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

    # -----------------------------------------------------------------------
    # Reduce task callbacks
    # -----------------------------------------------------------------------

    def _handle_reduce_output_ready(self, partition_id: int, bundle: RefBundle) -> None:
        self._output_queue.append(bundle)
        self._reduce_metrics.on_output_queued(bundle)
        self._reduce_metrics.on_task_output_generated(
            task_index=partition_id, output=bundle
        )
        _, num_outputs, num_rows = estimate_total_num_of_blocks(
            self._num_reduce_tasks_submitted,
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
        input_bundle: RefBundle,
        exc: Optional[Exception],
        task_exec_stats: Optional[TaskExecWorkerStats],
        task_exec_driver_stats: Optional[TaskExecDriverStats],
    ) -> None:
        """Callback when a reduce task finishes (with or without exception)."""
        input_bundle.destroy_if_owned()
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
        if self._shuffle_reduce_tasks or self._output_queue:
            return False
        return super().has_execution_finished()

    def has_completed(self) -> bool:
        return (
            not self._shuffle_reduce_tasks
            and not self._output_queue
            and super().has_completed()
        )

    # -----------------------------------------------------------------------
    # Shutdown
    # -----------------------------------------------------------------------

    def _do_shutdown(self, force: bool = False) -> None:
        super()._do_shutdown(force)
        self._shuffle_reduce_tasks.clear()
        self._output_queue.clear()

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
        upstream = self.input_dependencies[0]
        assert isinstance(upstream, ShuffleMapOp)
        return upstream.num_output_rows_total()

    def current_logical_usage(self) -> ExecutionResources:
        usage = ExecutionResources.zero()
        for task in self._shuffle_reduce_tasks.values():
            bundle = task.get_requested_resource_bundle()
            usage = usage.add(ExecutionResources(cpu=bundle.cpu, memory=bundle.memory))
        return usage

    def incremental_resource_usage(self) -> ExecutionResources:
        """Per-task resource ask for the framework's budget allocator.

        Returns CPU + `2 × avg_partition_bytes` of memory once
        `ShuffleMapOp` has begun reporting per-partition sizes;
        memory is 0 before then.

        `object_store_memory` is intentionally omitted — a reducer
        task is net-neutral in plasma (it consumes one partition's
        input and produces a similar-sized output), but the framework's
        budget check treats per-task plasma asks as purely additive.
        Declaring it can deadlock the scheduler if upstream already
        filled plasma: the allocator sees "no plasma budget for
        reducer" and refuses to submit, even though running a reducer
        would free its input plasma.  Standard MapOperator-style
        tracking of in-flight reducer outputs via
        `_metrics.on_output_queued` is enough.
        """
        upstream = self.input_dependencies[0]
        assert isinstance(upstream, ShuffleMapOp)
        partition_bytes = upstream.get_partition_bytes()
        memory = 0
        sizes = [b for b in partition_bytes.values() if b > 0]
        if sizes:
            avg_bytes = sum(sizes) / len(sizes)
            memory = int(avg_bytes * 2)
        return ExecutionResources(
            cpu=self._shuffle_reduce_task_num_cpus,
            memory=memory,
        )

    def min_scheduling_resources(self) -> ExecutionResources:
        return self.incremental_resource_usage()

    # -----------------------------------------------------------------------
    # Progress bar support
    # -----------------------------------------------------------------------

    def progress_str(self) -> str:
        submitted = self._num_reduce_tasks_submitted
        done = submitted - len(self._shuffle_reduce_tasks)
        return f"reduce: {done}/{submitted}"

    def get_sub_progress_bar_names(self) -> Optional[List[str]]:
        return ["Reduce"]

    def set_sub_progress_bar(self, name: str, pg: "BaseProgressBar") -> None:
        if name == "Reduce":
            self._reduce_bar = pg

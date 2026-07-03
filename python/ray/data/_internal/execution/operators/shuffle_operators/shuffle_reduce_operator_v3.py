"""ShuffleReduceOpV3 — reduce phase of the v3 file-transport hash shuffle.

Consumes the N ``partition wrapper`` bundles emitted by
``ShuffleMapOpV3`` (one per partition_id, each carrying a shared
handle-list ref + a ``__partition__<pid>`` sentinel in metadata) and
dispatches one ``v3_reduce_task`` per bundle. Each reduce task is a Ray
streaming generator that yields ``(block, pickled metadata)`` pairs
matching the v2 ``_shuffle_reduce_task`` protocol; we wrap each
generator in a ``DataOpTask`` so the executor sees ordinary streaming
output bundles.

Structurally identical to v2 ``ShuffleReduceOp``: one input bundle in,
one reduce task out (via ``_add_input_inner``). The executor's normal
backpressure + resource-manager machinery therefore gates reducer
dispatch — N gating points (one per partition wrapper), same as v2.

The only v3-specific twist is what the input bundle carries: instead
of M shard ObjectRefs (v2), a v3 wrapper carries ONE ref pointing at a
shared plasma object holding the full mapper-handle list. That single
ref, plus the sentinel-encoded partition_id, is everything
``v3_reduce_task`` needs to pull its partition's bytes from every source
node's ``ShuffleManager``.

Barrier semantics: ``ShuffleMapOpV3.all_inputs_done`` only emits the N
wrappers after every mapper has finished, so ``ShuffleReduceOpV3``
still can't dispatch a reducer before map is fully closed — same
effective ordering as v2 (v2's ``ShuffleMapOp._maybe_emit_partition_bundles``
also gates on ``_inputs_complete``).
"""

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
from ray.data._internal.execution.operators.hash_shuffle_v3 import (
    ReduceFn,
    v3_reduce_task,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
    extract_partition_id,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator_v3 import (  # noqa: E501
    ShuffleMapOpV3,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    SHUFFLE_PEAK_MEMORY_MULTIPLIER,
)
from ray.data._internal.execution.operators.sub_progress import (
    SubProgressBarMixin,
)
from ray.data.block import BlockStats, TaskExecWorkerStats, to_stats
from ray.data.context import DataContext
from ray.types import ObjectRef

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.operators.map_transformer import (
        MapTransformer,
    )
    from ray.data._internal.progress.base_progress import BaseProgressBar

logger = logging.getLogger(__name__)


class ShuffleReduceOpV3(PhysicalOperator, SubProgressBarMixin):

    _DEFAULT_REDUCE_NUM_CPUS = 1.0
    _DEFAULT_MAX_BYTES_PER_FETCH = 256 * 1024 * 1024  # 256 MiB

    def __init__(
        self,
        input_op: ShuffleMapOpV3,
        data_context: DataContext,
        *,
        num_partitions: int,
        reduce_fn: ReduceFn,
        streaming_reduce: bool = True,
        coalesce_output: bool = False,
        max_bytes_per_fetch: int = _DEFAULT_MAX_BYTES_PER_FETCH,
        reduce_prefetch_dir: Optional[str] = None,
        reduce_cpus: Optional[float] = None,
        name: str = "ShuffleReduceV3",
        downstream_map_transformer: Optional["MapTransformer"] = None,
        downstream_map_task_kwargs: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            name=name,
            input_dependencies=[input_op],
            data_context=data_context,
        )

        self._num_partitions: int = num_partitions
        self._reduce_fn: ReduceFn = reduce_fn
        # When set, OperatorFusionRule has absorbed a downstream MapOperator
        # (typically Write) into this reduce op. v3_reduce_task applies it
        # to each emitted block before yielding, so the downstream task
        # never has to run -- output of this op IS the downstream's output
        # (e.g., the write-stats blocks Write would have produced).
        self._downstream_map_transformer: Optional[
            "MapTransformer"
        ] = downstream_map_transformer
        # map_task_kwargs the absorbed downstream MapOperator would have
        # received via its scheduler (e.g. Write's ``{"write_uuid": ...}``).
        # v3_reduce_task threads this into the TaskContext it builds around
        # the transformer so datasinks that read ``ctx.kwargs[...]`` see
        # the same values they would in the un-fused path.
        self._downstream_map_task_kwargs: Dict[str, Any] = (
            downstream_map_task_kwargs or {}
        )
        self._coalesce_output: bool = coalesce_output
        self._streaming_reduce: bool = streaming_reduce
        self._max_bytes_per_fetch: int = max_bytes_per_fetch
        self._reduce_prefetch_dir: Optional[str] = reduce_prefetch_dir
        self._reduce_num_cpus: float = (
            reduce_cpus if reduce_cpus is not None else self._DEFAULT_REDUCE_NUM_CPUS
        )

        # -- Reduce task tracking --
        # ShuffleMapOpV3 emits one bundle per partition (never repeats a
        # pid), so this dict never sees a collision.
        self._shuffle_reduce_tasks: Dict[int, DataOpTask] = {}
        self._num_reduce_tasks_submitted: int = 0

        # -- Output queue --
        # Streaming generator yields (block, pickled metadata) pairs; the
        # DataOpTask harness assembles each into a RefBundle and our
        # callback pushes it here.
        self._output_queue: deque = deque()

        # -- Stats --
        self._output_blocks_stats: List[BlockStats] = []

        # -- Sub-progress bar --
        self._reduce_bar: Optional["BaseProgressBar"] = None

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        """Executor calls this with one partition wrapper at a time (bundle
        contains 1 block-ref = ``shared_handles_ref`` + partition_id
        sentinel in metadata). We extract the two pieces and dispatch one
        reduce task. Backpressure is fully executor-driven — no
        accumulation, no dispatch loop.
        """
        assert input_index == 0
        if not refs.block_refs:
            refs.destroy_if_owned()
            return

        partition_id = extract_partition_id(refs)
        # The wrapper's sole block-ref IS the shared handle-list plasma
        # object built by ShuffleMapOpV3.all_inputs_done. Ray dispatch sees
        # 1 nested borrowed ref per reduce task (not M).
        handles_ref = refs.block_refs[0]
        # size_bytes comes from the wrapper's metadata (populated by
        # ShuffleMapOpV3 from its per-partition decoded-byte accumulator).
        estimated_bytes = sum((m.size_bytes or 0) for m in refs.metadata)

        self._dispatch_one_reducer(
            partition_id,
            handles_ref,
            estimated_bytes,
            self.data_context.target_max_block_size,
        )
        # Wrapper was built with owns_blocks=False (shared_handles_ref is
        # owned by the upstream map op), so this is a no-op for the ref —
        # it just releases the Python-side RefBundle.
        refs.destroy_if_owned()

    def _dispatch_one_reducer(
        self,
        partition_id: int,
        handles_ref: ObjectRef,
        estimated_bytes: int,
        target_max_block_size: Optional[int],
    ) -> None:
        # Per-partition memory ask: 2× the decoded byte total this reducer
        # will see (bounds peak heap from accum + reshape carry). Estimate
        # comes from the wrapper's metadata (populated by the map op from
        # its per-partition decoded_bytes accumulator).
        reduce_resources: Dict[str, Any] = {"num_cpus": self._reduce_num_cpus}
        if estimated_bytes > 0:
            reduce_resources["memory"] = int(
                estimated_bytes * SHUFFLE_PEAK_MEMORY_MULTIPLIER
            )
        reduce_options: Dict[str, Any] = {
            **reduce_resources,
            "scheduling_strategy": "SPREAD",
            "num_returns": "streaming",
        }

        block_gen = v3_reduce_task.options(**reduce_options).remote(
            handles_ref,
            partition_id,
            self._reduce_fn,
            self._reduce_prefetch_dir,
            self._max_bytes_per_fetch,
            target_max_block_size,
            self._streaming_reduce,
            self._downstream_map_transformer,
            self.name,
            self._downstream_map_task_kwargs,
            self._coalesce_output,
        )

        data_task = DataOpTask(
            task_index=partition_id,
            streaming_gen=block_gen,
            block_ref_counter=self._block_ref_counter,
            producer_id=self.id,
            output_ready_callback=functools.partial(
                self._handle_reduce_output_ready, partition_id
            ),
            task_done_callback=functools.partial(
                self._handle_reduce_done, partition_id
            ),
            task_resource_bundle=ExecutionResources.from_resource_dict(
                reduce_resources
            ),
            operator_name=self.name,
        )

        # Map emits each partition exactly once, so no collision possible.
        assert partition_id not in self._shuffle_reduce_tasks, (
            f"partition_id {partition_id} already has an in-flight reducer "
            f"— ShuffleMapOpV3 should emit each partition wrapper exactly once"
        )
        self._shuffle_reduce_tasks[partition_id] = data_task
        self._num_reduce_tasks_submitted += 1

        # For metrics, we don't have a partition-specific data bundle (the
        # wrapper's only ref is the shared handle list), so pass a synthetic
        # empty bundle — same pattern as before.
        self._metrics.on_task_submitted(
            partition_id,
            RefBundle((), schema=None, owns_blocks=False),
            task_id=data_task.get_task_id(),
        )

    def _handle_reduce_output_ready(self, partition_id: int, bundle: RefBundle) -> None:
        """Callback for each yielded (block, metadata) pair from a reducer."""
        self._output_queue.append(bundle)
        self._metrics.on_output_queued(bundle)
        self._metrics.on_task_output_generated(task_index=partition_id, output=bundle)
        _, num_outputs, num_rows = estimate_total_num_of_blocks(
            self._num_reduce_tasks_submitted,
            self.upstream_op_num_outputs(),
            self._metrics,
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
        """Callback when a reduce streaming generator finishes."""
        self._shuffle_reduce_tasks.pop(partition_id, None)
        self._metrics.on_task_finished(
            task_index=partition_id,
            exception=exc,
            task_exec_stats=task_exec_stats,
            task_exec_driver_stats=task_exec_driver_stats,
        )
        if exc:
            logger.error(
                "Reduce of partition %d failed: %s",
                partition_id,
                exc,
                exc_info=exc,
            )

    def has_next(self) -> bool:
        return len(self._output_queue) > 0

    def _get_next_inner(self) -> RefBundle:
        bundle: RefBundle = self._output_queue.popleft()
        self._metrics.on_output_dequeued(bundle)
        self._output_blocks_stats.extend(to_stats(bundle.metadata))
        return bundle

    def get_active_tasks(self) -> List[OpTask]:
        return list(self._shuffle_reduce_tasks.values())

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

    def _do_shutdown(self, force: bool = False) -> None:
        super()._do_shutdown(force)
        self._shuffle_reduce_tasks.clear()
        self._output_queue.clear()
        # ShuffleManager actors + handle refs are owned by ShuffleMapOpV3
        # (via its ``_shared_handles_ref`` + pinned input bundles). Its
        # own ``_do_shutdown`` releases them; nothing to clean here.

    # Stats / progress
    def get_stats(self) -> Dict[str, List[BlockStats]]:
        return {self._name: self._output_blocks_stats}

    def num_output_rows_total(self) -> Optional[int]:
        upstream = self.input_dependencies[0]
        assert isinstance(upstream, ShuffleMapOpV3)
        return upstream.num_output_rows_total()

    def current_logical_usage(self) -> ExecutionResources:
        usage = ExecutionResources.zero()
        for task in self._shuffle_reduce_tasks.values():
            bundle = task.get_requested_resource_bundle()
            if bundle is None:
                continue
            usage = usage.add(ExecutionResources(cpu=bundle.cpu, memory=bundle.memory))
        return usage

    def incremental_resource_usage(self) -> ExecutionResources:
        """Per-task resource ask for the framework's budget allocator.

        Uses the upstream mapper op's per-partition decoded byte totals
        (same source v2 uses, see shuffle_reduce_operator.py:311-324).
        The avg-over-partitions estimate matches v2's policy: it's a
        typical-case admission hint; per-task ``.options(memory=...)``
        in ``_dispatch_one_reducer`` uses the *exact* per-partition bytes
        so skewed partitions are sized correctly at Ray-core level.
        """
        upstream = self.input_dependencies[0]
        assert isinstance(upstream, ShuffleMapOpV3)
        partition_bytes = upstream.get_partition_bytes()
        memory = 0
        sizes = [b for b in partition_bytes.values() if b > 0]
        if sizes:
            avg_bytes = sum(sizes) / len(sizes)
            memory = int(avg_bytes * SHUFFLE_PEAK_MEMORY_MULTIPLIER)
        return ExecutionResources(cpu=self._reduce_num_cpus, memory=memory)

    def min_scheduling_resources(self) -> ExecutionResources:
        return self.incremental_resource_usage()

    def progress_str(self) -> str:
        submitted = self._num_reduce_tasks_submitted
        done = submitted - len(self._shuffle_reduce_tasks)
        return f"reduce: {done}/{submitted}"

    def get_sub_progress_bar_names(self) -> Optional[List[str]]:
        return ["Reduce"]

    def set_sub_progress_bar(self, name: str, pg: "BaseProgressBar") -> None:
        if name == "Reduce":
            self._reduce_bar = pg

import functools
import math
from collections import deque
from typing import Deque, Dict, List, Optional

import ray
from ray.data._internal.execution.bundle_queue import BaseBundleQueue, FIFOBundleQueue
from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    MetadataOpTask,
    OpTask,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
)
from ray.data._internal.planner.exchange.sort_task_spec import (
    SortKey,
    SortTaskSpec,
    _sample_block,
)
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.stats import StatsDict
from ray.data._internal.util import _estimate_available_parallelism
from ray.data.block import Block
from ray.data.context import DataContext
from ray.types import ObjectRef

SORT_SAMPLE_ROWS_PER_BLOCK = 20

SORT_SAMPLE_TASK_NUM_CPUS = 1.0


class SortSamplingOp(InternalQueueOperatorMixin, PhysicalOperator):
    """Sample every input block and forward it after boundaries are available.

    Sampling starts as each block arrives, so it overlaps with upstream execution.
    The original bundles are retained until the upstream and all sampling tasks are
    complete because range partitioning requires one set of global boundaries.

    This is a custom operator instead of a ``MapOperator`` because sampling produces
    driver-side metadata while forwarding the original block references unchanged.
    """

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        *,
        num_partitions: int,
        sort_key: SortKey,
        estimated_num_input_blocks: Optional[int] = None,
        name: str = "SortSample",
    ):
        if num_partitions <= 0:
            raise ValueError("num_partitions must be positive")

        super().__init__(
            name=name,
            input_dependencies=[input_op],
            data_context=data_context,
        )

        self._num_partitions = num_partitions
        self._sort_key = sort_key
        if estimated_num_input_blocks:
            rows_per_block = math.ceil(
                SortTaskSpec.SORT_SAMPLE_POINTS_PER_PARTITION
                * num_partitions
                / estimated_num_input_blocks
            )
        else:
            rows_per_block = SORT_SAMPLE_ROWS_PER_BLOCK
        self._sample_rows_per_block = max(1, rows_per_block)
        self._input_buffer = FIFOBundleQueue()
        self._output_buffer = FIFOBundleQueue()

        self._sample_tasks: Dict[int, MetadataOpTask] = {}
        self._sample_results: List[Block] = []
        self._next_sample_task_idx = 0
        # This op opts out of the resource allocator while sampling (see
        # ``throttling_disabled``), so it bounds its own task submission: at most
        # one in-flight sampling task per available CPU, the rest wait here.
        self._pending_sample_block_refs: Deque[ObjectRef[Block]] = deque()
        self._max_sample_tasks_in_flight: Optional[int] = None
        self._sample_resource_usage = ExecutionResources.zero()
        self._boundaries: Optional[List] = None
        self._stats: StatsDict = {name: []}

    @property
    def boundaries(self) -> Optional[List]:
        return self._boundaries

    @property
    def _input_queues(self) -> List[BaseBundleQueue]:
        return [self._input_buffer]

    @property
    def _output_queues(self) -> List[BaseBundleQueue]:
        return [self._output_buffer]

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert input_index == 0
        # Validate at execution time because schemas after UDFs may be unknown
        # while the physical plan is being constructed.
        self._sort_key.validate_schema(refs.schema)

        self._input_buffer.add(refs)
        self._metrics.on_input_queued(refs, input_index=input_index)

        self._pending_sample_block_refs.extend(refs.block_refs)
        self._submit_pending_sample_tasks()

    def _submit_pending_sample_tasks(self) -> None:
        if self._max_sample_tasks_in_flight is None:
            self._max_sample_tasks_in_flight = max(1, _estimate_available_parallelism())
        while (
            self._pending_sample_block_refs
            and len(self._sample_tasks) < self._max_sample_tasks_in_flight
        ):
            self._submit_sample_task(self._pending_sample_block_refs.popleft())

    def _submit_sample_task(self, block_ref: ObjectRef[Block]) -> None:
        sample_block = cached_remote_fn(_sample_block)
        label_selector = self.data_context.execution_options.label_selector
        if label_selector:
            sample_block = sample_block.options(label_selector=label_selector)

        task_idx = self._next_sample_task_idx
        self._next_sample_task_idx += 1
        resources = ExecutionResources(cpu=SORT_SAMPLE_TASK_NUM_CPUS)
        sample_ref = sample_block.remote(
            block_ref,
            self._sample_rows_per_block,
            self._sort_key,
        )
        self._sample_tasks[task_idx] = MetadataOpTask(
            task_index=task_idx,
            object_ref=sample_ref,
            task_done_callback=functools.partial(
                self._handle_sample_done,
                task_idx,
            ),
            task_resource_bundle=resources,
        )
        self._sample_resource_usage = self._sample_resource_usage.add(resources)

    def _handle_sample_done(self, task_idx: int) -> None:
        task = self._sample_tasks.pop(task_idx)
        requested = task.get_requested_resource_bundle()
        assert requested is not None
        self._sample_resource_usage = self._sample_resource_usage.subtract(requested)
        self._sample_results.append(ray.get(task.get_waitable()))
        self._submit_pending_sample_tasks()
        self._maybe_finish_sampling()

    def all_inputs_done(self) -> None:
        super().all_inputs_done()
        self._maybe_finish_sampling()

    def _maybe_finish_sampling(self) -> None:
        if (
            not self._inputs_complete
            or self._sample_tasks
            or self._pending_sample_block_refs
        ):
            return
        if self._boundaries is not None:
            return

        boundaries = SortTaskSpec.get_boundaries_from_samples(
            self._sample_results,
            self._sort_key,
            self._num_partitions,
        )
        empty_boundary = tuple(None for _ in self._sort_key.get_columns())
        self._boundaries = [
            empty_boundary if boundary is None else boundary for boundary in boundaries
        ]
        self._sample_results.clear()

        while self._input_buffer.has_next():
            bundle = self._input_buffer.get_next()
            self._metrics.on_input_dequeued(bundle, input_index=0)
            self._output_buffer.add(bundle)
            self._metrics.on_output_queued(bundle)

    def has_next(self) -> bool:
        return self._output_buffer.has_next()

    def _get_next_inner(self) -> RefBundle:
        bundle = self._output_buffer.get_next()
        self._metrics.on_output_dequeued(bundle)
        return bundle

    def get_active_tasks(self) -> List[OpTask]:
        return list(self._sample_tasks.values())

    def get_stats(self) -> StatsDict:
        return self._stats

    def num_outputs_total(self) -> Optional[int]:
        return self.input_dependencies[0].num_outputs_total()

    def num_output_rows_total(self) -> Optional[int]:
        return self.input_dependencies[0].num_output_rows_total()

    def throttling_disabled(self) -> bool:
        # Opt out of resource allocation until boundaries exist. While sampling,
        # this op holds every upstream block and emits nothing, so every throttle
        # the executor evaluates against it (ResourceBudget output limits,
        # DownstreamCapacity, and the liveness unblock in ``should_unblock``)
        # sees a consumer that never drains and stalls the upstream op; with the
        # idle detector as the only escape, blocks trickle in at ~1 per 10 s
        # (11.92 GiB sort: 366 s vs 20 s). Being ineligible makes the executor
        # judge the upstream against the next eligible op, the idle
        # ``SortShuffleMapOp``, which is the shape the allocator already handles
        # for ``AllToAllOperator``. Sampling task submission is bounded in-op
        # instead (``_max_sample_tasks_in_flight``).
        return self._boundaries is None

    def current_logical_usage(self) -> ExecutionResources:
        return self._sample_resource_usage

    def incremental_resource_usage(self) -> ExecutionResources:
        return ExecutionResources(cpu=SORT_SAMPLE_TASK_NUM_CPUS)

    def progress_str(self) -> str:
        completed = self._next_sample_task_idx - len(self._sample_tasks)
        total = self._next_sample_task_idx + len(self._pending_sample_block_refs)
        return f"sample: {completed}/{total}"

    def _do_shutdown(self, force: bool = False) -> None:
        super()._do_shutdown(force)
        self._sample_tasks.clear()
        self._pending_sample_block_refs.clear()
        for queue in (self._input_buffer, self._output_buffer):
            while queue.has_next():
                queue.get_next().destroy_if_owned()
        self._sample_results.clear()

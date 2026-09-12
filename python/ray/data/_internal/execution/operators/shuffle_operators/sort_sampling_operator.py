import functools
from typing import Dict, List, Optional

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
from ray.data.block import Block
from ray.data.context import DataContext
from ray.types import ObjectRef

# Keeping the sample size independent of the final number of input blocks lets
# sampling pipeline with upstream execution. Adaptive sampling can be added
# separately if large block counts make the total sample set too expensive.
SORT_SAMPLE_ROWS_PER_BLOCK = 20


class SortSamplingOp(InternalQueueOperatorMixin, PhysicalOperator):
    """Sample every input block and forward it after boundaries are available.

    Sampling starts as each block arrives, so it overlaps with upstream execution.
    The original bundles are retained until the upstream and all sampling tasks are
    complete because range partitioning requires one set of global boundaries.

    This is a custom operator instead of a ``MapOperator`` because sampling produces
    driver-side metadata while forwarding the original block references unchanged.
    It intentionally does not support operator fusion for now.
    """

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        *,
        num_partitions: int,
        sort_key: SortKey,
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
        self._input_buffer = FIFOBundleQueue()
        self._output_buffer = FIFOBundleQueue()

        self._sample_tasks: Dict[int, MetadataOpTask] = {}
        self._sample_results: List[Block] = []
        self._next_sample_task_idx = 0
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

        for block_ref in refs.block_refs:
            self._submit_sample_task(block_ref)

    def _submit_sample_task(self, block_ref: ObjectRef[Block]) -> None:
        sample_block = cached_remote_fn(_sample_block)
        label_selector = self.data_context.execution_options.label_selector
        if label_selector:
            sample_block = sample_block.options(label_selector=label_selector)

        task_idx = self._next_sample_task_idx
        self._next_sample_task_idx += 1
        resources = ExecutionResources(cpu=1)
        sample_ref = sample_block.remote(
            block_ref,
            SORT_SAMPLE_ROWS_PER_BLOCK,
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
        self._maybe_finish_sampling()

    def all_inputs_done(self) -> None:
        super().all_inputs_done()
        self._maybe_finish_sampling()

    def _maybe_finish_sampling(self) -> None:
        if not self._inputs_complete or self._sample_tasks:
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
        # Until boundaries are available, this operator is a materialization
        # barrier that must consume every upstream block. Throttling it based on
        # retained object-store memory could prevent the remaining blocks from
        # arriving, so match the legacy blocking all-to-all behavior here.
        return self._boundaries is None

    def current_logical_usage(self) -> ExecutionResources:
        return self._sample_resource_usage

    def incremental_resource_usage(self) -> ExecutionResources:
        return ExecutionResources(cpu=1)

    def progress_str(self) -> str:
        completed = self._next_sample_task_idx - len(self._sample_tasks)
        return f"sample: {completed}/{self._next_sample_task_idx}"

    def _do_shutdown(self, force: bool = False) -> None:
        super()._do_shutdown(force)
        self._sample_tasks.clear()
        for queue in (self._input_buffer, self._output_buffer):
            while queue.has_next():
                queue.get_next().destroy_if_owned()
        self._sample_results.clear()

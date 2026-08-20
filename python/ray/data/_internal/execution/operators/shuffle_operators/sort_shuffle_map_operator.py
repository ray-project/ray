import functools
import typing
from collections import deque
from typing import Any, Deque, Dict, List, Optional

import ray
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
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
    ShuffleMapOp,
)
from ray.data._internal.execution.operators.sort_shuffle import make_range_partition_fn
from ray.data._internal.planner.exchange.sort_task_spec import (
    SortKey,
    SortTaskSpec,
    _sample_block,
)
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.util import _estimate_available_parallelism
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.types import ObjectRef

if typing.TYPE_CHECKING:
    from ray.data._internal.progress.base_progress import BaseProgressBar


class SortShuffleMapOp(ShuffleMapOp):
    """Shuffle-v2 map phase with range-boundary sampling.

    Before boundaries are known, all input bundles are retained by the driver.
    Once all inputs arrive, every block is sampled with bounded task concurrency
    so the resulting range boundaries represent the entire dataset without
    flooding Ray Core with pending tasks. After sampling finishes, the buffered
    bundles are replayed into ``ShuffleMapOp`` with a local-sort/range-partition
    function.

    User-provided boundaries bypass the sampling phase.
    """

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        *,
        num_partitions: int,
        sort_key: SortKey,
        map_runtime_env: Optional[Dict[str, Any]] = None,
        map_cpus: float = ShuffleMapOp._DEFAULT_SHUFFLE_MAP_TASK_NUM_CPUS,
        name: str = "SortShuffleMap",
    ):
        if num_partitions <= 0:
            raise ValueError("num_partitions must be positive")

        self._sort_key = sort_key
        self._buffered_bundles = FIFOBundleQueue()
        self._pending_sample_block_refs: Deque[ObjectRef[Block]] = deque()
        self._sample_tasks: Dict[int, MetadataOpTask] = {}
        self._sample_results: List[Block] = []
        self._num_sample_tasks_total = 0
        self._num_samples_per_block: Optional[int] = None
        self._max_num_sampling_tasks_in_flight: Optional[int] = None
        self._next_sample_task_idx = 0
        self._sampling_started = False
        self._sample_resource_usage = ExecutionResources.zero()
        self._sample_bar: Optional["BaseProgressBar"] = None

        boundaries = self._user_boundaries(sort_key)
        self._boundaries: Optional[List] = boundaries
        partition_fn = (
            make_range_partition_fn(boundaries, sort_key, data_context)
            if boundaries is not None
            else self._uninitialized_partition_fn
        )

        super().__init__(
            input_op,
            data_context,
            num_partitions=num_partitions,
            partition_fn=partition_fn,
            map_runtime_env=map_runtime_env,
            map_cpus=map_cpus,
            name=name,
        )

    @staticmethod
    def _user_boundaries(sort_key: SortKey) -> Optional[List]:
        if not sort_key.boundaries:
            return None
        boundaries = [(boundary,) for boundary in sort_key.boundaries]
        if sort_key.get_descending()[0]:
            boundaries.reverse()
        return boundaries

    @staticmethod
    def _uninitialized_partition_fn(block):
        raise RuntimeError("Sort shuffle boundaries have not been sampled yet")

    @property
    def boundaries(self) -> Optional[List]:
        return self._boundaries

    @property
    def _input_queues(self) -> List[BaseBundleQueue]:
        return [self._buffered_bundles, *super()._input_queues]

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert input_index == 0
        # Match sort V1's validation before any sampling or partitioning work.
        # Validate at execution time because schemas after UDFs may be unknown
        # while the physical plan is being constructed.
        self._sort_key.validate_schema(refs.schema)
        if self._boundaries is not None:
            super()._add_input_inner(refs, input_index)
            return

        self._buffered_bundles.add(refs)
        self._metrics.on_input_queued(refs, input_index=input_index)
        self._pending_sample_block_refs.extend(refs.block_refs)

    def _get_max_num_sampling_tasks_in_flight(self) -> int:
        # Sampling tasks each require one CPU. Keep at most one task submitted per
        # available CPU so datasets with many blocks don't flood Ray Core with an
        # unbounded number of pending tasks. The estimate also accounts for the
        # current placement group when Dataset execution is colocated with one.
        return max(1, _estimate_available_parallelism())

    def _start_sampling(self) -> None:
        if self._sampling_started or self._boundaries is not None:
            return
        assert self._inputs_complete
        self._sampling_started = True

        if not self._pending_sample_block_refs:
            self._set_boundaries([None] * (self._num_partitions - 1))
            return

        # Match sort V1's 10 samples-per-output-partition budget while ensuring
        # that every non-empty block contributes at least one sample.
        self._num_sample_tasks_total = len(self._pending_sample_block_refs)
        self._num_samples_per_block = max(
            1,
            int(self._num_partitions * 10 / self._num_sample_tasks_total),
        )
        self._max_num_sampling_tasks_in_flight = (
            self._get_max_num_sampling_tasks_in_flight()
        )
        if self._sample_bar is not None:
            self._sample_bar.update(
                total=self._num_sample_tasks_total * self._num_samples_per_block
            )
        self._submit_available_sample_tasks()

    def _submit_available_sample_tasks(self) -> None:
        assert self._sampling_started
        assert self._num_samples_per_block is not None
        assert self._max_num_sampling_tasks_in_flight is not None

        sample_block = cached_remote_fn(_sample_block)
        label_selector = self.data_context.execution_options.label_selector
        if label_selector:
            sample_block = sample_block.options(label_selector=label_selector)

        resources = ExecutionResources(cpu=1)
        while (
            self._pending_sample_block_refs
            and len(self._sample_tasks) < self._max_num_sampling_tasks_in_flight
        ):
            block_ref = self._pending_sample_block_refs.popleft()
            task_idx = self._next_sample_task_idx
            self._next_sample_task_idx += 1
            sample_ref = sample_block.remote(
                block_ref, self._num_samples_per_block, self._sort_key
            )
            self._sample_tasks[task_idx] = MetadataOpTask(
                task_index=task_idx,
                object_ref=sample_ref,
                task_done_callback=functools.partial(
                    self._handle_sample_done, task_idx
                ),
                task_resource_bundle=resources,
            )
            self._sample_resource_usage = self._sample_resource_usage.add(resources)

    def _handle_sample_done(self, task_idx: int) -> None:
        task = self._sample_tasks.pop(task_idx)
        requested = task.get_requested_resource_bundle()
        assert requested is not None
        self._sample_resource_usage = self._sample_resource_usage.subtract(requested)

        sample = ray.get(task.get_waitable())
        self._sample_results.append(sample)
        if self._sample_bar is not None:
            self._sample_bar.update(
                increment=BlockAccessor.for_block(sample).num_rows()
            )

        self._submit_available_sample_tasks()
        if not self._pending_sample_block_refs and not self._sample_tasks:
            boundaries = SortTaskSpec.get_boundaries_from_samples(
                self._sample_results,
                self._sort_key,
                self._num_partitions,
            )
            self._set_boundaries(boundaries)

    def _set_boundaries(self, boundaries: List) -> None:
        # The shared V1 sampling helper represents an empty dataset with bare
        # ``None`` boundaries. Range partitioning expects each boundary to be a
        # tuple, so normalize the empty-dataset fallback to the one-column key
        # shape accepted by ``find_partition_index``.
        boundaries = [
            (None,) if boundary is None else boundary for boundary in boundaries
        ]
        self._boundaries = boundaries
        self._pending_sample_block_refs.clear()
        self._sample_results.clear()
        self._partition_fn = make_range_partition_fn(
            boundaries, self._sort_key, self.data_context
        )
        while self._buffered_bundles.has_next():
            bundle = self._buffered_bundles.get_next()
            self._metrics.on_input_dequeued(bundle, input_index=0)
            super()._add_input_inner(bundle, 0)
        if self._inputs_complete:
            self._finish_map_inputs()

    def all_inputs_done(self) -> None:
        # Mark the upstream complete without flushing ShuffleMapOp's merge
        # buffers until range boundaries are available.
        PhysicalOperator.all_inputs_done(self)
        if self._boundaries is None:
            self._start_sampling()
        else:
            self._finish_map_inputs()

    def get_active_tasks(self) -> List[OpTask]:
        return [*self._sample_tasks.values(), *super().get_active_tasks()]

    def has_execution_finished(self) -> bool:
        if (
            self._pending_sample_block_refs
            or self._sample_tasks
            or self._buffered_bundles
            or self._boundaries is None
        ):
            return False
        return super().has_execution_finished()

    def has_completed(self) -> bool:
        return (
            not self._pending_sample_block_refs
            and not self._sample_tasks
            and not self._buffered_bundles
            and self._boundaries is not None
            and super().has_completed()
        )

    def current_logical_usage(self) -> ExecutionResources:
        return super().current_logical_usage().add(self._sample_resource_usage)

    def progress_str(self) -> str:
        sample_done = self._next_sample_task_idx - len(self._sample_tasks)
        sample_progress = f"sample: {sample_done}/{self._num_sample_tasks_total}"
        return f"{sample_progress}, {super().progress_str()}"

    def get_sub_progress_bar_names(self) -> Optional[List[str]]:
        return [SortTaskSpec.SORT_SAMPLE_SUB_PROGRESS_BAR_NAME, "Map"]

    def set_sub_progress_bar(self, name: str, pg: "BaseProgressBar") -> None:
        if name == SortTaskSpec.SORT_SAMPLE_SUB_PROGRESS_BAR_NAME:
            self._sample_bar = pg
        else:
            super().set_sub_progress_bar(name, pg)

    def _do_shutdown(self, force: bool = False) -> None:
        super()._do_shutdown(force)
        self._sample_tasks.clear()
        for bundle in self._buffered_bundles:
            bundle.destroy_if_owned()
        self._buffered_bundles.clear()
        self._pending_sample_block_refs.clear()
        self._sample_results.clear()

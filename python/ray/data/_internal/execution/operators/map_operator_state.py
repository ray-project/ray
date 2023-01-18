from dataclasses import dataclass
from typing import Callable, Optional, List, Dict, Any, Union, Tuple, Iterator

import ray
from ray.data.block import Block, BlockMetadata
from ray.data.context import DatasetContext
from ray.data._internal.compute import (
    ComputeStrategy,
    TaskPoolStrategy,
    ActorPoolStrategy,
)
from ray.data._internal.execution.util import merge_ref_bundles
from ray.data._internal.execution.interfaces import (
    RefBundle,
    ExecutionResources,
)
from ray.data._internal.execution.operators.map_task_submitter import MapTaskSubmitter
from ray.data._internal.execution.operators.actor_pool_submitter import (
    ActorPoolSubmitter,
)
from ray.data._internal.execution.operators.task_pool_submitter import TaskPoolSubmitter
from ray.data._internal.memory_tracing import trace_allocation
from ray.types import ObjectRef
from ray._raylet import ObjectRefGenerator


class MapOperatorState:
    def __init__(
        self,
        transform_fn: Callable[[Iterator[Block]], Iterator[Block]],
        compute_strategy: ComputeStrategy,
        ray_remote_args: Optional[Dict[str, Any]],
        min_rows_per_bundle: Optional[int],
        incremental_cpu: int,
        incremental_gpu: int,
    ):
        # Execution arguments.
        self._min_rows_per_bundle: Optional[int] = min_rows_per_bundle

        # Put the function def in the object store to avoid repeated serialization
        # in case it's large (i.e., closure captures large objects).
        transform_fn_ref = ray.put(transform_fn)

        # Submitter of Ray tasks mapping transform_fn over data.
        if ray_remote_args is None:
            ray_remote_args = {}
        if isinstance(compute_strategy, TaskPoolStrategy):
            task_submitter = TaskPoolSubmitter(transform_fn_ref, ray_remote_args)
        elif isinstance(compute_strategy, ActorPoolStrategy):
            # TODO(Clark): Better mapping from configured min/max pool size to static
            # pool size?
            pool_size = compute_strategy.max_size
            if pool_size == float("inf"):
                # Use min_size if max_size is unbounded (default).
                pool_size = compute_strategy.min_size
            task_submitter = ActorPoolSubmitter(
                transform_fn_ref, ray_remote_args, pool_size
            )
        else:
            raise ValueError(f"Unsupported execution strategy {compute_strategy}")
        self._task_submitter: MapTaskSubmitter = task_submitter
        # Whether we have started the task submitter yet.
        self._have_started_submitter = False

        # The temporary block bundle used to accumulate inputs until they meet the
        # min_rows_per_bundle requirement.
        self._block_bundle: Optional[RefBundle] = None

        # Execution state.
        self._tasks: Dict[ObjectRef[Union[ObjectRefGenerator, Block]], _TaskState] = {}
        self._tasks_by_output_order: Dict[int, _TaskState] = {}
        self._next_task_index: int = 0
        self._next_output_index: int = 0
        self._obj_store_mem_alloc: int = 0
        self._obj_store_mem_freed: int = 0
        self._obj_store_mem_cur: int = 0
        self._obj_store_mem_peak: int = 0
        self._incremental_cpu: int = incremental_cpu
        self._incremental_gpu: int = incremental_gpu

    def progress_str(self) -> str:
        return self._task_submitter.progress_str()

    def add_input(self, bundle: RefBundle) -> None:
        if not self._have_started_submitter:
            # Start the task submitter on the first input.
            self._task_submitter.start()
            self._have_started_submitter = True

        if self._min_rows_per_bundle is None:
            self._create_task(bundle)
            return

        def get_num_rows(bundle: Optional[RefBundle]):
            if bundle is None:
                return 0
            if bundle.num_rows() is None:
                return float("inf")
            return bundle.num_rows()

        bundle_rows = get_num_rows(bundle)
        acc_num_rows = get_num_rows(self._block_bundle) + bundle_rows
        if acc_num_rows > self._min_rows_per_bundle:
            if self._block_bundle:
                if get_num_rows(self._block_bundle) > 0:
                    self._create_task(self._block_bundle)
                self._block_bundle = bundle
            else:
                self._create_task(bundle)
        else:
            # TODO(ekl) add a warning if we merge 10+ blocks per bundle.
            self._block_bundle = merge_ref_bundles(self._block_bundle, bundle)

    def inputs_done(self) -> None:
        if self._block_bundle:
            self._create_task(self._block_bundle)
            self._block_bundle = None
        self._task_submitter.task_submission_done()

    def work_completed(self, ref: ObjectRef[Union[ObjectRefGenerator, Block]]) -> None:
        self._task_submitter.task_done(ref)
        task: _TaskState = self._tasks.pop(ref)
        if task.block_metadata_ref is not None:
            # Non-dynamic block splitting path.
            # TODO(Clark): Remove this special case once dynamic block splitting is
            # supported for actors.
            block_refs = [ref]
            block_metas = [ray.get(task.block_metadata_ref)]
        else:
            # Dynamic block splitting path.
            all_refs = list(ray.get(ref))
            del ref
            block_refs = all_refs[:-1]
            block_metas = ray.get(all_refs[-1])
        assert len(block_metas) == len(block_refs), (block_refs, block_metas)
        for ref in block_refs:
            trace_allocation(ref, "map_operator_work_completed")
        task.output = RefBundle(list(zip(block_refs, block_metas)), owns_blocks=True)
        allocated = task.output.size_bytes()
        self._obj_store_mem_alloc += allocated
        self._obj_store_mem_cur += allocated
        # TODO(ekl) this isn't strictly correct if multiple operators depend on this
        # bundle, but it doesn't happen in linear dags for now.
        task.inputs.destroy_if_owned()
        freed = task.inputs.size_bytes()
        self._obj_store_mem_freed += freed
        self._obj_store_mem_cur -= freed
        if self._obj_store_mem_cur > self._obj_store_mem_peak:
            self._obj_store_mem_peak = self._obj_store_mem_cur

    def has_next(self) -> bool:
        i = self._next_output_index
        return (
            i in self._tasks_by_output_order
            and self._tasks_by_output_order[i].output is not None
        )

    def get_next(self) -> RefBundle:
        i = self._next_output_index
        self._next_output_index += 1
        bundle = self._tasks_by_output_order.pop(i).output
        self._obj_store_mem_cur -= bundle.size_bytes()
        return bundle

    def get_work_refs(self) -> List[ray.ObjectRef]:
        return list(self._tasks.keys())

    def num_active_work_refs(self) -> int:
        return len(self._tasks)

    def shutdown(self) -> None:
        self._task_submitter.shutdown(self.get_work_refs())

    @property
    def obj_store_mem_alloc(self) -> int:
        """Return the object store memory allocated by this operator execution."""
        return self._obj_store_mem_alloc

    @property
    def obj_store_mem_freed(self) -> int:
        """Return the object store memory freed by this operator execution."""
        return self._obj_store_mem_freed

    @property
    def obj_store_mem_peak(self) -> int:
        """Return the peak object store memory utilization during this operator
        execution.
        """
        return self._obj_store_mem_peak

    def _create_task(self, bundle: RefBundle) -> None:
        input_blocks = []
        for block, _ in bundle.blocks:
            input_blocks.append(block)
        # TODO fix for Ray client: https://github.com/ray-project/ray/issues/30458
        if not DatasetContext.get_current().block_splitting_enabled:
            raise NotImplementedError("New backend requires block splitting")
        ref: Union[
            ObjectRef[ObjectRefGenerator],
            Tuple[ObjectRef[Block], ObjectRef[BlockMetadata]],
        ] = self._task_submitter.submit(input_blocks)
        task = _TaskState(bundle)
        if isinstance(ref, tuple):
            # Task submitter returned a block ref and block metadata ref tuple; we make
            # the block ref the canonical task ref, and store the block metadata ref for
            # future resolution, when the task completes.
            # TODO(Clark): Remove this special case once dynamic block splitting is
            # supported for actors.
            ref, block_metadata_ref = ref
            task.block_metadata_ref = block_metadata_ref
        self._tasks[ref] = task
        self._tasks_by_output_order[self._next_task_index] = task
        self._next_task_index += 1
        self._obj_store_mem_cur += bundle.size_bytes()
        if self._obj_store_mem_cur > self._obj_store_mem_peak:
            self._obj_store_mem_peak = self._obj_store_mem_cur

    def current_resource_usage(self) -> ExecutionResources:
        if isinstance(self._task_submitter, ActorPoolSubmitter):
            num_active_workers = self._task_submitter._actor_pool.size()
        else:
            num_active_workers = self.num_active_work_refs()
        return ExecutionResources(
            cpu=self._incremental_cpu * num_active_workers,
            gpu=self._incremental_gpu * num_active_workers,
            object_store_memory=self._obj_store_mem_cur,
        )

    def incremental_resource_usage(self) -> ExecutionResources:
        if isinstance(self._task_submitter, ActorPoolSubmitter):
            # TODO(ekl) this should be non-zero for autoscaling actor pools.
            return ExecutionResources(cpu=0, gpu=0)
        return ExecutionResources(
            cpu=self._incremental_cpu,
            gpu=self._incremental_gpu,
        )


@dataclass
class _TaskState:
    """Tracks the driver-side state for an MapOperator task.

    Attributes:
        inputs: The input ref bundle.
        output: The output ref bundle that is set when the task completes.
        block_metadata_ref: A future for the block metadata; this will only be set for
            the ActorPoolTaskSubmitter, which doesn't yet support dynamic block
            splitting.
    """

    inputs: RefBundle
    output: Optional[RefBundle] = None
    #  TODO(Clark): Remove this once dynamic block splitting is supported for actors.
    block_metadata_ref: Optional[ObjectRef[BlockMetadata]] = None

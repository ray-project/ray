from typing import Callable, Optional, List, Dict, TYPE_CHECKING

import ray
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.memory_tracing import trace_allocation
from ray.data._internal.execution.interfaces import (
    RefBundle,
)
from ray.data._internal.execution.util import _merge_ref_bundles
from ray.data.block import Block, BlockAccessor, BlockExecStats
from ray.types import ObjectRef
from ray._raylet import ObjectRefGenerator

if TYPE_CHECKING:
    from ray.data._internal.execution.operators.map_operator import MapOperator


def _map_task(fn: Callable, *blocks: List[Block]):
    """Remote function for a single operator task.

    Args:
        fn: The callable that takes Iterator[Block] as input and returns
            Iterator[Block] as output.
        blocks: The concrete block values from the task ref bundle.

    Returns:
        A generator of blocks, followed by the list of BlockMetadata for the blocks
        as the last generator return.
    """
    output_metadata = []
    stats = BlockExecStats.builder()
    for b_out in fn(blocks):
        m_out = BlockAccessor.for_block(b_out).get_metadata([], None)
        m_out.exec_stats = stats.build()
        output_metadata.append(m_out)
        yield b_out
        stats = BlockExecStats.builder()
    yield output_metadata


class _TaskState:
    """Tracks the driver-side state for an MapOperator task.

    Attributes:
        inputs: The input ref bundle.
        output: The output ref bundle that is set when the task completes.
    """

    def __init__(self, inputs: RefBundle):
        self.inputs: RefBundle = inputs
        self.output: Optional[RefBundle] = None


class MapOperatorTasksImpl:
    def __init__(self, op: "MapOperator"):
        self._transform_fn = op.get_transform_fn()
        self._ray_remote_args = op.ray_remote_args()
        self._tasks: Dict[ObjectRef[ObjectRefGenerator], _TaskState] = {}
        self._tasks_by_output_order: Dict[int, _TaskState] = {}
        self._block_bundle = None
        self._target_block_size = op.target_block_size
        self._input_deps_done = 0
        self._op = op
        self._next_task_index = 0
        self._next_output_index = 0
        self._obj_store_mem_alloc = 0
        self._obj_store_mem_freed = 0
        self._obj_store_mem_cur = 0
        self._obj_store_mem_peak = 0

    def add_input(self, bundle: RefBundle) -> None:
        if self._target_block_size is None:
            self._create_task(bundle)
            return

        def get_num_rows(bundle: RefBundle):
            if bundle is None:
                return 0
            if bundle.num_rows() is None:
                return float("inf")
            return bundle.num_rows()

        bundle_rows = get_num_rows(bundle)
        if bundle_rows == 0:
            return

        num_rows = get_num_rows(self._block_bundle) + bundle_rows
        if num_rows > self._target_block_size:
            if self._block_bundle:
                self._create_task(self._block_bundle)
                self._block_bundle = bundle
            else:
                self._create_task(bundle)
        else:
            self._block_bundle = _merge_ref_bundles(self._block_bundle, bundle)

    def inputs_done(self, input_index: int) -> None:
        self._input_deps_done += 1
        assert self._input_deps_done <= len(self._op._input_dependencies)
        if (
            self._input_deps_done == len(self._op._input_dependencies)
            and self._block_bundle
        ):
            self._create_task(self._block_bundle)
            self._block_bundle = None

    def work_completed(self, ref: ObjectRef[ObjectRefGenerator]) -> None:
        task = self._tasks.pop(ref)
        all_refs = list(ray.get(ref))
        block_refs = all_refs[:-1]
        block_metas = ray.get(all_refs[-1])
        del ref
        assert len(block_metas) == len(block_refs), (block_refs, block_metas)
        for ref in block_refs:
            trace_allocation(ref, "map_operator_work_completed")
        task.output = RefBundle(list(zip(block_refs, block_metas)), owns_blocks=True)
        allocated = task.output.size_bytes()
        self._obj_store_mem_alloc += allocated
        self._obj_store_mem_cur += allocated
        # TODO(ekl) this isn't strictly correct if multiple operators depend on this
        # bundle, but it doesn't happen in linear dags for now.
        freed = task.inputs.destroy_if_owned()
        if freed:
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

    def get_next(self) -> bool:
        i = self._next_output_index
        self._next_output_index += 1
        return self._tasks_by_output_order.pop(i).output

    def get_work_refs(self) -> List[ray.ObjectRef]:
        return list(self._tasks)

    def shutdown(self) -> None:
        # Cancel all active tasks.
        for task in self._tasks:
            ray.cancel(task)
        # Wait until all tasks have failed or been cancelled.
        for task in self._tasks:
            try:
                ray.get(task)
            except ray.exceptions.RayError:
                # Cancellation either succeeded, or the task had already failed with
                # a different error, or cancellation failed. In all cases, we
                # swallow the exception.
                pass

    def _create_task(self, bundle: RefBundle) -> None:
        input_blocks = []
        for block, _ in bundle.blocks:
            input_blocks.append(block)
        map_task = cached_remote_fn(_map_task, num_returns="dynamic")
        generator_ref = map_task.options(**self._ray_remote_args).remote(
            self._transform_fn, *input_blocks
        )
        task = _TaskState(bundle)
        self._tasks[generator_ref] = task
        self._tasks_by_output_order[self._next_task_index] = task
        self._next_task_index += 1
        self._obj_store_mem_cur += bundle.size_bytes()
        if self._obj_store_mem_cur > self._obj_store_mem_peak:
            self._obj_store_mem_peak = self._obj_store_mem_cur

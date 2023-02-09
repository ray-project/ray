from typing import List, Optional, Callable, Iterator, Dict, Any

import ray
from ray.data.block import Block
from ray.data._internal.execution.interfaces import (
    RefBundle,
    ExecutionResources,
    PhysicalOperator,
    TaskContext,
)
from ray.data._internal.execution.operators.map_operator import (
    MapOperator,
    _TaskState,
    _map_task,
)
from ray.data._internal.remote_fn import cached_remote_fn
from ray.types import ObjectRef
from ray._raylet import ObjectRefGenerator


class TaskPoolMapOperator(MapOperator):
    """A MapOperator implementation that executes tasks on a task pool."""

    def __init__(
        self,
        transform_fn: Callable[[Iterator[Block]], Iterator[Block]],
        input_op: PhysicalOperator,
        name: str = "TaskPoolMap",
        min_rows_per_bundle: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """Create an TaskPoolMapOperator instance.

        Args:
            transform_fn: The function to apply to each ref bundle input.
            input_op: Operator generating input data for this op.
            name: The name of this operator.
            min_rows_per_bundle: The number of rows to gather per batch passed to the
                transform_fn, or None to use the block size. Setting the batch size is
                important for the performance of GPU-accelerated transform functions.
                The actual rows passed may be less if the dataset is small.
            ray_remote_args: Customize the ray remote args for this op's tasks.
        """
        super().__init__(
            transform_fn, input_op, name, min_rows_per_bundle, ray_remote_args
        )
        self._tasks: Dict[ObjectRef[ObjectRefGenerator], _TaskState] = {}
        self._next_task_idx = 0

    def _add_bundled_input(self, bundle: RefBundle):
        # Submit the task as a normal Ray task.
        map_task = cached_remote_fn(_map_task, num_returns="dynamic")
        input_blocks = [block for block, _ in bundle.blocks]
        ctx = TaskContext(task_idx=self._next_task_idx)
        ref = map_task.options(**self._ray_remote_args).remote(
            self._transform_fn_ref, ctx, *input_blocks
        )
        self._next_task_idx += 1
        task = _TaskState(bundle)
        self._tasks[ref] = task
        self._handle_task_submitted(task)

    def notify_work_completed(self, ref: ObjectRef[ObjectRefGenerator]):
        task: _TaskState = self._tasks.pop(ref)
        task.output = self._map_ref_to_ref_bundle(ref)
        self._handle_task_done(task)

    def shutdown(self):
        task_refs = self.get_work_refs()
        # Cancel all active tasks.
        for task in task_refs:
            ray.cancel(task)
        # Wait until all tasks have failed or been cancelled.
        for task in task_refs:
            try:
                ray.get(task)
            except ray.exceptions.RayError:
                # Cancellation either succeeded, or the task had already failed with
                # a different error, or cancellation failed. In all cases, we
                # swallow the exception.
                pass
        super().shutdown()

    def progress_str(self) -> str:
        return ""

    def get_work_refs(self) -> List[ray.ObjectRef]:
        return list(self._tasks.keys())

    def num_active_work_refs(self) -> int:
        return len(self.get_work_refs())

    def base_resource_usage(self) -> ExecutionResources:
        return ExecutionResources()

    def current_resource_usage(self) -> ExecutionResources:
        num_active_workers = self.num_active_work_refs()
        return ExecutionResources(
            cpu=self._ray_remote_args.get("num_cpus", 0) * num_active_workers,
            gpu=self._ray_remote_args.get("num_gpus", 0) * num_active_workers,
            object_store_memory=self._metrics.cur,
        )

    def incremental_resource_usage(self) -> ExecutionResources:
        return ExecutionResources(
            cpu=self._ray_remote_args.get("num_cpus", 0),
            gpu=self._ray_remote_args.get("num_gpus", 0),
        )

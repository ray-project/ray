from typing import Any, Dict, Optional

import ray
from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.operators.map_operator import MapOperator, _map_task
from ray.data._internal.execution.operators.map_transformer import MapTransformer
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data.context import DataContext


class TaskPoolMapOperator(MapOperator):
    """A MapOperator implementation that executes tasks on a task pool."""

    def __init__(
        self,
        map_transformer: MapTransformer,
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
            map_transformer, input_op, name, min_rows_per_bundle, ray_remote_args
        )

    def _add_bundled_input(self, bundle: RefBundle):
        # Submit the task as a normal Ray task.
        map_task = cached_remote_fn(_map_task, num_returns="streaming")
        input_blocks = [block for block, _ in bundle.blocks]
        ctx = TaskContext(task_idx=self._next_data_task_idx)
        gen = map_task.options(
            **self._get_runtime_ray_remote_args(input_bundle=bundle), name=self.name
        ).remote(
            self._map_transformer_ref,
            DataContext.get_current(),
            ctx,
            *input_blocks,
        )
        self._submit_data_task(gen, bundle)

    def shutdown(self):
        # Cancel all active tasks.
        for _, task in self._data_tasks.items():
            ray.cancel(task.get_waitable())
        # Wait until all tasks have failed or been cancelled.
        for _, task in self._data_tasks.items():
            try:
                ray.get(task.get_waitable())
            except ray.exceptions.RayError:
                # Cancellation either succeeded, or the task had already failed with
                # a different error, or cancellation failed. In all cases, we
                # swallow the exception.
                pass
        super().shutdown()

    def progress_str(self) -> str:
        return ""

    def base_resource_usage(self) -> ExecutionResources:
        return ExecutionResources()

    def current_resource_usage(self) -> ExecutionResources:
        num_active_workers = self.num_active_tasks()
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

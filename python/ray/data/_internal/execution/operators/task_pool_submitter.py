from typing import Dict, Any, Iterator, Callable, Union, List

import ray
from ray.data.block import Block
from ray.data._internal.execution.operators.map_task_submitter import (
    MapTaskSubmitter,
    _map_task,
)
from ray.data._internal.remote_fn import cached_remote_fn
from ray.types import ObjectRef
from ray._raylet import ObjectRefGenerator


class TaskPoolSubmitter(MapTaskSubmitter):
    """A task submitter for MapOperator that uses normal Ray tasks."""

    def __init__(
        self,
        transform_fn_ref: ObjectRef[Callable[[Iterator[Block]], Iterator[Block]]],
        ray_remote_args: Dict[str, Any],
    ):
        """Create a TaskPoolSubmitter instance.

        Args:
            transform_fn_ref: The function to apply to a block bundle in the submitted
                map task.
            ray_remote_args: Remote arguments for the Ray tasks to be launched.
        """
        self._transform_fn_ref = transform_fn_ref
        self._ray_remote_args = ray_remote_args

    def submit(
        self, input_blocks: List[ObjectRef[Block]]
    ) -> ObjectRef[ObjectRefGenerator]:
        # Submit the task as a normal Ray task.
        map_task = cached_remote_fn(_map_task, num_returns="dynamic")
        return map_task.options(**self._ray_remote_args).remote(
            self._transform_fn_ref, *input_blocks
        )

    def shutdown(self, task_refs: List[ObjectRef[Union[ObjectRefGenerator, Block]]]):
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

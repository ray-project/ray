from typing import Dict, Any, Iterator, Callable, Union, List

import ray
from ray.data.block import Block, BlockAccessor, BlockMetadata, BlockExecStats
from ray.data._internal.execution.operators.map_task_submitter import MapTaskSubmitter
from ray.data._internal.remote_fn import cached_remote_fn
from ray.types import ObjectRef
from ray._raylet import ObjectRefGenerator


class TaskPoolSubmitter(MapTaskSubmitter):
    """A task submitter for MapOperator that uses normal Ray tasks."""

    def __init__(self, ray_remote_args: Dict[str, Any]):
        """Create a TaskPoolSubmitter instance.

        Args:
            ray_remote_args: Remote arguments for the Ray tasks to be launched.
        """
        self._ray_remote_args = ray_remote_args

    def submit(
        self,
        transform_fn: ObjectRef[Callable[[Iterator[Block]], Iterator[Block]]],
        input_blocks: List[ObjectRef[Block]],
    ) -> ObjectRef[ObjectRefGenerator]:
        # Submit the task as a normal Ray task.
        map_task = cached_remote_fn(_map_task, num_returns="dynamic")
        return map_task.options(**self._ray_remote_args).remote(
            transform_fn, *input_blocks
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


def _map_task(
    fn: Callable[[Iterator[Block]], Iterator[Block]],
    *blocks: Block,
) -> Iterator[Union[Block, List[BlockMetadata]]]:
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
    for b_out in fn(iter(blocks)):
        # TODO(Clark): Add input file propagation from input blocks.
        m_out = BlockAccessor.for_block(b_out).get_metadata([], None)
        m_out.exec_stats = stats.build()
        output_metadata.append(m_out)
        yield b_out
        stats = BlockExecStats.builder()
    yield output_metadata

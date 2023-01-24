from abc import ABC, abstractmethod
from typing import Dict, Any, List, Union, Tuple, Callable, Iterator

import ray
from ray.data.block import Block, BlockAccessor, BlockMetadata, BlockExecStats
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
)
from ray.types import ObjectRef
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray._raylet import ObjectRefGenerator


class MapTaskSubmitter(ABC):
    """A task submitter for MapOperator.

    This abstraction is in charge of submitting tasks, reserving resources for their
    execution, and cleaning up said resources when a task completes or when task
    submission is done.
    """

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

    def start(self, options: ExecutionOptions):
        """Start the task submitter so it's ready to submit tasks.

        This is called when execution of the map operator actually starts, and is where
        the submitter can initialize expensive state, reserve resources, start workers,
        etc.
        """
        if options.locality_with_output:
            self._ray_remote_args[
                "scheduling_strategy"
            ] = NodeAffinitySchedulingStrategy(
                ray.get_runtime_context().get_node_id(),
                soft=True,
            )

    @abstractmethod
    def submit(
        self, input_blocks: List[ObjectRef[Block]]
    ) -> Union[
        ObjectRef[ObjectRefGenerator], Tuple[ObjectRef[Block], ObjectRef[BlockMetadata]]
    ]:
        """Submit a map task.

        Args:
            input_blocks: The block bundle on which to apply transform_fn.

        Returns:
            An object ref representing the output of the map task.
        """
        raise NotImplementedError

    def task_done(self, task_ref: ObjectRef[Union[ObjectRefGenerator, Block]]):
        """Indicates that the task that output the provided ref is done.

        Args:
            task_ref: The output ref for the task that's done.
        """
        pass

    def task_submission_done(self):
        """Indicates that no more tasks will be submitter."""
        pass

    def progress_str(self) -> str:
        """Pass through progress string for operators."""
        raise NotImplementedError

    @abstractmethod
    def shutdown(self, task_refs: List[ObjectRef[Union[ObjectRefGenerator, Block]]]):
        """Shutdown the submitter, i.e. release any reserved resources.

        Args:
            task_refs: The output refs for all of the tasks submitted by this submitter.
        """
        raise NotImplementedError


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

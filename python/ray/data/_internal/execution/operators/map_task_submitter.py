from abc import ABC, abstractmethod
from typing import List, Union, Tuple
from ray.data.block import Block, BlockMetadata
from ray.types import ObjectRef
from ray._raylet import ObjectRefGenerator


class MapTaskSubmitter(ABC):
    """A task submitter for MapOperator.

    This abstraction is in charge of submitting tasks, reserving resources for their
    execution, and cleaning up said resources when a task completes or when task
    submission is done.
    """

    def start(self):
        """Start the task submitter so it's ready to submit tasks.

        This is called when execution of the map operator actually starts, and is where
        the submitter can initialize expensive state, reserve resources, start workers,
        etc.
        """
        pass

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

    @abstractmethod
    def shutdown(self, task_refs: List[ObjectRef[Union[ObjectRefGenerator, Block]]]):
        """Shutdown the submitter, i.e. release any reserved resources.

        Args:
            task_refs: The output refs for all of the tasks submitted by this submitter.
        """
        raise NotImplementedError

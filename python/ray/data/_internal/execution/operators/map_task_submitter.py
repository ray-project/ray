from abc import ABC, abstractmethod
from typing import Callable, List, Union, Tuple, Iterator
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
        self,
        transform_fn: ObjectRef[Callable[[Iterator[Block]], Iterator[Block]]],
        input_blocks: List[ObjectRef[Block]],
    ) -> Union[
        ObjectRef[ObjectRefGenerator], Tuple[ObjectRef[Block], ObjectRef[BlockMetadata]]
    ]:
        """Submit a map task.

        Args:
            transform_fn: The function to apply to a block bundle in the submitted
                map task.
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
    def cancellable(self) -> bool:
        """Whether the submitted tasks are cancellable."""
        raise NotImplementedError

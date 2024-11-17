from abc import ABC, abstractmethod

from typing import List


class _SynchronousGroup(ABC):
    """
    Represents a group of actors that participate in a synchronous operation.
    """

    def __init__(self):
        # Task idxs in a compiled DAG.
        self.task_idxs: List[int] = []

    @abstractmethod
    def execute(self, *args, **kwargs) -> None:
        """
        Execute the synchronous operation.

        This method is used in `ExecutableTask._compute`.
        """
        raise NotImplementedError

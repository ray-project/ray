from abc import ABC, abstractmethod
from typing import List, Set, Any


class _NcclOperation(ABC):
    """
    Represent metadata for a NCCL operation.
    """

    def __init__(self):
        # Task indices in a compiled DAG. The list of indices are in topological order
        # if tasks have dependencies, such as P2P send/recv tasks.
        self.task_idxs: List[int] = []
        # Indices of tasks that are ready with a zero in-degree.
        self.ready_task_idxs: Set[int] = set()
        # Whether all the tasks have been added to the execution schedule.
        self.scheduled: bool = False

    @property
    def is_ready(self) -> bool:
        """
        Return true when all the tasks are ready.
        """
        return len(self.ready_task_idxs) == len(self.task_idxs)

    @abstractmethod
    def execute(self, *args, **kwargs) -> Any:
        """
        Execute the NCCL operation in `ExecutableTask`.
        """
        raise NotImplementedError

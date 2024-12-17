from abc import ABC, abstractmethod
from typing import List, Set


class _NcclOperation(ABC):
    """
    Represent metadata for a NCCL operation.
    """

    def __init__(self):
        # Task indices in a compiled DAG.
        self.task_idxs: List[int] = []
        # Indices of tasks that are ready with a zero in-degree.
        self.ready_task_idxs: Set[int] = set()
        # Whether all the taskss have been added to the execution schedule.
        self.scheduled: bool = False

    @property
    def is_ready(self) -> bool:
        """
        Return true when all the tasks are ready.
        """
        return len(self.ready_task_idxs) == len(self.task_idxs)

    @abstractmethod
    def execute(self, *args, **kwargs) -> None:
        """
        Execute the NCCL operation in `ExecutableTask`.
        """
        raise NotImplementedError

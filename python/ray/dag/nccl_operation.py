from abc import ABC, abstractmethod

from typing import List, Set


class _NcclOperation(ABC):
    """
    [CL]
    Represents a group of actors that participate in a NCCL op.
    """

    def __init__(self):
        # Task idxs in a compiled DAG.
        self.task_idxs: List[int] = []
        # Indices of tasks that are ready (in-degree=0).
        self.ready_task_idxs: Set[int] = set()
        # Whether the group has been added to the execution schedule.
        self.scheduled: bool = False

    @abstractmethod
    def execute(self, *args, **kwargs) -> None:
        """
        Execute the NCCL op. This is called in `ExecutableTask._compute`.
        """
        raise NotImplementedError

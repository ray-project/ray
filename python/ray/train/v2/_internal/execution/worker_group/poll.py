from dataclasses import dataclass
from typing import Dict

from ray.types import ObjectRef

from typing import Optional

from ray.train._internal.session import _TrainingResult


@dataclass
class WorkerStatus:
    running: bool
    error: Optional[Exception] = None
    training_result: Optional[_TrainingResult] = None


@dataclass(frozen=True)
class WorkerGroupPollStatus:
    worker_statuses: Dict[int, WorkerStatus]

    @property
    def errors(self) -> Dict[int, Exception]:
        return {
            world_rank: status.error
            for world_rank, status in self.worker_statuses.items()
            if status.error is not None
        }

    @property
    def finished(self) -> bool:
        return self.worker_statuses and all(
            not status.running for status in self.worker_statuses.values()
        )

    def get_error_string(self) -> str:
        return "\n".join(
            f"[Rank {world_rank}]\n{error}" for world_rank, error in self.errors.items()
        )


@dataclass(frozen=True)
class PollTask:
    """Represents a poll task for a worker.

    Attributes:
        start_time: The time when the poll task was started.
        task: The ObjectRef representing the poll task.
    """

    start_time: float
    task: ObjectRef

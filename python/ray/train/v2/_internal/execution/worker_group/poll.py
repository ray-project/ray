from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Optional

from ray.train._internal.session import _TrainingResult
from ray.train.v2._internal.exceptions import WorkerHealthCheckFailedError
from ray.types import ObjectRef

ERR_CHAR_LIMIT = 1000


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
        """
        Returns a string representation worker group errors.
        """

        def truncate_error_str(error_str: str) -> str:
            """Truncates error strings to a maximum length of ERR_CHAR_LIMIT"""
            if len(error_str) > ERR_CHAR_LIMIT:
                return error_str[:ERR_CHAR_LIMIT] + "..."
            return error_str

        error_to_rank = defaultdict(list)
        show_full_error = set()
        for world_rank, status in self.worker_statuses.items():
            # Exclude errors from running workers
            if status.error and not status.running:
                error_str = str(status.error)
                error_to_rank[error_str].append(str(world_rank))

                # Fully show errors for non-graceful worker failures
                if isinstance(status.error, WorkerHealthCheckFailedError):
                    show_full_error.add(error_str)

        for error, ranks in error_to_rank.items():
            error_to_rank[error] = ", ".join(ranks)

        errors = []
        for error, ranks in error_to_rank.items():
            if error in show_full_error:
                errors.append(f"[Rank {ranks}]:\n{error}")
            else:
                errors.append(f"[Rank {ranks}]:\n{truncate_error_str(error)}")

        error_str = "\n".join(errors)

        if "..." in error_str:
            error_str += "\nView individual worker logs for more details."

        return error_str


@dataclass(frozen=True)
class PollTask:
    """Represents a poll task for a worker.

    Attributes:
        start_time: The time when the poll task was started.
        task: The ObjectRef representing the poll task.
    """

    start_time: float
    task: ObjectRef

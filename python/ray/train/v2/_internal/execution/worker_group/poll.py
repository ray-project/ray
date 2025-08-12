import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Optional

from ray._private.ray_logging import NUMBERS
from ray.train._internal.session import _TrainingResult
from ray.train.v2._internal.exceptions import WorkerHealthCheckFailedError
from ray.train.v2.api.exceptions import WorkerGroupError
from ray.types import ObjectRef

ERR_CHAR_LIMIT = 1000


def _normalize_error_string(error_str: str) -> str:
    # Replace numbers with <NUM> based on NUMBERS regex
    normalized = re.sub(NUMBERS, "<NUM>", error_str)
    return normalized


def _truncate_error_string(error_str: str) -> str:
    """
    Truncates error strings to include the first ERR_CHAR_LIMIT // 2
    characters and the last ERR_CHAR_LIMIT // 2 characters.
    """
    if len(error_str) >= ERR_CHAR_LIMIT:
        return (
            error_str[: ERR_CHAR_LIMIT // 2]
            + "...\n... (Output truncated. See individual worker logs for full details) ...\n"
            + error_str[len(error_str) - ERR_CHAR_LIMIT // 2 :]
        )
    return error_str


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

    def get_worker_group_error(self) -> WorkerGroupError:
        return WorkerGroupError(
            error_message=self.get_error_string(),
            worker_failures=self.errors,
        )

    @property
    def finished(self) -> bool:
        return self.worker_statuses and all(
            not status.running for status in self.worker_statuses.values()
        )

    def get_error_string(self) -> str:
        """
        Returns a string representation of worker group errors.
        Groups similar errors (ignoring numbers) and shows original error examples.
        """
        # Group errors by normalized strings (ignoring numbers)
        normalized_error_to_ranks = defaultdict(list)
        normalized_error_to_original = {}
        show_full_error = set()

        for world_rank, status in self.worker_statuses.items():
            if status.error:
                error_str = str(status.error)
                normalized_error = _normalize_error_string(error_str)

                normalized_error_to_ranks[normalized_error].append(str(world_rank))

                # Store the first original error for this normalized group
                if normalized_error not in normalized_error_to_original:
                    normalized_error_to_original[normalized_error] = error_str

                # Fully show errors for non-graceful worker failures or running workers
                if (
                    isinstance(status.error, WorkerHealthCheckFailedError)
                    or status.running
                ):
                    show_full_error.add(normalized_error)

        errors = []
        for normalized_error, ranks in normalized_error_to_ranks.items():
            # Show the original error
            orig_error = normalized_error_to_original[normalized_error]

            # Convert rank list to comma-separated strings
            ranks_str = ",".join(ranks)

            if normalized_error in show_full_error:
                errors.append(f"[Rank {ranks_str} Error Snippet]:\n{orig_error}")
            else:
                errors.append(
                    f"[Rank {ranks_str} Error Snippet]:\n{_truncate_error_string(orig_error)}"
                )

        error_str = "\n".join(errors)

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

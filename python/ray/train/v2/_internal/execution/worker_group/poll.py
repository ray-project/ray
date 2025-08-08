import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Optional

from ray.train._internal.session import _TrainingResult
from ray.train.v2._internal.exceptions import WorkerHealthCheckFailedError
from ray.types import ObjectRef

ERR_CHAR_LIMIT = 1000


def _normalize_error_string(error_str: str) -> str:
    """Normalize error string by replacing numbers with placeholders for grouping.

    This allows errors that are similar except for specific numbers to be grouped together.
    For example, "Error on line 42" and "Error on line 123" would both become "Error on line <NUM>".
    """
    # Replace sequences of digits with <NUM> placeholder
    normalized = re.sub(r"\b\d+\b", "<NUM>", error_str)
    # Also replace hex numbers (0x...) with <HEX>
    normalized = re.sub(r"\b0x[0-9a-fA-F]+\b", "<HEX>", normalized)
    # Replace memory addresses in parentheses like "object at 0x7f8b..."
    normalized = re.sub(r"\bat 0x[0-9a-fA-F]+\b", "at <ADDR>", normalized)
    return normalized


def _truncate_error_string(error_str: str) -> str:
    """Truncates error strings to a maximum length of ERR_CHAR_LIMIT."""
    if len(error_str) > ERR_CHAR_LIMIT:
        return error_str[:ERR_CHAR_LIMIT] + "..."
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

    @property
    def finished(self) -> bool:
        return self.worker_statuses and all(
            not status.running for status in self.worker_statuses.values()
        )

    def get_error_string(self) -> str:
        """
        Returns a string representation worker group errors.
        Groups similar errors (ignoring numbers) and shows original error examples.
        """
        # Group errors by normalized strings (ignoring numbers)
        normalized_error_to_ranks = defaultdict(list)
        show_full_error = set()

        for world_rank, status in self.worker_statuses.items():
            # Exclude errors from running workers
            if status.error and not status.running:
                error_str = str(status.error)
                normalized_error = _normalize_error_string(error_str)

                normalized_error_to_ranks[normalized_error].append(str(world_rank))

                # Fully show errors for non-graceful worker failures
                if isinstance(status.error, WorkerHealthCheckFailedError):
                    show_full_error.add(normalized_error)

        # Convert rank lists to comma-separated strings
        for normalized_error, ranks in normalized_error_to_ranks.items():
            normalized_error_to_ranks[normalized_error] = ", ".join(ranks)

        errors = []
        for normalized_error, ranks in normalized_error_to_ranks.items():
            if normalized_error in show_full_error:
                errors.append(f"[Rank {ranks}]:\n{normalized_error}")
            else:
                errors.append(
                    f"[Rank {ranks}]:\n{_truncate_error_string(normalized_error)}"
                )

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

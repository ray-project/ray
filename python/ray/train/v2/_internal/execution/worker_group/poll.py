import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set

from ray._private.ray_logging import NUMBERS
from ray.exceptions import RayActorError
from ray.train.v2._internal.exceptions import (
    UserExceptionWithTraceback,
    WorkerHealthCheckFailedError,
)
from ray.train.v2._internal.execution.preemption import PreemptionInfo
from ray.train.v2._internal.execution.training_report import _TrainingReport
from ray.train.v2.api.exceptions import WorkerGroupError
from ray.types import ObjectRef

ERR_CHAR_LIMIT = 1000

# Sentinel node id used when a preemption is inferred from a worker's death
# (RayActorError.preempted) rather than an advance drain signal, where the
# preempted node id is not known.
_UNKNOWN_PREEMPTED_NODE = "<unknown-preempted-node>"


def _is_preemption_death(error: Optional[Exception]) -> bool:
    """Whether a worker error is a node-preemption-caused death.

    A worker killed by a node reclaim surfaces as a WorkerHealthCheckFailedError
    wrapping a RayActorError whose ``preempted`` flag is set.
    """
    if isinstance(error, WorkerHealthCheckFailedError):
        error = error.health_check_failure
    return isinstance(error, RayActorError) and error.preempted


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
    training_report: Optional[_TrainingReport] = None
    return_value: Any = field(default=None)
    preemption_info: Optional[PreemptionInfo] = None


@dataclass(frozen=True)
class WorkerGroupPollStatus:
    worker_statuses: Dict[int, WorkerStatus]
    worker_rank_to_replica_group_rank: Optional[Dict[int, int]] = None

    @property
    def all_replica_group_indices(self) -> Set[int]:
        """Return the set of all replica group indices."""
        if self.worker_rank_to_replica_group_rank is None:
            return set()
        return set(self.worker_rank_to_replica_group_rank.values())

    @property
    def failing_replica_group_indices(self) -> Set[int]:
        """Return the set of replica group indices that have failing workers."""
        if self.worker_rank_to_replica_group_rank is None:
            return set()
        return {
            self.worker_rank_to_replica_group_rank[rank]
            for rank in self.errors
            if rank in self.worker_rank_to_replica_group_rank
        }

    @property
    def errors(self) -> Dict[int, Exception]:
        errors = {}
        for world_rank, status in self.worker_statuses.items():
            if status.error is not None:
                error = status.error
                if isinstance(error, UserExceptionWithTraceback):
                    error = error._base_exc
                errors[world_rank] = error
        return errors

    def get_worker_group_error(self) -> WorkerGroupError:
        return WorkerGroupError(
            error_message=self.get_error_string(),
            worker_failures=self.errors,
        )

    def get_preemption_info(self) -> Optional[PreemptionInfo]:
        """Return the active preemption signal, or None.

        Prefers the advance signal echoed by a worker (the PreemptionWatcher
        fans the same PreemptionInfo out to every worker, so any non-None echo
        reflects the current preemption). If there is no advance signal but a
        worker was killed by a node reclaim -- detected via
        ``RayActorError.preempted`` -- synthesize a PreemptionInfo for the
        affected ranks so the reclaim is still charged to the preemption budget
        rather than ``max_failures`` (the node id is unknown in this case).
        """
        for status in self.worker_statuses.values():
            if status.preemption_info is not None:
                return status.preemption_info

        preempted_ranks = sorted(
            rank for rank, error in self.errors.items() if _is_preemption_death(error)
        )
        if preempted_ranks:
            return PreemptionInfo(
                deadline_ms=None,
                preempted_node_to_ranks={_UNKNOWN_PREEMPTED_NODE: preempted_ranks},
            )
        return None

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

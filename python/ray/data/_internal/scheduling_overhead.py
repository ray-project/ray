from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Tuple

import numpy as np

from ray.core.generated.common_pb2 import TaskStatus
from ray.util.state.common import TaskState


class DataTaskName(str, Enum):
    """Names of tasks used by Ray Data operators."""

    MAP_TASK = "_map_task"
    MAP_WORKER = "_MapWorker"


class SchedulingPhase(str, Enum):
    """Phases of the Ray task scheduling lifecycle.

    See ``src/ray/protobuf/common.proto`` TaskStatus for the source of truth.

    Normal tasks::

        PENDING_ARGS_AVAIL
        -> PENDING_NODE_ASSIGNMENT
            -> PENDING_OBJ_STORE_MEM_AVAIL  (sub-state, metrics only)
            -> PENDING_ARGS_FETCH           (sub-state, metrics only)
        -> SUBMITTED_TO_WORKER
        -> RUNNING
        -> FINISHED

    Actor tasks::

        PENDING_ARGS_AVAIL
        -> PENDING_NODE_ASSIGNMENT
            -> PENDING_OBJ_STORE_MEM_AVAIL  (sub-state, metrics only)
            -> PENDING_ARGS_FETCH           (sub-state, metrics only)
        -> SUBMITTED_TO_WORKER
        -> PENDING_ACTOR_TASK_ARGS_FETCH
        -> PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY
        -> RUNNING
        -> FINISHED

    Each member maps to a (start, end) pair of ``TaskStatus`` values.
    Use ``phase.start_name`` / ``phase.end_name`` to get the string names
    that appear in task event dicts.
    """

    # Fine-grained phases (consecutive state transitions)
    ARGS_WAIT_MS = "args_wait_ms"
    NODE_ASSIGNMENT_MS = "node_assignment_ms"
    OBJ_STORE_MEM_WAIT_MS = "obj_store_mem_wait_ms"
    ARGS_FETCH_MS = "args_fetch_ms"
    WORKER_DISPATCH_MS = "worker_dispatch_ms"
    ACTOR_TASK_ARGS_FETCH_MS = "actor_task_args_fetch_ms"
    ACTOR_TASK_ORDERING_MS = "actor_task_ordering_ms"
    EXECUTION_MS = "execution_ms"

    # Coarse aggregate phases
    SCHEDULING_MS = "scheduling_ms"
    WORKER_STARTUP_MS = "worker_startup_ms"
    TOTAL_OVERHEAD_MS = "total_overhead_ms"

    @property
    def boundaries(self) -> Tuple[int, int]:
        """Return the (start_status, end_status) TaskStatus pair for this phase."""
        S = TaskStatus
        match self:
            # Fine-grained
            case SchedulingPhase.ARGS_WAIT_MS:
                return (S.PENDING_ARGS_AVAIL, S.PENDING_NODE_ASSIGNMENT)
            case SchedulingPhase.NODE_ASSIGNMENT_MS:
                return (S.PENDING_NODE_ASSIGNMENT, S.PENDING_OBJ_STORE_MEM_AVAIL)
            case SchedulingPhase.OBJ_STORE_MEM_WAIT_MS:
                return (S.PENDING_OBJ_STORE_MEM_AVAIL, S.PENDING_ARGS_FETCH)
            case SchedulingPhase.ARGS_FETCH_MS:
                return (S.PENDING_ARGS_FETCH, S.SUBMITTED_TO_WORKER)
            case SchedulingPhase.WORKER_DISPATCH_MS:
                return (S.SUBMITTED_TO_WORKER, S.PENDING_ACTOR_TASK_ARGS_FETCH)
            case SchedulingPhase.ACTOR_TASK_ARGS_FETCH_MS:
                return (
                    S.PENDING_ACTOR_TASK_ARGS_FETCH,
                    S.PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY,
                )
            case SchedulingPhase.ACTOR_TASK_ORDERING_MS:
                return (S.PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY, S.RUNNING)
            case SchedulingPhase.EXECUTION_MS:
                return (S.RUNNING, S.FINISHED)
            # Coarse aggregates
            case SchedulingPhase.SCHEDULING_MS:
                return (S.PENDING_NODE_ASSIGNMENT, S.SUBMITTED_TO_WORKER)
            case SchedulingPhase.WORKER_STARTUP_MS:
                return (S.SUBMITTED_TO_WORKER, S.RUNNING)
            case SchedulingPhase.TOTAL_OVERHEAD_MS:
                return (S.PENDING_ARGS_AVAIL, S.RUNNING)

    @property
    def start_name(self) -> str:
        """TaskStatus string name for the start boundary."""
        return TaskStatus.Name(self.boundaries[0])

    @property
    def end_name(self) -> str:
        """TaskStatus string name for the end boundary."""
        return TaskStatus.Name(self.boundaries[1])


@dataclass
class PhaseStats:
    """Aggregated statistics for a single scheduling phase (all values in ms)."""

    p50: float
    p99: float
    mean: float
    max: float
    min: float


@dataclass
class SchedulingOverheadSummary:
    """Summary of scheduling overhead across matched tasks."""

    num_tasks: int
    phases: Dict[SchedulingPhase, PhaseStats]

    @classmethod
    def from_task_states(cls, tasks: List[TaskState]) -> "SchedulingOverheadSummary":
        """Build a summary from a list of TaskState objects.

        Computes per-phase scheduling stats by deriving durations from
        each task's event timestamps.
        """

        phase_durations: Dict[SchedulingPhase, List[float]] = defaultdict(list)

        for t in tasks:
            if not t.events:
                continue

            ts: Dict[str, float] = {e["state"]: e["created_ms"] for e in t.events}

            for phase in SchedulingPhase:
                if phase.start_name in ts and phase.end_name in ts:
                    phase_durations[phase].append(
                        ts[phase.end_name] - ts[phase.start_name]
                    )

        phases: Dict[SchedulingPhase, PhaseStats] = {}
        for phase, vals in phase_durations.items():
            arr = np.array(vals)
            phases[phase] = PhaseStats(
                p50=float(np.percentile(arr, 50)),
                p99=float(np.percentile(arr, 99)),
                mean=float(np.mean(arr)),
                max=float(np.max(arr)),
                min=float(np.min(arr)),
            )

        return cls(num_tasks=len(tasks), phases=phases)


def collect_scheduling_overhead() -> Dict[str, SchedulingOverheadSummary]:
    """Collect per-task-name scheduling overhead from the Ray State API.

    Calls ``list_tasks(detail=True)`` for each ``DataTaskName`` and returns
    a dict mapping task name to its ``SchedulingOverheadSummary``.
    """
    from ray.util.state.api import list_tasks

    summaries: Dict[str, SchedulingOverheadSummary] = {}
    for name in DataTaskName:
        tasks = list_tasks(
            filters=[("func_or_class_name", "=", name)],
            detail=True,
            # 10_000 is chosen because it's the default max limit. Therefore, some
            # tasks may be truncated.
            limit=10_000,
            raise_on_missing_output=False,
        )
        summaries[name] = SchedulingOverheadSummary.from_task_states(tasks)
    return summaries

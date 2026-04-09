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


@dataclass
class BucketedSchedulingOverhead:
    """Scheduling overhead for a single operator within a time bucket."""

    start_ms: float
    end_ms: float
    summary: SchedulingOverheadSummary


def collect_scheduling_overhead(
    num_buckets: int = 4,
) -> Dict[str, List[BucketedSchedulingOverhead]]:
    """Collect per-operator scheduling overhead from the Ray State API,
    bucketed into ``num_buckets`` equal time intervals.

    Fetches all Ray Data tasks (``_map_task`` and ``_MapWorker``), determines
    the global time range from ``creation_time_ms``, splits into equal
    intervals, and returns a dict mapping operator name to its list of
    time-bucketed scheduling overhead summaries.
    """
    from ray.util.state.api import list_tasks

    all_tasks: List[TaskState] = []
    for task_type in DataTaskName:
        all_tasks.extend(
            list_tasks(
                filters=[("func_or_class_name", "=", task_type)],
                detail=True,
                limit=10_000,
                raise_on_missing_output=False,
            )
        )

    # 1. Sort all tasks by creation_time_ms.
    all_tasks = [t for t in all_tasks if t.creation_time_ms is not None]
    if not all_tasks:
        return {}
    all_tasks.sort(key=lambda t: t.creation_time_ms)

    min_ts: int = all_tasks[0].creation_time_ms
    max_ts: int = all_tasks[-1].creation_time_ms
    bucket_width = (max_ts - min_ts) / num_buckets if num_buckets > 0 else 0

    # 2. Build bucket boundaries.
    if bucket_width == 0:
        boundaries = [(min_ts, max_ts + 1)]
    else:
        boundaries = []
        for i in range(num_buckets):
            lo = min_ts + i * bucket_width
            hi = lo + bucket_width
            boundaries.append((lo, hi))

    # Pre-allocate per-bucket, per-operator task lists.
    # bucket_tasks[bucket_idx][operator_name] -> list of tasks
    bucket_tasks: List[Dict[str, List[TaskState]]] = [
        defaultdict(list) for _ in boundaries
    ]

    # 3. Single pass: assign each sorted task to its bucket.
    def _in_bucket(t: TaskStatus, bucket: Tuple[int, int]) -> bool:
        return bucket[0] <= t.creation_time_ms < bucket[1]

    bucket_idx = 0
    for t in all_tasks:
        while bucket_idx < len(boundaries) - 1 and not _in_bucket(
            t=t, bucket=boundaries[bucket_idx]
        ):
            bucket_idx += 1
        bucket_tasks[bucket_idx][t.name].append(t)

    # Build the result dict: operator_name -> list of bucketed summaries.
    result: Dict[str, List[BucketedSchedulingOverhead]] = defaultdict(list)
    for i, (lo, hi) in enumerate(boundaries):
        for op_name, tasks in bucket_tasks[i].items():
            result[op_name].append(
                BucketedSchedulingOverhead(
                    start_ms=lo,
                    end_ms=hi,
                    summary=SchedulingOverheadSummary.from_task_states(tasks),
                )
            )

    return dict(result)

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, DefaultDict, Dict, List, Optional, Union

import requests

import ray
from ray.data._internal.issue_detection.issue_detector import (
    Issue,
    IssueDetector,
    IssueType,
)
from ray.util.state import get_task
from ray.util.state.common import TaskState
from ray.util.state.exception import RayStateApiException

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )
    from ray.data._internal.execution.streaming_executor import StreamingExecutor

# Default minimum count of tasks before using adaptive thresholds
DEFAULT_OP_TASK_STATS_MIN_COUNT = 10
# Default multiple of standard deviations to use as hanging threshold
DEFAULT_OP_TASK_STATS_STD_FACTOR = 10
# Default detection time interval.
DEFAULT_DETECTION_TIME_INTERVAL_S = 30.0

logger = logging.getLogger(__name__)

OpId = str
TaskIdx = int
# Map of operator id -> task index -> hanging execution state.
HangingOpTasks = DefaultDict[OpId, Dict[TaskIdx, "HangingExecutionState"]]


def _format_timestamp(epoch: float) -> str:
    """Format a ``time.time()`` epoch value as a human-readable UTC string."""
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S %Z"
    )


# ---------------------------------------------------------------------------
# Snapshot types — the common currency for both sync and async detection
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RunningTaskSnapshot:
    task_id: ray.TaskID
    bytes_output: int
    time_since_last_update: float


@dataclass(frozen=True)
class OpDetectionSnapshot:
    op_id: str
    op_name: str
    dataset_id: str
    has_execution_finished: bool
    running_tasks: Dict[TaskIdx, RunningTaskSnapshot]
    task_stats_count: int
    task_stats_mean: float
    task_stats_stddev: float

    @classmethod
    def from_operator(
        cls, op: "PhysicalOperator", dataset_id: str
    ) -> "OpDetectionSnapshot":
        """Build a snapshot from a live operator on the scheduling thread."""
        now = time.perf_counter()
        metrics = op.metrics
        running_tasks: Dict[TaskIdx, RunningTaskSnapshot] = {}
        for task_idx, task_info in metrics._running_tasks.items():
            running_tasks[task_idx] = RunningTaskSnapshot(
                task_id=task_info.task_id,
                bytes_output=task_info.bytes_output,
                time_since_last_update=now - task_info.last_updated,
            )
        stats = metrics._op_task_duration_stats
        return cls(
            op_id=op.id,
            op_name=op.name,
            dataset_id=dataset_id,
            has_execution_finished=op.has_execution_finished(),
            running_tasks=running_tasks,
            task_stats_count=stats.count(),
            task_stats_mean=stats.mean(),
            task_stats_stddev=stats.stddev(),
        )


# ---------------------------------------------------------------------------
# Internal state types
# ---------------------------------------------------------------------------


@dataclass
class TaskMetadata:
    """Subset of TaskState fields relevant for hanging detection."""

    attempt_number: int
    node_id: str
    pid: int

    @classmethod
    def from_task_state(cls, task_state: TaskState) -> "TaskMetadata":
        return cls(
            attempt_number=task_state.attempt_number,
            node_id=task_state.node_id,
            pid=task_state.worker_pid,
        )


@dataclass
class HangingExecutionState:
    operator_id: OpId
    task_idx: TaskIdx
    task_id: ray.TaskID
    task_metadata: Optional[TaskMetadata]
    bytes_output: int
    time_since_last_update: float


@dataclass
class HangingExecutionIssueDetectorConfig:
    op_task_stats_min_count: int = field(default=DEFAULT_OP_TASK_STATS_MIN_COUNT)
    op_task_stats_std_factor: float = field(default=DEFAULT_OP_TASK_STATS_STD_FACTOR)
    detection_time_interval_s: float = DEFAULT_DETECTION_TIME_INTERVAL_S


class HangingExecutionIssueDetector(IssueDetector):
    def __init__(
        self,
        config: HangingExecutionIssueDetectorConfig,
        dataset_id: str = "",
        operators: Optional[List["PhysicalOperator"]] = None,
    ):
        self._dataset_id = dataset_id
        self._operators = operators or []
        self._detector_cfg = config

        self._op_task_stats_min_count = self._detector_cfg.op_task_stats_min_count
        self._op_task_stats_std_factor_threshold = (
            self._detector_cfg.op_task_stats_std_factor
        )

        self._hanging_op_tasks: HangingOpTasks = defaultdict(dict)

    @classmethod
    def from_executor(
        cls, executor: "StreamingExecutor"
    ) -> "HangingExecutionIssueDetector":
        """Factory method to create a HangingExecutionIssueDetector from a StreamingExecutor."""
        operators = list(executor._topology.keys()) if executor._topology else []
        ctx = executor._data_context
        return cls(
            config=ctx.issue_detectors_config.hanging_detector_config,
            dataset_id=executor._dataset_id,
            operators=operators,
        )

    def _create_issue(
        self,
        snap: OpDetectionSnapshot,
        hes: HangingExecutionState,
    ) -> Issue:
        meta = hes.task_metadata
        task_info = ""
        if meta is not None:
            task_info = f"(pid={meta.pid}, node_id={meta.node_id}, attempt={meta.attempt_number}) "

        hanging_time = hes.time_since_last_update
        hanging_since = _format_timestamp(time.time() - hanging_time)

        message = (
            f"A task (task_id={hes.task_id}) of operator "
            f"{snap.op_name} {task_info}has been running or stuck in scheduling for "
            f"{hanging_time:.2f}s, which is longer than the average task "
            f"duration + z-score * stddev of this operator "
            f"({snap.task_stats_mean:.2f} + "
            f"{self._op_task_stats_std_factor_threshold} * "
            f"{snap.task_stats_stddev:.2f}s). "
            f"Last time task produced output or made any progress was {hanging_since}. "
            f"If this message persists, please check "
            f"the stack trace of the task for potential hanging "
            f"issues. To adjust the z-score value, set "
            f"`ray.data.DataContext.get_current()"
            f".issue_detectors_config.hanging_detector_config"
            f".op_task_stats_std_factor`."
        )

        return Issue(
            dataset_name=snap.dataset_id,
            operator_id=hes.operator_id,
            issue_type=IssueType.HANGING,
            message=message,
        )

    def detect(self) -> List[Issue]:
        """Sync entry point — builds snapshots from live operators and delegates."""
        snapshots = [
            OpDetectionSnapshot.from_operator(op, self._dataset_id)
            for op in self._operators
            if not op.has_execution_finished()
        ]
        return self.detect_from_snapshots(snapshots)

    def detect_from_snapshots(
        self, snapshots: List[OpDetectionSnapshot]
    ) -> List[Issue]:
        """Core detection logic operating on serializable snapshots.

        Used by both the sync ``detect()`` path and the async service task.
        """
        issues: List[Issue] = []
        hanging_op_tasks: HangingOpTasks = defaultdict(dict)

        for snap in snapshots:
            if snap.has_execution_finished:
                continue

            if snap.task_stats_count < self._op_task_stats_min_count:
                continue

            threshold = (
                snap.task_stats_mean
                + self._op_task_stats_std_factor_threshold * snap.task_stats_stddev
            )

            for task_idx, task_snap in snap.running_tasks.items():
                if task_snap.time_since_last_update <= threshold:
                    continue

                old_state = self._hanging_op_tasks.get(snap.op_id, {}).get(task_idx)

                task_metadata: Optional[TaskMetadata] = None
                if old_state is not None:
                    task_metadata = old_state.task_metadata
                else:
                    task_metadata = get_latest_state_for_task(task_snap.task_id)

                new_state = HangingExecutionState(
                    operator_id=snap.op_id,
                    task_idx=task_idx,
                    task_id=task_snap.task_id,
                    task_metadata=task_metadata,
                    bytes_output=task_snap.bytes_output,
                    time_since_last_update=task_snap.time_since_last_update,
                )

                hanging_op_tasks[snap.op_id][task_idx] = new_state

                if (
                    old_state is not None
                    and old_state.bytes_output == new_state.bytes_output
                ):
                    continue

                issues.append(self._create_issue(snap, new_state))

        self._hanging_op_tasks = hanging_op_tasks
        return issues

    def detection_time_interval_s(self) -> float:
        return self._detector_cfg.detection_time_interval_s


def get_latest_state_for_task(task_id: ray.TaskID) -> Optional[TaskMetadata]:
    """Query the Ray State API for the latest attempt of a task.

    Returns a TaskMetadata with the highest attempt_number when multiple
    attempts exist, or None if the task is not found (can happen when
    get_task() is called shortly after submission, before the state API
    has indexed it) or the API is unreachable.
    """
    try:
        # NOTE: timeout is set to 1 because ray will take max(1, timeout).
        # TODO(Justin): Make this asynchronous
        task_state: Union[TaskState, List[TaskState], None] = get_task(
            task_id.hex(),
            timeout=1,
            _explain=True,
        )
    except (RayStateApiException, requests.exceptions.RequestException):
        logger.debug(f"Failed to grab task state with task_id={task_id}", exc_info=True)
        return None
    except Exception:
        logger.debug(
            f"Unexpected error when grabbing task state with task_id={task_id}",
            exc_info=True,
        )
        return None
    if isinstance(task_state, list):
        # get the latest task
        task_state = max(task_state, key=lambda ts: ts.attempt_number, default=None)
    if task_state is None:
        return None
    return TaskMetadata.from_task_state(task_state)

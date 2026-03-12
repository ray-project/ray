import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, DefaultDict, Dict, List

import requests

import ray
from ray.data._internal.execution.interfaces.op_runtime_metrics import RunningTaskInfo
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
# Max failed attempts to fetch task state before giving up for a task.
# 1 is a lower # chosen for now, because the get_state API is known
# to be slow when processing many http requests.
_MAX_STATE_FETCH_FAILED_ATTEMPTS = 1

logger = logging.getLogger(__name__)

OpId = str
TaskIdx = int


def _format_timestamp(epoch: float) -> str:
    """Format a ``time.time()`` epoch value as a human-readable UTC string."""
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S %Z"
    )


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
    # Equality includes everything except task_metadata: a change
    # in output means the task made progress and warrants a new log entry.
    # A change in metadata alone (node/worker reassignment) doesn't
    # indicate progress, so it's excluded from comparison.  The remaining
    # fields are static identifiers or timestamps.
    operator_id: OpId
    task_idx: TaskIdx
    task_id: ray.TaskID
    task_metadata: TaskMetadata
    bytes_output: int
    task_start_time: float
    start_time_hanging: float

    def hanging_time(self):
        return time.time() - self.start_time_hanging


@dataclass
class HangingExecutionIssueDetectorConfig:
    op_task_stats_min_count: int = field(default=DEFAULT_OP_TASK_STATS_MIN_COUNT)
    op_task_stats_std_factor: float = field(default=DEFAULT_OP_TASK_STATS_STD_FACTOR)
    detection_time_interval_s: float = DEFAULT_DETECTION_TIME_INTERVAL_S


class HangingExecutionIssueDetector(IssueDetector):
    def __init__(
        self,
        dataset_id: str,
        operators: List["PhysicalOperator"],
        config: HangingExecutionIssueDetectorConfig,
    ):
        self._dataset_id = dataset_id
        self._operators = operators
        self._detector_cfg = config

        self._op_task_stats_min_count = self._detector_cfg.op_task_stats_min_count
        self._op_task_stats_std_factor_threshold = (
            self._detector_cfg.op_task_stats_std_factor
        )

        # Map of operator id to Dict[task index, state]
        self._hanging_op_tasks: DefaultDict[
            OpId, Dict[TaskIdx, HangingExecutionState]
        ] = defaultdict(dict)
        # Per-task count of failed get_latest_state_for_task attempts.
        # After _MAX_STATE_FETCH_FAILED_ATTEMPTS, we stop retrying for that task.
        self._state_fetch_failures: DefaultDict[OpId, Dict[TaskIdx, int]] = defaultdict(
            dict
        )

    @classmethod
    def from_executor(
        cls, executor: "StreamingExecutor"
    ) -> "HangingExecutionIssueDetector":
        """Factory method to create a HangingExecutionIssueDetector from a StreamingExecutor.

        Args:
            executor: The StreamingExecutor instance to extract dependencies from.

        Returns:
            An instance of HangingExecutionIssueDetector.
        """
        operators = list(executor._topology.keys()) if executor._topology else []
        ctx = executor._data_context
        return cls(
            dataset_id=executor._dataset_id,
            operators=operators,
            config=ctx.issue_detectors_config.hanging_detector_config,
        )

    def _create_issue(
        self,
        operator: "PhysicalOperator",
        hanging_execution_state: HangingExecutionState,
    ) -> Issue:

        hes = hanging_execution_state
        op_task_stats = operator.metrics._op_task_duration_stats
        avg_duration = op_task_stats.mean()
        stdev = op_task_stats.stddev()

        meta = hes.task_metadata
        metadata_info = ""
        if meta is not None:
            metadata_info = f"(pid={meta.pid}, node_id={meta.node_id}, attempt={meta.attempt_number}) "

        task_started = _format_timestamp(hes.task_start_time)
        hanging_since = _format_timestamp(hes.start_time_hanging)

        message = (
            f"A task (task_id={hes.task_id}) of operator "
            f"{operator.name} {metadata_info}has been running or stuck in scheduling for "
            f"{hes.hanging_time():.2f}s, which is longer than the average task "
            f"duration + z-score * stddev of this operator "
            f"({avg_duration:.2f} + "
            f"{self._op_task_stats_std_factor_threshold} * "
            f"{stdev:.2f}s). "
            f"Task started at {task_started} and "
            f"last time task produced output was {hanging_since}. "
            f"If this message persists, please check "
            f"the stack trace of the task for potential hanging "
            f"issues. To adjust the z-score value, set "
            f"`ray.data.DataContext.get_current()"
            f".issue_detectors_config.hanging_detector_config"
            f".op_task_stats_std_factor`."
        )

        return Issue(
            dataset_name=self._dataset_id,
            operator_id=hes.operator_id,
            issue_type=IssueType.HANGING,
            message=message,
        )

    def _should_retry_metadata_fetch(self, op_id: OpId, task_idx: TaskIdx) -> bool:
        """Whether we haven't exhausted fetch attempts for this task."""
        return (
            self._state_fetch_failures[op_id].get(task_idx, 0)
            < _MAX_STATE_FETCH_FAILED_ATTEMPTS
        )

    def _reset_fetch_failures(self, op_id: OpId, task_idx: TaskIdx) -> None:
        self._state_fetch_failures[op_id][task_idx] = 0

    def _record_fetch_failure(self, op_id: OpId, task_idx: TaskIdx) -> None:
        failures = self._state_fetch_failures[op_id]
        failures[task_idx] = failures.get(task_idx, 0) + 1

    def _refresh_state(
        self,
        operator: "PhysicalOperator",
        task_idx: TaskIdx,
        old_state: HangingExecutionState | None,
        task_info: RunningTaskInfo,
    ) -> HangingExecutionState:
        """Build a HangingExecutionState, fetching task metadata lazily.

        Task metadata (pid, node_id, attempt) is fetched from the Ray
        State API only when unknown or potentially stale (e.g. after the
        task made progress then stalled again).  Fetches that fail are
        counted in ``state_fetch_failures`` (mutated in-place) and
        skipped after ``_MAX_STATE_FETCH_FAILED_ATTEMPTS``.
        """
        task_metadata: TaskMetadata | None = None
        bytes_output: int | None = None
        if old_state is not None:
            task_metadata = old_state.task_metadata
            bytes_output = old_state.bytes_output

        if bytes_output != task_info.bytes_output:
            self._reset_fetch_failures(operator.id, task_idx)
            task_metadata = None

        if task_metadata is None and self._should_retry_metadata_fetch(
            operator.id, task_idx
        ):
            task_state = get_latest_state_for_task(task_info.task_id)
            if task_state is None:
                self._record_fetch_failure(operator.id, task_idx)
            else:
                task_metadata = TaskMetadata.from_task_state(task_state)

        return HangingExecutionState(
            operator_id=operator.id,
            task_idx=task_idx,
            task_id=task_info.task_id,
            task_metadata=task_metadata,
            bytes_output=task_info.bytes_output,
            task_start_time=task_info.start_time,
            start_time_hanging=task_info.last_updated,
        )

    def detect(self) -> List[Issue]:

        issues: List[Issue] = []
        # Build fresh map each cycle so that tasks which finished or
        # dropped below the threshold are automatically pruned.
        hanging_op_tasks: DefaultDict[
            OpId, Dict[TaskIdx, HangingExecutionState]
        ] = defaultdict(dict)

        for operator in self._operators:
            if operator.has_execution_finished():
                continue

            op_metrics = operator.metrics
            op_task_stats = op_metrics._op_task_duration_stats
            # 1) Skip if not reached minimum task count
            if op_task_stats.count() < self._op_task_stats_min_count:
                continue

            # 2) Skip if under threshold of mean + z-score * stddev
            mean = op_task_stats.mean()
            stddev = op_task_stats.stddev()
            threshold = mean + self._op_task_stats_std_factor_threshold * stddev

            for task_idx, task_info in op_metrics._running_tasks.items():

                time_since_last_update = time.time() - task_info.last_updated
                if time_since_last_update < threshold:
                    continue

                old_state = self._hanging_op_tasks[operator.id].get(task_idx)
                new_state = self._refresh_state(
                    operator=operator,
                    task_idx=task_idx,
                    old_state=old_state,
                    task_info=task_info,
                )

                hanging_op_tasks[operator.id][task_idx] = new_state

                # 3) Skip if there wasn't any meaningful update to the task. For example,
                # a task may have made some progress (yield bytes), or a task is retried
                # giving a different node_id/attempt_number. Also skip, if we still have
                # metadata retry attempts (so that we don't double log w/ and w/o the metadata.)
                if old_state == new_state or (
                    new_state.task_metadata is None
                    and self._should_retry_metadata_fetch(operator.id, task_idx)
                ):
                    continue

                issues.append(
                    self._create_issue(
                        operator=operator, hanging_execution_state=new_state
                    )
                )

        self._hanging_op_tasks = hanging_op_tasks
        return issues

    def detection_time_interval_s(self) -> float:
        return self._detector_cfg.detection_time_interval_s


def get_latest_state_for_task(task_id: ray.TaskID) -> TaskState | None:
    """Query the Ray State API for the latest attempt of a task.

    Returns the TaskState with the highest attempt_number when multiple
    attempts exist, or None if the task is not found (can happen when
    get_task() is called shortly after submission, before the state API
    has indexed it) or the API is unreachable.
    """
    try:
        task_state: TaskState | List[TaskState] | None = get_task(
            task_id.hex(),
            timeout=0,
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
        task_state = max(task_state, key=lambda ts: ts.attempt_number)
    return task_state

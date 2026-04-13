import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, DefaultDict, Dict, List, Optional, Union

import requests

import ray
from ray.data._internal.observability.diagnostics.issue_detector import (
    Issue,
    IssueDetector,
    IssueType,
)
from ray.data._internal.observability.op_runtime_metrics import RunningTaskInfo
from ray.util.state import get_task
from ray.util.state.common import TaskState
from ray.util.state.exception import RayStateApiException

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor
    from ray.data._internal.physical.physical_operator import (
        PhysicalOperator,
    )

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
    # NOTE This is from perf_couinter()
    start_time_hanging: float

    def hanging_time(self):
        return time.perf_counter() - self.start_time_hanging


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
        self._hanging_op_tasks: HangingOpTasks = defaultdict(dict)

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
        task_info = ""
        if meta is not None:
            task_info = f"(pid={meta.pid}, node_id={meta.node_id}, attempt={meta.attempt_number}) "

        hanging_time = hes.hanging_time()
        hanging_since = _format_timestamp(time.time() - hanging_time)

        message = (
            f"A task (task_id={hes.task_id}) of operator "
            f"{operator.name} {task_info}has been running or stuck in scheduling for "
            f"{hanging_time:.2f}s, which is longer than the average task "
            f"duration + z-score * stddev of this operator "
            f"({avg_duration:.2f} + "
            f"{self._op_task_stats_std_factor_threshold} * "
            f"{stdev:.2f}s). "
            f"Last time task produced output or made any progress was {hanging_since}. "
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

    def _refresh_state(
        self,
        operator: "PhysicalOperator",
        task_idx: TaskIdx,
        old_state: Optional[HangingExecutionState],
        task_info: RunningTaskInfo,
    ) -> HangingExecutionState:
        """Build a HangingExecutionState, fetching task metadata lazily.

        Task metadata (pid, node_id, attempt) is fetched from the Ray
        State API only when unknown or potentially stale (e.g. after the
        task made progress then stalled again).
        """
        task_metadata: Optional[TaskMetadata] = None
        if old_state is not None:
            task_metadata = old_state.task_metadata
        else:
            task_metadata = get_latest_state_for_task(task_info.task_id)

        return HangingExecutionState(
            operator_id=operator.id,
            task_idx=task_idx,
            task_id=task_info.task_id,
            task_metadata=task_metadata,
            bytes_output=task_info.bytes_output,
            start_time_hanging=task_info.last_updated,
        )

    def detect(self) -> List[Issue]:

        issues: List[Issue] = []
        # Build fresh maps each cycle so that tasks which finished or
        # dropped below the threshold are automatically pruned.
        hanging_op_tasks: HangingOpTasks = defaultdict(dict)

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

                time_since_last_update = time.perf_counter() - task_info.last_updated
                if time_since_last_update <= threshold:
                    continue

                old_state = self._hanging_op_tasks[operator.id].get(task_idx)

                new_state = self._refresh_state(
                    operator=operator,
                    task_idx=task_idx,
                    old_state=old_state,
                    task_info=task_info,
                )

                hanging_op_tasks[operator.id][task_idx] = new_state

                if old_state == new_state:
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

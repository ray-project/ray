import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional, Set

from ray.anyscale.data.issue_detection.issue_detector import Issue, IssueDetector

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor
    from ray.data.context import DataContext

# Default minimum count of tasks before using adaptive thresholds
DEFAULT_OP_TASK_STATS_MIN_COUNT = 10
# Default multiple of standard deviations to use as hanging threshold
DEFAULT_OP_TASK_STATS_STD_FACTOR = 10
# Default detection time interval.
DEFAULT_DETECTION_TIME_INTERVAL_S = 30.0


@dataclass
class HangingExecutionState:
    operator_name: str
    task_idx: int
    bytes_output: int
    start_time_hanging: float


@dataclass
class HangingExecutionIssueDetectorConfig:
    op_task_stats_min_count: int = field(default=DEFAULT_OP_TASK_STATS_MIN_COUNT)
    op_task_stats_std_factor: float = field(default=DEFAULT_OP_TASK_STATS_STD_FACTOR)
    detection_time_interval_s: float = DEFAULT_DETECTION_TIME_INTERVAL_S


class HangingExecutionIssueDetector(IssueDetector):
    def __init__(self, executor: "StreamingExecutor", ctx: "DataContext"):
        super().__init__(executor, ctx)
        self._detector_cfg: HangingExecutionIssueDetectorConfig = (
            ctx.issue_detectors_config.hanging_detector_config
        )
        self._op_task_stats_min_count = self._detector_cfg.op_task_stats_min_count
        self._op_task_stats_std_factor_threshold = (
            self._detector_cfg.op_task_stats_std_factor
        )

        # Map of operator name to dict of task_idx to hanging execution info (bytes read and
        # start time for hanging time calculation)
        self._state_map: Dict[str, Dict[int, HangingExecutionState]] = defaultdict(dict)
        # Map of operator name to set of task_idx that are hanging
        self._hanging_op_tasks: Dict[str, Set[int]] = defaultdict(set)

    def _create_issues(
        self, hanging_op_tasks: Set[HangingExecutionState]
    ) -> List[Issue]:
        issues = []
        for state in hanging_op_tasks:
            if state.task_idx not in self._hanging_op_tasks[state.operator_name]:
                issues.append(
                    Issue(
                        message=(
                            f"A task for operator {state.operator_name} with task index {state.task_idx} "
                            f"has been hanging for >{time.perf_counter() - state.start_time_hanging}s."
                        )
                    )
                )
                self._hanging_op_tasks[state.operator_name].add(state.task_idx)

        return issues

    def detect(self) -> Optional[Issue]:
        op_task_stats_map = {}
        for operator, op_state in self._executor._topology.items():
            op_metrics = operator.metrics
            op_task_stats_map[operator.name] = op_metrics._op_task_duration_stats
            if op_state._finished:
                # Remove finished operators / tasks from the state map
                if operator.name in self._state_map:
                    del self._state_map[operator.name]
                if operator.name in self._hanging_op_tasks:
                    del self._hanging_op_tasks[operator.name]
            else:
                active_tasks_idx = set()
                for task in operator.get_active_tasks():
                    task_info = op_metrics._running_tasks.get(task.task_index(), None)
                    if task_info is None:
                        # if the task is not in the running tasks map, it has finished
                        # remove it from the state map and hanging op tasks, if present
                        self._state_map[operator.name].pop(task.task_index(), None)
                        self._hanging_op_tasks[operator.name].discard(task.task_index())
                        continue

                    active_tasks_idx.add(task.task_index())
                    bytes_output = task_info.bytes_outputs

                    prev_state_value = self._state_map[operator.name].get(
                        task.task_index(), None
                    )

                    if (
                        prev_state_value is None
                        or bytes_output != prev_state_value.bytes_output
                    ):
                        self._state_map[operator.name][
                            task.task_index()
                        ] = HangingExecutionState(
                            operator_name=operator.name,
                            task_idx=task.task_index(),
                            bytes_output=bytes_output,
                            start_time_hanging=time.perf_counter(),
                        )

                # Remove any tasks that are no longer active
                task_idxs_to_remove = (
                    set(self._state_map[operator.name].keys()) - active_tasks_idx
                )
                for task_idx in task_idxs_to_remove:
                    del self._state_map[operator.name][task_idx]

        hanging_op_tasks = []
        for op_name, op_state_values in self._state_map.items():
            op_task_stats = op_task_stats_map[op_name]
            for task_idx, state_value in op_state_values.items():
                curr_time = time.perf_counter() - state_value.start_time_hanging
                if op_task_stats.count() > self._op_task_stats_min_count:
                    mean = op_task_stats.mean()
                    stddev = op_task_stats.stddev()
                    threshold = mean + self._op_task_stats_std_factor_threshold * stddev

                    if curr_time > threshold:
                        hanging_op_tasks.append(state_value)

        # create issues for newly detected hanging tasks, then update the hanging task set
        issues = self._create_issues(hanging_op_tasks=hanging_op_tasks)

        return issues

    def detection_time_interval_s(self) -> float:
        return self._detector_cfg.detection_time_interval_s

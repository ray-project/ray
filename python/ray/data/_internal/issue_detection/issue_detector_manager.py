import logging
import time
from typing import TYPE_CHECKING, Dict, List

from ray.data._internal.issue_detection.issue_detector import (
    Issue,
    IssueDetector,
    IssueType,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )
    from ray.data._internal.execution.streaming_executor import StreamingExecutor

logger = logging.getLogger(__name__)


class IssueDetectorManager:
    def __init__(self, executor: "StreamingExecutor"):
        ctx = executor._data_context
        self._issue_detectors: List[IssueDetector] = [
            cls(executor, ctx) for cls in ctx.issue_detectors_config.detectors
        ]
        self._last_detection_times: Dict[IssueDetector, float] = {
            detector: time.perf_counter() for detector in self._issue_detectors
        }
        self.executor = executor

    def invoke_detectors(self) -> None:
        curr_time = time.perf_counter()
        issues = []
        for detector in self._issue_detectors:
            if detector.detection_time_interval_s() == -1:
                continue

            if (
                curr_time - self._last_detection_times[detector]
                > detector.detection_time_interval_s()
            ):
                issues.extend(detector.detect())

                self._last_detection_times[detector] = time.perf_counter()

        self._report_issues(issues)

    def _report_issues(self, issues: List[Issue]) -> None:
        operators: Dict[str, "PhysicalOperator"] = {}
        for operator in self.executor._topology.keys():
            operators[operator.id] = operator
            # Reset issue detector metrics for each operator so that previous issues
            # don't affect the current ones.
            operator.metrics._issue_detector_hanging = 0
            operator.metrics._issue_detector_high_memory = 0

        for issue in issues:
            logger.warning(issue.message)
            operator = operators.get(issue.operator_id)
            if not operator:
                continue
            if issue.issue_type == IssueType.HANGING:
                operator.metrics._issue_detector_hanging += 1
            if issue.issue_type == IssueType.HIGH_MEMORY:
                operator.metrics._issue_detector_high_memory += 1
        if len(issues) > 0:
            logger.warning(
                "To disable issue detection, run DataContext.get_current().issue_detectors_config.detectors = []."
            )

import logging
import time
from typing import TYPE_CHECKING, List

from ray.anyscale.data.issue_detection.issue_detector import Issue, IssueDetector

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor

logger = logging.getLogger(__name__)


class IssueDetectorManager:
    def __init__(self, executor: "StreamingExecutor"):
        ctx = executor._data_context
        self._issue_detectors: List[IssueDetector] = [
            cls(executor, ctx) for cls in ctx.issue_detectors_config.detectors
        ]

        self._last_detection_time = time.perf_counter()
        self._detection_time_interval_s = (
            ctx.issue_detectors_config.detection_time_interval_s
        )

    def invoke_detectors(self) -> None:
        # TODO(mowen): Add support to allow individual detectors to have their
        # own intervals.
        curr_time = time.perf_counter()
        if curr_time - self._last_detection_time > self._detection_time_interval_s:
            issues = []
            for detector in self._issue_detectors:
                issues.extend(detector.detect())

            self._report_issues(issues)
            self._last_detection_time = time.perf_counter()

    def _report_issues(self, issues: List[Issue]) -> None:
        for issue in issues:
            logger.warning(issue.message)
        if len(issues) > 0:
            logger.warning(
                "To disable issue detection, run DataContext.get_current().issue_detectors_config.detectors = []."
            )

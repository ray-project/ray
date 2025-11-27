import logging
import time
from typing import TYPE_CHECKING, Dict, List

from ray.core.generated.export_dataset_operator_event_pb2 import (
    ExportDatasetOperatorEventData as ProtoOperatorEventData,
)
from ray.data._internal.issue_detection.issue_detector import (
    Issue,
    IssueDetector,
    IssueType,
)
from ray.data._internal.operator_event_exporter import (
    OperatorEvent,
    format_export_issue_event_name,
    get_operator_event_exporter,
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
            cls.from_executor(executor) for cls in ctx.issue_detectors_config.detectors
        ]
        self._last_detection_times: Dict[IssueDetector, float] = {
            detector: time.perf_counter() for detector in self._issue_detectors
        }
        self.executor = executor
        self._operator_event_exporter = get_operator_event_exporter()

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
        op_to_id: Dict["PhysicalOperator", str] = {}
        for i, operator in enumerate(self.executor._topology.keys()):
            operators[operator.id] = operator
            op_to_id[operator] = self.executor._get_operator_id(operator, i)
            # Reset issue detector metrics for each operator so that previous issues
            # don't affect the current ones.
            operator.metrics._issue_detector_hanging = 0
            operator.metrics._issue_detector_high_memory = 0

        for issue in issues:
            logger.warning(issue.message)
            operator = operators.get(issue.operator_id)
            if not operator:
                continue

            issue_event_type = format_export_issue_event_name(issue.issue_type)
            if (
                self._operator_event_exporter is not None
                and issue_event_type
                in ProtoOperatorEventData.DatasetOperatorEventType.keys()
            ):
                event_time = time.time()
                operator_event = OperatorEvent(
                    dataset_id=issue.dataset_name,
                    operator_id=op_to_id[operator],
                    operator_name=operator.name,
                    event_time=event_time,
                    event_type=issue_event_type,
                    message=issue.message,
                )
                self._operator_event_exporter.export_operator_event(operator_event)

            if issue.issue_type == IssueType.HANGING:
                operator.metrics._issue_detector_hanging += 1
            if issue.issue_type == IssueType.HIGH_MEMORY:
                operator.metrics._issue_detector_high_memory += 1
        if len(issues) > 0:
            logger.warning(
                "To disable issue detection, run DataContext.get_current().issue_detectors_config.detectors = []."
            )

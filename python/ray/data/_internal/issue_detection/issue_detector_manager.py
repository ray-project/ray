import logging
import random
import textwrap
import time
from typing import TYPE_CHECKING, Dict, List

from ray.data._internal.issue_detection.issue_detector import (
    Issue,
    IssueDetector,
    IssueType,
)
from ray.data._internal.operator_event_exporter import (
    OperatorEvent,
    get_operator_event_exporter,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )
    from ray.data._internal.execution.streaming_executor import StreamingExecutor

logger = logging.getLogger(__name__)


ISSUE_TYPE_TO_EXPORT_OPERATOR_EVENT_TYPE = {
    IssueType.HANGING: "ISSUE_DETECTION_HANGING",
    IssueType.HIGH_MEMORY: "ISSUE_DETECTION_HIGH_MEMORY",
}


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

        # generate HANGING issues
        if random.random() > 0.8:
            operator = random.choice(list(self.executor._topology.keys()))
            issues.append(
                Issue(
                    dataset_name=self.executor._dataset_id,
                    operator_id=operator.id,
                    issue_type=IssueType.HANGING,
                    message=(
                        f"A task of operator {operator.name} with task index "
                        f"xxx has been running for xxxs, which is longer"
                        f" than the average task duration of this operator (xxxs)."
                        f" If this message persists, please check the stack trace of the "
                        "task for potential hanging issues."
                    ),
                )
            )

        # generate HIGH MEMORY issues
        if random.random() > 0.8:
            operator = random.choice(list(self.executor._topology.keys()))
            message = f"""
Operator '{operator.name}' uses xxx of memory per task on average, but Ray
only requests xxx per task at the start of the pipeline.
To avoid out-of-memory errors, consider setting `memory=xxx` in the
appropriate function or method call. (This might be unnecessary if the number of
concurrent tasks is low.)
To change the frequency of this warning, set
`DataContext.get_current().issue_detectors_config.high_memory_detector_config.detection_time_interval_s`,
or disable the warning by setting value to -1. (current value: xxx)
            """
            formatted_paragraphs = []
            for paragraph in message.split("\n\n"):
                formatted_paragraph = textwrap.fill(
                    paragraph, break_long_words=False
                ).strip()
                formatted_paragraphs.append(formatted_paragraph)
            formatted_message = "\n\n".join(formatted_paragraphs)
            message = "\n\n" + formatted_message + "\n"
            issues.append(
                Issue(
                    dataset_name=self.executor._dataset_id,
                    operator_id=operator.id,
                    issue_type=IssueType.HIGH_MEMORY,
                    message=message,
                )
            )

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

            if (
                self._operator_event_exporter is not None
                and issue.issue_type in ISSUE_TYPE_TO_EXPORT_OPERATOR_EVENT_TYPE
            ):
                event_time = time.time()
                operator_event = OperatorEvent(
                    dataset_id=issue.dataset_name,
                    operator_id=op_to_id[operator],
                    operator_name=operator.name,
                    event_time=event_time,
                    event_type=ISSUE_TYPE_TO_EXPORT_OPERATOR_EVENT_TYPE[
                        issue.issue_type
                    ],
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

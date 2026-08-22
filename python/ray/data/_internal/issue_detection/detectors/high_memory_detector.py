import math
import textwrap
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Union

from ray.data._internal.execution.operators.map_operator import (
    MapOperator,
    get_safe_default_logical_memory,
)
from ray.data._internal.execution.util import memory_string
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

HIGH_MEMORY_PERIODIC_WARNING = """
Operator '{op_name}' uses {memory_per_task} of memory per task on average, but Ray
only requests {initial_memory_request} per task at the start of the pipeline.

To avoid out-of-memory errors, consider setting `memory={recommended_memory_bytes}`
({recommended_memory}) in the appropriate function or method call. (This might be
unnecessary if the number of concurrent tasks is low.)

To change the frequency of this warning, set
`DataContext.get_current().issue_detectors_config.high_memory_detector_config.detection_time_interval_s`,
or disable the warning by setting value to -1. (current value:
{detection_time_interval_s})
"""  # noqa: E501

HIGH_MEMORY_FINAL_WARNING = """
Operator '{op_name}' used up to {max_memory} of memory per worker.
{memory_configuration} To avoid out-of-memory errors, set
`memory={recommended_memory_bytes}` ({recommended_memory}) in the appropriate
function or method call.
"""


@dataclass
class HighMemoryIssueDetectorConfig:
    detection_time_interval_s: float = 30


class HighMemoryIssueDetector(IssueDetector):
    def __init__(
        self,
        dataset_id: str,
        operators: List["PhysicalOperator"],
        config: HighMemoryIssueDetectorConfig,
    ):
        self._dataset_id = dataset_id
        self._detector_cfg = config
        self._completion_checked_operators: Set[MapOperator] = set()

        self._initial_memory_requests: Dict[MapOperator, Optional[int]] = {}
        for op in operators:
            if isinstance(op, MapOperator):
                self._initial_memory_requests[
                    op
                ] = op._get_dynamic_ray_remote_args().get("memory")

    @classmethod
    def from_executor(cls, executor: "StreamingExecutor") -> "HighMemoryIssueDetector":
        """Factory method to create a HighMemoryIssueDetector from a StreamingExecutor.

        Args:
            executor: The StreamingExecutor instance to extract dependencies from.

        Returns:
            An instance of HighMemoryIssueDetector.
        """
        operators = list(executor._topology.keys()) if executor._topology else []
        ctx = executor._data_context
        return cls(
            dataset_id=executor._dataset_id,
            operators=operators,
            config=ctx.issue_detectors_config.high_memory_detector_config,
        )

    def detect(self) -> List[Issue]:
        issues = []
        for op, memory_request in self._initial_memory_requests.items():
            if op.has_completed():
                if op in self._completion_checked_operators:
                    continue
                self._completion_checked_operators.add(op)
                issue = self._detect_issue_on_operator_completion(op, memory_request)
                if issue is not None:
                    issues.append(issue)
                continue

            if op.metrics.average_max_uss_per_task is None:
                continue

            remote_args = op._get_dynamic_ray_remote_args()
            safe_memory_per_task = get_safe_default_logical_memory(remote_args)
            initial_memory_request = memory_request or 0

            if (
                op.metrics.average_max_uss_per_task > initial_memory_request
                and op.metrics.average_max_uss_per_task >= safe_memory_per_task
            ):
                recommended_memory = _get_recommended_memory(
                    op.metrics.average_max_uss_per_task
                )
                message = HIGH_MEMORY_PERIODIC_WARNING.format(
                    op_name=op.name,
                    memory_per_task=memory_string(op.metrics.average_max_uss_per_task),
                    initial_memory_request=memory_string(initial_memory_request),
                    recommended_memory=memory_string(recommended_memory),
                    recommended_memory_bytes=recommended_memory,
                    detection_time_interval_s=self.detection_time_interval_s(),
                )
                issues.append(
                    Issue(
                        dataset_name=self._dataset_id,
                        operator_id=op.id,
                        issue_type=IssueType.HIGH_MEMORY,
                        message=_format_message(message),
                    )
                )

        return issues

    def _detect_issue_on_operator_completion(
        self, op: MapOperator, memory_request: Optional[int]
    ) -> Optional[Issue]:
        if memory_request is None:
            return None

        max_uss_bytes = op.metrics.max_uss_bytes.max
        if max_uss_bytes is None:
            return None

        max_uss_bytes = int(max_uss_bytes)
        recommended_memory = _get_recommended_memory(max_uss_bytes)
        if recommended_memory <= memory_request:
            return None

        memory_configuration = (
            f"The configured logical memory was {memory_string(memory_request)}."
        )
        message = HIGH_MEMORY_FINAL_WARNING.format(
            op_name=op.name,
            max_memory=memory_string(max_uss_bytes),
            memory_configuration=memory_configuration,
            recommended_memory=memory_string(recommended_memory),
            recommended_memory_bytes=recommended_memory,
        )
        return Issue(
            dataset_name=self._dataset_id,
            operator_id=op.id,
            issue_type=IssueType.HIGH_MEMORY,
            message=_format_message(message),
        )

    def detection_time_interval_s(self) -> float:
        return self._detector_cfg.detection_time_interval_s


def _get_recommended_memory(memory_usage: Union[int, float]) -> int:
    if isinstance(memory_usage, int):
        return (5 * memory_usage + 3) // 4
    return math.ceil(5 * memory_usage / 4)


def _format_message(message: str) -> str:
    # Apply some formatting to make the message look nicer when printed.
    formatted_paragraphs = []
    for paragraph in message.split("\n\n"):
        formatted_paragraph = textwrap.fill(paragraph, break_long_words=False).strip()
        formatted_paragraphs.append(formatted_paragraph)
    formatted_message = "\n\n".join(formatted_paragraphs)
    return "\n\n" + formatted_message + "\n"

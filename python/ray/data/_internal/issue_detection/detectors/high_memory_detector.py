import textwrap
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List

from ray.data._internal.execution.operators.map_operator import MapOperator
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

To avoid out-of-memory errors, consider setting `memory={memory_per_task}` in the
appropriate function or method call. (This might be unnecessary if the number of
concurrent tasks is low.)

To change the frequency of this warning, set
`DataContext.get_current().issue_detectors_config.high_memory_detector_config.detection_time_interval_s`,
or disable the warning by setting value to -1. (current value:
{detection_time_interval_s})
"""  # noqa: E501


@dataclass
class HighMemoryIssueDetectorConfig:
    detection_time_interval_s: float = 30


class HighMemoryIssueDetector(IssueDetector):
    # Many nodes have a 4 GiB : 1 core ratio, but this isn't always the case (e.g., for
    # high memory nodes).
    _MEMORY_PER_CORE_ESTIMATE = 4 * 1024**3

    def __init__(
        self,
        dataset_id: str,
        operators: List["PhysicalOperator"],
        config: HighMemoryIssueDetectorConfig,
    ):
        self._dataset_id = dataset_id
        self._detector_cfg = config
        self._operators = operators

        self._initial_memory_requests: Dict[MapOperator, int] = {}
        for op in operators:
            if isinstance(op, MapOperator):
                self._initial_memory_requests[op] = (
                    op._get_dynamic_ray_remote_args().get("memory") or 0
                )

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
        for op in self._operators:
            if not isinstance(op, MapOperator):
                continue

            if op.metrics.average_max_uss_per_task is None:
                continue

            remote_args = op._get_dynamic_ray_remote_args()
            num_cpus_per_task = remote_args.get("num_cpus", 1)
            max_memory_per_task = self._MEMORY_PER_CORE_ESTIMATE * num_cpus_per_task

            if (
                op.metrics.average_max_uss_per_task > self._initial_memory_requests[op]
                and op.metrics.average_max_uss_per_task >= max_memory_per_task
            ):
                message = HIGH_MEMORY_PERIODIC_WARNING.format(
                    op_name=op.name,
                    memory_per_task=memory_string(op.metrics.average_max_uss_per_task),
                    initial_memory_request=memory_string(
                        self._initial_memory_requests[op]
                    ),
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

    def detection_time_interval_s(self) -> float:
        return self._detector_cfg.detection_time_interval_s


def _format_message(message: str) -> str:
    # Apply some formatting to make the message look nicer when printed.
    formatted_paragraphs = []
    for paragraph in message.split("\n\n"):
        formatted_paragraph = textwrap.fill(paragraph, break_long_words=False).strip()
        formatted_paragraphs.append(formatted_paragraph)
    formatted_message = "\n\n".join(formatted_paragraphs)
    return "\n\n" + formatted_message + "\n"

import textwrap
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional

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


# ---------------------------------------------------------------------------
# Snapshot type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HighMemoryOpSnapshot:
    op_id: str
    op_name: str
    dataset_id: str
    average_max_uss_per_task: Optional[int]
    num_cpus_per_task: float
    initial_memory_request: int
    detection_time_interval_s: float

    @classmethod
    def from_operator(
        cls,
        op: "PhysicalOperator",
        dataset_id: str,
        initial_memory_request: int,
        detection_time_interval_s: float,
    ) -> Optional["HighMemoryOpSnapshot"]:
        """Build a snapshot from a live MapOperator. Returns None for non-map ops."""
        if not isinstance(op, MapOperator):
            return None
        remote_args = op._get_dynamic_ray_remote_args()
        return cls(
            op_id=op.id,
            op_name=op.name,
            dataset_id=dataset_id,
            average_max_uss_per_task=op.metrics.average_max_uss_per_task,
            num_cpus_per_task=remote_args.get("num_cpus", 1),
            initial_memory_request=initial_memory_request,
            detection_time_interval_s=detection_time_interval_s,
        )


# ---------------------------------------------------------------------------
# Detector
# ---------------------------------------------------------------------------


class HighMemoryIssueDetector(IssueDetector):
    _MEMORY_PER_CORE_ESTIMATE = 4 * 1024**3

    def __init__(
        self,
        config: HighMemoryIssueDetectorConfig,
        dataset_id: str = "",
        operators: Optional[List["PhysicalOperator"]] = None,
    ):
        self._dataset_id = dataset_id
        self._detector_cfg = config
        self._operators = operators or []

        self._initial_memory_requests: Dict[MapOperator, int] = {}
        for op in self._operators:
            if isinstance(op, MapOperator):
                self._initial_memory_requests[op] = (
                    op._get_dynamic_ray_remote_args().get("memory") or 0
                )

    @classmethod
    def from_executor(cls, executor: "StreamingExecutor") -> "HighMemoryIssueDetector":
        operators = list(executor._topology.keys()) if executor._topology else []
        ctx = executor._data_context
        return cls(
            config=ctx.issue_detectors_config.high_memory_detector_config,
            dataset_id=executor._dataset_id,
            operators=operators,
        )

    def detect(self) -> List[Issue]:
        """Sync entry point — builds snapshots from live operators and delegates."""
        snapshots = []
        for op in self._operators:
            snap = HighMemoryOpSnapshot.from_operator(
                op,
                self._dataset_id,
                self._initial_memory_requests.get(op, 0),
                self.detection_time_interval_s(),
            )
            if snap is not None:
                snapshots.append(snap)
        return self.detect_from_snapshots(snapshots)

    def detect_from_snapshots(
        self, snapshots: List[HighMemoryOpSnapshot]
    ) -> List[Issue]:
        """Core detection logic operating on serializable snapshots."""
        issues: List[Issue] = []
        for snap in snapshots:
            if snap.average_max_uss_per_task is None:
                continue

            max_memory_per_task = (
                self._MEMORY_PER_CORE_ESTIMATE * snap.num_cpus_per_task
            )

            if (
                snap.average_max_uss_per_task > snap.initial_memory_request
                and snap.average_max_uss_per_task >= max_memory_per_task
            ):
                message = HIGH_MEMORY_PERIODIC_WARNING.format(
                    op_name=snap.op_name,
                    memory_per_task=memory_string(snap.average_max_uss_per_task),
                    initial_memory_request=memory_string(snap.initial_memory_request),
                    detection_time_interval_s=snap.detection_time_interval_s,
                )
                issues.append(
                    Issue(
                        dataset_name=snap.dataset_id,
                        operator_id=snap.op_id,
                        issue_type=IssueType.HIGH_MEMORY,
                        message=_format_message(message),
                    )
                )
        return issues

    def detection_time_interval_s(self) -> float:
        return self._detector_cfg.detection_time_interval_s


def _format_message(message: str) -> str:
    formatted_paragraphs = []
    for paragraph in message.split("\n\n"):
        formatted_paragraph = textwrap.fill(paragraph, break_long_words=False).strip()
        formatted_paragraphs.append(formatted_paragraph)
    formatted_message = "\n\n".join(formatted_paragraphs)
    return "\n\n" + formatted_message + "\n"

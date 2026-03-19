import logging
from concurrent import futures
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Optional

from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.execution.interfaces.async_service import AsyncCallee
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.issue_detection.detectors.hanging_detector import (
    HangingExecutionIssueDetector,
    HangingExecutionIssueDetectorConfig,
    OpDetectionSnapshot,
)
from ray.data._internal.issue_detection.detectors.hash_shuffle_detector import (
    HashShuffleAggregatorIssueDetector,
    HashShuffleAggregatorIssueDetectorConfig,
    HashShuffleOpSnapshot,
)
from ray.data._internal.issue_detection.detectors.high_memory_detector import (
    HighMemoryIssueDetector,
    HighMemoryIssueDetectorConfig,
    HighMemoryOpSnapshot,
)
from ray.data._internal.issue_detection.issue_detector import Issue

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IssueDetectionInput:
    """Combined snapshot input for all issue detectors."""

    hanging_snapshots: List[OpDetectionSnapshot]
    high_memory_snapshots: List[HighMemoryOpSnapshot]
    hash_shuffle_snapshots: List[HashShuffleOpSnapshot]


class IssueDetectionTask(AsyncCallee[IssueDetectionInput, List[Issue]]):
    """Runs all issue detectors in the async service actor process.

    Each detector delegates to ``detect_from_snapshots()`` so all
    detection logic lives in the respective detector modules.
    Submission interval is enforced by ``AsyncServiceHandle``.
    """

    def __init__(
        self,
        hanging_config: HangingExecutionIssueDetectorConfig,
        high_memory_config: HighMemoryIssueDetectorConfig,
        hash_shuffle_config: HashShuffleAggregatorIssueDetectorConfig,
        min_interval_s: float,
    ):
        self.min_interval_s = min_interval_s
        self._hanging_detector = HangingExecutionIssueDetector(hanging_config)
        self._high_memory_detector = HighMemoryIssueDetector(high_memory_config)
        self._hash_shuffle_detector = HashShuffleAggregatorIssueDetector(
            hash_shuffle_config
        )

    def run(
        self,
        input: IssueDetectionInput,
        tpe: futures.ThreadPoolExecutor,
    ) -> List[Issue]:
        issues: List[Issue] = []
        issues.extend(
            self._hanging_detector.detect_from_snapshots(input.hanging_snapshots)
        )
        issues.extend(
            self._high_memory_detector.detect_from_snapshots(
                input.high_memory_snapshots
            )
        )
        issues.extend(
            self._hash_shuffle_detector.detect_from_snapshots(
                input.hash_shuffle_snapshots
            )
        )
        return issues


class IssueDetectionExecutionCallback(ExecutionCallback):
    """ExecutionCallback that runs all issue detection via the async service.

    When the async task is registered, all detectors run asynchronously.
    ``on_execution_step`` acts as a sync fallback — it only runs when the
    executor has not registered this callback with the async service.
    """

    _NON_SERIALIZABLE_FIELDS = (
        "_executor",
        "_issue_detector_manager",
        "_initial_memory_requests",
    )

    def __getstate__(self):
        return {
            k: v
            for k, v in self.__dict__.items()
            if k not in self._NON_SERIALIZABLE_FIELDS
        }

    def before_execution_starts(self, executor: "StreamingExecutor"):
        self._executor = executor

        from ray.data._internal.issue_detection.issue_detector_manager import (
            IssueDetectorManager,
        )

        self._issue_detector_manager = IssueDetectorManager(executor)
        executor._issue_detector_manager = self._issue_detector_manager

        self._initial_memory_requests: dict = {}
        for op in executor._topology:
            if isinstance(op, MapOperator):
                self._initial_memory_requests[op] = (
                    op._get_dynamic_ray_remote_args().get("memory") or 0
                )

    def on_execution_step(self, executor: "StreamingExecutor"):
        if not executor._uses_async_service(self):
            self._issue_detector_manager.invoke_detectors()

    # -- Async path --

    def create_async_task(self) -> Optional[AsyncCallee]:
        cfg = self._executor._data_context.issue_detectors_config
        intervals = [
            cfg.hanging_detector_config.detection_time_interval_s,
            cfg.high_memory_detector_config.detection_time_interval_s,
            cfg.hash_shuffle_detector_config.detection_time_interval_s,
        ]
        return IssueDetectionTask(
            hanging_config=cfg.hanging_detector_config,
            high_memory_config=cfg.high_memory_detector_config,
            hash_shuffle_config=cfg.hash_shuffle_detector_config,
            min_interval_s=min(intervals),  # TODO(Justin): Fix
        )

    def build_refresh_input(self) -> IssueDetectionInput:
        executor = self._executor
        dataset_id = executor._dataset_id
        cfg = executor._data_context.issue_detectors_config

        hanging_snapshots: List[OpDetectionSnapshot] = []
        high_memory_snapshots: List[HighMemoryOpSnapshot] = []
        hash_shuffle_snapshots: List[HashShuffleOpSnapshot] = []

        for op in executor._topology:
            hanging_snapshots.append(OpDetectionSnapshot.from_operator(op, dataset_id))

            hm_snap = HighMemoryOpSnapshot.from_operator(
                op,
                dataset_id,
                self._initial_memory_requests.get(op, 0),
                cfg.high_memory_detector_config.detection_time_interval_s,
            )
            if hm_snap is not None:
                high_memory_snapshots.append(hm_snap)

            hs_snap = HashShuffleOpSnapshot.from_operator(op, dataset_id)
            if hs_snap is not None:
                hash_shuffle_snapshots.append(hs_snap)

        return IssueDetectionInput(
            hanging_snapshots=hanging_snapshots,
            high_memory_snapshots=high_memory_snapshots,
            hash_shuffle_snapshots=hash_shuffle_snapshots,
        )

    def apply_refresh_result(self, result: Any):
        self._issue_detector_manager._report_issues(result)

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional

import ray
from ray.data._internal.execution.operators.hash_shuffle import (
    AggregatorHealthInfo,
    HashShuffleOperator,
)
from ray.data._internal.issue_detection.issue_detector import (
    Issue,
    IssueDetector,
    IssueType,
)
from ray.data._internal.util import GiB

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )
    from ray.data._internal.execution.streaming_executor import StreamingExecutor


@dataclass
class HashShuffleAggregatorIssueDetectorConfig:
    """Configuration for HashShuffleAggregatorIssueDetector."""

    detection_time_interval_s: float = 30.0
    min_wait_time_s: float = 300.0


# ---------------------------------------------------------------------------
# Snapshot type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HashShuffleOpSnapshot:
    op_id: str
    dataset_id: str
    aggregator_health_info: Optional[AggregatorHealthInfo]

    @classmethod
    def from_operator(
        cls,
        op: "PhysicalOperator",
        dataset_id: str,
    ) -> Optional["HashShuffleOpSnapshot"]:
        """Build a snapshot from a live HashShuffleOperator. Returns None for other ops."""
        if not isinstance(op, HashShuffleOperator):
            return None
        if op._aggregator_pool is None:
            return None
        return cls(
            op_id=op.id,
            dataset_id=dataset_id,
            aggregator_health_info=op._aggregator_pool.check_aggregator_health(),
        )


# ---------------------------------------------------------------------------
# Detector
# ---------------------------------------------------------------------------


class HashShuffleAggregatorIssueDetector(IssueDetector):
    """Detector for hash shuffle aggregator health issues."""

    def __init__(
        self,
        config: HashShuffleAggregatorIssueDetectorConfig,
        dataset_id: str = "",
        operators: Optional[List["PhysicalOperator"]] = None,
    ):
        self._dataset_id = dataset_id
        self._operators = operators or []
        self._detector_cfg = config
        self._last_warning_times: Dict[str, float] = {}

    @classmethod
    def from_executor(
        cls, executor: "StreamingExecutor"
    ) -> "HashShuffleAggregatorIssueDetector":
        operators = list(executor._topology.keys()) if executor._topology else []
        ctx = executor._data_context
        return cls(
            config=ctx.issue_detectors_config.hash_shuffle_detector_config,
            dataset_id=executor._dataset_id,
            operators=operators,
        )

    def detect(self) -> List[Issue]:
        """Sync entry point — builds snapshots from live operators and delegates."""
        snapshots = []
        for op in self._operators:
            snap = HashShuffleOpSnapshot.from_operator(op, self._dataset_id)
            if snap is not None:
                snapshots.append(snap)
        return self.detect_from_snapshots(snapshots)

    def detect_from_snapshots(
        self, snapshots: List[HashShuffleOpSnapshot]
    ) -> List[Issue]:
        """Core detection logic operating on serializable snapshots."""
        issues: List[Issue] = []
        current_time = time.time()

        for snap in snapshots:
            if snap.aggregator_health_info is None:
                continue

            should_warn = self._should_emit_warning(
                snap.op_id, current_time, snap.aggregator_health_info
            )

            if should_warn:
                message = self._format_health_warning(snap.aggregator_health_info)
                issues.append(
                    Issue(
                        dataset_name=snap.dataset_id,
                        operator_id=snap.op_id,
                        issue_type=IssueType.HANGING,
                        message=message,
                    )
                )
                self._last_warning_times[snap.op_id] = current_time

        return issues

    def detection_time_interval_s(self) -> float:
        return self._detector_cfg.detection_time_interval_s

    def _should_emit_warning(
        self, op_id: str, current_time: float, info: AggregatorHealthInfo
    ) -> bool:
        if not info.has_unready_aggregators:
            self._last_warning_times.pop(op_id, None)
            return False

        if current_time - info.started_at < self._detector_cfg.min_wait_time_s:
            return False

        last_warning = self._last_warning_times.get(op_id)
        if last_warning is None:
            return True

        return current_time - last_warning >= self.detection_time_interval_s()

    def _format_health_warning(self, info: AggregatorHealthInfo) -> str:
        available_resources = ray.available_resources()
        available_cpus = available_resources.get("CPU", 0)
        cluster_resources = ray.cluster_resources()
        total_memory = cluster_resources.get("memory", 0)
        available_memory = available_resources.get("memory", 0)

        return (
            f"Only {info.ready_aggregators} out of {info.total_aggregators} "
            f"hash-shuffle aggregators are ready after {info.wait_time:.1f} secs. "
            f"This might indicate resource contention for cluster resources "
            f"(available CPUs: {available_cpus}, required CPUs: {info.required_resources.cpu}). "
            f"Cluster only has {available_memory / GiB:.2f} GiB available memory, required memory: {info.required_resources.memory / GiB:.2f} GiB. "
            f"{total_memory / GiB:.2f} GiB total memory. "
            f"Consider increasing cluster size or reducing the number of aggregators "
            f"via `DataContext.max_hash_shuffle_aggregators`. "
            f"Will continue checking every {self.detection_time_interval_s()}s."
        )

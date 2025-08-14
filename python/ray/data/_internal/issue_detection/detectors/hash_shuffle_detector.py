import time
from typing import TYPE_CHECKING, List

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
    from ray.data._internal.execution.streaming_executor import StreamingExecutor
    from ray.data.context import DataContext


class HashShuffleAggregatorIssueDetector(IssueDetector):
    """Detector for hash shuffle aggregator health issues."""

    def __init__(self, executor: "StreamingExecutor", ctx: "DataContext"):
        super().__init__(executor, ctx)
        self._last_warning_times = {}  # Track per-operator warning times

    def detect(self) -> List[Issue]:
        issues = []
        current_time = time.time()

        # Find all hash shuffle operators in the topology
        for op in self._executor._topology.keys():
            if not isinstance(op, HashShuffleOperator):
                continue

            # Skip if operator doesn't have aggregator pool yet
            if op._aggregator_pool is None:
                continue

            pool = op._aggregator_pool
            aggregator_info = pool.check_aggregator_health()

            if aggregator_info is None:
                continue

            # Check if we should emit a warning for this operator
            should_warn = self._should_emit_warning(
                op.id, current_time, aggregator_info
            )

            if should_warn:
                message = self._format_health_warning(aggregator_info)
                issues.append(
                    Issue(
                        dataset_name=self._executor._dataset_id,
                        operator_id=op.id,
                        issue_type=IssueType.HANGING,
                        message=message,
                    )
                )
                self._last_warning_times[op.id] = current_time

        return issues

    def detection_time_interval_s(self) -> float:
        return self._ctx.hash_shuffle_aggregator_health_warning_interval_s

    def _should_emit_warning(
        self, op_id: str, current_time: float, info: AggregatorHealthInfo
    ) -> bool:
        """Check if we should emit a warning for this operator."""
        if not info.has_unready_aggregators:
            # Clear warning time if all aggregators are healthy
            self._last_warning_times.pop(op_id, None)
            return False

        # Check if enough time has passed since start
        if (
            current_time - info.started_at
            < self._ctx.min_hash_shuffle_aggregator_wait_time_in_s
        ):
            return False

        # Check if enough time has passed since last warning
        last_warning = self._last_warning_times.get(op_id)
        if last_warning is None:
            return True

        return current_time - last_warning >= self.detection_time_interval_s()

    def _format_health_warning(self, info: AggregatorHealthInfo) -> str:
        """Format the health warning message."""
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

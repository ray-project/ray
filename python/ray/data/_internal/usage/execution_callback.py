"""Execution-side usage-stats hook.

The callback is injected with the logical plan during planning. It
records the workload entry (DAG, env, configs) before execution starts, then
adds performance data and issues detected after execution
finishes, flushing the payload to GCS at execution start and end so attempted executions
are captured even if they fail.
"""

import logging
import time
import uuid
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.execution.interfaces.distribution_tracker import (
    DistributionTracker,
)
from ray.data._internal.usage import collector, util
from ray.data._internal.usage.collector import (
    ExecutionId,
    LineageReconstructionInfo,
    MemoryStats,
    OpConfig,
    PipelinePerf,
    UsageInfo,
    WorkloadInfo,
)
from ray.data._internal.usage.poller import get_poller

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor
    from ray.data._internal.issue_detection.issue_detector import IssueType
    from ray.data._internal.logical.interfaces.logical_operator import LogicalOperator
    from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan

logger = logging.getLogger(__name__)


class UsageCallback(ExecutionCallback):
    """Records per-execution usage data."""

    def __init__(self, logical_plan: "LogicalPlan"):
        self._logical_plan = logical_plan
        # Globally unique per-execution id, used for deduplicating executions for usage collection
        self._execution_id: ExecutionId = uuid.uuid4().hex
        # id(logical_op) -> usage_id, built while assembling the payload and used
        # to label operators so they reference the workload payload.
        self._usage_id_map: Dict[int, str] = {}
        # The workload tree and usage-id map derive only from the (immutable)
        # logical plan, so they're computed once in the start, cached for the execution end
        self._workload: Optional[WorkloadInfo] = None
        self._started_at: Optional[float] = None
        # To measure cumulative metrics stored in Prometheus counters/gauges (for e.g. bytes spilled)
        # we track the delta between the value at execution start and the value at execution end.
        # The start baseline is a copy of the poller's latest snapshot captured
        # at execution start (None if no poll had completed by then).
        self._baseline_snapshot: Optional[Dict[str, Optional[int]]] = None
        self._cluster_deltas: Optional[Dict[str, Optional[int]]] = None
        self._executor: Optional["StreamingExecutor"] = None
        self._finished = False
        # Set once the start entry is recorded, so per-step sampling skips the
        # opt-out check and is a no-op when collection is disabled or failed
        # to start.
        self._collection_enabled = False
        # Ray Data's object store usage, sampled on every scheduling loop step.
        self._object_store_memory = DistributionTracker()
        # Aggregates across operators, computed once at execution end.
        self._task_max_uss_bytes: Optional[MemoryStats] = None
        self._num_object_lost_task_failures: Optional[int] = None

    # --- ExecutionCallback interface ---

    def before_execution_starts(self, executor: "StreamingExecutor") -> None:
        if collector.usage_collection_disabled():
            return
        try:
            self._executor = executor
            self.on_collection_start(executor)
            collector.record_usage_info(self.build_usage_info())
            self._collection_enabled = True
        except Exception:
            logger.debug("Usage collection failed at start", exc_info=True)

    def on_execution_step(self, executor: "StreamingExecutor") -> None:
        if not self._collection_enabled:
            return
        try:
            self._object_store_memory.add_sample(
                executor._resource_manager.get_global_usage().object_store_memory
            )
        except Exception:
            logger.debug("Usage collection failed at step", exc_info=True)

    def after_execution_succeeds(self, executor: "StreamingExecutor") -> None:
        self._finish(executor, None)

    def after_execution_fails(
        self, executor: "StreamingExecutor", error: Exception
    ) -> None:
        self._finish(executor, error)

    def _finish(
        self, executor: "StreamingExecutor", error: Optional[Exception]
    ) -> None:
        if collector.usage_collection_disabled():
            return
        try:
            self._executor = executor
            self._finished = True
            self._collection_enabled = False
            self.on_collection_end(executor, error)
            collector.record_usage_info(self.build_usage_info())
        except Exception:
            logger.debug("Usage collection failed at finish", exc_info=True)

    # --- extension surface ---

    def collect_op_config(self, op: "LogicalOperator") -> Optional[OpConfig]:
        """Build the config entry for one operator in the workload payload."""
        return collector.collect_op_config(op)

    def anonymize_op_name(self, op: "LogicalOperator") -> str:
        """Anonymized name for one operator in the workload payload.

        The default policy lives in
        ``ray.data._internal.usage.util.anonymize_op_name`` because it's a
        utility shared with the legacy ``record_operators_usage`` path.
        """
        return util.anonymize_op_name(op)

    def on_collection_start(self, executor: "StreamingExecutor") -> None:
        """Called once before execution starts. Records start timing and captures
        this execution's baseline as a copy of the poller's latest snapshot
        (None if no poll has completed yet)."""
        self._started_at = time.time()
        self._baseline_snapshot = get_poller().latest_snapshot()

    def on_collection_end(
        self, executor: "StreamingExecutor", error: Optional[Exception]
    ) -> None:
        """Called once after execution succeeds or fails. Compute the deltas between execution start and end for each metric recorded on the cluster.
        ``executor`` is a reference to the StreamingExecutor and ``error`` is the failure (or ``None`` on success);
        subclasses may override to capture either."""
        self._cluster_deltas = self._compute_cluster_deltas()
        try:
            self._collect_op_metrics(executor)
        except Exception:
            logger.debug("Failed to collect operator metrics", exc_info=True)

    def _collect_op_metrics(self, executor: "StreamingExecutor") -> None:
        """Aggregate per-operator task memory and object-lost failure counts
        across all operators of the execution."""
        from ray.data._internal.execution.operators.hash_shuffle import (
            HashShufflingOperatorBase,
        )

        topology = executor._topology
        if topology is None:
            return
        task_max_uss_bytes = DistributionTracker()
        num_object_lost_task_failures = 0
        for op in topology:
            op_metrics = [op.metrics]
            if isinstance(op, HashShufflingOperatorBase):
                # Finalize tasks are recorded apart from ``op.metrics``.
                op_metrics.append(op._reduce_metrics)
            for metrics in op_metrics:
                task_max_uss_bytes.merge(metrics.max_uss_bytes)
                num_object_lost_task_failures += metrics.num_tasks_failed_object_lost
        self._task_max_uss_bytes = collector.collect_memory_stats(task_max_uss_bytes)
        self._num_object_lost_task_failures = num_object_lost_task_failures

    def _compute_cluster_deltas(self) -> Dict[str, Optional[int]]:
        """Compute delta for each metric between the poller's latest snapshot and the
        the current execution's baseline snapshot. When no poll had completed by the time the
        execution started (baseline recorded as None), fall back to the first snapshot the
        poller took."""
        poller = get_poller()
        baseline_snapshot = self._baseline_snapshot
        if baseline_snapshot is None:
            baseline_snapshot = poller.first_snapshot()
        baseline_snapshot = baseline_snapshot or {}
        latest_snapshot = poller.latest_snapshot() or {}
        return {
            metric_name: util.compute_delta(
                baseline_snapshot.get(metric_name), latest_snapshot.get(metric_name)
            )
            for metric_name in latest_snapshot
        }

    def build_usage_info(self) -> UsageInfo:
        """Assemble the usage collection payload for this execution."""
        # Both are populated before this runs: on_collection_start sets
        # _started_at, and before_execution_starts/_finish set _executor.
        assert self._started_at is not None
        assert self._executor is not None
        if self._workload is None:
            self._usage_id_map = collector.build_usage_id_map(
                self._logical_plan, self.anonymize_op_name
            )
            self._workload = collector.collect_workload(
                self._logical_plan, self.collect_op_config, self.anonymize_op_name
            )
        performance = None
        if self._finished:
            metric_deltas = self._cluster_deltas or {}
            if self._executor.ray_data_reconstruction_enabled:
                lineage_tracker = self._executor._lineage_tracker
                assert lineage_tracker is not None
                object_reconstruction_attempts = (
                    lineage_tracker.num_object_reconstruction_attempts
                )
            else:
                object_reconstruction_attempts = metric_deltas.get(
                    collector.METRIC_OBJECT_RECONSTRUCTION_ATTEMPTS
                )
            performance = PipelinePerf(
                bytes_spilled=metric_deltas.get(collector.METRIC_BYTES_SPILLED),
                node_deaths=metric_deltas.get(collector.METRIC_NODE_DEATHS),
                oom_kills=metric_deltas.get(collector.METRIC_OOM_KILLS),
                unexpected_worker_kills=metric_deltas.get(
                    collector.METRIC_UNEXPECTED_WORKER_KILLS
                ),
                task_retries=metric_deltas.get(collector.METRIC_TASK_RETRIES),
                object_store_memory_bytes=collector.collect_memory_stats(
                    self._object_store_memory
                ),
                task_max_uss_bytes=self._task_max_uss_bytes,
                lineage_reconstruction=LineageReconstructionInfo(
                    object_reconstruction_attempts=object_reconstruction_attempts,
                    num_object_lost_task_failures=self._num_object_lost_task_failures,
                ),
            )
        return UsageInfo(
            id=self._execution_id,
            started_at=self._started_at,
            env=collector.collect_env(self._executor),
            workload=self._workload,
            performance=performance,
            detected_issues=collector.collect_issues(
                self._collect_detected_issues(self._executor)
            ),
        )

    def _collect_detected_issues(
        self, executor: "StreamingExecutor"
    ) -> List[Tuple["IssueType", str]]:
        # The manager is None when issue detection isn't registered.
        manager = executor.issue_detector_manager
        if manager is None:
            return []
        issues = (
            (
                issue_type,
                collector.physical_op_name_with_id(
                    operator, self._usage_id_map, self.anonymize_op_name
                ),
            )
            for issue_type, operator in manager.get_detected_issues()
        )
        # Sort by the issue type's string value, then by the operator name.
        return sorted(issues, key=lambda issue: (issue[0].value, issue[1]))

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
from ray.data._internal.usage import collector
from ray.data._internal.usage.collector import (
    EnvInfo,
    MetricReader,
    OpConfig,
    PipelinePerf,
    UsageInfo,
    WorkloadInfo,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor
    from ray.data._internal.issue_detection.issue_detector import IssueType
    from ray.data._internal.logical.interfaces.logical_operator import LogicalOperator
    from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan

logger = logging.getLogger(__name__)


class UsageCallback(ExecutionCallback):
    """Records per-execution usage data."""

    def __init__(
        self,
        logical_plan: "LogicalPlan",
        get_cluster_spilled_bytes: MetricReader = collector.cluster_spilled_bytes,
        get_dead_node_count: MetricReader = collector.cluster_dead_node_count,
    ):
        self._logical_plan = logical_plan
        # Globally unique per-execution id, used for deduplicating executions for usage collection
        self._execution_id = uuid.uuid4().hex
        # id(logical_op) -> usage_id, built while assembling the payload and used
        # to label operators so they reference the workload payload.
        self._usage_id_map: Dict[int, str] = {}
        self._get_cluster_spilled_bytes = get_cluster_spilled_bytes
        self._get_dead_node_count = get_dead_node_count
        self._started_at: Optional[float] = None
        self._spilled_at_start: Optional[int] = None
        self._spilled_at_end: Optional[int] = None
        self._dead_nodes_at_start: Optional[int] = None
        self._dead_nodes_at_end: Optional[int] = None
        self._executor: Optional["StreamingExecutor"] = None
        self._finished = False

    # --- ExecutionCallback interface ---

    def before_execution_starts(self, executor: "StreamingExecutor") -> None:
        if collector.usage_collection_disabled():
            return
        try:
            self._executor = executor
            self.on_collection_start(executor)
            collector.record_usage_info(self.build_usage_info())
        except Exception:
            logger.debug("Usage collection failed at start", exc_info=True)

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
            self.on_collection_end(executor, error)
            collector.record_usage_info(self.build_usage_info())
        except Exception:
            logger.debug("Usage collection failed at finish", exc_info=True)

    # --- extension surface ---

    def on_collection_start(self, executor: "StreamingExecutor") -> None:
        """Called once before execution starts. Records start timing and the
        cluster metric baselines used to compute per-execution deltas."""
        self._started_at = time.time()
        self._spilled_at_start = self._get_cluster_spilled_bytes()
        self._dead_nodes_at_start = self._get_dead_node_count()

    def on_collection_end(
        self, executor: "StreamingExecutor", error: Optional[Exception]
    ) -> None:
        """Called once after execution succeeds or fails. Records the ending
        cluster metric samples."""
        self._spilled_at_end = self._get_cluster_spilled_bytes()
        self._dead_nodes_at_end = self._get_dead_node_count()

    def collect_env(self) -> EnvInfo:
        """Process-wide environment info for this execution."""
        return collector.collect_env()

    def collect_op_config(self, op: "LogicalOperator") -> Optional[OpConfig]:
        """Per-op config recorded in the workload payload."""
        return collector.collect_op_config(op)

    def collect_workload(self) -> WorkloadInfo:
        """Anonymized plan tree, text rendering, and per-op config for this
        execution."""
        return collector.collect_workload(self._logical_plan, self.collect_op_config)

    def collect_performance(self) -> Optional[PipelinePerf]:
        """Post-execution performance. Returns ``None`` before execution
        finishes (the pre-start payload carries no performance data)."""
        if not self._finished:
            return None
        return collector.PipelinePerf(
            bytes_spilled=collector.compute_delta(
                self._spilled_at_start, self._spilled_at_end
            ),
            node_deaths=collector.compute_delta(
                self._dead_nodes_at_start, self._dead_nodes_at_end
            ),
        )

    def build_usage_info(self) -> UsageInfo:
        """Assemble the payload flushed for this execution.

        Called both before execution starts and after execution finishes.
        Subclasses can override to return a richer ``UsageInfo`` structure.
        """
        self._usage_id_map = collector.build_usage_id_map(self._logical_plan)
        return UsageInfo(
            id=self._execution_id,
            started_at=self._started_at,
            env=self.collect_env(),
            workload=self.collect_workload(),
            performance=self.collect_performance(),
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
                collector.physical_op_name_with_id(operator, self._usage_id_map),
            )
            for issue_type, operator in manager.get_detected_issues()
        )
        # Sort by the issue type's string value, then by the operator name.
        return sorted(issues, key=lambda issue: (issue[0].value, issue[1]))

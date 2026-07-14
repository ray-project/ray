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
from concurrent.futures import Future
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.usage import collector, util
from ray.data._internal.usage.collector import (
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

    def __init__(self, logical_plan: "LogicalPlan"):
        self._logical_plan = logical_plan
        # Globally unique per-execution id, used for deduplicating executions for usage collection
        self._execution_id = uuid.uuid4().hex
        # id(logical_op) -> usage_id, built while assembling the payload and used
        # to label operators so they reference the workload payload.
        self._usage_id_map: Dict[int, str] = {}
        # The workload tree and usage-id map derive only from the (immutable)
        # logical plan, so they're computed once in the start, cached for the execution end
        self._workload: Optional[WorkloadInfo] = None
        self._started_at: Optional[float] = None
        self._spilled_at_start: Optional[int] = None
        self._spilled_at_end: Optional[int] = None
        self._dead_nodes_at_start: Optional[int] = None
        self._dead_nodes_at_end: Optional[int] = None
        self._oom_kills_at_start: Optional[int] = None
        self._oom_kills_at_end: Optional[int] = None
        self._unexpected_worker_kills_at_start: Optional[int] = None
        self._unexpected_worker_kills_at_end: Optional[int] = None
        # Cluster metrics are sampled on background threads off the start path.
        # Each reader's start sample is fired before execution starts and joined at execution end.
        self._spilled_sample: Optional[Future] = None
        self._dead_nodes_sample: Optional[Future] = None
        self._oom_kills_sample: Optional[Future] = None
        self._unexpected_worker_kills_sample: Optional[Future] = None
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
        """Called once before execution starts. Records start timing and the
        cluster metric baselines used to compute per-execution deltas."""
        self._started_at = time.time()
        # Fire every cluster reader on its own background thread so they run
        # concurrently (and during execution) instead of blocking the start
        # path; each is joined at execution end.
        self._spilled_sample = util.start_metric_sample(collector.cluster_spilled_bytes)
        self._dead_nodes_sample = util.start_metric_sample(
            collector.cluster_dead_node_count
        )
        self._oom_kills_sample = util.start_metric_sample(collector.cluster_oom_kills)
        self._unexpected_worker_kills_sample = util.start_metric_sample(
            collector.cluster_unexpected_worker_kills
        )

    def on_collection_end(
        self, executor: "StreamingExecutor", error: Optional[Exception]
    ) -> None:
        """Called once after execution succeeds or fails. Records the ending
        cluster metric samples. ``error`` is the failure (or ``None`` on
        success); subclasses may override to capture it."""
        # Fire every metric reader concurrently for the execution end samples,
        # so they overlap with the joins for the start samples below
        end_spilled = util.start_metric_sample(collector.cluster_spilled_bytes)
        end_dead_nodes = util.start_metric_sample(collector.cluster_dead_node_count)
        end_oom_kills = util.start_metric_sample(collector.cluster_oom_kills)
        end_unexpected_worker_kills = util.start_metric_sample(
            collector.cluster_unexpected_worker_kills
        )

        # Join all start, end samples concurrently under a single timeout
        # ceiling. Joining sequentially would block for up to
        # timeout * n_samples if every reader hangs. Results unpack positionally,
        # so the targets below must stay in the same order as the samples passed in.
        # Tuple unpacking syntax
        (
            self._spilled_at_start,
            self._dead_nodes_at_start,
            self._oom_kills_at_start,
            self._unexpected_worker_kills_at_start,
            self._spilled_at_end,
            self._dead_nodes_at_end,
            self._oom_kills_at_end,
            self._unexpected_worker_kills_at_end,
        ) = util.join_metric_samples(
            [
                self._spilled_sample,
                self._dead_nodes_sample,
                self._oom_kills_sample,
                self._unexpected_worker_kills_sample,
                end_spilled,
                end_dead_nodes,
                end_oom_kills,
                end_unexpected_worker_kills,
            ],
        )

    def build_usage_info(self) -> UsageInfo:
        """Assemble the usage collection payload for this execution."""
        if self._workload is None:
            self._usage_id_map = collector.build_usage_id_map(
                self._logical_plan, self.anonymize_op_name
            )
            self._workload = collector.collect_workload(
                self._logical_plan, self.collect_op_config, self.anonymize_op_name
            )
        performance = None
        if self._finished:
            performance = PipelinePerf(
                bytes_spilled=collector.compute_delta(
                    self._spilled_at_start, self._spilled_at_end
                ),
                node_deaths=collector.compute_delta(
                    self._dead_nodes_at_start, self._dead_nodes_at_end
                ),
                oom_kills=collector.compute_delta(
                    self._oom_kills_at_start, self._oom_kills_at_end
                ),
                unexpected_worker_kills=collector.compute_delta(
                    self._unexpected_worker_kills_at_start,
                    self._unexpected_worker_kills_at_end,
                ),
            )
        # Both are populated before this runs: on_collection_start sets
        # _started_at, and before_execution_starts/_finish set _executor.
        assert self._started_at is not None
        assert self._executor is not None
        return UsageInfo(
            id=self._execution_id,
            started_at=self._started_at,
            env=collector.collect_env(),
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

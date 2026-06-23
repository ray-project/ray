"""Execution-side usage-stats hook.

The callback is constructor-injected with the logical plan
during planning. The callback records the workload entry (DAG, env, configs)
before execution starts, and also records performance info after execution finishes.
"""

import logging
import uuid
from typing import TYPE_CHECKING, List, Tuple

from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.usage.collector import (
    build_usage_uuid_map,
    record_execution_result,
    record_workload,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor
    from ray.data._internal.issue_detection.issue_detector import IssueType
    from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan

logger = logging.getLogger(__name__)


class UsageCallback(ExecutionCallback):
    """Records per-execution usage data."""

    def __init__(self, logical_plan: "LogicalPlan"):
        self._logical_plan = logical_plan
        # Globally unique per-execution id, used for deduplicating executions for usage collection
        self._execution_id = uuid.uuid4().hex

    def before_execution_starts(self, executor: "StreamingExecutor") -> None:
        try:
            record_workload(self._execution_id, self._logical_plan)
            executor._usage_uuid_map = build_usage_uuid_map(self._logical_plan)
        except Exception:
            logger.debug("Usage record_workload failed", exc_info=True)

    def after_execution_succeeds(self, executor: "StreamingExecutor") -> None:
        self._finish(executor)

    def after_execution_fails(
        self, executor: "StreamingExecutor", error: Exception
    ) -> None:
        self._finish(executor)

    def _finish(self, executor: "StreamingExecutor") -> None:
        try:
            record_execution_result(
                self._execution_id,
                detected_issues=self._collect_detected_issues(executor),
            )
        except Exception:
            logger.debug("Usage record_execution_result failed", exc_info=True)

    def _collect_detected_issues(
        self, executor: "StreamingExecutor"
    ) -> List[Tuple["IssueType", str]]:
        # The manager is absent when issue detection isn't registered.
        manager = getattr(executor, "_issue_detector_manager", None)
        if manager is None:
            return []
        return sorted(manager.get_detected_issues())

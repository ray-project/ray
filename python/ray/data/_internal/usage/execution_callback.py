"""Execution-side usage-stats hook.

The callback is constructor-injected with the logical plan
during planning. The callback records the workload entry (DAG, env, configs)
before execution starts, and also records performance and error info after execution finishes.
"""

import logging
from typing import TYPE_CHECKING, Optional

from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.usage.collector import (
    record_execution_result,
    record_workload,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor
    from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan

logger = logging.getLogger(__name__)


class UsageCallback(ExecutionCallback):
    """Records per-execution usage data."""

    def __init__(self, logical_plan: "LogicalPlan"):
        self._logical_plan = logical_plan

    def before_execution_starts(self, executor: "StreamingExecutor") -> None:
        try:
            record_workload(executor, self._logical_plan)
        except Exception:
            logger.debug("Usage record_workload failed", exc_info=True)

    def after_execution_succeeds(self, executor: "StreamingExecutor") -> None:
        self._finish(executor, error=None)

    def after_execution_fails(
        self, executor: "StreamingExecutor", error: Exception
    ) -> None:
        self._finish(executor, error=error)

    def _finish(
        self, executor: "StreamingExecutor", error: Optional[BaseException]
    ) -> None:
        try:
            record_execution_result(executor, error)
        except Exception:
            logger.debug("Usage record_execution_result failed", exc_info=True)

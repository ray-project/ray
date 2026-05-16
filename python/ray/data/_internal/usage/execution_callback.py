"""Execution-side usage-stats hook.

Pairs with ``ray.data._internal.usage.collector``. The planning-side
hook in ``legacy_compat._execute_dag`` records the workload entry; this
callback fires when the executor finishes and fills in performance + error.
"""

import logging
from typing import TYPE_CHECKING, Optional

from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.usage.collector import record_execution_result

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor

logger = logging.getLogger(__name__)


class UsageCallback(ExecutionCallback):
    """Flushes per-execution usage data on success or failure.

    Writes into Ray's usage-stats subsystem (``record_extra_usage_tag``,
    ``usage_lib``, ``usage_stats`` namespace).
    """

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

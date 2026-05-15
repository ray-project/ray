"""Execution-side telemetry hook.

Pairs with ``ray.data._internal.telemetry.collector``. The planning-side
hook in ``legacy_compat._execute_dag`` records the workload entry; this
callback fires when the executor finishes and fills in performance + error.
"""

import logging
from typing import TYPE_CHECKING, Optional

from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.telemetry.collector import record_execution_result

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor

logger = logging.getLogger(__name__)


class TelemetryCallback(ExecutionCallback):
    """Flushes per-execution telemetry on success or failure."""

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
            logger.debug("Telemetry record_execution_result failed", exc_info=True)

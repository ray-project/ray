from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor


class ExecutionCallback:
    """Callback interface for execution events."""

    def before_execution_starts(self, executor: "StreamingExecutor"):
        """Called before the Dataset execution starts."""
        ...

    def on_execution_step(self, executor: "StreamingExecutor"):
        """Called at each step of the Dataset execution loop."""
        ...

    def after_execution_succeeds(self, executor: "StreamingExecutor"):
        """Called after the Dataset execution succeeds."""
        ...

    def after_execution_fails(self, executor: "StreamingExecutor", error: Exception):
        """Called after the Dataset execution fails."""
        ...

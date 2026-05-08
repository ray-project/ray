from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor
    from ray.data.context import DataContext


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


def remove_execution_callback(
    callback: "ExecutionCallback", context: "DataContext"
) -> None:
    """No-op compatibility shim.

    LoadCheckpointCallback is added directly to the planner's callbacks list
    and passed to StreamingExecutor — it is never registered in DataContext.
    This function exists so LoadCheckpointCallback.after_execution_succeeds/fails
    can call it without branching on the execution path.
    """
    pass

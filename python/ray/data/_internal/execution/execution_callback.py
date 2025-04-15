from typing import List, TYPE_CHECKING

from ray.data.context import DataContext

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor

EXECUTION_CALLBACKS_CONFIG_KEY = "execution_callbacks"


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


def get_execution_callbacks(context: DataContext) -> List[ExecutionCallback]:
    """Get all ExecutionCallbacks from the DataContext."""
    return context.get_config(EXECUTION_CALLBACKS_CONFIG_KEY, [])


def add_execution_callback(callback: ExecutionCallback, context: DataContext):
    """Add an ExecutionCallback to the DataContext."""
    execution_callbacks = context.get_config(EXECUTION_CALLBACKS_CONFIG_KEY, [])
    execution_callbacks.append(callback)
    context.set_config(EXECUTION_CALLBACKS_CONFIG_KEY, execution_callbacks)


def remove_execution_callback(callback: ExecutionCallback, context: DataContext):
    """Remove an ExecutionCallback from the DataContext."""
    execution_callbacks = context.get_config(EXECUTION_CALLBACKS_CONFIG_KEY, [])
    execution_callbacks.remove(callback)
    context.set_config(EXECUTION_CALLBACKS_CONFIG_KEY, execution_callbacks)

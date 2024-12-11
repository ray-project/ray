from typing import List

from ray.data.context import DataContext

EXECUTION_CALLBACKS_CONFIG_KEY = "execution_callbacks"


class ExecutionCallback:
    """Callback interface for execution events."""

    def before_execution_starts(self):
        """Called before the Dataset execution starts."""
        ...

    def after_execution_succeeds(self):
        """Called after the Dataset execution succeeds."""
        ...

    def after_execution_fails(self, error: Exception):
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

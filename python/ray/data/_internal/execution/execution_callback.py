import importlib
import os
from typing import TYPE_CHECKING, List

from ray.data.context import DataContext

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor

EXECUTION_CALLBACKS_CONFIG_KEY = "execution_callbacks"
EXECUTION_CALLBACKS_ENV_VAR = "RAY_DATA_EXECUTION_CALLBACKS"
ENV_CALLBACKS_INITIALIZED_KEY = "_env_callbacks_initialized"


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


def _initialize_env_callbacks(context: DataContext) -> None:
    """Initialize callbacks from environment variable and add them to the context."""

    # firstly add the callbacks that should always be executed
    from ray.data._internal.execution.callbacks.execution_idx_update_callback import (
        ExecutionIdxUpdateCallback,
    )
    from ray.data._internal.execution.callbacks.insert_issue_detectors import (
        IssueDetectionExecutionCallback,
    )

    add_execution_callback(ExecutionIdxUpdateCallback(), context)
    add_execution_callback(IssueDetectionExecutionCallback(), context)

    callbacks_str = os.environ.get(EXECUTION_CALLBACKS_ENV_VAR, "")
    if not callbacks_str:
        return

    for callback_path in callbacks_str.split(","):
        callback_path = callback_path.strip()
        if not callback_path:
            continue

        try:
            module_path, class_name = callback_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            callback_cls = getattr(module, class_name)
            callback = callback_cls()
            add_execution_callback(callback, context)
        except (ImportError, AttributeError, ValueError) as e:
            raise ValueError(f"Failed to import callback from '{callback_path}': {e}")


def get_execution_callbacks(context: DataContext) -> List[ExecutionCallback]:
    """Get all ExecutionCallbacks from the DataContext."""
    # Initialize environment callbacks if not already done for this context
    if not context.get_config(ENV_CALLBACKS_INITIALIZED_KEY, False):
        _initialize_env_callbacks(context)
        context.set_config(ENV_CALLBACKS_INITIALIZED_KEY, True)
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

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor

EXECUTION_CALLBACKS_CONFIG_KEY = "execution_callbacks"
EXECUTION_CALLBACKS_ENV_VAR = "RAY_DATA_EXECUTION_CALLBACKS"
ENV_CALLBACKS_INITIALIZED_KEY = "_env_callbacks_initialized"


class ExecutionCallback:
    """Callback interface for execution events."""

    @classmethod
    def from_executor(
        cls, _executor: "StreamingExecutor"
    ) -> Optional["ExecutionCallback"]:
        """Factory method to create a callback instance from the executor.

        This allows the callback to extract necessary state (like configs) from
        the executor or context before instantiation.

        Args:
            _executor: The executor instance to initialize from.

        Returns:
            An instance of the callback, or None if the callback should be skipped
            (e.g. if a required config is missing).
        """

        return cls()

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

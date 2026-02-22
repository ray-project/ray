import logging

from ray.data._internal.execution.execution_callback import (
    ExecutionCallback,
    remove_execution_callback,
)
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data.checkpoint import CheckpointConfig
from ray.data.checkpoint.checkpoint_manager import (
    CheckpointManager,
)

logger = logging.getLogger(__name__)


class LoadCheckpointCallback(ExecutionCallback):
    """ExecutionCallback that handles checkpoints."""

    def __init__(self, config: CheckpointConfig):
        assert config is not None
        self._config = config
        self._ckpt_manager = None

    def before_execution_starts(self, executor: StreamingExecutor):
        assert self._config is executor._data_context.checkpoint_config

        # create checkpoint manager
        self._ckpt_manager = CheckpointManager(self._config)

    def after_execution_succeeds(self, executor: StreamingExecutor):
        assert self._config is executor._data_context.checkpoint_config

        # Remove the callback from the DataContext.
        remove_execution_callback(self, executor._data_context)
        # Delete checkpoint data.
        try:
            if self._config.delete_checkpoint_on_success:
                self._ckpt_manager.delete_checkpoint()
        except Exception:
            logger.warning("Failed to delete checkpoint data.", exc_info=True)

    def after_execution_fails(self, executor: StreamingExecutor, error: Exception):
        assert self._config is executor._data_context.checkpoint_config

        # Remove the callback from the DataContext.
        remove_execution_callback(self, executor._data_context)

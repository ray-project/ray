import logging

from ray.data._internal.execution.execution_callback import (
    ExecutionCallback,
    remove_execution_callback,
)
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data.checkpoint import CheckpointConfig
from ray.data.datasource.path_util import _unwrap_protocol

logger = logging.getLogger(__name__)


class LoadCheckpointCallback(ExecutionCallback):
    """
    ExecutionCallback that handles checkpoints. This Callback is responsible for
    deleting the checkpoint directory when these conditions are met:
    1. `delete_checkpoint_on_success` is True
    2. The job finishes successfully.
    """

    def __init__(self, config: CheckpointConfig):
        assert config is not None
        self._config = config

    def before_execution_starts(self, executor: StreamingExecutor):
        assert self._config is executor._data_context.checkpoint_config

    def _delete_checkpoint(self):
        checkpoint_path_unwrapped = _unwrap_protocol(self._config.checkpoint_path)
        filesystem = self._config.filesystem
        filesystem.delete_dir(checkpoint_path_unwrapped)

    def after_execution_succeeds(self, executor: StreamingExecutor):
        assert self._config is executor._data_context.checkpoint_config

        # Remove the callback from the DataContext.
        remove_execution_callback(self, executor._data_context)
        # Delete checkpoint data.
        try:
            if self._config.delete_checkpoint_on_success:
                self._delete_checkpoint()
        except Exception:
            logger.warning("Failed to delete checkpoint data.", exc_info=True)

    def after_execution_fails(self, executor: StreamingExecutor, error: Exception):
        assert self._config is executor._data_context.checkpoint_config

        # Remove the callback from the DataContext.
        remove_execution_callback(self, executor._data_context)

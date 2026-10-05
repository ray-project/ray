import logging
from typing import Optional

from ray.data._internal.execution.execution_callback import (
    ExecutionCallback,
)
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data.checkpoint import CheckpointConfig
from ray.data.checkpoint.generated_id import GeneratedIdCheckpoint
from ray.data.datasource.path_util import _unwrap_protocol

logger = logging.getLogger(__name__)


class LoadCheckpointCallback(ExecutionCallback):
    """
    ExecutionCallback that handles checkpoints. This Callback is responsible for
    deleting the checkpoint directory when these conditions are met:
    1. `delete_checkpoint_on_success` is True.
    2. The job finishes successfully.

    For ``generated_id_column`` configs it also loads the checkpoint when
    execution starts; the planned ``ListFiles`` / ``ReadFiles`` operators
    read it through :meth:`load_checkpoint`.
    """

    def __init__(
        self,
        config: CheckpointConfig,
        *,
        delete_on_execution_success: bool = True,
    ):
        assert config is not None
        self._config = config
        self._delete_on_execution_success = delete_on_execution_success
        self._generated_id_checkpoint: Optional[GeneratedIdCheckpoint] = None

    def before_execution_starts(self, executor: StreamingExecutor):
        assert self._config is executor._data_context.checkpoint_config
        # A run that doesn't restore (e.g. a later epoch) plans no operator
        # that reads the checkpoint, so don't load it.
        if self._config.has_generated_id_column and self._config._should_restore:
            # Lazy import: ``checkpoint_filter`` imports ``ray.data.context``.
            from ray.data.checkpoint.checkpoint_filter import (
                GeneratedIdColumnCheckpointManager,
            )

            # Pending checkpoints were already cleaned up while planning.
            manager = GeneratedIdColumnCheckpointManager(
                self._config, executor._data_context
            )
            self._generated_id_checkpoint = manager.load_generated_id_checkpoint()

    def load_checkpoint(self) -> GeneratedIdCheckpoint:
        """Return the generated-ID checkpoint loaded when execution started."""
        assert self._generated_id_checkpoint is not None, (
            "The generated-ID checkpoint is loaded when execution starts; "
            "only generated_id_column configs load one."
        )
        return self._generated_id_checkpoint

    def _delete_checkpoint(self):
        checkpoint_path_unwrapped = _unwrap_protocol(self._config.checkpoint_path)
        filesystem = self._config.filesystem
        filesystem.delete_dir(checkpoint_path_unwrapped)

    def after_execution_succeeds(self, executor: StreamingExecutor):
        assert self._config is executor._data_context.checkpoint_config

        # Disable checkpoint restoration for subsequent executions
        # of the same dataset (e.g., later epochs).
        self._config._should_restore = False

        # Delete checkpoint data.
        try:
            if (
                self._delete_on_execution_success
                and self._config.delete_checkpoint_on_success
            ):
                self._delete_checkpoint()
        except Exception:
            logger.warning("Failed to delete checkpoint data.", exc_info=True)

    def after_execution_fails(self, executor: StreamingExecutor, error: Exception):
        assert self._config is executor._data_context.checkpoint_config

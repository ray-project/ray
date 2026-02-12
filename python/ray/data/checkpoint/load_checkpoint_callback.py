import logging

import ray
from ray.data._internal.execution.execution_callback import (
    ExecutionCallback,
    remove_execution_callback,
)
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data.checkpoint import CheckpointConfig
from ray.data.checkpoint.checkpoint_filter import BatchBasedCheckpointFilter

logger = logging.getLogger(__name__)


class LoadCheckpointCallback(ExecutionCallback):
    """ExecutionCallback that handles checkpoints."""

    def __init__(self, config: CheckpointConfig):
        assert config is not None
        self._config = config

        self._ckpt_filter = None

    def _create_checkpoint_filter(self, config: CheckpointConfig):
        """Create the global checkpoint filter."""
        job_id = ray.get_runtime_context().get_job_id()
        self._ckpt_filter = BatchBasedCheckpointFilter.options(
            name=f"checkpoint_filter_{job_id}",
            lifetime="detached",
        ).remote(config)
        ray.get(self._ckpt_filter.ready.remote())
        logger.info("create checkpoint filter done")

    def before_execution_starts(self, executor: StreamingExecutor):
        assert self._config is executor._data_context.checkpoint_config

        # create global checkpoint_filter actor
        self._create_checkpoint_filter(self._config)

    def after_execution_succeeds(self, executor: StreamingExecutor):
        assert self._config is executor._data_context.checkpoint_config

        # Remove the callback from the DataContext.
        remove_execution_callback(self, executor._data_context)
        # Delete checkpoint data.
        try:
            if self._config.delete_checkpoint_on_success:
                ray.get(self._ckpt_filter.delete_checkpoint.remote())
        except Exception:
            logger.warning("Failed to delete checkpoint data.", exc_info=True)

        # Kill the global checkpoint_filter actor
        try:
            ray.kill(self._ckpt_filter)
        except Exception as e:
            logger.error(f"Failed to kill global checkpoint_filter actor. Error: {e}")

    def after_execution_fails(self, executor: StreamingExecutor, error: Exception):
        assert self._config is executor._data_context.checkpoint_config

        # Remove the callback from the DataContext.
        remove_execution_callback(self, executor._data_context)

        # Kill the global checkpoint_filter actor
        try:
            ray.kill(self._ckpt_filter)
        except Exception as e:
            logger.error(f"Failed to kill global checkpoint_filter actor. Error: {e}")

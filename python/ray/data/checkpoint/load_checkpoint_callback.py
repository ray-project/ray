import logging
from typing import Optional

from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data.block import Block
from ray.data.checkpoint import CheckpointConfig
from ray.data.checkpoint.checkpoint_filter import BatchBasedCheckpointFilter
from ray.types import ObjectRef

logger = logging.getLogger(__name__)


class LoadCheckpointCallback(ExecutionCallback):
    """ExecutionCallback that handles checkpoints."""

    def __init__(self):
        self._config: Optional[CheckpointConfig] = None
        self._ckpt_filter: Optional[BatchBasedCheckpointFilter] = None
        self._checkpoint_ref: Optional[ObjectRef[Block]] = None

    @classmethod
    def from_executor(
        cls, executor: StreamingExecutor
    ) -> Optional["LoadCheckpointCallback"]:
        config = executor._data_context.checkpoint_config

        if config is None:
            return None

        return cls(config)

    def _create_checkpoint_filter(
        self, config: CheckpointConfig
    ) -> BatchBasedCheckpointFilter:
        """Factory method to create the checkpoint filter.

        Subclasses can override this to use a different filter implementation.
        """
        return BatchBasedCheckpointFilter(config)

    def before_execution_starts(self, executor: StreamingExecutor):
        config = executor._data_context.checkpoint_config

        if config is None:
            return

        self._config = config
        self._ckpt_filter = self._create_checkpoint_filter(config)

        # Load checkpoint data before execution starts.
        self._checkpoint_ref = self._ckpt_filter.load_checkpoint()

    def after_execution_succeeds(self, executor: StreamingExecutor):
        if self._config is None:
            return

        # Delete checkpoint data
        try:
            if self._config.delete_checkpoint_on_success:
                # Ensure filter was created
                if self._ckpt_filter:
                    self._ckpt_filter.delete_checkpoint()
        except Exception:
            logger.warning("Failed to delete checkpoint data.", exc_info=True)

    def after_execution_fails(self, executor: StreamingExecutor, error: Exception):
        pass

    def load_checkpoint(self) -> ObjectRef[Block]:
        assert self._checkpoint_ref is not None
        return self._checkpoint_ref

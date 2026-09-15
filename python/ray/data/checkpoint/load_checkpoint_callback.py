import logging
from typing import Optional

import pyarrow.fs

from ray.data._internal.execution.execution_callback import (
    ExecutionCallback,
)
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data.block import Block
from ray.data.checkpoint import CheckpointConfig
from ray.data.datasource.path_util import _unwrap_protocol
from ray.types import ObjectRef

logger = logging.getLogger(__name__)


class LoadCheckpointCallback(ExecutionCallback):
    """
    ExecutionCallback that handles checkpoints.

    1. For ``generated_id_column``: loads the compact checkpoint block before
       execution starts and exposes it via :meth:`load_checkpoint` so the
       ``ListFiles`` plan function can inject it into listing tasks.
    2. For regular ``id_column``: no loading here — the actor-pool filter
       loads inside its plan function.
    3. For both: deletes the checkpoint directory when
       `delete_checkpoint_on_success` is True and the job finishes
       successfully. On failure, nothing is deleted: pending checkpoint files
       are the recovery record consumed on the next run.
    """

    def __init__(
        self,
        config: CheckpointConfig,
        data_file_dir: Optional[str] = None,
        data_file_filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ):
        assert config is not None
        self._config = config
        self._data_file_dir = data_file_dir
        self._data_file_filesystem = data_file_filesystem
        self._checkpoint_ref: Optional[ObjectRef[Block]] = None

    def before_execution_starts(self, executor: StreamingExecutor):
        assert self._config is executor._data_context.checkpoint_config

        if self._config.has_generated_id_column:
            # Import here to avoid a checkpoint_filter <-> callback cycle.
            from ray.data.checkpoint.checkpoint_filter import (
                load_generated_id_checkpoint_as_block,
            )

            self._checkpoint_ref = load_generated_id_checkpoint_as_block(
                self._config,
                data_file_dir=self._data_file_dir,
                data_file_filesystem=self._data_file_filesystem,
                data_context=executor._data_context,
            )

    def load_checkpoint(self) -> ObjectRef[Block]:
        """Return the cached compact checkpoint block ref.

        Only valid for the ``generated_id_column`` path, after
        ``before_execution_starts`` has run.
        """
        assert self._config.has_generated_id_column, (
            "load_checkpoint() is only valid for generated_id_column. "
            "Regular id_column uses the actor-pool pattern."
        )
        assert self._checkpoint_ref is not None
        return self._checkpoint_ref

    def _delete_checkpoint(self):
        checkpoint_path_unwrapped = _unwrap_protocol(self._config.checkpoint_path)
        filesystem = self._config.filesystem
        filesystem.delete_dir(checkpoint_path_unwrapped)

    def after_execution_succeeds(self, executor: StreamingExecutor):
        assert self._config is executor._data_context.checkpoint_config

        # Delete checkpoint data.
        try:
            if self._config.delete_checkpoint_on_success:
                self._delete_checkpoint()
        except Exception:
            logger.warning("Failed to delete checkpoint data.", exc_info=True)

    def after_execution_fails(self, executor: StreamingExecutor, error: Exception):
        assert self._config is executor._data_context.checkpoint_config

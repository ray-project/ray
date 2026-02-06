import logging
from typing import TYPE_CHECKING, Callable, Optional

from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data.checkpoint.checkpoint_filter import BatchBasedCheckpointFilter

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor
    from ray.data.block import Block
    from ray.data.checkpoint.interfaces import CheckpointConfig
    from ray.types import ObjectRef

logger = logging.getLogger(__name__)


class LoadCheckpointCallback(ExecutionCallback):
    """ExecutionCallback that handles checkpoints."""

    def __init__(self, config: Optional["CheckpointConfig"]):
        self._config = config
        self._ckpt_filter = None

        if self._config:
            self._ckpt_filter = self._create_checkpoint_filter(self._config)

    @classmethod
    def from_executor(cls, executor: "StreamingExecutor") -> "ExecutionCallback":
        return cls(config=executor._data_context.checkpoint_config)

    @classmethod
    def get_loader(
        cls, config: Optional["CheckpointConfig"]
    ) -> Optional[Callable[[], "ObjectRef[Block]"]]:
        """
        Returns the loading logic (closure) without creating a callback instance.
        Used by the Planner.
        """
        if not config:
            return None

        ckpt_filter = cls._create_checkpoint_filter(config)
        cache = {}

        def load_fn() -> "ObjectRef[Block]":
            if "ref" in cache:
                return cache["ref"]

            ref = ckpt_filter.load_checkpoint()
            cache["ref"] = ref
            return ref

        return load_fn

    @classmethod
    def _create_checkpoint_filter(
        cls, config: "CheckpointConfig"
    ) -> BatchBasedCheckpointFilter:
        """
        Factory method to create the checkpoint filter.
        Subclasses can override this to use a different filter implementation.
        """
        return BatchBasedCheckpointFilter(config)

    def before_execution_starts(self, executor: "StreamingExecutor"):
        pass

    def after_execution_succeeds(self, executor: "StreamingExecutor"):
        if self._config is None or self._ckpt_filter is None:
            return

        ctx = executor._data_context
        registered_classes = getattr(ctx, "execution_callback_classes", [])

        # Find the most specific (last registered) LoadCheckpointCallback subclass
        # This matches the Planner's logic in planner.py which iterates in reverse
        most_specific_cls = None
        for cls in reversed(registered_classes):
            if issubclass(cls, LoadCheckpointCallback):
                most_specific_cls = cls
                break

        # Only the most specific class should perform cleanup
        if most_specific_cls is not type(self):
            return

        try:
            if self._config.delete_checkpoint_on_success:
                self._ckpt_filter.delete_checkpoint()
        except Exception:
            logger.warning("Failed to delete checkpoint data.", exc_info=True)

    def after_execution_fails(self, executor: "StreamingExecutor", error: Exception):
        pass

import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Set, Type

from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data.checkpoint.checkpoint_filter import BatchBasedCheckpointFilter

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor
    from ray.data.block import Block
    from ray.data.checkpoint.interfaces import CheckpointConfig
    from ray.types import ObjectRef

logger = logging.getLogger(__name__)


def get_checkpoint_loader(
    cls: Type["LoadCheckpointCallback"], config: Optional["CheckpointConfig"]
) -> Optional[Callable[[], "ObjectRef[Block]"]]:
    """
    Helper used by the Planner. Delegates to the class method to ensure subclass
    overrides are respected.
    """
    return cls.get_loader(config)


class LoadCheckpointCallback(ExecutionCallback):
    """ExecutionCallback that handles checkpoints."""

    # Registry to track which checkpoint configs were actually used by the Planner.
    _enabled_configs: Set[int] = set()

    def __init__(self, config: Optional["CheckpointConfig"]):
        self._config = config
        # We create the filter here exclusively for DELETION logic.
        # We do NOT use this filter for loading data.
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

        cls._enabled_configs.add(id(config))

        ckpt_filter = cls._create_checkpoint_filter(config)
        checkpoint_ref: Dict[str, Any] = {}

        def load_fn() -> "ObjectRef[Block]":
            if "ref" in checkpoint_ref:
                return checkpoint_ref["ref"]
            ref = ckpt_filter.load_checkpoint()
            checkpoint_ref["ref"] = ref
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

        # Only delete if this config was explicitly ENABLED by the Planner.
        if id(self._config) not in self._enabled_configs:
            return

        try:
            if self._config.delete_checkpoint_on_success:
                self._ckpt_filter.delete_checkpoint()
        except Exception:
            logger.warning("Failed to delete checkpoint data.", exc_info=True)
        finally:
            self._enabled_configs.discard(id(self._config))

    def after_execution_fails(self, executor: "StreamingExecutor", error: Exception):
        # Cleanup registry on failure
        if self._config:
            self._enabled_configs.discard(id(self._config))

import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Type

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

    # Maps config_id -> The specific Class Type that claimed it.
    # This prevents multiple callback instances (base + subclass) from
    # attempting to delete the same checkpoint.
    _assigned_owners: Dict[int, Type["LoadCheckpointCallback"]] = {}

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

        # Explicitly mark that ONLY this class type (cls)
        # is authorized to handle cleanup for this specific config.
        LoadCheckpointCallback._assigned_owners[id(config)] = cls

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

        owner_cls = LoadCheckpointCallback._assigned_owners.get(id(self._config))

        # If I am not the specific class type that the Planner selected,
        # I must NOT touch the data.
        if owner_cls is not type(self):
            return

        try:
            if self._config.delete_checkpoint_on_success:
                self._ckpt_filter.delete_checkpoint()
        except Exception:
            logger.warning("Failed to delete checkpoint data.", exc_info=True)
        finally:
            # Cleanup registry (only the owner cleans up the entry)
            LoadCheckpointCallback._assigned_owners.pop(id(self._config), None)

    def after_execution_fails(self, executor: "StreamingExecutor", error: Exception):
        # Cleanup registry on failure (only the owner cleans up the entry)
        if self._config:
            owner_cls = LoadCheckpointCallback._assigned_owners.get(id(self._config))

            if owner_cls is type(self):
                LoadCheckpointCallback._assigned_owners.pop(id(self._config), None)

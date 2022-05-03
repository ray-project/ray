# coding: utf-8
import logging
from typing import Any, Callable, Optional

from ray.tune.result import NODE_IP
from ray.util.ml_utils.checkpoint_manager import (
    CheckpointStrategy,
    MIN,
    MAX,
    CheckpointManager as CommonCheckpointManager,
    _TrackedCheckpoint,
)

logger = logging.getLogger(__name__)


class _TuneCheckpoint:
    """Describes a checkpoint of trial state.

    Checkpoint may be saved in different storage.

    Attributes:
        storage: Storage type.
        value: If storage==MEMORY, it is a Python object.
            If storage==PERSISTENT, it is a path to persistent storage,
            or a future that will be resolved to such a path.
    """

    MEMORY = "memory"
    PERSISTENT = "persistent"

    def __init__(
        self,
        storage: str,
        value: Any,
        result: Optional[dict] = None,
        node_ip: Optional[str] = None,
    ):
        self.storage = storage
        self.value = value
        self.result = result or {}
        self.node_ip = node_ip or self.result.get(NODE_IP, None)
        # The logical order of checkpoints (both in memory and persistent)
        # The more recent checkpoints have larger order.
        # The most recent checkpoint is used to restore the trial.
        self.order = 0

    @staticmethod
    def from_object(value=None):
        """Creates a checkpoint from a Python object."""
        return _TuneCheckpoint(_TuneCheckpoint.MEMORY, value)

    @property
    def is_ready(self):
        """Returns whether the checkpoint is ready to be used for restoration.

        A PERSISTENT checkpoint is considered ready once its value is resolved
        to an actual path. MEMORY checkpoints are always considered ready since
        they are transient.
        """
        if self.storage == _TuneCheckpoint.PERSISTENT:
            return isinstance(self.value, str)
        return self.storage == _TuneCheckpoint.MEMORY

    def __repr__(self):
        return f"Checkpoint({self.storage}, {self.value})"


class QueueItem:
    def __init__(self, priority, value):
        self.priority = priority
        self.value = value

    def __lt__(self, other):
        return self.priority < other.priority

    def __repr__(self):
        return f"QueueItem({repr(self.value)})"


class CheckpointManager(CommonCheckpointManager):
    """Initializes a new CheckpointManager.

    `newest_persistent_checkpoint` and `newest_memory_checkpoint` are
    initialized to Checkpoint objects with values of None.

    Args:
        keep_checkpoints_num: Keep at least this many checkpoints.
        checkpoint_score_attr: Attribute to use to determine which
            checkpoints to keep.
        delete_fn: Function that deletes checkpoints. Must be
            idempotent.
    """

    def __init__(
        self,
        keep_checkpoints_num: int,
        checkpoint_score_attr: str,
        delete_fn: Callable[[str], None],
    ):
        if keep_checkpoints_num == 0:
            raise RuntimeError(
                "If checkpointing is enabled, Ray Tune requires `keep_checkpoints_num` "
                "to be None or a number greater than 0"
            )

        checkpoint_score_desc = checkpoint_score_attr.startswith("min-")
        if checkpoint_score_desc:
            checkpoint_score_attr = checkpoint_score_attr[4:]
        else:
            checkpoint_score_attr = checkpoint_score_attr

        checkpoint_strategy = CheckpointStrategy(
            num_to_keep=keep_checkpoints_num,
            checkpoint_score_attribute=checkpoint_score_attr,
            checkpoint_score_order=MIN if checkpoint_score_desc else MAX,
        )

        self._delete_fn = delete_fn

        super().__init__(checkpoint_strategy=checkpoint_strategy)

    def on_checkpoint(self, checkpoint: _TrackedCheckpoint):
        if checkpoint.storage_mode == _TrackedCheckpoint.MEMORY:
            self._replace_latest_memory_checkpoint(checkpoint)
        else:
            assert checkpoint.storage_mode == _TrackedCheckpoint.PERSISTENT
            assert (
                self._checkpoint_strategy.num_to_keep is None
                or self._checkpoint_strategy.num_to_keep > 0
            )
            self._decide_what_to_do_with_checkpoint(checkpoint)

    def _skip_persisted_checkpoint(self, persisted_checkpoint: _TrackedCheckpoint):
        assert persisted_checkpoint.storage_mode == _TrackedCheckpoint.PERSISTENT
        # Ray Tune always keeps track of the latest persisted checkpoint
        self._replace_latest_persisted_checkpoint(
            persisted_checkpoint=persisted_checkpoint
        )
        logger.debug(f"Skipping checkpoint due to low score: {persisted_checkpoint}.")

    def _delete_persisted_checkpoint(self, persisted_checkpoint: _TrackedCheckpoint):
        if persisted_checkpoint == self._latest_persisted_checkpoint:
            self._checkpoints_to_clean_up.add(persisted_checkpoint)
        else:
            persisted_checkpoint.delete()

    # Tune-specific properties

    @property
    def newest_persistent_checkpoint(self):
        return self._latest_persisted_checkpoint or _TrackedCheckpoint(
            checkpoint_dir_or_data=None,
            checkpoint_id=0,
            storage_mode=_TrackedCheckpoint.PERSISTENT,
        )

    @property
    def newest_checkpoint(self):
        """Returns the newest checkpoint (based on training iteration)."""
        newest_checkpoint = max(
            [self.newest_persistent_checkpoint, self.newest_memory_checkpoint],
            key=lambda c: c.checkpoint_id,
        )
        return newest_checkpoint

    @property
    def newest_memory_checkpoint(self):
        return self._latest_memory_checkpoint

    def best_checkpoints(self):
        """Returns best PERSISTENT checkpoints, sorted by score."""
        checkpoints = sorted(self._top_persisted_checkpoints, key=lambda c: c.priority)
        return [queue_item.value for queue_item in checkpoints]

    def __getstate__(self):
        state = super().__getstate__()
        # Avoid serializing delete fn as it may contain cyclical dependencies
        state.pop("_delete_fn", None)
        return state

    def __setstate__(self, state):
        state["_delete_fn"] = None
        super().__setstate__(state)

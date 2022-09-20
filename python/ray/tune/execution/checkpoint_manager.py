# coding: utf-8
import logging
from typing import Callable, Optional

from ray.tune.result import TRAINING_ITERATION
from ray.air.config import CheckpointConfig, MIN, MAX
from ray.air._internal.checkpoint_manager import (
    _CheckpointManager as CommonCheckpointManager,
    _TrackedCheckpoint,
    CheckpointStorage,
)

logger = logging.getLogger(__name__)


class _CheckpointManager(CommonCheckpointManager):
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

    _persist_memory_checkpoints = False

    def __init__(
        self,
        keep_checkpoints_num: int,
        checkpoint_score_attr: Optional[str],
        delete_fn: Optional[Callable[["_TrackedCheckpoint"], None]] = None,
    ):
        if keep_checkpoints_num == 0:
            raise RuntimeError(
                "If checkpointing is enabled, Ray Tune requires `keep_checkpoints_num` "
                "to be None or a number greater than 0"
            )

        checkpoint_score_attr = checkpoint_score_attr or TRAINING_ITERATION

        checkpoint_score_desc = checkpoint_score_attr.startswith("min-")
        if checkpoint_score_desc:
            checkpoint_score_attr = checkpoint_score_attr[4:]
        else:
            checkpoint_score_attr = checkpoint_score_attr

        checkpoint_strategy = CheckpointConfig(
            num_to_keep=keep_checkpoints_num,
            checkpoint_score_attribute=checkpoint_score_attr,
            checkpoint_score_order=MIN if checkpoint_score_desc else MAX,
        )

        # NOTE: Determines whether the current memory checkpoint should be preferred
        # over a persistent checkpoint. An example of when this is used when the
        # in-memory checkpoint has been set by a scheduler, and the scheduler needs
        # that specific checkpoint be used for trial restore.
        self._prefer_memory_checkpoint = False

        super().__init__(checkpoint_strategy=checkpoint_strategy, delete_fn=delete_fn)

    def handle_checkpoint(self, checkpoint: _TrackedCheckpoint):
        # Set checkpoint ID
        if checkpoint.id is None:
            checkpoint.id = self._latest_checkpoint_id
        self._latest_checkpoint_id += 1

        if checkpoint.storage_mode == CheckpointStorage.MEMORY:
            self._replace_latest_memory_checkpoint(checkpoint)
        else:
            assert checkpoint.storage_mode == CheckpointStorage.PERSISTENT
            assert (
                self._checkpoint_strategy.num_to_keep is None
                or self._checkpoint_strategy.num_to_keep > 0
            )
            self._process_persistent_checkpoint(checkpoint)

    def on_checkpoint(
        self, checkpoint: _TrackedCheckpoint, prefer_memory_checkpoint: bool = False
    ):
        """Ray Tune's entry point to handle a checkpoint.

        Args:
            checkpoint: In-memory or persistent checkpoint for the manager to track.
            prefer_memory_checkpoint: Whether to force the manager to use return the
                in-memory checkpoint over the persistent checkpoint. This preference
                will be in effect until this function gets called on another checkpoint
                of any type. This flag can only be True if registering an in-memory
                checkpoint.
        """
        self._prefer_memory_checkpoint = prefer_memory_checkpoint
        if prefer_memory_checkpoint:
            assert (
                checkpoint.storage_mode == CheckpointStorage.MEMORY
            ), "Must store an in-memory checkpoint to prefer it to be used"
        # Todo (krfricke): Replace with handle_checkpoint.
        self.handle_checkpoint(checkpoint)

    def _skip_persisted_checkpoint(self, persisted_checkpoint: _TrackedCheckpoint):
        assert persisted_checkpoint.storage_mode == CheckpointStorage.PERSISTENT
        super()._skip_persisted_checkpoint(persisted_checkpoint=persisted_checkpoint)
        # Ray Tune always keeps track of the latest persisted checkpoint.
        # Note that this checkpoint will be deleted once it is not the
        # latest checkpoint anymore
        self._replace_latest_persisted_checkpoint(
            persisted_checkpoint=persisted_checkpoint
        )

    # Tune-specific properties

    @property
    def newest_persistent_checkpoint(self):
        return self._latest_persisted_checkpoint or _TrackedCheckpoint(
            dir_or_data=None,
            checkpoint_id=-1,
            storage_mode=CheckpointStorage.PERSISTENT,
        )

    @property
    def checkpoint(self):
        """Returns the newest checkpoint. Prefers in-memory checkpoints when
        `_prefer_memory_checkpoints` has been on the latest call to
        `CheckpointManager.on_checkpoint`.

        `Trial.checkpoint` relies on this method to determine which checkpoint to use
        for restore.
        This method chooses the checkpoint to use (MEMORY or PERSISTENT) based on:
        1.) In-memory checkpoint preference (tracked by `_prefer_memory_checkpoints`)
        2.) Checkpoint recency (i.e. checkpoint id)
        """
        if self._prefer_memory_checkpoint:
            assert (
                self.newest_memory_checkpoint.id != -1
            ), "Must have an in-memory checkpoint to prefer it to be used"
            return self.newest_memory_checkpoint
        checkpoints = [self.newest_memory_checkpoint, self.newest_persistent_checkpoint]
        newest_checkpoint = max(checkpoints, key=lambda c: c.id)
        return newest_checkpoint

    @property
    def newest_memory_checkpoint(self):
        return self._latest_memory_checkpoint or _TrackedCheckpoint(
            dir_or_data=None,
            checkpoint_id=-1,
            storage_mode=CheckpointStorage.MEMORY,
        )

    def best_checkpoints(self):
        """Returns best PERSISTENT checkpoints, sorted by score."""
        checkpoints = sorted(self._top_persisted_checkpoints, key=lambda c: c.priority)
        return [wrapped.tracked_checkpoint for wrapped in checkpoints]

    def __getstate__(self):
        state = self.__dict__.copy()
        # Avoid serializing the memory checkpoint.
        state["_newest_memory_checkpoint"] = _TrackedCheckpoint(
            CheckpointStorage.MEMORY, None
        )
        # Avoid serializing lambda since it may capture cyclical dependencies.
        state.pop("_delete_fn")
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._delete_fn = None

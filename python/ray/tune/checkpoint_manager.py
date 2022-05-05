# coding: utf-8
import logging
from typing import Callable, Optional

from ray.util.ml_utils.checkpoint_manager import (
    CheckpointStrategy,
    MIN,
    MAX,
    CheckpointManager as CommonCheckpointManager,
    _TrackedCheckpoint,
)

logger = logging.getLogger(__name__)


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
        delete_fn: Optional[Callable[["_TrackedCheckpoint"], None]] = None,
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

        super().__init__(checkpoint_strategy=checkpoint_strategy, delete_fn=delete_fn)

    def on_checkpoint(self, checkpoint: _TrackedCheckpoint):
        # Set checkpoint ID
        checkpoint.checkpoint_id = (
            checkpoint.checkpoint_id or self._latest_checkpoint_id
        )
        self._latest_checkpoint_id += 1

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
            checkpoint_dir_or_data=None,
            checkpoint_id=-1,
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
        return self._latest_memory_checkpoint or _TrackedCheckpoint(
            checkpoint_dir_or_data=None,
            checkpoint_id=-1,
            storage_mode=_TrackedCheckpoint.MEMORY,
        )

    def best_checkpoints(self):
        """Returns best PERSISTENT checkpoints, sorted by score."""
        checkpoints = sorted(self._top_persisted_checkpoints, key=lambda c: c.priority)
        return [wrapped.tracked_checkpoint for wrapped in checkpoints]

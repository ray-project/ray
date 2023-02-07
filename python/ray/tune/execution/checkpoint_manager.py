# coding: utf-8
import logging
from typing import Callable, Optional

from ray.air.config import CheckpointConfig
from ray.air._internal.checkpoint_manager import (
    _CheckpointManager as CommonCheckpointManager,
    _TrackedCheckpoint,
)

logger = logging.getLogger(__name__)


class _CheckpointManager(CommonCheckpointManager):
    """Initializes a new CheckpointManager.

    `newest_checkpoint` is initialized to Checkpoint objects with values of None.

    Args:
        checkpoint_config: An optional checkpoint configuration that
            determines checkpointing strategy.
        delete_fn: Function that deletes checkpoints. Must be
            idempotent.
    """

    def __init__(
        self,
        checkpoint_config: Optional[CheckpointConfig] = None,
        delete_fn: Optional[Callable[["_TrackedCheckpoint"], None]] = None,
    ):
        checkpoint_config = checkpoint_config or CheckpointConfig()
        super().__init__(checkpoint_strategy=checkpoint_config, delete_fn=delete_fn)

    def handle_checkpoint(self, checkpoint: _TrackedCheckpoint):
        # Set checkpoint ID
        if checkpoint.id is None:
            checkpoint.id = self._latest_checkpoint_id
        self._latest_checkpoint_id += 1

        assert (
            self._checkpoint_strategy.num_to_keep is None
            or self._checkpoint_strategy.num_to_keep > 0
        )
        self._process_persistent_checkpoint(checkpoint)

    def on_checkpoint(self, checkpoint: _TrackedCheckpoint):
        """Ray Tune's entry point to handle a checkpoint."""
        # Todo (krfricke): Replace with handle_checkpoint.
        self.handle_checkpoint(checkpoint)

    def _skip_checkpoint(self, checkpoint: _TrackedCheckpoint):
        super()._skip_checkpoint(checkpoint=checkpoint)
        # Ray Tune always keeps track of the latest checkpoint.
        # Note that this checkpoint will be deleted once it is not the
        # latest checkpoint anymore
        self._replace_latest_checkpoint(checkpoint=checkpoint)

    # Tune-specific properties

    @property
    def newest_checkpoint(self):
        return self._latest_checkpoint or _TrackedCheckpoint(
            dir_or_data=None,
            checkpoint_id=-1,
        )

    def best_checkpoints(self):
        """Returns best checkpoints, sorted by score."""
        checkpoints = sorted(self._top_checkpoints, key=lambda c: c.priority)
        return [wrapped.tracked_checkpoint for wrapped in checkpoints]

    def __getstate__(self):
        state = self.__dict__.copy()
        # Avoid serializing lambda since it may capture cyclical dependencies.
        state.pop("_delete_fn")
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._delete_fn = None

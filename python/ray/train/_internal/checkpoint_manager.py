from dataclasses import dataclass
import heapq
import logging
import numbers
from typing import Any, Dict, List, Optional, Set, Tuple

from ray._private.dict import flatten_dict
from ray.air.config import MAX
from ray.air._internal.util import is_nan
from ray.train import CheckpointConfig
from ray.train._internal.storage import _delete_fs_path
from ray.train.checkpoint import Checkpoint


logger = logging.getLogger(__name__)


@dataclass
class _TrackedCheckpoint:
    """Checkpoint tracked by a checkpoint manager."""

    checkpoint: Checkpoint
    metrics: Dict[str, Any]
    index: int = -1


class _HeapCheckpointWrapper:
    def __init__(self, priority: Any, tracked_checkpoint: _TrackedCheckpoint):
        # Ex: 2 wrapped checkpoints, with checkpoint_score_order = MAX
        # Priority of ckpt 1: (not_is_nan=True, metric=5, id=0)
        # Priority of ckpt 2: (not_is_nan=True, metric=6, id=1)
        # In this case, the min-heap should pop ckpt 1 first, since
        # the metric is smaller.
        self.priority = priority
        self.tracked_checkpoint = tracked_checkpoint

    def __lt__(self, other):
        return self.priority < other.priority

    def __repr__(self) -> str:
        return (
            f"_HeapCheckpointWrapper(\n"
            "  priority={self.priority}, "
            f"tracked_checkpoint={repr(self.tracked_checkpoint)}\n)"
        )


class _CheckpointManager:
    """Checkpoint manager that handles checkpoint book-keeping for a trial.

    Args:
        checkpoint_config: Defines how many and which checkpoints to keep.
        latest_checkpoint_id: First checkpoint ID to use (e.g., in case we
            resume an existing experiment).
    """

    def __init__(
        self,
        checkpoint_config: Optional[CheckpointConfig],
        latest_checkpoint_index: int = 0,
    ):
        self._checkpoint_config = checkpoint_config or CheckpointConfig()

        # Incremental unique checkpoint ID of this run.
        self._latest_checkpoint_index = latest_checkpoint_index

        # A heap that tracks the top K checkpoints.
        self._top_k_checkpoints: List[_TrackedCheckpoint] = []

        # The most recently registered checkpoint.
        # The latest checkpoint should never be deleted immediately,
        # even if it's the worst checkpoint.
        self._latest_checkpoint: Optional[_TrackedCheckpoint] = None

        # Deletion of some checkpoints should be deferred. Specifically, if the
        # latest persisted checkpoint should be deleted, we will only delete it
        # once a new checkpoint came in (so that `_latest_checkpoint` is available).
        self._checkpoints_to_clean_up: Set[_TrackedCheckpoint] = set()

    def register_checkpoint(self, tracked_checkpoint: _TrackedCheckpoint):
        """Register new checkpoint and add to bookkeeping.

        This method will register a new checkpoint and add it to the internal
        bookkeeping logic. This means the checkpoint manager will decide if
        this checkpoint should be kept, and if older or worse performing
        checkpoints should be deleted.

        Args:
            checkpoints: Tracked checkpoint object to add to bookkeeping.
        """
        # Mark the index of the checkpoint, for use in determining the
        # order in which checkpoints were registered.
        # TODO(justinvyu): Should this be unified with the checkpoint index
        # tracked in the session / used in the checkpoint dirname?
        tracked_checkpoint.index = self._latest_checkpoint_index
        self._latest_checkpoint = tracked_checkpoint

        # Add to heap.
        checkpoint_priority = self._get_checkpoint_score(tracked_checkpoint)
        wrapped_checkpoint = _HeapCheckpointWrapper(
            checkpoint_priority, tracked_checkpoint
        )
        heapq.heappush(self._top_k_checkpoints, wrapped_checkpoint)

        # Delete the worst checkpoint if we're over the `num_to_keep` limit.
        if (
            self._checkpoint_config.num_to_keep is not None
            and len(self._top_k_checkpoints) > self._checkpoint_config.num_to_keep
        ):
            worst_checkpoint = heapq.heappop(self._top_k_checkpoints)
            self._maybe_delete_checkpoint(worst_checkpoint.tracked_checkpoint)

        # Delete any checkpoints that were deferred.
        self._cleanup_checkpoints()

        self._latest_checkpoint_id += 1

    def _get_checkpoint_score(
        self, checkpoint: _TrackedCheckpoint
    ) -> Tuple[bool, numbers.Number, int]:
        """Get scoring tuple for a checkpoint, according to checkpoint strategy.

        We sort checkpoints by this score. Checkpoints with a higher score are kept.
        To achieve the desired ordering, we return a tuple of
        (is_not_na: bool, metric: Number, index: int).

        The first index means that checkpoints that are NaN are rated worst.
        The second index sorts the checkpoints by metric value. The third index
        sorts checkpoints with the same metric value by their index - more recent
        checkpoints are rated higher.
        """
        checkpoint_score_attribute = self._checkpoint_config.checkpoint_score_attribute
        if checkpoint_score_attribute:
            flat_metrics = flatten_dict(checkpoint.metrics)
            try:
                checkpoint_result = flat_metrics[checkpoint_score_attribute]
            except KeyError:
                valid_keys = list(flat_metrics.keys())
                logger.error(
                    f"Result dict has no key: {checkpoint_score_attribute}. "
                    f"checkpoint_score_attr must be set to a key in the "
                    f"result dict. Valid keys are: {valid_keys}"
                )
                checkpoint_result = float("-inf")
        else:
            checkpoint_result = float("-inf")

        checkpoint_score_order = self._checkpoint_config.checkpoint_score_order
        order_factor = 1.0 if checkpoint_score_order == MAX else -1.0

        checkpoint_score = order_factor * checkpoint_result

        if not isinstance(checkpoint_score, numbers.Number):
            raise ValueError(
                f"Unable to persist checkpoint for "
                f"checkpoint_score_attribute: "
                f"{checkpoint_score_attribute} with value "
                f"{checkpoint_score}. "
                f"This attribute must be numerical."
            )

        return (
            (not is_nan(checkpoint_score), checkpoint_score, checkpoint.index)
            if not is_nan(checkpoint_score)
            else (False, float("-inf"), checkpoint.index)
        )

    def _maybe_delete_checkpoint(self, tracked_checkpoint: _TrackedCheckpoint):
        """Delete a checkpoint as long as it's not the most recent one."""
        if tracked_checkpoint == self._latest_persisted_checkpoint:
            self._checkpoints_to_clean_up.add(tracked_checkpoint)
        else:
            self._delete_checkpoint(tracked_checkpoint)

    def _delete_checkpoint(self, tracked_checkpoint: _TrackedCheckpoint):
        checkpoint = tracked_checkpoint.checkpoint
        _delete_fs_path(fs=checkpoint.filesystem, fs_path=checkpoint.path)
        self._checkpoints_to_clean_up.discard(tracked_checkpoint)

    def _cleanup_checkpoints(self):
        """Delete any checkpoints that were deferred for deletion."""
        for tracked_checkpoint in list(self._checkpoints_to_clean_up):
            self._maybe_delete_checkpoint(tracked_checkpoint)

    def __del__(self):
        self._cleanup_checkpoints()

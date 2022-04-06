import heapq
import logging
import numbers
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Union

import ray
from ray.train.constants import TIMESTAMP, TUNE_INSTALLED
from ray.tune.result import NODE_IP
from ray.util import PublicAPI

if TUNE_INSTALLED:
    pass
else:
    tune = None

MAX = "max"
MIN = "min"

logger = logging.getLogger(__name__)


class _TrackedCheckpoint:
    MEMORY = "memory"
    PERSISTENT = "persistent"

    def __init__(self,
                 checkpoint_dir_or_data: Union[str, Path, Dict, ray.ObjectRef],
                 checkpoint_id: int,
                 storage_mode: str,
                 result: Optional[Dict] = None,
                 node_ip: Optional[str] = None,
                 ):
        self.checkpoint_dir_or_data = checkpoint_dir_or_data
        self.checkpoint_id = checkpoint_id
        self.storage_mode = storage_mode

        # Todo: What to do if result is a subset of checkpoint_dir_or_data (dict)
        self.result = result or {}
        self.node_ip = node_ip or self.result.get(NODE_IP, None)

    def commit(self):
        pass

    def delete(self):
        pass


class _HeapCheckpointWrapper:
    def __init__(self, priority: numbers.Number, tracked_checkpoint: _TrackedCheckpoint):
        self.priority = priority
        self.tracked_checkpoint = tracked_checkpoint

    def __lt__(self, other):
        return self.priority < other.priority

    def __repr__(self):
        return f"_HeapCheckpoint({repr(self.tracked_checkpoint)})"


@PublicAPI(stability="beta")
@dataclass
class CheckpointStrategy:
    """Configurable parameters for defining the Train checkpointing strategy.

    Default behavior is to persist all checkpoints to disk. If
    ``num_to_keep`` is set, the default retention policy is to keep the
    checkpoints with maximum timestamp, i.e. the most recent checkpoints.

    Args:
        num_to_keep (Optional[int]): The number of checkpoints to keep
            on disk for this run. If a checkpoint is persisted to disk after
            there are already this many checkpoints, then an existing
            checkpoint will be deleted. If this is ``None`` then checkpoints
            will not be deleted. If this is ``0`` then no checkpoints will be
            persisted to disk.
        checkpoint_score_attribute (str): The attribute that will be used to
            score checkpoints to determine which checkpoints should be kept
            on disk when there are greater than ``num_to_keep`` checkpoints.
            This attribute must be a key from the checkpoint
            dictionary which has a numerical value.
        checkpoint_score_order (str). Either "max" or "min".
            If "max", then checkpoints with highest values of
            ``checkpoint_score_attribute`` will be kept.
            If "min", then checkpoints with lowest values of
            ``checkpoint_score_attribute`` will be kept.
    """

    num_to_keep: Optional[int] = None
    checkpoint_score_attribute: str = TIMESTAMP
    checkpoint_score_order: str = MAX

    def __post_init__(self):
        if self.num_to_keep is not None and self.num_to_keep < 0:
            raise ValueError(
                f"Received invalidate num_to_keep: "
                f"{self.num_to_keep}. "
                f"Must be None or non-negative integer."
            )
        if self.checkpoint_score_order not in (MAX, MIN):
            raise ValueError(
                f"checkpoint_score_order must be either " f'"{MAX}" or "{MIN}".'
            )


class CheckpointManager:
    def __init__(self, checkpoint_strategy: CheckpointStrategy, latest_checkpoint_id: int = 0):
        self._latest_checkpoint_id = latest_checkpoint_id  # Todo (krfricke): Review if needed
        self._checkpoint_strategy = checkpoint_strategy

        self.latest_checkpoint = None

        # Incremental unique checkpoint ID of this run.
        self._latest_checkpoint_id = 0

        # Used for keeping top K checkpoints.
        self._top_persisted_checkpoints = []

        # Best checkpoint altogether.
        # Used for exposing best_checkpoint_path.
        self._best_persisted_checkpoint_wrapped: Optional[_HeapCheckpointWrapper] = None

        self._last_persisted_checkpoint: Optional[_TrackedCheckpoint] = None

        self._last_memory_checkpoint: Optional[_TrackedCheckpoint] = None

    # Do we need this at all?
    @property
    def _best_persisted_checkpoint(self) -> _TrackedCheckpoint:
        return self._best_persisted_checkpoint_wrapped.tracked_checkpoint

    def decide_what_to_do_with_checkpoint(self, checkpoint: _TrackedCheckpoint):
        if checkpoint.storage_mode == _TrackedCheckpoint.MEMORY:
            self._last_memory_checkpoint = checkpoint

        second_to_last_persisted_checkpoint = self._last_persisted_checkpoint
        self._last_persisted_checkpoint = checkpoint

        num_to_keep = self._checkpoint_strategy.num_to_keep

        if num_to_keep == 0:
            if second_to_last_persisted_checkpoint:
                second_to_last_persisted_checkpoint.delete()

            # Checkpoints should not be persisted to disk.
            return

        checkpoint_score_attribute = (
            self._checkpoint_strategy.checkpoint_score_attribute
        )

        if checkpoint_score_attribute not in checkpoint:
            raise ValueError(
                f"Unable to persist checkpoint for "
                f"checkpoint_score_attribute: "
                f"{checkpoint_score_attribute}. "
                f"Include this attribute in the call to "
                f"train.save_checkpoint."
            )

        checkpoint_score_order = self._checkpoint_strategy.checkpoint_score_order
        if checkpoint_score_order == MIN:
            order_factor = 1.
        else:
            order_factor = -1.

        checkpoint_score = order_factor * checkpoint.result[checkpoint_score_attribute]

        if not isinstance(checkpoint_score, numbers.Number):
            raise ValueError(
                f"Unable to persist checkpoint for "
                f"checkpoint_score_attribute: "
                f"{checkpoint_score_attribute} with value "
                f"{checkpoint_score}. "
                f"This attribute must be numerical."
            )

        wrapped_checkpoint = _HeapCheckpointWrapper(priority=checkpoint_score, tracked_checkpoint=checkpoint)

        if num_to_keep is None:
            # Keep all checkpoints.
            checkpoint.commit()
            # Todo: track as latest available persisted checkpoint
            pass
        elif len(self._top_persisted_checkpoints) < num_to_keep:
            # Keep this checkpoint
            checkpoint.commit()
            heapq.heappush(self._top_persisted_checkpoints, wrapped_checkpoint)
        elif (
            wrapped_checkpoint.priority > self._top_persisted_checkpoints[0].priority
        ):
            # Write checkpoint to disk if not yet persisted
            checkpoint.commit()
            worst_checkpoint = heapq.heappushpop(self._top_persisted_checkpoints, wrapped_checkpoint).tracked_checkpoint
            worst_checkpoint.delete()
            logger.debug(f"Removed worst checkpoint from " f"{worst_checkpoint}.")
        else:
            # If the latest checkpoint has the same or lower priority, skip it.
            # Todo: fix this
            logger.debug(
                f"Skipping checkpoint due to low score:" f"{self.next_checkpoint_path}."
            )

        # Update single best checkpoint.
        if (
            self._best_persisted_checkpoint is None
            or wrapped_checkpoint.priority > self._best_persisted_checkpoint_wrapped.priority
        ):
            # If the latest checkpoint has the same or lower priority, skip it.
            self._best_persisted_checkpoint_wrapped = checkpoint

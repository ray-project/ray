import gc
import heapq
import logging
import numbers
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Union, Callable, Tuple, List

import ray
from ray.train.constants import TIMESTAMP, TUNE_INSTALLED
from ray.tune.result import NODE_IP
from ray.util import PublicAPI
from ray.util.ml_utils.util import is_nan

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

    def __init__(
        self,
        checkpoint_dir_or_data: Optional[Union[str, Path, Dict, ray.ObjectRef]],
        storage_mode: str,
        checkpoint_id: int = 0,
        result: Optional[Dict] = None,
        node_ip: Optional[str] = None,
        delete_fn: Optional[Callable[["_TrackedCheckpoint"], None]] = None,
    ):
        self.checkpoint_dir_or_data = checkpoint_dir_or_data
        self.checkpoint_id = checkpoint_id
        self.storage_mode = storage_mode

        # Todo: What to do if result is a subset of checkpoint_dir_or_data (dict)
        self.result = result or {}
        self.node_ip = node_ip or self.result.get(NODE_IP, None)

        self._delete_fn = delete_fn or _default_delete_fn

    def commit(self, path: Optional[Path] = None):
        pass

    def delete(self):
        self._delete_fn(self)


def _default_delete_fn(checkpoint: _TrackedCheckpoint):
    if checkpoint.storage_mode != _TrackedCheckpoint.PERSISTENT:
        return

    if os.path.isfile(checkpoint.checkpoint_dir_or_data):
        os.remove(checkpoint.checkpoint_dir_or_data)
    elif os.path.isdir(checkpoint.checkpoint_dir_or_data):
        shutil.rmtree(checkpoint.checkpoint_dir_or_data)
    else:
        logger.warning(
            f"Could not delete checkpoint {checkpoint} from disk as it is "
            f"neither file not directory. Path: {checkpoint.checkpoint_dir_or_data}."
        )


class _HeapCheckpointWrapper:
    def __init__(
        self, priority: numbers.Number, tracked_checkpoint: _TrackedCheckpoint
    ):
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
    def __init__(
        self, checkpoint_strategy: CheckpointStrategy, latest_checkpoint_id: int = 0
    ):
        self._checkpoint_strategy = checkpoint_strategy

        self.latest_checkpoint = None

        # Incremental unique checkpoint ID of this run.
        self._latest_checkpoint_id = latest_checkpoint_id

        # Used for keeping top K checkpoints.
        self._top_persisted_checkpoints: List[_HeapCheckpointWrapper] = []

        # Best checkpoint altogether.
        # Used for exposing best_checkpoint_path.
        self._best_persisted_checkpoint: Optional[_TrackedCheckpoint] = None
        self._latest_persisted_checkpoint: Optional[_TrackedCheckpoint] = None
        self._latest_memory_checkpoint: Optional[_TrackedCheckpoint] = None

        # Checkpoints that are not immediately removed
        self._checkpoints_to_clean_up = set()

    def _replace_latest_memory_checkpoint(self, memory_checkpoint: _TrackedCheckpoint):
        assert memory_checkpoint.storage_mode == _TrackedCheckpoint.MEMORY
        self._latest_memory_checkpoint = memory_checkpoint
        # Avoid memory leaks on k8s pods
        gc.collect()

    def _replace_latest_persisted_checkpoint(
        self, persisted_checkpoint: _TrackedCheckpoint
    ):
        second_to_latest_persisted_checkpoint = self._latest_persisted_checkpoint
        self._latest_persisted_checkpoint = persisted_checkpoint

        if self._checkpoint_strategy.num_to_keep == 0:
            self._delete_persisted_checkpoint(second_to_latest_persisted_checkpoint)

    def _maybe_replace_best_persisted_checkpoint(
        self, persisted_checkpoint: _TrackedCheckpoint
    ):
        if self._best_persisted_checkpoint is None:
            self._best_persisted_checkpoint = persisted_checkpoint
        else:
            old_score = self._get_checkpoint_score(self._best_persisted_checkpoint)
            candidate_score = self._get_checkpoint_score(persisted_checkpoint)
            if candidate_score >= old_score:
                self._best_persisted_checkpoint = persisted_checkpoint

    def _get_checkpoint_score(
        self, checkpoint: _TrackedCheckpoint
    ) -> Tuple[bool, numbers.Number, int]:
        checkpoint_score_attribute = (
            self._checkpoint_strategy.checkpoint_score_attribute
        )
        checkpoint_score_order = self._checkpoint_strategy.checkpoint_score_order
        if checkpoint_score_order == MIN:
            order_factor = 1.0
        else:
            order_factor = -1.0

        checkpoint_score = order_factor * checkpoint.result[checkpoint_score_attribute]

        if not isinstance(checkpoint_score, numbers.Number):
            raise ValueError(
                f"Unable to persist checkpoint for "
                f"checkpoint_score_attribute: "
                f"{checkpoint_score_attribute} with value "
                f"{checkpoint_score}. "
                f"This attribute must be numerical."
            )

        return (
            not is_nan(checkpoint_score),
            checkpoint_score if not is_nan(checkpoint_score) else 0,
            checkpoint.checkpoint_id,
        )

    def _decide_what_to_do_with_checkpoint(self, checkpoint: _TrackedCheckpoint):
        checkpoint_score = self._get_checkpoint_score(checkpoint)
        wrapped_checkpoint = _HeapCheckpointWrapper(
            priority=checkpoint_score, tracked_checkpoint=checkpoint
        )

        if self._checkpoint_strategy.num_to_keep is None:
            # Keep all checkpoints
            checkpoint.commit(path=self._get_next_checkpoint_path())
            self._replace_latest_persisted_checkpoint(checkpoint)
        elif (
            len(self._top_persisted_checkpoints) < self._checkpoint_strategy.num_to_keep
        ):
            # Heap is not full yet, so keep this checkpoint
            checkpoint.commit(path=self._get_next_checkpoint_path())
            heapq.heappush(self._top_persisted_checkpoints, wrapped_checkpoint)
            self._replace_latest_persisted_checkpoint(checkpoint)
        elif wrapped_checkpoint.priority > self._top_persisted_checkpoints[0].priority:
            # Priority is higher than current worst checkpoint, so replace worst
            checkpoint.commit(path=self._get_next_checkpoint_path())
            worst_checkpoint = heapq.heappushpop(
                self._top_persisted_checkpoints, wrapped_checkpoint
            ).tracked_checkpoint
            self._delete_persisted_checkpoint(worst_checkpoint)
            logger.debug(f"Removed worst checkpoint from " f"{worst_checkpoint}.")
        else:
            # If the latest checkpoint has the same or lower priority, skip it.
            self._skip_persisted_checkpoint(checkpoint)

        self._maybe_replace_best_persisted_checkpoint(persisted_checkpoint=checkpoint)
        self._cleanup_checkpoints()

    def _delete_persisted_checkpoint(self, persisted_checkpoint: _TrackedCheckpoint):
        if persisted_checkpoint == self._latest_persisted_checkpoint:
            self._checkpoints_to_clean_up.add(persisted_checkpoint)
        else:
            persisted_checkpoint.delete()

    def _cleanup_checkpoints(self):
        for checkpoint in self._checkpoints_to_clean_up:
            self._delete_persisted_checkpoint(persisted_checkpoint=checkpoint)

    def _skip_persisted_checkpoint(self, persisted_checkpoint: _TrackedCheckpoint):
        logger.debug(f"Skipping checkpoint due to low score: {persisted_checkpoint}.")

    def _get_next_checkpoint_path(self) -> Optional[Path]:
        return None

    def __getstate__(self):
        state = self.__dict__.copy()
        # Avoid serializing the memory checkpoint.
        state["_newest_memory_checkpoint"] = _TrackedCheckpoint(
            checkpoint_dir_or_data=None,
            checkpoint_id=0,
            storage_mode=_TrackedCheckpoint.MEMORY,
        )
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

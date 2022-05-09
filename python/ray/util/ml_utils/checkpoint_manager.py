import gc
import heapq
import logging
import numbers
import os
import shutil

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Union, Callable, Tuple, List, Any

import ray
from ray.tune.result import NODE_IP
from ray.util import PublicAPI
from ray.util.ml_utils.util import is_nan

MAX = "max"
MIN = "min"

logger = logging.getLogger(__name__)


class _TrackedCheckpoint:
    MEMORY = "memory"
    PERSISTENT = "persistent"

    def __init__(
        self,
        dir_or_data: Optional[Union[str, Path, Dict, ray.ObjectRef]],
        storage_mode: str,
        checkpoint_id: Optional[int] = None,
        result: Optional[Dict] = None,
        node_ip: Optional[str] = None,
    ):
        self.dir_or_data = dir_or_data
        self.id = checkpoint_id
        self.storage_mode = storage_mode

        # Todo: What to do if result is a subset of dir_or_data (dict)
        self.result = result or {}
        self.node_ip = node_ip or self.result.get(NODE_IP, None)

    def commit(self, path: Optional[Path] = None):
        """Commit checkpoint to disk, if needed."""
        pass

    def delete(
        self, delete_fn: Optional[Callable[["_TrackedCheckpoint"], None]] = None
    ):
        """Delete checkpoint from disk, if needed."""
        delete_fn = delete_fn or _default_delete_fn
        delete_fn(self)

    def __repr__(self):
        if self.storage_mode == _TrackedCheckpoint.MEMORY:
            return f"<_TrackedCheckpoint storage='MEMORY' result={self.result}>"

        return (
            f"<_TrackedCheckpoint storage='PERSISTENT' "
            f"dir_or_data={self.dir_or_data}>"
        )


def _default_delete_fn(checkpoint: _TrackedCheckpoint):
    if checkpoint.storage_mode != _TrackedCheckpoint.PERSISTENT:
        return

    if isinstance(checkpoint.dir_or_data, (str, bytes, os.PathLike)):
        if os.path.isfile(checkpoint.dir_or_data):
            os.remove(checkpoint.dir_or_data)
            return
        elif os.path.isdir(checkpoint.dir_or_data):
            shutil.rmtree(checkpoint.dir_or_data)
            return
    logger.warning(
        f"Could not delete checkpoint {checkpoint} from disk as it is "
        f"neither file not directory. Path: {checkpoint.dir_or_data}."
    )


class _HeapCheckpointWrapper:
    def __init__(self, priority: Any, tracked_checkpoint: _TrackedCheckpoint):
        self.priority = priority
        self.tracked_checkpoint = tracked_checkpoint

    def __lt__(self, other):
        return self.priority < other.priority

    def __repr__(self):
        return f"_HeapCheckpoint({repr(self.tracked_checkpoint)})"


@PublicAPI(stability="beta")
@dataclass
class CheckpointStrategy:
    """Configurable parameters for defining the checkpointing strategy.

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
            dictionary which has a numerical value. Per default, the last
            checkpoints will be kept.
        checkpoint_score_order (str). Either "max" or "min".
            If "max", then checkpoints with highest values of
            ``checkpoint_score_attribute`` will be kept.
            If "min", then checkpoints with lowest values of
            ``checkpoint_score_attribute`` will be kept.
    """

    num_to_keep: Optional[int] = None
    checkpoint_score_attribute: Optional[str] = None
    checkpoint_score_order: str = MAX

    def __post_init__(self):
        if self.num_to_keep is not None and self.num_to_keep < 0:
            raise ValueError(
                f"Received invalid num_to_keep: "
                f"{self.num_to_keep}. "
                f"Must be None or non-negative integer."
            )
        if self.checkpoint_score_order not in (MAX, MIN):
            raise ValueError(
                f"checkpoint_score_order must be either " f'"{MAX}" or "{MIN}".'
            )


class CheckpointManager:
    """Common checkpoint management and bookkeeping class for Ray Train and Tune.

    This class acts as the common core for checkpoint bookkeeping in Ray ML libraries.
    On a high level, this manager keeps a reference to all stored checkpoints
    (both in-memory and on-disk checkpoints). For on-disk checkpoints, it
    keeps a configured number of checkpoints according to specified metrics.

    The manager supports lazy data writing by utilizing the
    ``_TrackedCheckpoint.commit()`` API, which is only invoked if the checkpoint
    should be persisted to disk.
    """

    def __init__(
        self,
        checkpoint_strategy: CheckpointStrategy,
        latest_checkpoint_id: int = 0,
        delete_fn: Optional[Callable[["_TrackedCheckpoint"], None]] = None,
    ):
        self._checkpoint_strategy = checkpoint_strategy

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
        self._delete_fn = delete_fn

    def set_delete_fn(
        self, delete_fn: Optional[Callable[["_TrackedCheckpoint"], None]]
    ):
        self._delete_fn = delete_fn

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
            self._maybe_delete_persisted_checkpoint(
                second_to_latest_persisted_checkpoint
            )

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
        if checkpoint_score_attribute not in checkpoint.result:
            logger.error(
                f"Result dict has no key: {checkpoint_score_attribute}. "
                f"checkpoint_score_attr must be set to a key in the "
                f"result dict."
            )
            checkpoint_result = float("-inf")
        else:
            checkpoint_result = checkpoint.result[checkpoint_score_attribute]

        checkpoint_score_order = self._checkpoint_strategy.checkpoint_score_order
        if checkpoint_score_order == MAX:
            order_factor = 1.0
        else:
            order_factor = -1.0

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
            not is_nan(checkpoint_score),
            checkpoint_score if not is_nan(checkpoint_score) else 0,
            checkpoint.id,
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
            self._top_persisted_checkpoints.append(wrapped_checkpoint)
        elif (
            len(self._top_persisted_checkpoints) < self._checkpoint_strategy.num_to_keep
        ):
            # Heap is not full yet, so keep this checkpoint
            checkpoint.commit(path=self._get_next_checkpoint_path())
            heapq.heappush(self._top_persisted_checkpoints, wrapped_checkpoint)
            self._replace_latest_persisted_checkpoint(checkpoint)
        elif wrapped_checkpoint.priority >= self._top_persisted_checkpoints[0].priority:
            # Priority is higher than current worst checkpoint, so replace worst
            checkpoint.commit(path=self._get_next_checkpoint_path())
            worst_checkpoint = heapq.heappushpop(
                self._top_persisted_checkpoints, wrapped_checkpoint
            ).tracked_checkpoint

            # Only remove if checkpoint data is different
            if worst_checkpoint.dir_or_data != checkpoint.dir_or_data:
                self._maybe_delete_persisted_checkpoint(worst_checkpoint)
                logger.debug(f"Removed worst checkpoint from " f"{worst_checkpoint}.")

            self._replace_latest_persisted_checkpoint(checkpoint)
        else:
            # If the latest checkpoint has the same or lower priority, skip it.
            self._skip_persisted_checkpoint(checkpoint)

        self._maybe_replace_best_persisted_checkpoint(persisted_checkpoint=checkpoint)
        self._cleanup_checkpoints()

    def _maybe_delete_persisted_checkpoint(
        self, persisted_checkpoint: _TrackedCheckpoint
    ):
        if persisted_checkpoint == self._latest_persisted_checkpoint:
            self._checkpoints_to_clean_up.add(persisted_checkpoint)
        else:
            self._delete_persisted_checkpoint(persisted_checkpoint=persisted_checkpoint)

    def _delete_persisted_checkpoint(self, persisted_checkpoint: _TrackedCheckpoint):
        persisted_checkpoint.delete(delete_fn=self._delete_fn)
        self._checkpoints_to_clean_up.discard(persisted_checkpoint)

    def _cleanup_checkpoints(self):
        for checkpoint in list(self._checkpoints_to_clean_up):
            self._maybe_delete_persisted_checkpoint(persisted_checkpoint=checkpoint)

    def _skip_persisted_checkpoint(self, persisted_checkpoint: _TrackedCheckpoint):
        logger.debug(f"Skipping checkpoint due to low score: {persisted_checkpoint}.")
        self._checkpoints_to_clean_up.add(persisted_checkpoint)

    def _get_next_checkpoint_path(self) -> Optional[Path]:
        return None

    def __del__(self):
        self._cleanup_checkpoints()

    def __getstate__(self):
        state = self.__dict__.copy()

        # Do not serialize the delete fn
        state.pop("_delete_fn", None)

        # Avoid serializing the memory checkpoint.
        state["_newest_memory_checkpoint"] = _TrackedCheckpoint(
            dir_or_data=None,
            checkpoint_id=0,
            storage_mode=_TrackedCheckpoint.MEMORY,
        )
        return state

    def __setstate__(self, state):
        state["_delete_fn"] = None
        self.__dict__.update(state)

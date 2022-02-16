# coding: utf-8
import heapq
import gc
import logging
from typing import Dict, List, Tuple, Union

from ray.tune.utils.util import flatten_dict
from ray.util.ml_utils.checkpoint import DataCheckpoint, Checkpoint

logger = logging.getLogger(__name__)

MEMORY = "memory"
PERSISTENT = "persistent"


class _ManagedCheckpoint:
    """Describes a checkpoint of trial state.

    Checkpoint may be saved in different storage.

    Attributes:
        storage (str): Storage type.
        value (str): If storage==MEMORY, it is a Python object.
            If storage==PERSISTENT, it is a path to persistent storage,
            or a future that will be resolved to such a path.
    """

    def __init__(self, storage, value, result=None):
        self.storage = storage
        self.value = value
        self.result = result or {}
        # The logical order of checkpoints (both in memory and persistent)
        # The more recent checkpoints have larger order.
        # The most recent checkpoint is used to restore the trial.
        self.order = 0

    @staticmethod
    def from_object(value=None):
        """Creates a checkpoint from a Python object."""
        return _ManagedCheckpoint(MEMORY, value)

    @property
    def is_ready(self):
        """Returns whether the checkpoint is ready to be used for restoration.

        A PERSISTENT checkpoint is considered ready once its value is resolved
        to an actual path. MEMORY checkpoints are always considered ready since
        they are transient.
        """
        if self.storage == PERSISTENT:
            return isinstance(self.value, str)
        return self.storage == MEMORY

    def __repr__(self):
        return f"ManagedCheckpoint({self.storage}, {self.value})"


class QueueItem:
    def __init__(self, priority, value):
        self.priority = priority
        self.value = value

    def __lt__(self, other):
        return self.priority < other.priority

    def __repr__(self):
        return f"QueueItem({repr(self.value)})"


class CheckpointManager:
    """Manages checkpoints on the driver for a trial."""

    def __init__(self, keep_checkpoints_num, checkpoint_score_attr, delete_fn):
        """Initializes a new CheckpointManager.

        `newest_persistent_checkpoint` and `newest_memory_checkpoint` are
        initialized to Checkpoint objects with values of None.

        Args:
            keep_checkpoints_num (int): Keep at least this many checkpoints.
            checkpoint_score_attr (str): Attribute to use to determine which
                checkpoints to keep.
            delete_fn (function): Function that deletes checkpoints. Must be
                idempotent.
        """
        self.keep_checkpoints_num = keep_checkpoints_num or float("inf")
        assert (
            self.keep_checkpoints_num > 0
        ), "keep_checkpoints_num must be greater than 0."
        self._checkpoint_score_desc = checkpoint_score_attr.startswith("min-")
        if self._checkpoint_score_desc:
            self._checkpoint_score_attr = checkpoint_score_attr[4:]
        else:
            self._checkpoint_score_attr = checkpoint_score_attr

        self.delete = delete_fn
        self.newest_persistent_wrapped_checkpoint = _ManagedCheckpoint(PERSISTENT, None)
        self._newest_wrapped_memory_checkpoint = _ManagedCheckpoint(MEMORY, None)
        self._best_checkpoints = []
        self._membership = set()
        self._cur_order = 0

    @property
    def newest_checkpoint(self) -> Tuple[Checkpoint, Dict]:
        """Returns the newest checkpoint and result."""
        newest_checkpoint = max(
            [
                self.newest_persistent_wrapped_checkpoint,
                self._newest_wrapped_memory_checkpoint,
            ],
            key=lambda c: c.order,
        )
        return newest_checkpoint.value, newest_checkpoint.result

    @property
    def newest_memory_checkpoint(self) -> Tuple[Checkpoint, Dict]:
        checkpoint = self._newest_wrapped_memory_checkpoint
        return checkpoint.value, checkpoint.result

    def delete_newest_checkpoint(self):
        newest_checkpoint = max(
            [
                self.newest_persistent_wrapped_checkpoint,
                self._newest_wrapped_memory_checkpoint,
            ],
            key=lambda c: c.order,
        )
        # Drop from heap
        try:
            self._best_checkpoints.remove(newest_checkpoint)
        except Exception:
            pass
        # Drop from membership
        self._membership.discard(newest_checkpoint)
        # Delete data
        self.delete(newest_checkpoint)
        newest_checkpoint.value = None

    def replace_newest_memory_checkpoint(self, new_checkpoint: _ManagedCheckpoint):
        # Forcibly remove the memory checkpoint
        del self._newest_wrapped_memory_checkpoint
        # Apparently avoids memory leaks on k8s/k3s/pods
        gc.collect()
        self._newest_wrapped_memory_checkpoint = new_checkpoint

    def on_checkpoint(self, checkpoint: Checkpoint, result: Dict):
        """Starts tracking checkpoint metadata on checkpoint.

        Checkpoints get assigned with an `order` as they come in.
        The order is monotonically increasing.

        Sets the newest checkpoint. For PERSISTENT checkpoints: Deletes
        previous checkpoint as long as it isn't one of the best ones. Also
        deletes the worst checkpoint if at capacity.

        Args:
            checkpoint (Checkpoint): Trial state checkpoint.
        """
        wrapped_checkpoint = _ManagedCheckpoint(PERSISTENT, checkpoint, result)

        self._cur_order += 1
        wrapped_checkpoint.order = self._cur_order

        if isinstance(checkpoint, DataCheckpoint):
            wrapped_checkpoint.storage = MEMORY
            self.replace_newest_memory_checkpoint(wrapped_checkpoint)
            return

        old_checkpoint = self.newest_persistent_wrapped_checkpoint

        if old_checkpoint.value == wrapped_checkpoint.value:
            # Overwrite the order of the checkpoint.
            old_checkpoint.order = wrapped_checkpoint.order
            return

        self.newest_persistent_wrapped_checkpoint = wrapped_checkpoint

        # Remove the old checkpoint if it isn't one of the best ones.
        if old_checkpoint.value and old_checkpoint not in self._membership:
            self.delete(old_checkpoint.value)

        try:
            queue_item = QueueItem(
                self._priority(wrapped_checkpoint), wrapped_checkpoint
            )
        except KeyError:
            logger.error(
                "Result dict has no key: {}. "
                "checkpoint_score_attr must be set to a key in the "
                "result dict.".format(self._checkpoint_score_attr)
            )
            return

        if len(self._best_checkpoints) < self.keep_checkpoints_num:
            heapq.heappush(self._best_checkpoints, queue_item)
            self._membership.add(wrapped_checkpoint)
        elif queue_item.priority >= self._best_checkpoints[0].priority:
            worst = heapq.heappushpop(self._best_checkpoints, queue_item).value
            self._membership.add(wrapped_checkpoint)
            if worst in self._membership:
                self._membership.remove(worst)
            # Don't delete the newest checkpoint. It will be deleted on the
            # next on_checkpoint() call since it isn't in self._membership.
            if worst.value != wrapped_checkpoint.value:
                self.delete(worst.value)

    def best_checkpoints(
        self, return_results: bool = False
    ) -> Union[List[Checkpoint], List[Tuple[Checkpoint, Dict]]]:
        """Returns best PERSISTENT checkpoints, sorted by score."""
        checkpoints = sorted(self._best_checkpoints, key=lambda c: c.priority)
        if return_results:
            return [(qi.value.value, qi.value.result) for qi in checkpoints]
        return [qi.value.value for qi in checkpoints]

    def _priority(self, manager_checkpoint: _ManagedCheckpoint):
        result = flatten_dict(manager_checkpoint.result)
        priority = result[self._checkpoint_score_attr]
        return -priority if self._checkpoint_score_desc else priority

    def __getstate__(self):
        state = self.__dict__.copy()
        # Avoid serializing the memory checkpoint.
        state["_newest_memory_checkpoint"] = _ManagedCheckpoint(MEMORY, None)
        # Avoid serializing lambda since it may capture cyclical dependencies.
        state.pop("delete")
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.delete = None

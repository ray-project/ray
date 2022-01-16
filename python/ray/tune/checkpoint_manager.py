# coding: utf-8
import heapq
import gc
import logging

from ray.tune.utils.util import flatten_dict

logger = logging.getLogger(__name__)


class Checkpoint:
    """Describes a checkpoint of trial state.

    Checkpoint may be saved in different storage.

    Attributes:
        storage (str): Storage type.
        value (str): If storage==MEMORY, it is a Python object.
            If storage==PERSISTENT, it is a path to persistent storage,
            or a future that will be resolved to such a path.
    """

    MEMORY = "memory"
    PERSISTENT = "persistent"

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
        return Checkpoint(Checkpoint.MEMORY, value)

    @property
    def is_ready(self):
        """Returns whether the checkpoint is ready to be used for restoration.

        A PERSISTENT checkpoint is considered ready once its value is resolved
        to an actual path. MEMORY checkpoints are always considered ready since
        they are transient.
        """
        if self.storage == Checkpoint.PERSISTENT:
            return isinstance(self.value, str)
        return self.storage == Checkpoint.MEMORY

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
        assert self.keep_checkpoints_num > 0, (
            "keep_checkpoints_num must be greater than 0.")
        self._checkpoint_score_desc = checkpoint_score_attr.startswith("min-")
        if self._checkpoint_score_desc:
            self._checkpoint_score_attr = checkpoint_score_attr[4:]
        else:
            self._checkpoint_score_attr = checkpoint_score_attr

        self.delete = delete_fn
        self.newest_persistent_checkpoint = Checkpoint(Checkpoint.PERSISTENT,
                                                       None)
        self._newest_memory_checkpoint = Checkpoint(Checkpoint.MEMORY, None)
        self._best_checkpoints = []
        self._membership = set()
        self._cur_order = 0

    @property
    def newest_checkpoint(self):
        """Returns the newest checkpoint (based on training iteration)."""
        newest_checkpoint = max(
            [self.newest_persistent_checkpoint, self.newest_memory_checkpoint],
            key=lambda c: c.order)
        return newest_checkpoint

    @property
    def newest_memory_checkpoint(self):
        return self._newest_memory_checkpoint

    def replace_newest_memory_checkpoint(self, new_checkpoint):
        # Forcibly remove the memory checkpoint
        del self._newest_memory_checkpoint
        # Apparently avoids memory leaks on k8s/k3s/pods
        gc.collect()
        self._newest_memory_checkpoint = new_checkpoint

    def on_checkpoint(self, checkpoint):
        """Starts tracking checkpoint metadata on checkpoint.

        Checkpoints get assigned with an `order` as they come in.
        The order is monotonically increasing.

        Sets the newest checkpoint. For PERSISTENT checkpoints: Deletes
        previous checkpoint as long as it isn't one of the best ones. Also
        deletes the worst checkpoint if at capacity.

        Args:
            checkpoint (Checkpoint): Trial state checkpoint.
        """
        self._cur_order += 1
        checkpoint.order = self._cur_order

        if checkpoint.storage == Checkpoint.MEMORY:
            self.replace_newest_memory_checkpoint(checkpoint)
            return

        old_checkpoint = self.newest_persistent_checkpoint

        if old_checkpoint.value == checkpoint.value:
            # Overwrite the order of the checkpoint.
            old_checkpoint.order = checkpoint.order
            return

        self.newest_persistent_checkpoint = checkpoint

        # Remove the old checkpoint if it isn't one of the best ones.
        if old_checkpoint.value and old_checkpoint not in self._membership:
            self.delete(old_checkpoint)

        try:
            queue_item = QueueItem(self._priority(checkpoint), checkpoint)
        except KeyError:
            logger.error("Result dict has no key: {}. "
                         "checkpoint_score_attr must be set to a key in the "
                         "result dict.".format(self._checkpoint_score_attr))
            return

        if len(self._best_checkpoints) < self.keep_checkpoints_num:
            heapq.heappush(self._best_checkpoints, queue_item)
            self._membership.add(checkpoint)
        elif queue_item.priority >= self._best_checkpoints[0].priority:
            worst = heapq.heappushpop(self._best_checkpoints, queue_item).value
            self._membership.add(checkpoint)
            if worst in self._membership:
                self._membership.remove(worst)
            # Don't delete the newest checkpoint. It will be deleted on the
            # next on_checkpoint() call since it isn't in self._membership.
            if worst.value != checkpoint.value:
                self.delete(worst)

    def best_checkpoints(self):
        """Returns best PERSISTENT checkpoints, sorted by score."""
        checkpoints = sorted(self._best_checkpoints, key=lambda c: c.priority)
        return [queue_item.value for queue_item in checkpoints]

    def _priority(self, checkpoint):
        result = flatten_dict(checkpoint.result)
        priority = result[self._checkpoint_score_attr]
        return -priority if self._checkpoint_score_desc else priority

    def __getstate__(self):
        state = self.__dict__.copy()
        # Avoid serializing the memory checkpoint.
        state["_newest_memory_checkpoint"] = Checkpoint(
            Checkpoint.MEMORY, None)
        # Avoid serializing lambda since it may capture cyclical dependencies.
        state.pop("delete")
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.delete = None

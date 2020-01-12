# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import heapq
import logging

logger = logging.getLogger(__name__)


class Checkpoint:
    """Describes a checkpoint of trial state.

    Checkpoint may be saved in different storage.

    Attributes:
        storage (str): Storage type.
        value (str): If storage==MEMORY, it is a Python object.
            If storage==PERSISTENT, it is a path to persistent storage.
    """

    MEMORY = "memory"
    PERSISTENT = "persistent"

    def __init__(self, storage, value, result=None):
        self.storage = storage
        self.value = value
        self.result = result or {}

    @staticmethod
    def from_object(value=None):
        """Creates a checkpoint from a Python object."""
        return Checkpoint(Checkpoint.MEMORY, value)


class QueueItem:
    def __init__(self, priority, value):
        self.priority = priority
        self.value = value

    def __lt__(self, other):
        return self.priority < other.priority


class CheckpointManager:
    """Manages checkpoints on the driver for a trial."""

    def __init__(self, keep_checkpoints_num, checkpoint_score_attr, delete_fn):
        """Initializes a new CheckpointManager.

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
        self.newest_checkpoint = Checkpoint(Checkpoint.MEMORY, None)
        self._best_checkpoints = []
        self._membership = set()

    def on_checkpoint(self, checkpoint):
        """Starts tracking checkpoint metadata on checkpoint.

        Sets newest checkpoint. Deletes previous checkpoint as long as it isn't
        one of the best ones. Also deletes the worst checkpoint if at capacity.

        Args:
            checkpoint (Checkpoint): Trial state checkpoint.
        """
        old_checkpoint = self.newest_checkpoint
        self.newest_checkpoint = checkpoint

        try:
            queue_item = QueueItem(self._priority(checkpoint), checkpoint)
        except KeyError:
            if old_checkpoint not in self._membership:
                self.delete(old_checkpoint)
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
            self.delete(worst)

        # Remove the old checkpoint if it isn't one of the best ones.
        if old_checkpoint.value and old_checkpoint not in self._membership:
            self.delete(old_checkpoint)

    def best_checkpoints(self):
        """Returns best checkpoints, sorted by score."""
        checkpoints = sorted(self._best_checkpoints, key=lambda c: c.priority)
        return [queue_item.value for queue_item in checkpoints]

    def _priority(self, checkpoint):
        priority = checkpoint.result[self._checkpoint_score_attr]
        return -priority if self._checkpoint_score_desc else priority

    def __getstate__(self):
        state = self.__dict__.copy()
        # Avoid serializing lambda since it may capture cyclical dependencies.
        state.pop("delete")
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.delete = None

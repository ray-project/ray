# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import heapq
import logging
import os
import shutil

try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError


class Checkpoint(object):
    """Describes a checkpoint of trial state.

    Checkpoint may be saved in different storage.

    Attributes:
        storage (str): Storage type.
        value (str): If storage==MEMORY, value is a Python object.
            If storage==DISK, value is a path points to the checkpoint in disk.
    """

    MEMORY = "memory"
    DISK = "disk"

    def __init__(self, storage, value, result=None):
        self.storage = storage
        self.value = value
        self.result = result or {}

    def delete(self):
        """Deletes checkpoint data if disk checkpoint."""
        if self.storage == Checkpoint.DISK and self.value:
            checkpoint_dir = self.value
            if not os.path.exists(checkpoint_dir):
                raise FileNotFoundError(
                    "Attempted to delete checkpoint at {} but "
                    "path was not found.".format(checkpoint_dir))
            elif os.path.isfile(checkpoint_dir):
                shutil.rmtree(os.path.dirname(checkpoint_dir))
            else:
                shutil.rmtree(checkpoint_dir)

    @staticmethod
    def from_object(value=None):
        """Creates a checkpoint from a Python object."""
        return Checkpoint(Checkpoint.MEMORY, value)


class QueueItem(object):
    def __init__(self, priority, value):
        self.priority = priority
        self.value = value

    def __cmp__(self, other):
        # For python2.7 compatibility.
        if self.priority == other.priority:
            return 0
        return -1 if self.priority < other.priority else 1

    def __lt__(self, other):
        return self.priority < other.priority


class CheckpointManager(object):
    """Manages checkpoints on the driver for a trial."""

    def __init__(self, keep_checkpoints_num, checkpoint_policy):
        """Initializes a new CheckpointManager.

        Args:
            keep_checkpoints_num (int): Keep at least this many checkpoints.
            checkpoint_policy (CheckpointPolicy): Checkpoint policy.
        """
        self._keep_checkpoints_num = keep_checkpoints_num or float("inf")
        self.policy = checkpoint_policy
        self._membership = set()
        self._best_checkpoints = []
        self.newest_checkpoint = Checkpoint(Checkpoint.MEMORY, None)

        assert self._keep_checkpoints_num > 0, (
            "keep_checkpoints_num must be greater than 0.")

    def on_checkpoint(self, checkpoint):
        """Starts tracking checkpoint metadata on checkpoint.

        Sets newest checkpoint. Deletes previous checkpoint as long as it isn't
        one of the best ones. Also deletes the worst checkpoint if at capacity.

        Args:
            checkpoint (Checkpoint): Trial state checkpoint.
        """
        old_checkpoint = self.newest_checkpoint
        self.newest_checkpoint = checkpoint

        score = self.policy.checkpoint_score(checkpoint.result)
        if score is None:
            if old_checkpoint not in self._membership:
                old_checkpoint.delete()
            return

        queue_item = QueueItem(score, checkpoint)
        if len(self._best_checkpoints) < self._keep_checkpoints_num:
            heapq.heappush(self._best_checkpoints, queue_item)
            self._membership.add(checkpoint)
        elif queue_item.priority >= self._best_checkpoints[0].priority:
            worst = heapq.heappushpop(self._best_checkpoints, queue_item).value
            self._membership.add(checkpoint)
            if worst in self._membership:
                self._membership.remove(worst)
            worst.delete()

        # Remove the old checkpoint if it isn't one of the best ones.
        if old_checkpoint not in self._membership:
            old_checkpoint.delete()

    def best_checkpoints(self):
        """Returns best checkpoints, sorted by score."""
        checkpoints = sorted(self._best_checkpoints, key=lambda c: c.priority)
        return [queue_item.value for queue_item in checkpoints]

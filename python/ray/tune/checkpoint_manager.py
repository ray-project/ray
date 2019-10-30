# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import heapq
import os
import shutil

from ray.tune.error import TuneError


class Checkpoint(object):
    """Describes a checkpoint of trial state.

    Checkpoint may be saved in different storage.

    Attributes:
        storage (str): Storage type.
        value (str): If storage==MEMORY,value is a Python object.
            If storage==DISK,value is a path points to the checkpoint in disk.
    """

    MEMORY = "memory"
    DISK = "disk"

    def __init__(self, storage, value, last_result=None):
        self.storage = storage
        self.value = value
        self.last_result = last_result or {}

    @staticmethod
    def from_object(value=None):
        """Creates a checkpoint from a Python object."""
        return Checkpoint(Checkpoint.MEMORY, value)


class CheckpointManager(object):
    """Manages checkpoints on the driver for a trial."""

    def __init__(self, keep_checkpoints_num, checkpoint_score_attr):
        """Initializes a new TrialCheckpointManager.

        Args:
            keep_checkpoints_num (int): Keep at least this many checkpoints.
            checkpoint_score_attr (str): Attribute to use to determine which
                checkpoints to keep.
        """
        self.keep_checkpoints_num = keep_checkpoints_num
        self.best_checkpoints = []
        self.best_checkpoints_set = set()
        self.newest_checkpoint = None

        self.checkpoint_score_desc = checkpoint_score_attr.startswith("min-")
        if self.checkpoint_score_desc:
            self.checkpoint_score_attr = checkpoint_score_attr[4:]
            self.best_checkpoint_score = float("inf")
        else:
            self.checkpoint_score_attr = checkpoint_score_attr
            self.best_checkpoint_score = float("-inf")

    def add_checkpoint(self, checkpoint):
        """Adds checkpoint metadata.

        Deletes worst checkpoint when at capacity.

        Args:
            checkpoint (Checkpoint): Trial state checkpoint.
        """
        try:
            priority = checkpoint.last_result[self.checkpoint_score_attr]
        except KeyError:
            raise TuneError(
                "Result dict has no key: {}. keep_checkpoints_num flag will "
                "not work. checkpoint_score_attr must be set to a key in the "
                "result dict.".format(self.checkpoint_score_attr))

        old_checkpoint = self.newest_checkpoint
        self.newest_checkpoint = checkpoint

        priority = -priority if self.checkpoint_score_desc else priority
        queue_item = (priority, checkpoint)

        if len(self.best_checkpoints) < self.keep_checkpoints_num:
            heapq.heappush(self.best_checkpoints, queue_item)
            self.best_checkpoints_set.add(checkpoint)
        elif priority < self.best_checkpoints[0][0]:
            pass
        else:
            _, worst = heapq.heappushpop(self.best_checkpoints, queue_item)
            self.best_checkpoints_set.add(checkpoint)
            if worst in self.best_checkpoints_set:
                self.best_checkpoints_set.remove(worst)

            if worst.storage == Checkpoint.DISK:
                CheckpointManager.delete_checkpoint(worst.value)

        # Remove the old checkpoint if it isn't one of the best ones.
        if old_checkpoint not in self.best_checkpoints_set:
            self.delete_checkpoint(old_checkpoint.value)

    @classmethod
    def delete_checkpoint(cls, checkpoint_dir):
        """Removes subdirectory within the checkpoint folder.

        Args:
            checkpoint_dir (str): path to checkpoint
        """
        if not os.path.exists(checkpoint_dir):
            raise FileNotFoundError(
                "Attempted to delete checkpoint at {} but "
                "path was not found.".format(checkpoint_dir))
        elif os.path.isfile(checkpoint_dir):
            shutil.rmtree(os.path.dirname(checkpoint_dir))
        else:
            shutil.rmtree(checkpoint_dir)


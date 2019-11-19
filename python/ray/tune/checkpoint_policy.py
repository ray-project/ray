# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

from ray.tune.result import DONE, TRAINING_ITERATION

logger = logging.getLogger(__name__)


class CheckpointPolicy(object):
    """Interface for Trainable checkpoint policy."""

    def should_checkpoint(self, result):
        """Determines whether to trigger checkpoint based on the result.

        Args:
            result (Optional[dict]): result from last training step. None for
                first call.

        Returns:
            True if should take checkpoint, False otherwise.
        """
        raise NotImplementedError

    def checkpoint_score(self, result):
        """Determines which checkpoints to garbage collect first, if any.

        Called at time-of-checkpoint.

        Returns:
            Checkpoint score, None if it can't be scored.
        """
        raise NotImplementedError


class BasicCheckpointPolicy(CheckpointPolicy):
    def __init__(self,
                 frequency=None,
                 checkpoint_at_beginning=False,
                 checkpoint_at_end=False,
                 scoring_attribute=None,
                 score_desc=False):
        """Initializes the policy.

        Args:
            frequency (Optional[int]):
            checkpoint_at_beginning (bool):
            checkpoint_at_end (bool):
            scoring_attribute (Optional[str]):
            score_desc (bool):
        """
        self._frequency = frequency or float("inf")
        self._checkpoint_at_beginning = checkpoint_at_beginning
        self._checkpoint_at_end = checkpoint_at_end
        self._scoring_attribute = scoring_attribute or TRAINING_ITERATION
        self._score_desc = score_desc

    def should_checkpoint(self, result):
        if not result:
            return self._checkpoint_at_beginning
        if result.get(DONE) and self._checkpoint_at_end:
            return True
        return result.get(TRAINING_ITERATION, 0) % self._frequency == 0

    def checkpoint_score(self, result):
        if result is None:
            return float("inf") if self._score_desc else float("-inf")
        try:
            score = result[self._scoring_attribute]
            return -score if self._score_desc else score
        except KeyError:
            logger.error(
                "Result dict has no key: %s. scoring_attribute must be set "
                "to a key in the result dict.", self._scoring_attribute)
        return None


noop_policy = BasicCheckpointPolicy()

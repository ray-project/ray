import random

import gym
import numpy as np
from gym.spaces import Box

from ray.rllib.models.modelv2 import restore_original_dimensions
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TrainerConfigDict, ModelWeights


class GreedyPolicy(Policy):
    """Hand-coded policy that maximizes user clicks"""

    def __init__(self, observation_space: gym.spaces.Space,
                 action_space: gym.spaces.Space, config: TrainerConfigDict):
        super().__init__(observation_space, action_space, config)
        self.slate_size = len(action_space.nvec)

    @override(Policy)
    def compute_actions(self,
                        obs_batch,
                        state_batches=None,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        **kwargs):
        """Select the top documents that maximize user click probabilities"""
        # unflatten the observation
        obs = restore_original_dimensions(
            np.array(obs_batch), self.observation_space, tensorlib=np)

        # user.shape: [batch_size(=1), embedding_size]
        user = obs["user"]
        # doc.shape: [batch_size(=1), num_docs, embedding_size]
        doc = np.stack([item for item in obs["doc"].values()], axis=1)
        # "scores" represent how likely a doc will be clicked
        # scores.shape: [batch_size(=1), num_docs]
        scores = np.einsum("be,bde->bd", user, doc)

        # find the top n documents with highest scores
        sorted_idxes = np.argsort(scores, axis=1)
        actions = sorted_idxes[:, -self.slate_size:]
        return actions, [], {}

    @override(Policy)
    def get_weights(self) -> ModelWeights:
        """No weights to save."""
        return {}

    @override(Policy)
    def set_weights(self, weights: ModelWeights) -> None:
        """No weights to set."""
        pass

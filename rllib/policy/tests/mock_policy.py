import numpy as np
import random

from ray.rllib.evaluation.postprocessing import compute_advantages
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override


class MockPolicy(Policy):
    """A dummy Policy that returns a random (batched) int for compute_actions.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exploration = self._create_exploration()

    @override(Policy)
    def compute_actions(self,
                        obs_batch,
                        state_batches=None,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        episodes=None,
                        explore=None,
                        timestep=None,
                        **kwargs):
        return np.array([random.choice([0, 1])] * len(obs_batch)), [], {}

    @override(Policy)
    def compute_log_likelihoods(self,
                                actions,
                                obs_batch,
                                state_batches=None,
                                prev_action_batch=None,
                                prev_reward_batch=None):
        return np.array([random.random()] * len(obs_batch))

    @override(Policy)
    def postprocess_trajectory(self,
                               batch,
                               other_agent_batches=None,
                               episode=None):
        assert episode is not None
        return compute_advantages(
            batch, 100.0, 0.9, use_gae=False, use_critic=False)


class BadPolicy(MockPolicy):
    """A Policy that does not implement the mandatory `compute_actions` method.
    """

    @override(MockPolicy)
    def compute_actions(self,
                        obs_batch,
                        state_batches=None,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        episodes=None,
                        explore=None,
                        timestep=None,
                        **kwargs):
        raise Exception("Intentional error!")

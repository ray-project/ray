from abc import ABC
import gymnasium as gym
import numpy as np

import ray
from ray.rllib import Policy
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.examples.env.parametric_actions_cartpole import ParametricActionsCartPole
from ray.rllib.models.modelv2 import restore_original_dimensions
from ray.rllib.utils import override
from ray.rllib.utils.typing import ResultDict


class RandomParametricPolicy(Policy, ABC):
    """
    Just pick a random legal action
    The outputted state of the environment needs to be a dictionary with an
    'action_mask' key containing the legal actions for the agent.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exploration = self._create_exploration()

    @override(Policy)
    def compute_actions(
        self,
        obs_batch,
        state_batches=None,
        prev_action_batch=None,
        prev_reward_batch=None,
        info_batch=None,
        episodes=None,
        **kwargs
    ):

        obs_batch = restore_original_dimensions(
            np.array(obs_batch, dtype=np.float32), self.observation_space, tensorlib=np
        )

        def pick_legal_action(legal_action):
            return np.random.choice(
                len(legal_action), 1, p=(legal_action / legal_action.sum())
            )[0]

        return [pick_legal_action(x) for x in obs_batch["action_mask"]], [], {}

    def learn_on_batch(self, samples):
        pass

    def get_weights(self):
        pass

    def set_weights(self, weights):
        pass


class RandomParametricAlgorithm(Algorithm):
    """Algo with Policy and config defined above and overriding `training_step`.

    Overrides the `training_step` method, which only runs a (dummy)
    rollout and performs no learning.
    """

    @classmethod
    def get_default_policy_class(cls, config):
        return RandomParametricPolicy

    @override(Algorithm)
    def training_step(self) -> ResultDict:
        # Perform rollouts (only for collecting metrics later).
        synchronous_parallel_sample(worker_set=self.workers)

        # Return (empty) training metrics.
        return {}


def main():
    gym.register("pa_cartpole", lambda: ParametricActionsCartPole(10))
    algo = RandomParametricAlgorithm(env="pa_cartpole")
    result = algo.train()
    assert result["episode_reward_mean"] > 10, result
    print("Test: OK")


if __name__ == "__main__":
    ray.init()
    main()

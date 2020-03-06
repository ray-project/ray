"""
Example of a custom gym environment and model. Run this for a demo.

This example shows:
  - using a custom environment
  - using a custom model
  - using Tune for grid search

You can visualize experiment results in ~/ray_results using TensorBoard.
"""

import gym
from gym.spaces import Tuple, Discrete
import numpy as np

from ray.rllib.agents.ppo import PPOTrainer
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class RandomEnv(gym.Env):
    """
    A randomly acting environment that can be instantiated with arbitrary
    action and observation spaces.
    """

    def __init__(self, config):
        # Action space.
        self.action_space = config["action_space"]
        # Observation space from which to sample.
        self.observation_space = config["observation_space"]
        # Reward space from which to sample.
        self.reward_space = config.get(
            "reward_space",
            gym.spaces.Box(low=-1.0, high=1.0, shape=(), dtype=np.float32))
        # Chance that an episode ends at any step.
        self.p_done = config.get("p_done", 0.1)

    def reset(self):
        return self.observation_space.sample()

    def step(self, action):
        return self.observation_space.sample(), \
            float(self.reward_space.sample()), \
            bool(np.random.choice(
                [True, False], p=[self.p_done, 1.0 - self.p_done]
            )), {}


if __name__ == "__main__":
    trainer = PPOTrainer(
        config={
            "model": {
                "use_lstm": True,
            },
            "vf_share_layers": False,
            "num_workers": 0,  # no parallelism
            "env_config": {
                "action_space": Discrete(2),
                # Test a simple Tuple observation space.
                "observation_space": Tuple([Discrete(3),
                                            Discrete(2)])
            }
        },
        env=RandomEnv,
    )
    results = trainer.train()
    print(results)

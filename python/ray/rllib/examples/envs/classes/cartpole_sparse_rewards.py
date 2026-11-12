from copy import deepcopy

import gymnasium as gym
import numpy as np
from gymnasium.spaces import Box, Dict, Discrete


class CartPoleSparseRewards(gym.Env):
    """Wrapper for gym CartPole environment where reward is accumulated to the end."""

    def __init__(self, config=None):
        self.env = gym.make("CartPole-v1")
        self.action_space = Discrete(2)
        self.observation_space = Dict(
            {
                "obs": self.env.observation_space,
                "action_mask": Box(
                    low=0, high=1, shape=(self.action_space.n,), dtype=np.int8
                ),
            }
        )
        self.running_reward = 0

    def reset(self, *, seed=None, options=None):
        self.running_reward = 0
        obs, infos = self.env.reset()
        return {
            "obs": obs,
            "action_mask": np.array([1, 1], dtype=np.int8),
        }, infos

    def step(self, action):
        obs, rew, terminated, truncated, info = self.env.step(action)
        self.running_reward += rew
        score = self.running_reward if terminated else 0
        return (
            {"obs": obs, "action_mask": np.array([1, 1], dtype=np.int8)},
            score,
            terminated,
            truncated,
            info,
        )

    def set_state(self, state):
        self.running_reward = state[1]
        self.env = deepcopy(state[0])
        obs = np.array(list(self.env.unwrapped.state))
        return {"obs": obs, "action_mask": np.array([1, 1], dtype=np.int8)}

    def get_state(self):
        return deepcopy(self.env), self.running_reward

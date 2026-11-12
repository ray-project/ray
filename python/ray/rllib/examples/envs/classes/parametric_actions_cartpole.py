import random

import gymnasium as gym
import numpy as np
from gymnasium.spaces import Box, Dict, Discrete


class ParametricActionsCartPole(gym.Env):
    """Parametric action version of CartPole.

    In this env there are only ever two valid actions, but we pretend there are
    actually up to `max_avail_actions` actions that can be taken, and the two
    valid actions are randomly hidden among this set.

    At each step, we emit a dict of:
        - the actual cart observation
        - a mask of valid actions (e.g., [0, 0, 1, 0, 0, 1] for 6 max avail)
        - the list of action embeddings (w/ zeroes for invalid actions) (e.g.,
            [[0, 0],
             [0, 0],
             [-0.2322, -0.2569],
             [0, 0],
             [0, 0],
             [0.7878, 1.2297]] for max_avail_actions=6)

    In a real environment, the actions embeddings would be larger than two
    units of course, and also there would be a variable number of valid actions
    per step instead of always [LEFT, RIGHT].
    """

    def __init__(self, max_avail_actions):
        # Use simple random 2-unit action embeddings for [LEFT, RIGHT]
        self.left_action_embed = np.random.randn(2)
        self.right_action_embed = np.random.randn(2)
        self.action_space = Discrete(max_avail_actions)
        self.wrapped = gym.make("CartPole-v1")
        self.observation_space = Dict(
            {
                "action_mask": Box(0, 1, shape=(max_avail_actions,), dtype=np.int8),
                "avail_actions": Box(-10, 10, shape=(max_avail_actions, 2)),
                "cart": self.wrapped.observation_space,
            }
        )

    def update_avail_actions(self):
        self.action_assignments = np.array(
            [[0.0, 0.0]] * self.action_space.n, dtype=np.float32
        )
        self.action_mask = np.array([0.0] * self.action_space.n, dtype=np.int8)
        self.left_idx, self.right_idx = random.sample(range(self.action_space.n), 2)
        self.action_assignments[self.left_idx] = self.left_action_embed
        self.action_assignments[self.right_idx] = self.right_action_embed
        self.action_mask[self.left_idx] = 1
        self.action_mask[self.right_idx] = 1

    def reset(self, *, seed=None, options=None):
        self.update_avail_actions()
        obs, infos = self.wrapped.reset()
        return {
            "action_mask": self.action_mask,
            "avail_actions": self.action_assignments,
            "cart": obs,
        }, infos

    def step(self, action):
        if action == self.left_idx:
            actual_action = 0
        elif action == self.right_idx:
            actual_action = 1
        else:
            raise ValueError(
                "Chosen action was not one of the non-zero action embeddings",
                action,
                self.action_assignments,
                self.action_mask,
                self.left_idx,
                self.right_idx,
            )
        orig_obs, rew, done, truncated, info = self.wrapped.step(actual_action)
        self.update_avail_actions()
        self.action_mask = self.action_mask.astype(np.int8)
        obs = {
            "action_mask": self.action_mask,
            "avail_actions": self.action_assignments,
            "cart": orig_obs,
        }
        return obs, rew, done, truncated, info


class ParametricActionsCartPoleNoEmbeddings(gym.Env):
    """Same as the above ParametricActionsCartPole.

    However, action embeddings are not published inside observations,
    but will be learnt by the model.

    At each step, we emit a dict of:
        - the actual cart observation
        - a mask of valid actions (e.g., [0, 0, 1, 0, 0, 1] for 6 max avail)
        - action embeddings (w/ "dummy embedding" for invalid actions) are
          outsourced in the model and will be learned.
    """

    def __init__(self, max_avail_actions):
        # Randomly set which two actions are valid and available.
        self.left_idx, self.right_idx = random.sample(range(max_avail_actions), 2)
        self.valid_avail_actions_mask = np.array(
            [0.0] * max_avail_actions, dtype=np.int8
        )
        self.valid_avail_actions_mask[self.left_idx] = 1
        self.valid_avail_actions_mask[self.right_idx] = 1
        self.action_space = Discrete(max_avail_actions)
        self.wrapped = gym.make("CartPole-v1")
        self.observation_space = Dict(
            {
                "valid_avail_actions_mask": Box(0, 1, shape=(max_avail_actions,)),
                "cart": self.wrapped.observation_space,
            }
        )

    def reset(self, *, seed=None, options=None):
        obs, infos = self.wrapped.reset()
        return {
            "valid_avail_actions_mask": self.valid_avail_actions_mask,
            "cart": obs,
        }, infos

    def step(self, action):
        if action == self.left_idx:
            actual_action = 0
        elif action == self.right_idx:
            actual_action = 1
        else:
            raise ValueError(
                "Chosen action was not one of the non-zero action embeddings",
                action,
                self.valid_avail_actions_mask,
                self.left_idx,
                self.right_idx,
            )
        orig_obs, rew, done, truncated, info = self.wrapped.step(actual_action)
        obs = {
            "valid_avail_actions_mask": self.valid_avail_actions_mask,
            "cart": orig_obs,
        }
        return obs, rew, done, truncated, info

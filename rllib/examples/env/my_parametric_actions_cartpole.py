import gym
from gym.spaces import Box, Dict, Discrete
import numpy as np
import random


class MyParametricActionsCartPole(gym.Env):
    """My parametric action version of CartPole.

    In this env there are only ever two valid actions, but we pretend there are
    actually up to `max_avail_actions` actions that can be taken, and the two
    valid actions are randomly hidden among this set.

    At each step, we emit a dict of:
        - the actual cart observation
        - a mask of valid actions (e.g., [0, 0, 1, 0, 0, 1] for 6 max avail)
        - action embeddings (w/ "dummy embedding" for invalid actions) are 
          outsourced in the model and will be learned

    In a real environment, there would be a variable number of valid actions
    per step instead of always [LEFT, RIGHT].
    """

    def __init__(self, max_avail_actions):
        # Randomly set which two actions are valid and available
        self.left_idx, self.right_idx = random.sample(
            range(max_avail_actions), 2)
        self.valid_avail_actions_mask = np.array([0.] * max_avail_actions)
        self.valid_avail_actions_mask[self.left_idx] = 1
        self.valid_avail_actions_mask[self.right_idx] = 1
        self.action_space = Discrete(max_avail_actions)
        self.wrapped = gym.make("CartPole-v0")
        self.observation_space = Dict({
            "valid_avail_actions_mask": Box(0, 1, shape=(max_avail_actions, )),
            "cart": self.wrapped.observation_space,
        })

    def update_avail_actions(self):
        pass

    def reset(self):
        self.update_avail_actions()
        self.wrapped.render()
        return {
            "valid_avail_actions_mask": self.valid_avail_actions_mask,
            "cart": self.wrapped.reset(),
        }

    def step(self, action):
        if action == self.left_idx:
            actual_action = 0
        elif action == self.right_idx:
            actual_action = 1
        else:
            raise ValueError(
                "Chosen action was not one of the non-zero action embeddings",
                action, self.action_assignments, self.action_mask,
                self.left_idx, self.right_idx)
        orig_obs, rew, done, info = self.wrapped.step(actual_action)
        self.wrapped.render()
        self.update_avail_actions()
        obs = {
            "valid_avail_actions_mask": self.valid_avail_actions_mask,
            "cart": orig_obs,
        }
        return obs, rew, done, info

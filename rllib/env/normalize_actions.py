import gym
from gym import spaces
import numpy as np


class NormalizeActionWrapper(gym.ActionWrapper):
    """Rescale the action space of the environment."""

    def action(self, action):
        if not isinstance(self.env.action_space, spaces.Box):
            return action

        # rescale the action
        low, high = self.env.action_space.low, self.env.action_space.high
        scaled_action = low + (action + 1.0) * (high - low) / 2.0
        scaled_action = np.clip(scaled_action, low, high)

        return scaled_action

    def reverse_action(self, action):
        raise NotImplementedError

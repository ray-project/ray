import gym
from gym.spaces import Box, Dict, Discrete, Tuple
import numpy as np

from ray.rllib.utils import try_import_tree
from ray.rllib.utils.spaces.space_utils import flatten_space

tree = try_import_tree()


class NestedSpaceRepeatAfterMeEnv(gym.Env):
    """Env for which policy has to repeat the (possibly complex) observation.

    The action space and observation spaces are always the same and may be
    arbitrarily nested Dict/Tuple Spaces.
    Rewards are given for exactly matching Discrete sub-actions and for being
    as close as possible for Box sub-actions.
    """

    def __init__(self, config):
        self.observation_space = config.get(
            "space", Tuple([Discrete(2),
                            Dict({
                                "a": Box(-1.0, 1.0, (2, ))
                            })]))
        self.action_space = self.observation_space
        self.flattened_action_space = flatten_space(self.action_space)
        self.episode_len = config.get("episode_len", 100)

    def reset(self):
        self.steps = 0
        return self._next_obs()

    def step(self, action):
        self.steps += 1
        action = tree.flatten(action)
        reward = 0.0
        for a, o, space in zip(action, self.current_obs_flattened,
                               self.flattened_action_space):
            # Box: -abs(diff).
            if isinstance(space, gym.spaces.Box):
                reward -= np.abs(np.sum(a - o))
            # Discrete: +1.0 if exact match.
            if isinstance(space, gym.spaces.Discrete):
                reward += 1.0 if a == o else 0.0
        done = self.steps >= self.episode_len
        return self._next_obs(), reward, done, {}

    def _next_obs(self):
        self.current_obs = self.observation_space.sample()
        self.current_obs_flattened = tree.flatten(self.current_obs)
        return self.current_obs

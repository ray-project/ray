from gymnasium.spaces import Box, Dict, Discrete
import numpy as np

from ray.rllib.examples.envs.classes.random_env import RandomEnv


class ActionMaskEnv(RandomEnv):
    """A randomly acting environment that publishes an action-mask each step."""

    def __init__(self, config):
        super().__init__(config)
        # Masking only works for Discrete actions.
        assert isinstance(self.action_space, Discrete)
        # Add action_mask to observations.
        self.observation_space = Dict(
            {
                "action_mask": Box(0.0, 1.0, shape=(self.action_space.n,)),
                "observations": self.observation_space,
            }
        )
        self.valid_actions = None

    def reset(self, *, seed=None, options=None):
        obs, info = super().reset()
        self._fix_action_mask(obs)
        return obs, info

    def step(self, action):
        # Check whether action is valid.
        if not self.valid_actions[action]:
            raise ValueError(
                f"Invalid action ({action}) sent to env! "
                f"valid_actions={self.valid_actions}"
            )
        obs, rew, done, truncated, info = super().step(action)
        self._fix_action_mask(obs)
        return obs, rew, done, truncated, info

    def _fix_action_mask(self, obs):
        # Fix action-mask: Everything larger 0.5 is 1.0, everything else 0.0.
        self.valid_actions = np.round(obs["action_mask"])
        obs["action_mask"] = self.valid_actions

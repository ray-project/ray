import gymnasium as gym
from gymnasium import spaces

import numpy as np

try:
    from dm_env import specs
except ImportError:
    specs = None

from ray.rllib.utils.annotations import PublicAPI


def _convert_spec_to_space(spec):
    if isinstance(spec, dict):
        return spaces.Dict({k: _convert_spec_to_space(v) for k, v in spec.items()})
    if isinstance(spec, specs.DiscreteArray):
        return spaces.Discrete(spec.num_values)
    elif isinstance(spec, specs.BoundedArray):
        return spaces.Box(
            low=np.asscalar(spec.minimum),
            high=np.asscalar(spec.maximum),
            shape=spec.shape,
            dtype=spec.dtype,
        )
    elif isinstance(spec, specs.Array):
        return spaces.Box(
            low=-float("inf"), high=float("inf"), shape=spec.shape, dtype=spec.dtype
        )

    raise NotImplementedError(
        (
            "Could not convert `Array` spec of type {} to Gym space. "
            "Attempted to convert: {}"
        ).format(type(spec), spec)
    )


@PublicAPI
class DMEnv(gym.Env):
    """A `gym.Env` wrapper for the `dm_env` API."""

    metadata = {"render.modes": ["rgb_array"]}

    def __init__(self, dm_env):
        super(DMEnv, self).__init__()
        self._env = dm_env
        self._prev_obs = None

        if specs is None:
            raise RuntimeError(
                (
                    "The `specs` module from `dm_env` was not imported. Make sure "
                    "`dm_env` is installed and visible in the current python "
                    "environment."
                )
            )

    def step(self, action):
        ts = self._env.step(action)

        reward = ts.reward
        if reward is None:
            reward = 0.0

        return ts.observation, reward, ts.last(), False, {"discount": ts.discount}

    def reset(self, *, seed=None, options=None):
        ts = self._env.reset()
        return ts.observation, {}

    def render(self, mode="rgb_array"):
        if self._prev_obs is None:
            raise ValueError(
                "Environment not started. Make sure to reset before rendering."
            )

        if mode == "rgb_array":
            return self._prev_obs
        else:
            raise NotImplementedError("Render mode '{}' is not supported.".format(mode))

    @property
    def action_space(self):
        spec = self._env.action_spec()
        return _convert_spec_to_space(spec)

    @property
    def observation_space(self):
        spec = self._env.observation_spec()
        return _convert_spec_to_space(spec)

    @property
    def reward_range(self):
        spec = self._env.reward_spec()
        if isinstance(spec, specs.BoundedArray):
            return spec.minimum, spec.maximum
        return -float("inf"), float("inf")

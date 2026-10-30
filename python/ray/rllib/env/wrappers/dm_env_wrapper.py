from typing import Any, Dict, Optional, Tuple

import gymnasium as gym
import numpy as np
from gymnasium import spaces

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
            low=np.broadcast_to(spec.minimum, spec.shape),
            high=np.broadcast_to(spec.maximum, spec.shape),
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
    """A `gym.Env` wrapper for the `dm_env` API.

    Args:
        dm_env: The `dm_env.Environment` to wrap.
    """

    metadata = {"render.modes": ["rgb_array"]}

    def __init__(self, dm_env: Any):
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

    def step(self, action: Any) -> Tuple[Any, float, bool, bool, Dict[str, Any]]:
        """Steps the wrapped `dm_env` environment.

        Args:
            action: The action to pass to the wrapped env's `step()`.

        Returns:
            The time step's observation, its reward (0.0 if the reward is None),
            whether the time step is the episode's last one as `terminated`, False
            as `truncated`, and an info dict with the time step's `"discount"`.
        """
        ts = self._env.step(action)

        reward = ts.reward
        if reward is None:
            reward = 0.0

        return ts.observation, reward, ts.last(), False, {"discount": ts.discount}

    def reset(
        self, *, seed: Optional[int] = None, options: Optional[dict] = None
    ) -> Tuple[Any, Dict[str, Any]]:
        """Resets the wrapped `dm_env` environment.

        Args:
            seed: Ignored. The wrapped env's `reset()` takes no seed.
            options: Ignored. The wrapped env's `reset()` takes no options.

        Returns:
            The first time step's observation and an empty info dict.
        """
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
        """The wrapped env's action spec, converted to a gymnasium space."""
        spec = self._env.action_spec()
        return _convert_spec_to_space(spec)

    @property
    def observation_space(self):
        """The wrapped env's observation spec, converted to a gymnasium space."""
        spec = self._env.observation_spec()
        return _convert_spec_to_space(spec)

    @property
    def reward_range(self):
        """The minimum and maximum of the wrapped env's reward spec.

        If the reward spec isn't a `BoundedArray`, this is `(-inf, inf)`.
        """
        spec = self._env.reward_spec()
        if isinstance(spec, specs.BoundedArray):
            return spec.minimum, spec.maximum
        return -float("inf"), float("inf")

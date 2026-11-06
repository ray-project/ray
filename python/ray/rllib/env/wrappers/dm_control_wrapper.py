"""DeepMind Control Suite Wrapper directly sourced from:
https://github.com/denisyarats/dmc2gym

MIT License

Copyright (c) 2020 Denis Yarats

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
from typing import Any, Dict, Optional, Tuple

import gymnasium as gym

try:
    from dm_env import specs
except ImportError:
    specs = None
try:
    # Suppress MuJoCo warning (dm_control uses absl logging).
    import absl.logging

    absl.logging.set_verbosity("error")
    from dm_control import suite
except (ImportError, OSError):
    suite = None
import numpy as np

from ray.rllib.utils.annotations import DeveloperAPI, PublicAPI


def _spec_to_box(spec):
    def extract_min_max(s):
        assert s.dtype == np.float64 or s.dtype == np.float32
        dim = np.int_(np.prod(s.shape))
        if type(s) is specs.Array:
            bound = np.inf * np.ones(dim, dtype=np.float32)
            return -bound, bound
        elif type(s) is specs.BoundedArray:
            zeros = np.zeros(dim, dtype=np.float32)
            return s.minimum + zeros, s.maximum + zeros

    mins, maxs = [], []
    for s in spec:
        mn, mx = extract_min_max(s)
        mins.append(mn)
        maxs.append(mx)
    low = np.concatenate(mins, axis=0)
    high = np.concatenate(maxs, axis=0)
    assert low.shape == high.shape
    return gym.spaces.Box(low, high, dtype=np.float32)


def _flatten_obs(obs):
    obs_pieces = []
    for v in obs.values():
        flat = np.array([v]) if np.isscalar(v) else v.ravel()
        obs_pieces.append(flat)
    return np.concatenate(obs_pieces, axis=0)


@PublicAPI
class DMCEnv(gym.Env):
    """A `gym.Env` wrapper for a task from the DeepMind Control Suite.

    Actions are normalized to the range [-1.0, 1.0] and rescaled to the task's
    action bounds. Observations are either the task's flattened state or rendered
    pixels.

    Args:
        domain_name: The Control Suite domain to load, such as `"cartpole"`.
        task_name: The task within the domain to load, such as `"swingup"`.
        task_kwargs: Keyword arguments to pass to the task, through
            `dm_control.suite.load()`.
        visualize_reward: Whether to color the rendered scene by the reward,
            through `dm_control.suite.load()`.
        from_pixels: Whether observations are rendered images instead of the
            flattened task state.
        height: The height of rendered images, in pixels.
        width: The width of rendered images, in pixels.
        camera_id: The ID of the camera to render images from.
        frame_skip: The number of task steps to repeat each action for.
        environment_kwargs: Keyword arguments to pass to the environment, through
            `dm_control.suite.load()`.
        channels_first: Whether pixel observations have shape
            `(3, height, width)` instead of `(height, width, 3)`.
        preprocess: Whether to rescale pixel observations from [0, 255] to
            [-0.5, 0.5].
    """

    def __init__(
        self,
        domain_name: str,
        task_name: str,
        task_kwargs: Optional[dict] = None,
        visualize_reward: bool = False,
        from_pixels: bool = False,
        height: int = 64,
        width: int = 64,
        camera_id: int = 0,
        frame_skip: int = 2,
        environment_kwargs: Optional[dict] = None,
        channels_first: bool = True,
        preprocess: bool = True,
    ):
        self._from_pixels = from_pixels
        self._height = height
        self._width = width
        self._camera_id = camera_id
        self._frame_skip = frame_skip
        self._channels_first = channels_first
        self.preprocess = preprocess

        if specs is None:
            raise RuntimeError(
                (
                    "The `specs` module from `dm_env` was not imported. Make sure "
                    "`dm_env` is installed and visible in the current python "
                    "environment."
                )
            )
        if suite is None:
            raise RuntimeError(
                (
                    "The `suite` module from `dm_control` was not imported. Make "
                    "sure `dm_control` is installed and visible in the current "
                    "python enviornment."
                )
            )

        # create task
        self._env = suite.load(
            domain_name=domain_name,
            task_name=task_name,
            task_kwargs=task_kwargs,
            visualize_reward=visualize_reward,
            environment_kwargs=environment_kwargs,
        )

        # true and normalized action spaces
        self._true_action_space = _spec_to_box([self._env.action_spec()])
        self._norm_action_space = gym.spaces.Box(
            low=-1.0, high=1.0, shape=self._true_action_space.shape, dtype=np.float32
        )

        # create observation space
        if from_pixels:
            shape = [3, height, width] if channels_first else [height, width, 3]
            self._observation_space = gym.spaces.Box(
                low=0, high=255, shape=shape, dtype=np.uint8
            )
            if preprocess:
                self._observation_space = gym.spaces.Box(
                    low=-0.5, high=0.5, shape=shape, dtype=np.float32
                )
        else:
            self._observation_space = _spec_to_box(
                self._env.observation_spec().values()
            )

        self._state_space = _spec_to_box(self._env.observation_spec().values())

        self.current_state = None

    def __getattr__(self, name):
        return getattr(self._env, name)

    def _get_obs(self, time_step):
        if self._from_pixels:
            obs = self.render(
                height=self._height, width=self._width, camera_id=self._camera_id
            )
            if self._channels_first:
                obs = obs.transpose(2, 0, 1).copy()
            if self.preprocess:
                obs = obs / 255.0 - 0.5
        else:
            obs = _flatten_obs(time_step.observation)
        return obs.astype(np.float32)

    def _convert_action(self, action):
        action = action.astype(np.float64)
        true_delta = self._true_action_space.high - self._true_action_space.low
        norm_delta = self._norm_action_space.high - self._norm_action_space.low
        action = (action - self._norm_action_space.low) / norm_delta
        action = action * true_delta + self._true_action_space.low
        action = action.astype(np.float32)
        return action

    @property
    def observation_space(self):
        """The observation space, for pixel observations or the flattened state.

        For pixel observations, this is a uint8 `Box` from 0 to 255, or a float32
        `Box` from -0.5 to 0.5 if `preprocess` is set.
        """
        return self._observation_space

    @property
    def state_space(self):
        """The space of the task's flattened state, even with pixel observations."""
        return self._state_space

    @property
    def action_space(self):
        """The normalized action space, a float32 `Box` from -1.0 to 1.0."""
        return self._norm_action_space

    def step(
        self, action: np.ndarray
    ) -> Tuple[np.ndarray, float, bool, bool, Dict[str, Any]]:
        """Rescales the action and steps the task `frame_skip` times.

        Stops early if the task's episode ends.

        Args:
            action: The action, in the normalized action space.

        Returns:
            The observation after the last step, the sum of the rewards, False as
            `terminated`, whether the task's episode ended as `truncated`, and an
            info dict with the physics state from before the step as
            `"internal_state"` and the last time step's `"discount"`.
        """
        assert self._norm_action_space.contains(action)
        action = self._convert_action(action)
        assert self._true_action_space.contains(action)
        reward = 0.0
        extra = {"internal_state": self._env.physics.get_state().copy()}

        terminated = truncated = False
        for _ in range(self._frame_skip):
            time_step = self._env.step(action)
            reward += time_step.reward or 0.0
            terminated = False
            truncated = time_step.last()
            if terminated or truncated:
                break
        obs = self._get_obs(time_step)
        self.current_state = _flatten_obs(time_step.observation)
        extra["discount"] = time_step.discount
        return obs, reward, terminated, truncated, extra

    def reset(
        self, *, seed: Optional[int] = None, options: Optional[dict] = None
    ) -> Tuple[np.ndarray, Dict[str, Any]]:
        """Resets the task.

        Args:
            seed: Ignored. Pass a seed to the task through `task_kwargs` instead.
            options: Ignored.

        Returns:
            The first observation and an empty info dict.
        """
        time_step = self._env.reset()
        self.current_state = _flatten_obs(time_step.observation)
        obs = self._get_obs(time_step)
        return obs, {}

    def render(
        self,
        mode: str = "rgb_array",
        height: Optional[int] = None,
        width: Optional[int] = None,
        camera_id: int = 0,
    ) -> np.ndarray:
        """Renders an image of the task's current state.

        Args:
            mode: The render mode. Only `"rgb_array"` is supported.
            height: The image height in pixels. Defaults to the env's `height`.
            width: The image width in pixels. Defaults to the env's `width`.
            camera_id: The ID of the camera to render from. 0 falls back to the
                env's `camera_id`.

        Returns:
            The rendered RGB image.
        """
        assert mode == "rgb_array", "only support for rgb_array mode"
        height = height or self._height
        width = width or self._width
        camera_id = camera_id or self._camera_id
        return self._env.physics.render(height=height, width=width, camera_id=camera_id)


@DeveloperAPI
class ActionClip(gym.ActionWrapper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._low = -1.0
        self._high = 1.0
        self.action_space = gym.spaces.Box(
            self._low,
            self._high,
            self.action_space.shape,
            self.action_space.dtype,
        )

    def action(self, action):
        return np.clip(action, self._low, self._high)

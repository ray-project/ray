from collections import deque
from typing import Any, Dict, Iterator, Optional, SupportsFloat, Tuple, Type, Union

import gymnasium as gym
import numpy as np
from gymnasium import spaces
from gymnasium.core import ActType, ObsType

from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils.images import resize, rgb2gray


@PublicAPI
def is_atari(env: Union[gym.Env, str]) -> bool:
    """Returns, whether a given env object or env descriptor (str) is an Atari env.

    Args:
        env: The gym.Env object or a string descriptor of the env (for example,
            "ale_py:ALE/Pong-v5").

    Returns:
        Whether `env` is an Atari environment.
    """
    # If a gym.Env, check proper spaces as well as occurrence of the "Atari<ALE" string
    # in the class name.
    if not isinstance(env, str):
        if (
            hasattr(env.observation_space, "shape")
            and env.observation_space.shape is not None
            and len(env.observation_space.shape) <= 2
        ):
            return False
        return "AtariEnv<ALE" in str(env)
    # If string, check for "ale_py:ALE/" prefix.
    else:
        return env.startswith("ALE/") or env.startswith("ale_py:")


@PublicAPI
def get_wrapper_by_cls(env: gym.Env, cls: Type[gym.Wrapper]) -> Optional[gym.Env]:
    """Returns the gym env wrapper of the given class, or None.

    Walks the chain of `gym.Wrapper` objects from `env` inward, following each
    wrapper's `env` attribute.

    Args:
        env: The outermost env or wrapper to start the search from.
        cls: The wrapper class to look for.

    Returns:
        The first env in the chain that's an instance of `cls`, or None if no env
        in the chain is.
    """
    currentenv = env
    while True:
        if isinstance(currentenv, cls):
            return currentenv
        elif isinstance(currentenv, gym.Wrapper):
            currentenv = currentenv.env
        else:
            return None


@PublicAPI
class ClipRewardEnv(gym.RewardWrapper):
    """Clips each reward to -1.0, 0.0, or 1.0 by its sign.

    Args:
        env: The env to wrap.
    """

    def __init__(self, env: gym.Env):
        gym.RewardWrapper.__init__(self, env)

    def reward(self, reward: SupportsFloat) -> float:
        """Bin reward to {+1, 0, -1} by its sign.

        Args:
            reward: The reward from the wrapped env.

        Returns:
            The sign of `reward`, computed with `np.sign`.
        """
        return np.sign(reward)


@PublicAPI
class EpisodicLifeEnv(gym.Wrapper):
    """Make end-of-life == end-of-episode, but only reset on true game over.

    Done by DeepMind for the DQN and co. since it helps value estimation. The
    wrapped env must be an Atari env that exposes `unwrapped.ale.lives()`.

    Args:
        env: The Atari env to wrap.
    """

    def __init__(self, env: gym.Env):
        gym.Wrapper.__init__(self, env)
        self.lives = 0
        self.was_real_terminated = True

    def step(
        self, action: ActType
    ) -> Tuple[ObsType, SupportsFloat, bool, bool, Dict[str, Any]]:
        """Steps the env and reports a lost life as a terminated episode.

        If the number of lives drops and is still greater than 0, `terminated` is
        True. The wrapper records the env's own `terminated` flag, so that
        `reset()` only resets the env on a true game over.

        Args:
            action: The action to pass to the wrapped env.

        Returns:
            The observation, reward, terminated, truncated, and info from the
            wrapped env's `step()`, with `terminated` set to True on a lost life.
        """
        obs, reward, terminated, truncated, info = self.env.step(action)
        self.was_real_terminated = terminated
        # check current lives, make loss of life terminal,
        # then update lives to handle bonus lives
        lives = self.env.unwrapped.ale.lives()
        if lives < self.lives and lives > 0:
            # for Qbert sometimes we stay in lives == 0 condtion for a few fr
            # so its important to keep lives > 0, so that we only reset once
            # the environment advertises `terminated`.
            terminated = True
        self.lives = lives
        return obs, reward, terminated, truncated, info

    def reset(self, **kwargs: Any) -> Tuple[ObsType, Dict[str, Any]]:
        """Reset only when lives are exhausted.

        This way all states are still reachable even though lives are episodic,
        and the learner need not know about any of this behind-the-scenes. After a
        lost life, this takes a no-op step with action 0 instead of resetting.

        Args:
            **kwargs: Keyword arguments to pass to the wrapped env's `reset()`.

        Returns:
            The observation and info from the reset, or from the no-op step.
        """
        if self.was_real_terminated:
            obs, info = self.env.reset(**kwargs)
        else:
            # no-op step to advance from terminal/lost life state
            obs, _, _, _, info = self.env.step(0)
        self.lives = self.env.unwrapped.ale.lives()
        return obs, info


@PublicAPI
class FireResetEnv(gym.Wrapper):
    """Take action on reset.

    For environments that are fixed until firing. The wrapped env's action 1 must
    be "FIRE", and it must have at least 3 actions.

    Args:
        env: The Atari env to wrap.
    """

    def __init__(self, env: gym.Env):
        gym.Wrapper.__init__(self, env)
        assert env.unwrapped.get_action_meanings()[1] == "FIRE"
        assert len(env.unwrapped.get_action_meanings()) >= 3

    def reset(self, **kwargs: Any) -> Tuple[ObsType, Dict[str, Any]]:
        """Resets the env, then takes action 1 and then action 2.

        If either step ends the episode, resets the env again.

        Args:
            **kwargs: Keyword arguments to pass to the wrapped env's `reset()`.

        Returns:
            The observation and info from the step with action 2.
        """
        self.env.reset(**kwargs)
        obs, _, terminated, truncated, _ = self.env.step(1)
        if terminated or truncated:
            self.env.reset(**kwargs)
        obs, _, terminated, truncated, info = self.env.step(2)
        if terminated or truncated:
            self.env.reset(**kwargs)
        return obs, info

    def step(
        self, ac: ActType
    ) -> Tuple[ObsType, SupportsFloat, bool, bool, Dict[str, Any]]:
        """Steps the wrapped env with the given action.

        Args:
            ac: The action to pass to the wrapped env.

        Returns:
            The observation, reward, terminated, truncated, and info from the
            wrapped env's `step()`.
        """
        return self.env.step(ac)


@PublicAPI
class FrameStack(gym.Wrapper):
    """Stack k last frames.

    Concatenates the last `k` observations along the last axis, so an
    observation of shape `(height, width, channels)` becomes
    `(height, width, channels * k)`.

    Args:
        env: The env to wrap. Its observations must have three dimensions.
        k: The number of frames to stack.
    """

    def __init__(self, env: gym.Env, k: int):
        gym.Wrapper.__init__(self, env)
        self.k = k
        self.frames = deque([], maxlen=k)
        shp = env.observation_space.shape
        self.observation_space = spaces.Box(
            low=np.repeat(env.observation_space.low, repeats=k, axis=-1),
            high=np.repeat(env.observation_space.high, repeats=k, axis=-1),
            shape=(shp[0], shp[1], shp[2] * k),
            dtype=env.observation_space.dtype,
        )

    def reset(
        self, *, seed: Optional[int] = None, options: Optional[dict] = None
    ) -> Tuple[np.ndarray, Dict[str, Any]]:
        """Resets the env and fills the frame stack with the reset observation.

        Args:
            seed: The seed to pass to the wrapped env's `reset()`.
            options: The options to pass to the wrapped env's `reset()`.

        Returns:
            The stacked observation, made of `k` copies of the reset
            observation, and the info from the reset.
        """
        ob, infos = self.env.reset(seed=seed, options=options)
        for _ in range(self.k):
            self.frames.append(ob)
        return self._get_ob(), infos

    def step(
        self, action: ActType
    ) -> Tuple[np.ndarray, SupportsFloat, bool, bool, Dict[str, Any]]:
        """Steps the env and adds the new observation to the frame stack.

        Args:
            action: The action to pass to the wrapped env.

        Returns:
            The stacked observation, and the reward, terminated, truncated, and
            info from the wrapped env's `step()`.
        """
        ob, reward, terminated, truncated, info = self.env.step(action)
        self.frames.append(ob)
        return self._get_ob(), reward, terminated, truncated, info

    def _get_ob(self):
        assert len(self.frames) == self.k
        return np.concatenate(self.frames, axis=2)


@PublicAPI
class FrameStackTrajectoryView(gym.ObservationWrapper):
    """No stacking. Trajectory View API takes care of this.

    Drops the channel axis of single-channel observations, so an observation of
    shape `(height, width, 1)` becomes `(height, width)`.

    Args:
        env: The env to wrap. Its observations must have shape
            `(height, width, 1)`.
    """

    def __init__(self, env: gym.Env):
        gym.Wrapper.__init__(self, env)
        shp = env.observation_space.shape
        assert shp[2] == 1
        self.observation_space = spaces.Box(
            low=0, high=255, shape=(shp[0], shp[1]), dtype=env.observation_space.dtype
        )

    def observation(self, observation: np.ndarray) -> np.ndarray:
        """Removes the last axis of the observation.

        Args:
            observation: The observation from the wrapped env.

        Returns:
            The observation without its last axis.
        """
        return np.squeeze(observation, axis=-1)


@PublicAPI
class MaxAndSkipEnv(gym.Wrapper):
    """Return only every `skip`-th frame.

    Args:
        env: The env to wrap.
        skip: The number of env steps to repeat each action for.
    """

    def __init__(self, env: gym.Env, skip: int = 4):
        gym.Wrapper.__init__(self, env)
        # most recent raw observations (for max pooling across time steps)
        self._obs_buffer = np.zeros(
            (2,) + env.observation_space.shape, dtype=env.observation_space.dtype
        )
        self._skip = skip

    def step(
        self, action: ActType
    ) -> Tuple[np.ndarray, float, bool, bool, Dict[str, Any]]:
        """Repeat action, sum reward, and max over last observations.

        Repeats the action for `skip` env steps, or until the episode ends.

        Args:
            action: The action to pass to the wrapped env on each step.

        Returns:
            The element-wise maximum of the last two observations, the sum of the
            rewards, and the terminated, truncated, and info from the last step.
        """
        total_reward = 0.0
        terminated = truncated = info = None
        for i in range(self._skip):
            obs, reward, terminated, truncated, info = self.env.step(action)
            if i == self._skip - 2:
                self._obs_buffer[0] = obs
            if i == self._skip - 1:
                self._obs_buffer[1] = obs
            total_reward += reward
            if terminated or truncated:
                break
        # Note that the observation on the terminated|truncated=True frame
        # doesn't matter
        max_frame = self._obs_buffer.max(axis=0)

        return max_frame, total_reward, terminated, truncated, info

    def reset(self, **kwargs: Any) -> Tuple[ObsType, Dict[str, Any]]:
        """Resets the wrapped env.

        Args:
            **kwargs: Keyword arguments to pass to the wrapped env's `reset()`.

        Returns:
            The observation and info from the wrapped env's `reset()`.
        """
        return self.env.reset(**kwargs)


@PublicAPI
class MonitorEnv(gym.Wrapper):
    """Record episodes stats prior to EpisodicLifeEnv, etc.

    Tracks the return and length of each episode and the total number of steps.

    Args:
        env: The env to wrap.
    """

    def __init__(self, env: Optional[gym.Env] = None):
        gym.Wrapper.__init__(self, env)
        self._current_reward = None
        self._num_steps = None
        self._total_steps = None
        self._episode_rewards = []
        self._episode_lengths = []
        self._num_episodes = 0
        self._num_returned = 0

    def reset(self, **kwargs: Any) -> Tuple[ObsType, Dict[str, Any]]:
        """Resets the env and records the stats of the episode that just ended.

        Args:
            **kwargs: Keyword arguments to pass to the wrapped env's `reset()`.

        Returns:
            The observation and info from the wrapped env's `reset()`.
        """
        obs, info = self.env.reset(**kwargs)

        if self._total_steps is None:
            self._total_steps = sum(self._episode_lengths)

        if self._current_reward is not None:
            self._episode_rewards.append(self._current_reward)
            self._episode_lengths.append(self._num_steps)
            self._num_episodes += 1

        self._current_reward = 0
        self._num_steps = 0

        return obs, info

    def step(
        self, action: ActType
    ) -> Tuple[ObsType, SupportsFloat, bool, bool, Dict[str, Any]]:
        """Steps the env and adds the reward and step to the episode stats.

        Args:
            action: The action to pass to the wrapped env.

        Returns:
            The observation, reward, terminated, truncated, and info from the
            wrapped env's `step()`.
        """
        obs, rew, terminated, truncated, info = self.env.step(action)
        self._current_reward += rew
        self._num_steps += 1
        self._total_steps += 1
        return obs, rew, terminated, truncated, info

    def get_episode_rewards(self) -> list:
        """Returns the list of returns of all episodes recorded so far.

        Returns:
            The sum of rewards of each episode that a `reset()` call ended.
        """
        return self._episode_rewards

    def get_episode_lengths(self) -> list:
        """Returns the list of lengths of all episodes recorded so far.

        Returns:
            The number of steps of each episode that a `reset()` call ended.
        """
        return self._episode_lengths

    def get_total_steps(self) -> Optional[int]:
        """Returns the total number of env steps taken.

        Returns:
            The total number of env steps, or None before the first `reset()`.
        """
        return self._total_steps

    def next_episode_results(self) -> Iterator[Tuple[float, int]]:
        """Yields the results of the episodes not yet returned by this method.

        Yields:
            Tuple[float, int]: The return and length of each recorded episode not
            yet yielded.
        """
        for i in range(self._num_returned, len(self._episode_rewards)):
            yield (self._episode_rewards[i], self._episode_lengths[i])
        self._num_returned = len(self._episode_rewards)


@PublicAPI
class NoopResetEnv(gym.Wrapper):
    """Sample initial states by taking random number of no-ops on reset.

    No-op is assumed to be action 0. The wrapped env's action 0 must be "NOOP".

    Args:
        env: The Atari env to wrap.
        noop_max: The maximum number of no-op steps to take on reset.
    """

    def __init__(self, env: gym.Env, noop_max: int = 30):
        gym.Wrapper.__init__(self, env)
        self.noop_max = noop_max
        self.override_num_noops = None
        self.noop_action = 0
        assert env.unwrapped.get_action_meanings()[0] == "NOOP"

    def reset(self, **kwargs: Any) -> Tuple[ObsType, Dict[str, Any]]:
        """Do no-op action for a number of steps in [1, noop_max].

        If a no-op step ends the episode, resets the env again.

        Args:
            **kwargs: Keyword arguments to pass to the wrapped env's `reset()`.

        Returns:
            The observation and info from the last no-op step, or from the last
            reset if that came after it.
        """
        self.env.reset(**kwargs)
        if self.override_num_noops is not None:
            noops = self.override_num_noops
        else:
            # This environment now uses the pcg64 random number generator which
            # does not have `randint` as an attribute only has `integers`.
            try:
                noops = self.unwrapped.np_random.integers(1, self.noop_max + 1)
            # Also still support older versions.
            except AttributeError:
                noops = self.unwrapped.np_random.randint(1, self.noop_max + 1)
        assert noops > 0
        obs = None
        for _ in range(noops):
            obs, _, terminated, truncated, info = self.env.step(self.noop_action)
            if terminated or truncated:
                obs, info = self.env.reset(**kwargs)
        return obs, info

    def step(
        self, ac: ActType
    ) -> Tuple[ObsType, SupportsFloat, bool, bool, Dict[str, Any]]:
        """Steps the wrapped env with the given action.

        Args:
            ac: The action to pass to the wrapped env.

        Returns:
            The observation, reward, terminated, truncated, and info from the
            wrapped env's `step()`.
        """
        return self.env.step(ac)


@PublicAPI
class NormalizedImageEnv(gym.ObservationWrapper):
    """Rescales image observations from [0, 255] to about [-1.0, 1.0].

    Sets the observation space to a float32 `Box` from -1.0 to 1.0 with the
    wrapped env's observation shape.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.observation_space = gym.spaces.Box(
            -1.0,
            1.0,
            shape=self.observation_space.shape,
            dtype=np.float32,
        )

    # Divide by scale and center around 0.0, such that observations are in the range
    # of -1.0 and 1.0.
    def observation(self, observation: np.ndarray) -> np.ndarray:
        """Divides the observation by 128.0 and subtracts 1.0.

        Args:
            observation: The image observation from the wrapped env.

        Returns:
            The rescaled observation as float32.
        """
        return (observation.astype(np.float32) / 128.0) - 1.0


@PublicAPI
class GrayScaleAndResize(gym.ObservationWrapper):
    """Warp frames to the specified size (dim x dim).

    Args:
        env: The env to wrap. Its observations must be RGB images.
        dim: The height and width to resize observations to.
        grayscale: Whether to also convert observations to grayscale, with a
            single channel.
    """

    def __init__(self, env: gym.Env, dim: int, grayscale: bool = True):
        gym.ObservationWrapper.__init__(self, env)
        self.width = dim
        self.height = dim
        self.grayscale = grayscale
        self.observation_space = spaces.Box(
            low=0,
            high=255,
            shape=(self.height, self.width, 1 if grayscale else 3),
            dtype=np.uint8,
        )

    def observation(self, frame: np.ndarray) -> np.ndarray:
        """Resizes the frame, and converts it to grayscale if `grayscale` is set.

        Args:
            frame: The RGB image observation from the wrapped env.

        Returns:
            The resized frame, of shape `(dim, dim, 1)` if `grayscale` is set and
            `(dim, dim, 3)` otherwise.
        """
        if self.grayscale:
            frame = rgb2gray(frame)
            frame = resize(frame, height=self.height, width=self.width)
            return frame[:, :, None]
        else:
            return resize(frame, height=self.height, width=self.width)


WarpFrame = GrayScaleAndResize


@PublicAPI
def wrap_atari_for_new_api_stack(
    env: gym.Env,
    dim: int = 64,
    frameskip: int = 4,
    framestack: Optional[int] = None,
    grayscale: bool = True,
    # TODO (sven): Add option to NOT grayscale, in which case framestack must be None
    #  (b/c we are using the 3 color channels already as stacking frames).
) -> gym.Env:
    """Wraps `env` for new-API-stack-friendly RLlib Atari experiments.

    Note that we assume reward clipping is done outside the wrapper.

    Args:
        env: The env object to wrap.
        dim: Dimension to resize observations to (dim x dim).
        frameskip: Whether to skip n frames and max over them (keep brightest pixels).
        framestack: Whether to stack the last n (grayscaled) frames. Note that this
            step happens after(!) a possible frameskip step, meaning that if
            frameskip=4 and framestack=2, we would perform the following over this
            trajectory:
            actual env timesteps: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 -> ...
            frameskip:            ( max ) ( max ) ( max   ) ( max     )
            framestack:           ( stack       ) (stack              )
        grayscale: Whether to convert the observations to grayscale (single color
            channel) images.

    Returns:
        The wrapped gym.Env.
    """
    # Time limit.
    env = gym.wrappers.TimeLimit(env, max_episode_steps=108000)
    # Grayscale + resize.
    env = WarpFrame(env, dim=dim, grayscale=grayscale)
    # Normalize the image.
    env = NormalizedImageEnv(env)
    # Frameskip: Take max over these n frames.
    if frameskip > 1:
        assert env.spec is not None
        env = MaxAndSkipEnv(env, skip=frameskip)
    # Send n noop actions into env after reset to increase variance in the
    # "start states" of the trajectories. These dummy steps are NOT included in the
    # sampled data used for learning.
    env = NoopResetEnv(env, noop_max=30)
    # Each life is one episode.
    env = EpisodicLifeEnv(env)
    # Some envs only start playing after pressing fire. Unblock those.
    if "FIRE" in env.unwrapped.get_action_meanings():
        env = FireResetEnv(env)
    # Framestack.
    if framestack:
        env = FrameStack(env, k=framestack)
    return env


@PublicAPI
def wrap_deepmind(
    env: gym.Env,
    dim: int = 84,
    framestack: bool = True,
    noframeskip: bool = False,
) -> gym.Env:
    """Configure environment for DeepMind-style Atari.

    Note that we assume reward clipping is done outside the wrapper.

    Args:
        env: The env object to wrap.
        dim: Dimension to resize observations to (dim x dim).
        framestack: Whether to framestack observations.
        noframeskip: Whether the given env does NOT skip frames by itself, in which
            case a `MaxAndSkipEnv` wrapper (skip=4) is added on top.

    Returns:
        The wrapped gym.Env.
    """
    env = MonitorEnv(env)
    env = NoopResetEnv(env, noop_max=30)
    if env.spec is not None and noframeskip is True:
        env = MaxAndSkipEnv(env, skip=4)
    env = EpisodicLifeEnv(env)
    if "FIRE" in env.unwrapped.get_action_meanings():
        env = FireResetEnv(env)
    env = WarpFrame(env, dim)
    # env = ClipRewardEnv(env)  # reward clipping is handled by policy eval
    # 4x image framestacking.
    if framestack is True:
        env = FrameStack(env, 4)
    return env

"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
from collections import defaultdict
from functools import partial
from typing import Collection, List, Optional, Tuple, Union

import gymnasium as gym
from gymnasium.wrappers.vector import DictInfoToList
import numpy as np
import tree  # pip install dm_tree

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core import COMPONENT_RL_MODULE, DEFAULT_AGENT_ID, DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.env.wrappers.atari_wrappers import NoopResetEnv, MaxAndSkipEnv
from ray.rllib.env.wrappers.dm_control_wrapper import DMCEnv
from ray.rllib.env.utils import _gym_env_creator
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.metrics import (
    EPISODE_DURATION_SEC_MEAN,
    EPISODE_LEN_MAX,
    EPISODE_LEN_MEAN,
    EPISODE_LEN_MIN,
    EPISODE_RETURN_MAX,
    EPISODE_RETURN_MEAN,
    EPISODE_RETURN_MIN,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_SAMPLED_LIFETIME,
    NUM_EPISODES,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_MODULE_STEPS_SAMPLED,
    NUM_MODULE_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.numpy import convert_to_numpy, one_hot
from ray.rllib.utils.spaces.space_utils import batch, unbatch
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import ResultDict, StateDict
from ray.tune.registry import ENV_CREATOR, _global_registry

_, tf, _ = try_import_tf()
torch, _ = try_import_torch()


# TODO (sven): Use SingleAgentEnvRunner instead of this as soon as we have the new
#  ConnectorV2 example classes to make Atari work properly with these (w/o requiring the
#  classes at the bottom of this file here, e.g. `ActionClip`).
class DreamerV3EnvRunner(EnvRunner):
    """An environment runner to collect data from vectorized gymnasium environments."""

    def __init__(
        self,
        config: AlgorithmConfig,
        **kwargs,
    ):
        """Initializes a DreamerV3EnvRunner instance.

        Args:
            config: The config to use to setup this EnvRunner.
        """
        super().__init__(config=config)

        # Create the gym.vector.Env object.
        # Atari env.
        if self.config.env.startswith("ale_py:ALE/"):
            # TODO (sven): This import currently causes a Tune test to fail. Either way,
            #  we need to figure out how to properly setup the CI environment with
            #  the correct versions of all gymnasium-related packages.
            from supersuit.generic_wrappers import resize_v1

            # [2]: "We down-scale the 84 × 84 grayscale images to 64 × 64 pixels so that
            # we can apply the convolutional architecture of DreamerV1."
            # ...
            # "We follow the evaluation protocol of Machado et al. (2018) with 200M
            # environment steps, action repeat of 4, a time limit of 108,000 steps per
            # episode that correspond to 30 minutes of game play, no access to life
            # information, full action space, and sticky actions. Because the world
            # model integrates information over time, DreamerV2 does not use frame
            # stacking."
            # However, in Danijar's repo, Atari100k experiments are configured as:
            # noop=30, 64x64x3 (no grayscaling), sticky actions=False,
            # full action space=False,

            def _entry_point():
                return gym.make(
                    self.config.env,
                    **dict(
                        self.config.env_config,
                        **{
                            # "sticky actions" but not according to Danijar's 100k
                            # configs.
                            "repeat_action_probability": 0.0,
                            # "full action space" but not according to Danijar's 100k
                            # configs.
                            "full_action_space": False,
                            # Already done by MaxAndSkip wrapper: "action repeat" == 4.
                            "frameskip": 1,
                        },
                    ),
                )

            gym.register("rllib-single-agent-env-v0", entry_point=_entry_point)

            self.env = DictInfoToList(
                gym.make_vec(
                    "rllib-single-agent-env-v0",
                    num_envs=self.config.num_envs_per_env_runner,
                    vectorization_mode=(
                        "async" if self.config.remote_worker_envs else "sync"
                    ),
                    wrappers=[
                        partial(gym.wrappers.TimeLimit, max_episode_steps=108000),
                        partial(resize_v1, x_size=64, y_size=64),  # resize to 64x64
                        NormalizedImageEnv,
                        NoopResetEnv,
                        MaxAndSkipEnv,
                    ],
                )
            )
        # DeepMind Control.
        elif self.config.env.startswith("DMC/"):
            parts = self.config.env.split("/")
            assert len(parts) == 3, (
                "ERROR: DMC env must be formatted as 'DMC/[task]/[domain]', e.g. "
                f"'DMC/cartpole/swingup'! You provided '{self.config.env}'."
            )
            gym.register(
                "dmc_env-v0",
                lambda from_pixels=True: DMCEnv(
                    parts[1], parts[2], from_pixels=from_pixels, channels_first=False
                ),
            )
            self.env = DictInfoToList(
                gym.make_vec(
                    "dmc_env-v0",
                    wrappers=[ActionClip],
                    num_envs=self.config.num_envs_per_env_runner,
                    vectorization_mode=(
                        "async" if self.config.remote_worker_envs else "sync"
                    ),
                    **dict(self.config.env_config),
                )
            )
        # All other envs (gym or `tune.register_env()`'d by the user).
        else:
            # Register the env in this local context here.
            gym.register(
                "dreamerv3-custom-env-v0",
                partial(
                    _global_registry.get(ENV_CREATOR, self.config.env),
                    self.config.env_config,
                )
                if _global_registry.contains(ENV_CREATOR, self.config.env)
                else partial(
                    _gym_env_creator,
                    env_context=self.config.env_config,
                    env_descriptor=self.config.env,
                ),
            )
            # Wrap into `DictInfoToList` wrapper to get infos as lists.
            self.env = DictInfoToList(
                gym.make_vec(
                    "dreamerv3-custom-env-v0",
                    num_envs=self.config.num_envs_per_env_runner,
                    vectorization_mode=(
                        "async" if self.config.remote_worker_envs else "sync"
                    ),
                )
            )
        self.num_envs = self.env.num_envs
        assert self.num_envs == self.config.num_envs_per_env_runner

        # Create our RLModule to compute actions with.
        policy_dict, _ = self.config.get_multi_agent_setup(env=self.env)
        self.multi_rl_module_spec = self.config.get_multi_rl_module_spec(
            policy_dict=policy_dict
        )
        if self.config.share_module_between_env_runner_and_learner:
            # DreamerV3 Algorithm will set this to the local Learner's module.
            self.module = None
        # Create our own instance of a DreamerV3RLModule (which then needs to be
        # weight-synched each iteration).
        else:
            # TODO (sven): DreamerV3 is currently single-agent only.
            self.module = self.multi_rl_module_spec.build()[DEFAULT_MODULE_ID]

        self._cached_to_module = None

        self.metrics = MetricsLogger()

        self._device = None
        if (
            torch
            and torch.cuda.is_available()
            and self.config.framework_str == "torch"
            and self.config.share_module_between_env_runner_and_learner
            and self.config.num_gpus_per_learner > 0
        ):
            gpu_ids = ray.get_gpu_ids()
            self._device = f"cuda:{gpu_ids[0]}"
        self.convert_to_tensor = (
            partial(convert_to_torch_tensor, device=self._device)
            if self.config.framework_str == "torch"
            else tf.convert_to_tensor
        )

        self._needs_initial_reset = True
        self._episodes = [None for _ in range(self.num_envs)]
        self._states = [None for _ in range(self.num_envs)]

        # TODO (sven): Move metrics temp storage and collection out of EnvRunner
        #  and RolloutWorkers. These classes should not continue tracking some data
        #  that they have already returned (in a call to `sample()`). Instead, the
        #  episode data should be analyzed where it was sent to (the Algorithm itself
        #  via its replay buffer, etc..).
        self._done_episodes_for_metrics = []
        self._ongoing_episodes_for_metrics = defaultdict(list)

    @override(EnvRunner)
    def sample(
        self,
        *,
        num_timesteps: int = None,
        num_episodes: int = None,
        explore: bool = True,
        random_actions: bool = False,
    ) -> Tuple[List[SingleAgentEpisode], List[SingleAgentEpisode]]:
        """Runs and returns a sample (n timesteps or m episodes) on the environment(s).

        Timesteps or episodes are counted in total (across all vectorized
        sub-environments). For example, if self.num_envs=2 and num_timesteps=10, each
        sub-environment will be sampled for 5 steps. If self.num_envs=3 and
        num_episodes=30, each sub-environment will be sampled for 10 episodes.

        Args:
            num_timesteps: The number of timesteps to sample from the environment(s).
                Note that only exactly one of `num_timesteps` or `num_episodes` must be
                provided.
            num_episodes: The number of full episodes to sample from the environment(s).
                Note that only exactly one of `num_timesteps` or `num_episodes` must be
                provided.
            explore: Indicates whether to utilize exploration when picking actions.
            random_actions: Whether to only use random actions. If True, the value of
                `explore` is ignored.
            force_reset: Whether to reset the environment(s) before starting to sample.
                If False, will still reset the environment(s) if they were left in
                a terminated or truncated state during previous sample calls.

        Returns:
            A tuple consisting of a) list of Episode instances that are done and
            b) list of Episode instances that are still ongoing.
        """
        # If no execution details are provided, use self.config.
        if num_timesteps is None and num_episodes is None:
            if self.config.batch_mode == "truncate_episodes":
                num_timesteps = self.config.rollout_fragment_length * self.num_envs
            else:
                num_episodes = self.num_envs

        # Sample n timesteps.
        if num_timesteps is not None:
            return self._sample(
                num_timesteps=num_timesteps,
                explore=explore,
                random_actions=random_actions,
                force_reset=False,
            )
        # Sample n episodes.
        else:
            # `_sample_episodes` returns only one list (with completed episodes)
            # return empty list for incomplete ones.
            return (
                self._sample(
                    num_episodes=num_episodes,
                    explore=explore,
                    random_actions=random_actions,
                ),
                [],
            )

    def _sample(
        self,
        *,
        num_timesteps: Optional[int] = None,
        num_episodes: Optional[int] = None,
        explore: bool = True,
        random_actions: bool = False,
        force_reset: bool = False,
    ) -> List[SingleAgentEpisode]:
        """Helper method to sample n timesteps or m episodes."""

        done_episodes_to_return: List[SingleAgentEpisode] = []

        # Get initial states for all `batch_size_B` rows in the forward batch.
        initial_states = tree.map_structure(
            lambda s: np.repeat(s, self.num_envs, axis=0),
            convert_to_numpy(self.module.get_initial_state()),
        )

        # Have to reset the env (on all vector sub-envs).
        if force_reset or num_episodes is not None or self._needs_initial_reset:
            episodes = self._episodes = [None for _ in range(self.num_envs)]
            self._reset_envs(episodes, initial_states)
            # We just reset the env. Don't have to force this again in the next
            # call to `self._sample()`.
            self._needs_initial_reset = False

            # Set initial obs and states in the episodes.
            for i in range(self.num_envs):
                self._states[i] = None
        else:
            episodes = self._episodes

        # Loop through `num_timesteps` timesteps or `num_episodes` episodes.
        ts = 0
        eps = 0
        while (
            (ts < num_timesteps) if num_timesteps is not None else (eps < num_episodes)
        ):
            # Act randomly.
            if random_actions:
                actions = self.env.action_space.sample()
            # Compute an action using the RLModule.
            else:
                # Env-to-module connector (already cached).
                to_module = self._cached_to_module
                assert to_module is not None
                self._cached_to_module = None

                # RLModule forward pass: Explore or not.
                if explore:
                    to_env = self.module.forward_exploration(to_module)
                else:
                    to_env = self.module.forward_inference(to_module)

                # Model outputs one-hot actions (if discrete). Convert to int actions
                # as well.
                actions = convert_to_numpy(to_env[Columns.ACTIONS])
                if isinstance(self.env.single_action_space, gym.spaces.Discrete):
                    actions = np.argmax(actions, axis=-1)
                self._states = unbatch(convert_to_numpy(to_env[Columns.STATE_OUT]))

            observations, rewards, terminateds, truncateds, infos = self.env.step(
                actions
            )

            call_on_episode_start = set()
            for env_index in range(self.num_envs):
                # Episode has no data in it yet -> Was just reset and needs to be called
                # with its `add_env_reset()` method.
                if not episodes[env_index].is_reset:
                    episodes[env_index].add_env_reset(
                        observation=observations[env_index],
                        infos=infos[env_index],
                    )
                    call_on_episode_start.add(env_index)
                    self._states[env_index] = None

                # Call `add_env_step()` method on episode.
                else:
                    # Only increase ts when we actually stepped (not reset'd as a reset
                    # does not count as a timestep).
                    ts += 1
                    episodes[env_index].add_env_step(
                        observation=observations[env_index],
                        action=actions[env_index],
                        reward=rewards[env_index],
                        infos=infos[env_index],
                        terminated=terminateds[env_index],
                        truncated=truncateds[env_index],
                    )

            # Cache results as we will do the RLModule forward pass only in the next
            # `while`-iteration.
            if self.module is not None:
                is_first = np.zeros((self.num_envs,))
                for env_index, episode in enumerate(episodes):
                    if self._states[env_index] is None:
                        is_first[env_index] = 1.0
                        self._states[env_index] = {
                            k: s[env_index] for k, s in initial_states.items()
                        }
                self._cached_to_module = {
                    Columns.STATE_IN: tree.map_structure(
                        lambda s: self.convert_to_tensor(s), batch(self._states)
                    ),
                    Columns.OBS: self.convert_to_tensor(observations),
                    "is_first": self.convert_to_tensor(is_first),
                }

        for env_index in range(self.num_envs):
            # Episode is not done.
            if not episodes[env_index].is_done:
                continue

            eps += 1

            # Then numpy'ize the episode.
            done_episodes_to_return.append(episodes[env_index].to_numpy())

            # Also early-out if we reach the number of episodes within this
            # for-loop.
            if eps == num_episodes:
                break

            # Create a new episode object with no data in it and execute
            # `on_episode_created` callback (before the `env.reset()` call).
            episodes[env_index] = SingleAgentEpisode(
                observation_space=self.env.single_observation_space,
                action_space=self.env.single_action_space,
            )

        # Return done episodes ...
        # TODO (simon): Check, how much memory this attribute uses.
        self._done_episodes_for_metrics.extend(done_episodes_to_return)
        # ... and all ongoing episode chunks.

        # Also, make sure we start new episode chunks (continuing the ongoing episodes
        # from the to-be-returned chunks).
        ongoing_episodes_to_return = []
        # Only if we are doing individual timesteps: We have to maybe cut an ongoing
        # episode and continue building it on the next call to `sample()`.
        if num_timesteps is not None:
            ongoing_episodes_continuations = [
                episode.cut(len_lookback_buffer=self.config.episode_lookback_horizon)
                for episode in episodes
            ]

            for episode in episodes:
                # Just started Episodes do not have to be returned. There is no data
                # in them anyway.
                if episode.t == 0:
                    continue
                episode.validate()
                self._ongoing_episodes_for_metrics[episode.id_].append(episode)
                # Return numpy'ized Episodes.
                ongoing_episodes_to_return.append(episode.to_numpy())

            # Continue collecting into the cut Episode chunks.
            self._episodes = ongoing_episodes_continuations

        self._increase_sampled_metrics(ts)

        # Return collected episode data.
        return done_episodes_to_return + ongoing_episodes_to_return

    def get_spaces(self):
        return {
            INPUT_ENV_SPACES: (self.env.observation_space, self.env.action_space),
            DEFAULT_MODULE_ID: (
                self.env.single_observation_space,
                self.env.single_action_space,
            ),
        }

    def get_metrics(self) -> ResultDict:
        # Compute per-episode metrics (only on already completed episodes).
        for eps in self._done_episodes_for_metrics:
            assert eps.is_done

            episode_length = len(eps)
            episode_return = eps.get_return()
            episode_duration_s = eps.get_duration_s()

            # Don't forget about the already returned chunks of this episode.
            if eps.id_ in self._ongoing_episodes_for_metrics:
                for eps2 in self._ongoing_episodes_for_metrics[eps.id_]:
                    episode_length += len(eps2)
                    episode_return += eps2.get_return()
                del self._ongoing_episodes_for_metrics[eps.id_]

            self._log_episode_metrics(
                episode_length, episode_return, episode_duration_s
            )

        # Log num episodes counter for this iteration.
        self.metrics.log_value(
            NUM_EPISODES,
            len(self._done_episodes_for_metrics),
            reduce="sum",
            # Reset internal data on `reduce()` call below (not a lifetime count).
            clear_on_reduce=True,
        )

        # Now that we have logged everything, clear cache of done episodes.
        self._done_episodes_for_metrics.clear()

        # Return reduced metrics.
        return self.metrics.reduce()

    def get_state(
        self,
        components: Optional[Union[str, Collection[str]]] = None,
        *,
        not_components: Optional[Union[str, Collection[str]]] = None,
        **kwargs,
    ) -> StateDict:
        """Returns the weights of our (single-agent) RLModule."""
        if self.module is None:
            assert self.config.share_module_between_env_runner_and_learner
            return {}
        else:
            return {
                COMPONENT_RL_MODULE: {
                    DEFAULT_MODULE_ID: self.module.get_state(**kwargs),
                },
            }

    def set_state(self, state: StateDict) -> None:
        """Writes the weights of our (single-agent) RLModule."""
        if self.module is None:
            assert self.config.share_module_between_env_runner_and_learner
        else:
            self.module.set_state(state[COMPONENT_RL_MODULE][DEFAULT_MODULE_ID])

    @override(EnvRunner)
    def assert_healthy(self):
        # Make sure, we have built our gym.vector.Env and RLModule properly.
        assert self.env and self.module

    @override(EnvRunner)
    def stop(self):
        # Close our env object via gymnasium's API.
        self.env.close()

    def _reset_envs(self, episodes, initial_states):
        # Create n new episodes and make the `on_episode_created` callbacks.
        for env_index in range(self.num_envs):
            self._new_episode(env_index, episodes)

        # Erase all cached ongoing episodes (these will never be completed and
        # would thus never be returned/cleaned by `get_metrics` and cause a memory
        # leak).
        self._ongoing_episodes_for_metrics.clear()

        observations, infos = self.env.reset()
        observations = unbatch(observations)

        # Set initial obs and infos in the episodes.
        for env_index in range(self.num_envs):
            episodes[env_index].add_env_reset(
                observation=observations[env_index],
                infos=infos[env_index],
            )

        # Run the env-to-module connector to make sure the reset-obs/infos have
        # properly been processed (if applicable).
        self._cached_to_module = None
        if self.module:
            is_first = np.zeros((self.num_envs,))
            for i, eps in enumerate(self._episodes):
                if self._states[i] is None:
                    is_first[i] = 1.0
                    self._states[i] = {k: s[i] for k, s in initial_states.items()}
            self._cached_to_module = {
                Columns.STATE_IN: tree.map_structure(
                    lambda s: self.convert_to_tensor(s), batch(self._states)
                ),
                Columns.OBS: self.convert_to_tensor(observations),
                "is_first": self.convert_to_tensor(is_first),
            }
            # self._cached_to_module = TODO!!

    def _new_episode(self, env_index, episodes=None):
        episodes = episodes if episodes is not None else self._episodes
        episodes[env_index] = SingleAgentEpisode(
            observation_space=self.env.single_observation_space,
            action_space=self.env.single_action_space,
        )

    def _increase_sampled_metrics(self, num_steps):
        # Per sample cycle stats.
        self.metrics.log_value(
            NUM_ENV_STEPS_SAMPLED, num_steps, reduce="sum", clear_on_reduce=True
        )
        self.metrics.log_value(
            (NUM_AGENT_STEPS_SAMPLED, DEFAULT_AGENT_ID),
            num_steps,
            reduce="sum",
            clear_on_reduce=True,
        )
        self.metrics.log_value(
            (NUM_MODULE_STEPS_SAMPLED, DEFAULT_MODULE_ID),
            num_steps,
            reduce="sum",
            clear_on_reduce=True,
        )
        # Lifetime stats.
        self.metrics.log_value(NUM_ENV_STEPS_SAMPLED_LIFETIME, num_steps, reduce="sum")
        self.metrics.log_value(
            (NUM_AGENT_STEPS_SAMPLED_LIFETIME, DEFAULT_AGENT_ID),
            num_steps,
            reduce="sum",
        )
        self.metrics.log_value(
            (NUM_MODULE_STEPS_SAMPLED_LIFETIME, DEFAULT_MODULE_ID),
            num_steps,
            reduce="sum",
        )
        return num_steps

    def _log_episode_metrics(self, length, ret, sec):
        # Log general episode metrics.
        # To mimick the old API stack behavior, we'll use `window` here for
        # these particular stats (instead of the default EMA).
        win = self.config.metrics_num_episodes_for_smoothing
        self.metrics.log_value(EPISODE_LEN_MEAN, length, window=win)
        self.metrics.log_value(EPISODE_RETURN_MEAN, ret, window=win)
        self.metrics.log_value(EPISODE_DURATION_SEC_MEAN, sec, window=win)

        # For some metrics, log min/max as well.
        self.metrics.log_value(EPISODE_LEN_MIN, length, reduce="min")
        self.metrics.log_value(EPISODE_RETURN_MIN, ret, reduce="min")
        self.metrics.log_value(EPISODE_LEN_MAX, length, reduce="max")
        self.metrics.log_value(EPISODE_RETURN_MAX, ret, reduce="max")

    @Deprecated(
        new="DreamerV3EnvRunner.get_state(components='rl_module')",
        error=True,
    )
    def get_weights(self, *args, **kwargs):
        pass

    @Deprecated(
        new="DreamerV3EnvRunner.get_state()",
        error=True,
    )
    def set_weights(self, *args, **kwargs):
        pass


class NormalizedImageEnv(gym.ObservationWrapper):
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
    def observation(self, observation):
        return (observation.astype(np.float32) / 128.0) - 1.0


class OneHot(gym.ObservationWrapper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.observation_space = gym.spaces.Box(
            0.0, 1.0, shape=(self.observation_space.n,), dtype=np.float32
        )

    def reset(self, **kwargs):
        ret = self.env.reset(**kwargs)
        return self._get_obs(ret[0]), ret[1]

    def step(self, action):
        ret = self.env.step(action)
        return self._get_obs(ret[0]), ret[1], ret[2], ret[3], ret[4]

    def _get_obs(self, obs):
        return one_hot(obs, depth=self.observation_space.shape[0])


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

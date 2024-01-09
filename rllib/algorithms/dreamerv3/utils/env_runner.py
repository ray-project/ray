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
from typing import List, Tuple

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core.models.base import STATE_IN, STATE_OUT
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.wrappers.atari_wrappers import NoopResetEnv, MaxAndSkipEnv
from ray.rllib.env.wrappers.dm_control_wrapper import DMCEnv
from ray.rllib.env.utils import _gym_env_creator
from ray.rllib.evaluation.metrics import RolloutMetrics
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.numpy import one_hot
from ray.tune.registry import ENV_CREATOR, _global_registry

_, tf, _ = try_import_tf()


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
        if self.config.env.startswith("ALE/"):
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
            wrappers = [
                partial(gym.wrappers.TimeLimit, max_episode_steps=108000),
                partial(resize_v1, x_size=64, y_size=64),  # resize to 64x64
                NormalizedImageEnv,
                NoopResetEnv,
                MaxAndSkipEnv,
            ]

            self.env = gym.vector.make(
                "GymV26Environment-v0",
                env_id=self.config.env,
                wrappers=wrappers,
                num_envs=self.config.num_envs_per_worker,
                asynchronous=self.config.remote_worker_envs,
                make_kwargs=dict(
                    self.config.env_config, **{"render_mode": "rgb_array"}
                ),
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
            self.env = gym.vector.make(
                "dmc_env-v0",
                wrappers=[ActionClip],
                num_envs=self.config.num_envs_per_worker,
                asynchronous=self.config.remote_worker_envs,
                **dict(self.config.env_config),
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
            # Create the vectorized gymnasium env.
            self.env = gym.vector.make(
                "dreamerv3-custom-env-v0",
                num_envs=self.config.num_envs_per_worker,
                asynchronous=False,  # self.config.remote_worker_envs,
            )
        self.num_envs = self.env.num_envs
        assert self.num_envs == self.config.num_envs_per_worker

        # Create our RLModule to compute actions with.
        if self.config.share_module_between_env_runner_and_learner:
            # DreamerV3 Algorithm will set this to the local Learner's module.
            self.module = None
        # Create our own instance of a DreamerV3RLModule (which then needs to be
        # weight-synched each iteration).
        else:
            policy_dict, _ = self.config.get_multi_agent_setup(env=self.env)
            module_spec = self.config.get_marl_module_spec(policy_dict=policy_dict)
            # TODO (sven): DreamerV3 is currently single-agent only.
            self.module = module_spec.build()[DEFAULT_POLICY_ID]

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
        self._ts_since_last_metrics = 0

    @override(EnvRunner)
    def sample(
        self,
        *,
        num_timesteps: int = None,
        num_episodes: int = None,
        explore: bool = True,
        random_actions: bool = False,
        with_render_data: bool = False,
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
            with_render_data: If True, will record rendering images per timestep
                in the returned Episodes. This data can be used to create video
                reports.
                TODO (sven): Note that this is only supported for runnign with
                 `num_episodes` yet.

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
            return self._sample_timesteps(
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
                self._sample_episodes(
                    num_episodes=num_episodes,
                    explore=explore,
                    random_actions=random_actions,
                    with_render_data=with_render_data,
                ),
                [],
            )

    def _sample_timesteps(
        self,
        num_timesteps: int,
        explore: bool = True,
        random_actions: bool = False,
        force_reset: bool = False,
    ) -> Tuple[List[SingleAgentEpisode], List[SingleAgentEpisode]]:
        """Helper method to run n timesteps.

        See docstring of self.sample() for more details.
        """
        done_episodes_to_return = []

        # Get initial states for all `batch_size_B` rows in the forward batch.
        initial_states = tree.map_structure(
            lambda s: np.repeat(s, self.num_envs, axis=0),
            self.module.get_initial_state(),
        )

        # Have to reset the env (on all vector sub-envs).
        if force_reset or self._needs_initial_reset:
            obs, _ = self.env.reset()

            self._episodes = [SingleAgentEpisode() for _ in range(self.num_envs)]
            states = initial_states
            # Set is_first to True for all rows (all sub-envs just got reset).
            is_first = np.ones((self.num_envs,))
            self._needs_initial_reset = False

            # Set initial obs and states in the episodes.
            for i in range(self.num_envs):
                self._episodes[i].add_env_reset(observation=obs[i])
                self._states[i] = {k: s[i] for k, s in states.items()}
        # Don't reset existing envs; continue in already started episodes.
        else:
            # Pick up stored observations and states from previous timesteps.
            obs = np.stack([eps.observations[-1] for eps in self._episodes])
            # Compile the initial state for each batch row: If episode just started, use
            # model's initial state, if not, use state stored last in
            # SingleAgentEpisode.
            states = {
                k: np.stack(
                    [
                        initial_states[k][i]
                        if self._states[i] is None
                        else self._states[i][k]
                        for i, eps in enumerate(self._episodes)
                    ]
                )
                for k in initial_states.keys()
            }
            # If a batch row is at the beginning of an episode, set its `is_first` flag
            # to 1.0, otherwise 0.0.
            is_first = np.zeros((self.num_envs,))
            for i, eps in enumerate(self._episodes):
                if len(eps) == 0:
                    is_first[i] = 1.0

        # Loop through env for n timesteps.
        ts = 0
        while ts < num_timesteps:
            # Act randomly.
            if random_actions:
                actions = self.env.action_space.sample()
            # Compute an action using our RLModule.
            else:
                batch = {
                    STATE_IN: tree.map_structure(
                        lambda s: tf.convert_to_tensor(s), states
                    ),
                    SampleBatch.OBS: tf.convert_to_tensor(obs),
                    "is_first": tf.convert_to_tensor(is_first),
                }
                # Explore or not.
                if explore:
                    outs = self.module.forward_exploration(batch)
                else:
                    outs = self.module.forward_inference(batch)

                # Model outputs one-hot actions (if discrete). Convert to int actions
                # as well.
                actions = outs[SampleBatch.ACTIONS].numpy()
                if isinstance(self.env.single_action_space, gym.spaces.Discrete):
                    actions = np.argmax(actions, axis=-1)
                states = tree.map_structure(lambda s: s.numpy(), outs[STATE_OUT])

            obs, rewards, terminateds, truncateds, infos = self.env.step(actions)
            ts += self.num_envs

            for i in range(self.num_envs):
                s = {k: s[i] for k, s in states.items()}
                # The last entry in self.observations[i] is already the reset
                # obs of the new episode.
                if terminateds[i] or truncateds[i]:
                    # Finish the episode with the actual terminal observation stored in
                    # the info dict.
                    self._episodes[i].add_env_step(
                        observation=infos["final_observation"][i],
                        action=actions[i],
                        reward=rewards[i],
                        terminated=terminateds[i],
                        truncated=truncateds[i],
                    )
                    self._states[i] = s
                    # Reset h-states to the model's initial ones b/c we are starting a
                    # new episode.
                    for k, v in self.module.get_initial_state().items():
                        states[k][i] = v.numpy()
                    is_first[i] = True
                    done_episodes_to_return.append(self._episodes[i])
                    # Create a new episode object.
                    self._episodes[i] = SingleAgentEpisode(observations=[obs[i]])
                else:
                    self._episodes[i].add_env_step(
                        observation=obs[i],
                        action=actions[i],
                        reward=rewards[i],
                    )
                    is_first[i] = False

                self._states[i] = s

        # Return done episodes ...
        self._done_episodes_for_metrics.extend(done_episodes_to_return)
        # ... and all ongoing episode chunks. Also, make sure, we return
        # a copy and start new chunks so that callers of this function
        # don't alter our ongoing and returned Episode objects.
        ongoing_episodes = self._episodes
        self._episodes = [eps.cut() for eps in self._episodes]
        for eps in ongoing_episodes:
            self._ongoing_episodes_for_metrics[eps.id_].append(eps)

        self._ts_since_last_metrics += ts

        return done_episodes_to_return, ongoing_episodes

    def _sample_episodes(
        self,
        num_episodes: int,
        explore: bool = True,
        random_actions: bool = False,
        with_render_data: bool = False,
    ) -> List[SingleAgentEpisode]:
        """Helper method to run n episodes.

        See docstring of `self.sample()` for more details.
        """
        done_episodes_to_return = []

        obs, _ = self.env.reset()
        episodes = [SingleAgentEpisode() for _ in range(self.num_envs)]

        # Multiply states n times according to our vector env batch size (num_envs).
        states = tree.map_structure(
            lambda s: np.repeat(s, self.num_envs, axis=0),
            self.module.get_initial_state(),
        )
        is_first = np.ones((self.num_envs,))

        render_images = [None] * self.num_envs
        if with_render_data:
            render_images = [e.render() for e in self.env.envs]

        for i in range(self.num_envs):
            episodes[i].add_env_reset(
                observation=obs[i],
                render_image=render_images[i],
            )

        eps = 0
        while eps < num_episodes:
            if random_actions:
                actions = self.env.action_space.sample()
            else:
                batch = {
                    STATE_IN: tree.map_structure(
                        lambda s: tf.convert_to_tensor(s), states
                    ),
                    SampleBatch.OBS: tf.convert_to_tensor(obs),
                    "is_first": tf.convert_to_tensor(is_first),
                }

                if explore:
                    outs = self.module.forward_exploration(batch)
                else:
                    outs = self.module.forward_inference(batch)

                actions = outs[SampleBatch.ACTIONS].numpy()
                if isinstance(self.env.single_action_space, gym.spaces.Discrete):
                    actions = np.argmax(actions, axis=-1)
                states = tree.map_structure(lambda s: s.numpy(), outs[STATE_OUT])

            obs, rewards, terminateds, truncateds, infos = self.env.step(actions)
            if with_render_data:
                render_images = [e.render() for e in self.env.envs]

            for i in range(self.num_envs):
                # The last entry in self.observations[i] is already the reset
                # obs of the new episode.
                if terminateds[i] or truncateds[i]:
                    eps += 1

                    episodes[i].add_env_step(
                        observation=infos["final_observation"][i],
                        action=actions[i],
                        reward=rewards[i],
                        terminated=terminateds[i],
                        truncated=truncateds[i],
                    )
                    done_episodes_to_return.append(episodes[i])

                    # Also early-out if we reach the number of episodes within this
                    # for-loop.
                    if eps == num_episodes:
                        break

                    # Reset h-states to the model's initial ones b/c we are starting a
                    # new episode.
                    for k, v in self.module.get_initial_state().items():
                        states[k][i] = v.numpy()
                    is_first[i] = True

                    episodes[i] = SingleAgentEpisode(
                        observations=[obs[i]],
                        render_images=(
                            [render_images[i]] if with_render_data else None
                        ),
                    )
                else:
                    episodes[i].add_env_step(
                        observation=obs[i],
                        action=actions[i],
                        reward=rewards[i],
                        render_image=render_images[i],
                    )
                    is_first[i] = False

        self._done_episodes_for_metrics.extend(done_episodes_to_return)
        self._ts_since_last_metrics += sum(len(eps) for eps in done_episodes_to_return)

        # If user calls sample(num_timesteps=..) after this, we must reset again
        # at the beginning.
        self._needs_initial_reset = True

        return done_episodes_to_return

    # TODO (sven): Remove the requirement for EnvRunners/RolloutWorkers to have this
    #  API. Instead Algorithm should compile episode metrics itself via its local
    #  buffer.
    def get_metrics(self) -> List[RolloutMetrics]:
        # Compute per-episode metrics (only on already completed episodes).
        metrics = []
        for eps in self._done_episodes_for_metrics:
            episode_length = len(eps)
            episode_reward = eps.get_return()
            # Don't forget about the already returned chunks of this episode.
            if eps.id_ in self._ongoing_episodes_for_metrics:
                for eps2 in self._ongoing_episodes_for_metrics[eps.id_]:
                    episode_length += len(eps2)
                    episode_reward += eps2.get_return()
                del self._ongoing_episodes_for_metrics[eps.id_]

            metrics.append(
                RolloutMetrics(
                    episode_length=episode_length,
                    episode_reward=episode_reward,
                )
            )

        self._done_episodes_for_metrics.clear()
        self._ts_since_last_metrics = 0

        return metrics

    # TODO (sven): Remove the requirement for EnvRunners/RolloutWorkers to have this
    #  API. Replace by proper state overriding via `EnvRunner.set_state()`
    def set_weights(self, weights, global_vars=None):
        """Writes the weights of our (single-agent) RLModule."""
        if self.module is None:
            assert self.config.share_module_between_env_runner_and_learner
        else:
            self.module.set_state(weights[DEFAULT_POLICY_ID])

    @override(EnvRunner)
    def assert_healthy(self):
        # Make sure, we have built our gym.vector.Env and RLModule properly.
        assert self.env and self.module

    @override(EnvRunner)
    def stop(self):
        # Close our env object via gymnasium's API.
        self.env.close()


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

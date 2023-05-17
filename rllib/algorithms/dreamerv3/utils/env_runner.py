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
from supersuit.generic_wrappers import resize_v1, color_reduction_v0
import tensorflow as tf
import tree  # pip install dm_tree

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core.models.base import STATE_IN, STATE_OUT
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.wrappers.atari_wrappers import NoopResetEnv, MaxAndSkipEnv
from ray.rllib.env.wrappers.dm_control_wrapper import DMCEnv
from ray.rllib.evaluation.metrics import RolloutMetrics
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.replay_buffers.episode_replay_buffer import _Episode as Episode
from ray.rllib.utils.numpy import one_hot


class CountEnv(gym.ObservationWrapper):
    def reset(self, **kwargs):
        self.__counter = 0
        return super().reset(**kwargs)

    def observation(self, observation):
        # For gray-scaled observations.
        # observation[0][0] = self.__counter
        # For 3-color observations.
        observation[0][0][0] = self.__counter__
        self.__counter += 1
        return observation


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


class DreamerV3EnvRunner(EnvRunner):
    """An environment runner to collect data from vectorized gymnasium environments."""

    def __init__(
        self,
        config: AlgorithmConfig,
        *,
        _debug_count_env=False,
        **kwargs,
    ):
        """Initializes a DreamerV3EnvRunner instance.

        Args:
            config: The config to use to setup this EnvRunner.
        """
        super().__init__(config)

        # Create the gym.vector.Env object.
        if self.config.env.startswith("ALE"):
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
                # color_reduction_v0,  # grayscale
                partial(resize_v1, x_size=64, y_size=64),  # resize to 64x64
                NormalizedImageEnv,
                NoopResetEnv,
                MaxAndSkipEnv,
            ]
            if _debug_count_env:
                wrappers.append(CountEnv)

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

        elif self.config.env.startswith("DMC"):
            parts = self.config.env.split("/")
            assert len(parts) == 3, (
                "ERROR: DMC env must be formatted as 'DMC/[task]/[domain]', e.g. "
                f"'DMC/cartpole/swingup'! You provided '{self.config.env}'."
            )
            gym.register(
                "dmc_env-v0",
                lambda: DMCEnv(
                    parts[1], parts[2], from_pixels=True, channels_first=False
                ),
            )
            self.env = gym.vector.make(
                "dmc_env-v0",
                wrappers=[ActionClip],
                num_envs=self.config.num_envs_per_worker,
                asynchronous=self.config.remote_worker_envs,
                **dict(self.config.env_config),
            )
        else:
            wrappers = [] if self.config.env != "FrozenLake-v1" else [OneHot]
            self.env = gym.vector.make(
                self.config.env,
                wrappers=wrappers,
                num_envs=self.config.num_envs_per_worker,
                asynchronous=self.config.remote_worker_envs,
                **dict(self.config.env_config, **{"render_mode": "rgb_array"}),
            )
        self.num_envs = self.env.num_envs
        assert self.num_envs == self.config.num_envs_per_worker

        # Create our RLModule to compute actions with.
        module_spec = self.config.get_marl_module_spec(
            policy_dict=self.config.get_multi_agent_setup(
                env=self.env  # TODO(sven): remove this hack
            )[0],
            module_spec=None,
        )
        # TODO (sven): DreamerV3 is currently single-agent only.
        self.rl_module = module_spec.build()[DEFAULT_POLICY_ID]

        self._needs_initial_reset = True
        self._episodes = [None for _ in range(self.num_envs)]

        # TODO (sven): Move metrics temp storage and collection out of EnvRunner
        #  and RolloutWorkers. These classes should not continue tracking some data
        #  that they have already returned (in a call to `sample()`). Instead, the
        #  episode data should be analyzed where it was sent to (the Algorithm itself
        #  via its replay buffer, etc..).
        self._done_episodes_for_metrics = []
        self._ongoing_episodes_for_metrics = defaultdict(list)
        self._ts_since_last_metrics = 0

    @override(EnvRunner)
    def sample(self, explore: bool = True, random_actions: bool = False):
        if self.config.batch_mode == "complete_episodes":
            raise NotImplementedError
        else:
            return self.sample_timesteps(
                num_timesteps=(self.config.rollout_fragment_length * self.num_envs),
                explore=explore,
                random_actions=random_actions,
                force_reset=False,
            )

    def sample_timesteps(
        self,
        num_timesteps: int,
        explore: bool = True,
        random_actions: bool = False,
        force_reset: bool = False,
    ) -> Tuple[List[Episode], List[Episode]]:
        """Runs n timesteps on the environment(s) and returns experiences.

        Timesteps are counted in total (across all vectorized sub-environments). For
        example, if self.num_envs=2 and num_timesteps=10, each sub-environment
        will be sampled for 5 steps.

        Args:
            num_timesteps: The number of timesteps to sample from the environment(s).
            explore: Indicates whether to utilize exploration when picking actions.
            force_reset: Whether to reset the environment(s) before starting to sample.
                If False, will still reset the environment(s) if they were left in
                a terminated or truncated state during previous sample calls.

        Returns:
            A MultiAgentBatch holding the collected experiences.
        """
        done_episodes_to_return = []

        # Get initial states for all `batch_size_B` rows.
        initial_states = tree.map_structure(
            lambda s: np.repeat(s, self.num_envs, axis=0),
            self.rl_module.get_initial_state(),
        )

        # Have to reset the env (on all vector sub-envs).
        if force_reset or self._needs_initial_reset:
            obs, _ = self.env.reset()

            self._episodes = [Episode() for _ in range(self.num_envs)]
            states = initial_states
            is_first = np.ones((self.num_envs,), dtype=np.float32)
            self._needs_initial_reset = False

            for i, o in enumerate(self._split_by_env(obs)):
                self._episodes[i].add_initial_observation(
                    initial_observation=o,
                    initial_state={k: s[i] for k, s in states.items()},
                )
        # Don't reset existing envs; continue in already started episodes.
        else:
            obs = np.stack([eps.observations[-1] for eps in self._episodes])
            states = {
                k: np.stack(
                    [
                        initial_states[k][i] if eps.states is None else eps.states[k]
                        for i, eps in enumerate(self._episodes)
                    ]
                )
                for k in initial_states.keys()
            }
            is_first = np.zeros((self.num_envs,), dtype=np.float32)

        ts = 0

        while True:
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
                    outs = self.rl_module.forward_exploration(batch)
                else:
                    outs = self.rl_module.forward_inference(batch)

                actions = outs[SampleBatch.ACTIONS].numpy()
                if isinstance(self.env.single_action_space, gym.spaces.Discrete):
                    actions = np.argmax(actions, axis=-1)
                states = tree.map_structure(lambda s: s.numpy(), outs[STATE_OUT])

            obs, rewards, terminateds, truncateds, infos = self.env.step(actions)
            ts += self.num_envs

            for i, (o, a, r, term, trunc) in enumerate(
                zip(
                    self._split_by_env(obs),
                    self._split_by_env(actions),
                    self._split_by_env(rewards),
                    self._split_by_env(terminateds),
                    self._split_by_env(truncateds),
                )
            ):
                s = {k: s[i] for k, s in states.items()}
                # The last entry in self.observations[i] is already the reset
                # obs of the new episode.
                if term or trunc:
                    # Finish the episode with the actual terminal observation stored in
                    # the info dict.
                    self._episodes[i].add_timestep(
                        infos["final_observation"][i],
                        a,
                        r,
                        state=s,
                        is_terminated=True,
                    )
                    # Reset h-states to the model's initial ones b/c we are starting a
                    # new episode.
                    for k, v in self.rl_module.get_initial_state().items():
                        states[k][i] = v.numpy()
                    is_first[i] = True
                    done_episodes_to_return.append(self._episodes[i])

                    self._episodes[i] = Episode(observations=[o], states=s)
                else:
                    self._episodes[i].add_timestep(
                        o,
                        a,
                        r,
                        state=s,
                        is_terminated=False,
                    )
                    is_first[i] = False

            if ts >= num_timesteps:
                break

        # Return done episodes ...
        self._done_episodes_for_metrics.extend(done_episodes_to_return)
        # ... and all ongoing episode chunks. Also, make sure, we return
        # a copy and start new chunks so that callers of this function
        # don't alter our ongoing and returned Episode objects.
        ongoing_episodes = self._episodes
        self._episodes = [
            Episode(
                id_=eps.id_,
                observations=[eps.observations[-1]],
                states=eps.states,
            )
            for eps in self._episodes
        ]
        for eps in ongoing_episodes:
            self._ongoing_episodes_for_metrics[eps.id_].append(eps)

        self._ts_since_last_metrics += ts

        return done_episodes_to_return, ongoing_episodes

    def sample_episodes(
        self,
        num_episodes: int,
        explore: bool = True,
        random_actions: bool = False,
        with_render_data: bool = False,
    ):
        """Runs n episodes (reset first) on the environment(s) and returns experiences.

        Episodes are counted in total (across all vectorized sub-environments). For
        example, if self.num_envs=2 and num_episodes=10, each sub-environment
        will run 5 episodes.

        Args:
            num_episodes: The number of episodes to sample from the environment(s).
            explore: Indicates whether to utilize exploration when picking actions.
            force_reset: Whether to reset the environment(s) before starting to sample.
                If False, will still reset the environment(s) if they were left in
                a terminated or truncated state during previous sample calls.
        """

        done_episodes_to_return = []

        obs, _ = self.env.reset()

        episodes = [Episode() for _ in range(self.num_envs)]

        # Multiply states n times according to our vector env batch size (num_envs).
        states = tree.map_structure(
            lambda s: np.repeat(s, self.num_envs, axis=0),
            self.model.get_initial_state(),
        )
        is_first = np.ones((self.num_envs,), dtype=np.float32)

        render_images = [None] * self.num_envs
        if with_render_data:
            render_images = [e.render() for e in self.env.envs]

        for i, o in enumerate(self._split_by_env(obs)):
            episodes[i].add_initial_observation(
                initial_observation=o,
                initial_state={k: s[i] for k, s in states.items()},
                initial_render_image=render_images[i],
            )

        eps = 0

        while True:
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
                    outs = self.rl_module.forward_exploration(batch)
                else:
                    outs = self.rl_module.forward_inference(batch)

                actions = outs[SampleBatch.ACTIONS].numpy()
                if isinstance(self.env.single_action_space, gym.spaces.Discrete):
                    actions = np.argmax(actions, axis=-1)
                states = tree.map_structure(lambda s: s.numpy(), outs[STATE_OUT])

            obs, rewards, terminateds, truncateds, infos = self.env.step(actions)
            if with_render_data:
                render_images = [e.render() for e in self.env.envs]

            for i, (o, a, r, term, trunc) in enumerate(
                zip(
                    self._split_by_env(obs),
                    self._split_by_env(actions),
                    self._split_by_env(rewards),
                    self._split_by_env(terminateds),
                    self._split_by_env(truncateds),
                )
            ):
                s = {k: s[i] for k, s in states.items()}
                # The last entry in self.observations[i] is already the reset
                # obs of the new episode.
                if term or trunc:
                    eps += 1

                    episodes[i].add_timestep(
                        infos["final_observation"][i],
                        a,
                        r,
                        state=s,
                        is_terminated=True,
                    )
                    # Reset h-states to the model's initial ones b/c we are starting a
                    # new episode.
                    for k, v in self.rl_module.get_initial_state().items():
                        states[k][i] = v.numpy()
                    is_first[i] = True
                    done_episodes_to_return.append(episodes[i])

                    episodes[i] = Episode(
                        observations=[o],
                        states=s,
                        render_images=[render_images[i]],
                    )
                else:
                    episodes[i].add_timestep(
                        o,
                        a,
                        r,
                        state=s,
                        is_terminated=False,
                        render_image=render_images[i],
                    )
                    is_first[i] = False

            if eps >= num_episodes:
                break

        self._done_episodes_for_metrics.extend(done_episodes_to_return)
        self._ts_since_last_metrics += sum(len(eps) for eps in done_episodes_to_return)

        return done_episodes_to_return

    @override(EnvRunner)
    def get_metrics(self) -> List[RolloutMetrics]:

        # Compute per-episode metrics (only on already completed episodes).
        metrics = []
        for eps in self._done_episodes_for_metrics:
            #metric = RolloutMetrics()
            episode_length = len(eps)
            episode_reward = eps.get_return()
            # Don't forget about the already returned chunks of this episode.
            if eps.id_ in self._ongoing_episodes_for_metrics:
                for eps2 in self._ongoing_episodes_for_metrics[eps.id_]:
                    episode_length += len(eps2)
                    episode_reward += eps2.get_return()
                    #actions.extend(list(eps2.actions))
                del self._ongoing_episodes_for_metrics[eps.id_]

            metrics.append(RolloutMetrics(
                episode_length=episode_length,
                episode_reward=episode_reward,
            ))

        self._done_episodes_for_metrics.clear()
        self._ts_since_last_metrics = 0

        return metrics

    @override(EnvRunner)
    def assert_healthy(self):
        # Make sure, we have built our gym.vector.Env and RLModule properly.
        assert self.env and self.rl_module

    @override(EnvRunner)
    def stop(self):
        # Close our env object via gymnasium's API.
        self.env.close()

    @override(EnvRunner)
    def __del__(self):
        # TODO
        pass

    def _split_by_env(self, inputs):
        return [inputs[i] for i in range(self.num_envs)]


if __name__ == "__main__":
    config = (
        AlgorithmConfig()
        .environment(
            "ALE/MsPacman-v5",
            env_config={
                # [2]: "We follow the evaluation protocol of Machado et al. (2018) with 200M
                # environment steps, action repeat of 4, a time limit of 108,000 steps per
                # episode that correspond to 30 minutes of game play, no access to life
                # information, full action space, and sticky actions. Because the world model
                # integrates information over time, DreamerV2 does not use frame stacking.
                # The experiments use a single-task setup where a separate agent is trained
                # for each game. Moreover, each agent uses only a single environment instance.
                "repeat_action_probability": 0.25,  # "sticky actions"
                "full_action_space": True,  # "full action space"
                "frameskip": 1,  # already done by MaxAndSkip wrapper: "action repeat" == 4
            },
        )
        .rollouts(num_envs_per_worker=2, rollout_fragment_length=64)
    )
    env_runner = DreamerV3EnvRunner(
        # model=None,
        config=config,
        # _debug_count_env=True,
    )

    for _ in range(10):
        done_episodes = env_runner.sample_episodes(
            num_episodes=10, random_actions=True, with_render_data=True
        )
        for eps in done_episodes:
            assert eps.is_terminated
            print(
                f"done episode {eps.id_} obs[0]={eps.observations[0][0][0]} obs[-1]={eps.observations[-1][0][0]}"
            )

    for _ in range(10):
        done_episodes, ongoing_episodes = env_runner.sample(random_actions=True)
        for eps in done_episodes:
            assert eps.is_terminated
            print(
                f"done episode {eps.id_} obs[0]={eps.observations[0][0][0]} obs[-1]={eps.observations[-1][0][0]}"
            )
        for eps in ongoing_episodes:
            assert not eps.is_terminated
            print(
                f"ongoing episode {eps.id_} obs[0]={eps.observations[0][0][0]} obs[-1]={eps.observations[-1][0][0]}"
            )
        print()

    # obs, next_obs, actions, rewards, terminateds, truncateds = (
    #    env_runner.sample_episodes(num_episodes=10, random_actions=True)
    # )
    # mean_episode_return = np.mean([np.sum(rets) for rets in rewards])
    # print(len(obs))
    # print(f"mean(R)={mean_episode_return}")

from typing import List, Optional, Tuple

import gymnasium as gym

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.replay_buffers.episode_replay_buffer import _Episode as Episode


class SingleAgentGymEnvRunner(EnvRunner):
    """A simple single-agent EnvRunner subclass for testing purposes.

    Uses a gym.vector.Env environment and random actions.
    """

    def __init__(self, *, config: AlgorithmConfig, **kwargs):
        """Initializes a SingleAgentGymEnvRunner instance.

        Args:
            config: The config to use to setup this EnvRunner.
        """
        super().__init__(config=config, **kwargs)

        # Create the gym.vector.Env object.
        self.env = gym.vector.make(
            self.config.env,
            num_envs=self.config.num_envs_per_worker,
            asynchronous=self.config.remote_worker_envs,
            **dict(self.config.env_config, **{"render_mode": "rgb_array"}),
        )
        self.num_envs = self.env.num_envs

        self._needs_initial_reset = True
        self._episodes = [None for _ in range(self.num_envs)]

    @override(EnvRunner)
    def sample(
        self,
        *,
        num_timesteps: Optional[int] = None,
        num_episodes: Optional[int] = None,
        force_reset: bool = False,
        **kwargs,
    ) -> Tuple[List[Episode], List[Episode]]:
        """Returns a tuple (list of completed episodes, list of ongoing episodes).

        Args:
            num_timesteps: If provided, will step exactly this number of timesteps
                through the environment. Note that only one or none of `num_timesteps`
                and `num_episodes` may be provided, but never both. If both
                `num_timesteps` and `num_episodes` are None, will determine how to
                sample via `self.config`.
            num_episodes: If provided, will step through the env(s) until exactly this
                many episodes have been completed. Note that only one or none of
                `num_timesteps` and `num_episodes` may be provided, but never both.
                If both `num_timesteps` and `num_episodes` are None, will determine how
                to sample via `self.config`.
            force_reset: If True, will force-reset the env at the very beginning and
                thus begin sampling from freshly started episodes.
            **kwargs: Forward compatibility kwargs.

        Returns:
            A tuple consisting of: A list of Episode instances that are already
            done (either terminated or truncated, hence their `is_done` property is
            True), a list of Episode instances that are still ongoing
            (their `is_done` property is False).
        """
        assert not (num_timesteps is not None and num_episodes is not None)

        # If no counters are provided, use our configured default settings.
        if num_timesteps is None and num_episodes is None:
            # Truncate episodes -> num_timesteps = rollout fragment * num_envs.
            if self.config.batch_mode == "truncate_episodes":
                num_timesteps = self.config.rollout_fragment_length * self.num_envs
            # Complete episodes -> each env runs one episode.
            else:
                num_episodes = self.num_envs

        if num_timesteps is not None:
            return self._sample_timesteps(
                num_timesteps=num_timesteps,
                force_reset=force_reset,
            )
        else:
            return self._sample_episodes(num_episodes=num_episodes)

    def _sample_timesteps(
        self,
        num_timesteps: int,
        force_reset: bool = False,
    ) -> Tuple[List[Episode], List[Episode]]:
        """Runs n timesteps on the environment(s) and returns experiences.

        Timesteps are counted in total (across all vectorized sub-environments). For
        example, if self.num_envs=2 and num_timesteps=10, each sub-environment
        will be sampled for 5 steps.
        """
        done_episodes_to_return = []

        # Have to reset the env (on all vector sub-envs).
        if force_reset or self._needs_initial_reset:
            obs, _ = self.env.reset()
            # Start new episodes.
            # Set initial observations of the new episodes.
            self._episodes = [
                Episode(observations=[o]) for o in self._split_by_env(obs)
            ]
            self._needs_initial_reset = False

        # Loop for as long as we have not reached `num_timesteps` timesteps.
        ts = 0
        while ts < num_timesteps:
            # Act randomly.
            actions = self.env.action_space.sample()
            obs, rewards, terminateds, truncateds, infos = self.env.step(actions)
            # Count timesteps across all environments.
            ts += self.num_envs

            # Process env-returned data.
            for i, (o, a, r, term, trunc) in enumerate(
                zip(
                    self._split_by_env(obs),
                    self._split_by_env(actions),
                    self._split_by_env(rewards),
                    self._split_by_env(terminateds),
                    self._split_by_env(truncateds),
                )
            ):
                # Episode is done (terminated or truncated).
                # Note that gym.vector.Env reset done sub-environments automatically
                # (and start a new episode). The step-returned observation is then
                # the new episode's reset observation and the final observation of
                # the old episode can be found in the info dict.
                if term or trunc:
                    # Finish the episode object with the actual terminal observation
                    # stored in the info dict (`o` is already the reset obs of the new
                    # episode).
                    self._episodes[i].add_timestep(
                        infos["final_observation"][i],
                        a,
                        r,
                        is_terminated=term,
                        is_truncated=trunc,
                    )
                    # Add this finished episode to the list of completed ones.
                    done_episodes_to_return.append(self._episodes[i])
                    # Start a new episode and set its initial observation to `o`.
                    self._episodes[i] = Episode(observations=[o])
                # Episode is ongoing -> Add a timestep.
                else:
                    self._episodes[i].add_timestep(o, a, r)

        # Return done episodes and all ongoing episode chunks, then start new episode
        # chunks for those episodes that are still ongoing.
        ongoing_episodes = self._episodes
        # Create new chunks (using the same IDs and latest observations).
        self._episodes = [
            Episode(id_=eps.id_, observations=[eps.observations[-1]])
            for eps in self._episodes
        ]
        # Return tuple: done episodes, ongoing ones.
        return done_episodes_to_return, ongoing_episodes

    def _sample_episodes(
        self,
        num_episodes: int,
    ):
        """Runs n episodes (reset first) on the environment(s) and returns experiences.

        Episodes are counted in total (across all vectorized sub-environments). For
        example, if self.num_envs=2 and num_episodes=10, each sub-environment
        will run 5 episodes.
        """
        done_episodes_to_return = []

        obs, _ = self.env.reset()
        episodes = [Episode(observations=[o]) for o in self._split_by_env(obs)]

        eps = 0
        while eps < num_episodes:
            actions = self.env.action_space.sample()
            obs, rewards, terminateds, truncateds, infos = self.env.step(actions)

            for i, (o, a, r, term, trunc) in enumerate(
                zip(
                    self._split_by_env(obs),
                    self._split_by_env(actions),
                    self._split_by_env(rewards),
                    self._split_by_env(terminateds),
                    self._split_by_env(truncateds),
                )
            ):
                # Episode is done (terminated or truncated).
                # Note that gym.vector.Env reset done sub-environments automatically
                # (and start a new episode). The step-returned observation is then
                # the new episode's reset observation and the final observation of
                # the old episode can be found in the info dict.
                if term or trunc:
                    eps += 1
                    # Finish the episode object with the actual terminal observation
                    # stored in the info dict.
                    episodes[i].add_timestep(
                        infos["final_observation"][i],
                        a,
                        r,
                        is_terminated=term,
                        is_truncated=trunc,
                    )
                    # Add this finished episode to the list of completed ones.
                    done_episodes_to_return.append(episodes[i])

                    # Also early-out if we reach the number of episodes within this
                    # for-loop.
                    if eps == num_episodes:
                        break

                    # Start a new episode and set its initial observation to `o`.
                    episodes[i] = Episode(observations=[o])
                else:
                    episodes[i].add_timestep(o, a, r)

        # If user calls sample(num_timesteps=..) after this, we must reset again
        # at the beginning.
        self._needs_initial_reset = True

        # Return 2 lists: finished and ongoing episodes.
        return done_episodes_to_return, []

    @override(EnvRunner)
    def assert_healthy(self):
        # Make sure, we have built our gym.vector.Env properly.
        assert self.env

    @override(EnvRunner)
    def stop(self):
        # Close our env object via gymnasium's API.
        self.env.close()

    def _split_by_env(self, inputs):
        return [inputs[i] for i in range(self.num_envs)]

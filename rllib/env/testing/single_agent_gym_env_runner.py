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
        """Initializes a SimpleEnvRunner instance.

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
        explore: bool = True,
        random_actions: bool = False,
        force_reset: bool = False,
        **kwargs,
    ) -> Tuple[List[Episode], List[Episode]]:
        # So far, only sampling by num_timesteps is supported.
        assert num_episodes is None

        # If nothing provided, use our configured default settings.
        if num_timesteps is None and num_episodes is None:
            num_timesteps = self.config.rollout_fragment_length * self.num_envs

        return self._sample_timesteps(
            num_timesteps=num_timesteps,
            explore=explore,
            random_actions=random_actions,
            force_reset=force_reset,
        )

    def _sample_timesteps(
        self,
        num_timesteps: int,
        explore: bool = True,
        random_actions: bool = False,
        force_reset: bool = False,
    ):
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
            self._episodes = [Episode() for _ in range(self.num_envs)]
            self._needs_initial_reset = False
            # Set initial observations of the new episodes.
            for i, o in enumerate(self._split_by_env(obs)):
                self._episodes[i].add_initial_observation(initial_observation=o)

        # Loop for as long as we have not reached `num_timesteps` timesteps.
        ts = 0
        while ts < num_timesteps:
            # Act randomly.
            actions = self.env.action_space.sample()
            obs, rewards, terminateds, truncateds, infos = self.env.step(actions)
            # Count timesteps across all environments.
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
                # Episode is done (terminated or truncated).
                if term or trunc:
                    # Finish the episode object with the actual terminal observation
                    # stored in the info dict (`o` is already the reset obs of the new
                    # episode).
                    self._episodes[i].add_timestep(
                        infos["final_observation"][i], a, r, is_terminated=True
                    )
                    # Add this finished episode to the list of completed ones.
                    done_episodes_to_return.append(self._episodes[i])
                    # Start a new episode and set its initial observation to `o`.
                    self._episodes[i] = Episode(observations=[o])
                # Episode is ongoing -> Add a timestep.
                else:
                    self._episodes[i].add_timestep(o, a, r, is_terminated=False)

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

    @override(EnvRunner)
    def assert_healthy(self):
        # Make sure, we have built our gym.vector.Env and RLModule properly.
        assert self.env

    @override(EnvRunner)
    def stop(self):
        # Close our env object via gymnasium's API.
        self.env.close()

    def _split_by_env(self, inputs):
        return [inputs[i] for i in range(self.num_envs)]

import logging
import time
import ray

from pathlib import Path
from typing import Any, Dict, List

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core.columns import Columns
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    SAMPLE_TIMER,
    TIME_BETWEEN_SAMPLING,
)
from ray.rllib.utils.spaces.space_utils import unbatch

logger = logging.Logger(__file__)

# TODO (simon): This class can be agnostic to the episode type as it
# calls only get_state.


class OfflineSingleAgentEnvRunner(SingleAgentEnvRunner):
    """The environment runner to record the single agent case."""

    @override(SingleAgentEnvRunner)
    def __init__(self, config: AlgorithmConfig, **kwargs):
        # Initialize the parent.
        super().__init__(config, **kwargs)

        # Set the output write method.
        self.data_write_method = self.config.output_data_write_method
        self.data_write_method_kwargs = self.config.output_data_write_method_kwargs

        # Set the filesystem.
        self.filesystem = self.config.output_filesystem
        self.filesystem_kwargs = self.config.output_filesystem_kwargs
        # Set the output base path.
        self.output_path = self.config.output
        # Set the subdir (environment specific).
        self.subdir_path = self.config.env.lower()
        # Set the worker-specific path name. Note, this is
        # specifically to enable multi-threaded writing into
        # the same directory.
        self.worker_path = "run-" + f"{self.worker_index}".zfill(6) + "-"

        # If a specific filesystem is given, set it up. Note, this could
        # be `gcsfs` for GCS, `pyarrow` for S3 or `adlfs` for Azure Blob Storage.
        if self.filesystem:
            if self.filesystem == "gcs":
                import gcsfs

                self.filesystem_object = gcsfs.GCSFileSystem(**self.filesystem_kwargs)
            elif self.filesystem == "s3":
                from pyarrow import fs

                self.filesystem_object = fs.S3FileSystem(**self.filesystem_kwargs)
            elif self.filesystem == "abs":
                import adlfs

                self.filesystem_object = adlfs.AzureBlobFileSystem(
                    **self.filesystem_kwargs
                )
            else:
                raise ValueError(
                    f"Unknown filesystem: {self.filesystem}. Filesystems can be "
                    "'gcs' for GCS, "
                    "'s3' for S3, or 'abs'"
                )

        # If we should store `SingleAgentEpisodes` or column data.
        self.output_write_episodes = self.config.output_write_episodes

        # Counts how often `sample` is called to define the output path for
        # each file.
        self.sample_counter = 0

        self._episodes_data = []

    @override(SingleAgentEnvRunner)
    def sample(
        self,
        *,
        num_timesteps: int = None,
        num_episodes: int = None,
        explore: bool = None,
        random_actions: bool = False,
        force_reset: bool = False,
    ) -> List[SingleAgentEpisode]:

        # Call the super sample method.
        self.sample_counter += 1

        assert not (num_timesteps is not None and num_episodes is not None)

        if self._time_after_sampling is not None:
            self.metrics.log_value(
                key=TIME_BETWEEN_SAMPLING,
                value=time.perf_counter() - self._time_after_sampling,
            )

        with self.metrics.log_time(SAMPLE_TIMER):
            # If no execution details are provided, use the config to try to infer the
            # desired timesteps/episodes to sample and exploration behavior.
            if explore is None:
                explore = self.config.explore
            if (
                num_timesteps is None
                and num_episodes is None
                and self.config.batch_mode == "truncate_episodes"
            ):
                num_timesteps = (
                    self.config.get_rollout_fragment_length(self.worker_index)
                    * self.num_envs
                )

            # Sample n timesteps.
            if num_timesteps is not None:
                samples, samples_data = self._sample_timesteps(
                    num_timesteps=num_timesteps,
                    explore=explore,
                    random_actions=random_actions,
                    force_reset=force_reset,
                )
            # Sample m episodes.
            elif num_episodes is not None:
                samples, samples_data = self._sample_episodes(
                    num_episodes=num_episodes,
                    explore=explore,
                    random_actions=random_actions,
                )
            # For complete episodes mode, sample a single episode and
            # leave coordination of sampling to `synchronous_parallel_sample`.
            # TODO (simon, sven): The coordination will eventually move
            # to `EnvRunnerGroup` in the future. So from the algorithm one
            # would do `EnvRunnerGroup.sample()`.
            else:
                samples, samples_data = self._sample_episodes(
                    num_episodes=1,
                    explore=explore,
                    random_actions=random_actions,
                )

            # Make the `on_sample_end` callback.
            self._callbacks.on_sample_end(
                env_runner=self,
                metrics_logger=self.metrics,
                samples=samples,
            )

        self._time_after_sampling = time.perf_counter()

        # TODO (simon): Implement a num_rows_per_file.
        # if self.num_rows_per_file:
        #     self._samples_data.extend(samples_data)

        #     if len(self._samples_dara) >= self.num_rows_per_file:
        #         pass
        #         # Store
        # Write episodes as objects.
        if self.output_write_episodes:
            pass
        # Otherwise store as column data.
        else:
            samples_ds = ray.data.from_items(samples_data)
            try:
                path = (
                    Path(self.output_path)
                    .joinpath(self.subdir_path)
                    .joinpath(self.worker_path + f"-{self.sample_counter}".zfill(6))
                )
                getattr(samples_ds, self.data_write_method)(
                    path.as_posix(), **self.data_write_method_kwargs
                )

                logger.info("Wrote samples to storage.")
            except Exception as e:
                logger.error(e)

        # Finally return the samples as usual.
        return samples

    def _sample_timesteps(
        self,
        num_timesteps: int,
        explore: bool,
        random_actions: bool = False,
        force_reset: bool = False,
    ) -> List[SingleAgentEpisode]:
        """Helper method to sample n timesteps."""

        done_episodes_to_return: List[SingleAgentEpisode] = []
        done_episodes_to_return_data: List[Dict[str, Any]] = []

        # Have to reset the env (on all vector sub_envs).
        if force_reset or self._needs_initial_reset:
            # Create n new episodes.
            # TODO (sven): Add callback `on_episode_created` as soon as
            # `gymnasium-v1.0.0a2` PR is coming.
            self._episodes = []
            self._episodes_data = []
            for env_index in range(self.num_envs):
                self._episodes.append(self._new_episode())
                self._episodes_data.append([])
            self._shared_data = {}

            # Erase all cached ongoing episodes (these will never be completed and
            # would thus never be returned/cleaned by `get_metrics` and cause a memory
            # leak).
            self._ongoing_episodes_for_metrics.clear()

            # Reset the environment.
            # TODO (simon): Check, if we need here the seed from the config.
            obs, infos = self.env.reset()
            obs = unbatch(obs)
            self._cached_to_module = None

            # Call `on_episode_start()` callbacks.
            for env_index in range(self.num_envs):
                self._make_on_episode_callback("on_episode_start", env_index)

            # We just reset the env. Don't have to force this again in the next
            # call to `self._sample_timesteps()`.
            self._needs_initial_reset = False

            # Set initial obs and infos in the episodes.
            for env_index in range(self.num_envs):
                self._episodes[env_index].add_env_reset(
                    observation=obs[env_index],
                    infos=infos[env_index],
                )
                self._episodes_data[env_index].append(
                    {
                        Columns.OBS: obs[env_index],
                    }
                )

        # Loop through timesteps.
        ts = 0

        while ts < num_timesteps:
            # Act randomly.
            if random_actions:
                to_env = {
                    Columns.ACTIONS: self.env.action_space.sample(),
                }
            # Compute an action using the RLModule.
            else:
                # Env-to-module connector.
                to_module = self._cached_to_module or self._env_to_module(
                    rl_module=self.module,
                    episodes=self._episodes,
                    explore=explore,
                    shared_data=self._shared_data,
                )
                self._cached_to_module = None

                # RLModule forward pass: Explore or not.
                if explore:
                    env_steps_lifetime = (
                        self.metrics.peek(NUM_ENV_STEPS_SAMPLED_LIFETIME, default=0)
                        + ts
                    )
                    to_env = self.module.forward_exploration(
                        to_module, t=env_steps_lifetime
                    )
                else:
                    to_env = self.module.forward_inference(to_module)

                # Module-to-env connector.
                to_env = self._module_to_env(
                    rl_module=self.module,
                    data=to_env,
                    episodes=self._episodes,
                    explore=explore,
                    shared_data=self._shared_data,
                )

            # Extract the (vectorized) actions (to be sent to the env) from the
            # module/connector output. Note that these actions are fully ready (e.g.
            # already unsquashed/clipped) to be sent to the environment) and might not
            # be identical to the actions produced by the RLModule/distribution, which
            # are the ones stored permanently in the episode objects.
            actions = to_env.pop(Columns.ACTIONS)
            actions_for_env = to_env.pop(Columns.ACTIONS_FOR_ENV, actions)
            # Step the environment.
            obs, rewards, terminateds, truncateds, infos = self.env.step(
                actions_for_env
            )
            obs, actions = unbatch(obs), unbatch(actions)

            ts += self.num_envs

            for env_index in range(self.num_envs):
                # TODO (simon): This might be unfortunate if a user needs to set a
                #  certain env parameter during different episodes (for example for
                #  benchmarking).
                extra_model_output = {k: v[env_index] for k, v in to_env.items()}

                # In inference, we have only the action logits.
                if terminateds[env_index] or truncateds[env_index]:
                    # Finish the episode with the actual terminal observation stored in
                    # the info dict.
                    self._episodes[env_index].add_env_step(
                        # Gym vector env provides the `"final_observation"`.
                        # Pop these out of the infos dict so this information doesn't
                        # appear in the next episode as well (at index=0).
                        infos[env_index].pop("final_observation"),
                        actions[env_index],
                        rewards[env_index],
                        infos=infos[env_index].pop("final_info"),
                        terminated=terminateds[env_index],
                        truncated=truncateds[env_index],
                        extra_model_outputs=extra_model_output,
                    )
                    self._episodes_data[env_index][-1].update(
                        {
                            Columns.EPS_ID: self._episodes[env_index].id_,
                            Columns.AGENT_ID: self._episodes[env_index].agent_id,
                            Columns.MODULE_ID: self._episodes[env_index].module_id,
                            Columns.ACTIONS: actions[env_index],
                            Columns.REWARDS: rewards[env_index],
                            Columns.NEXT_OBS: obs[env_index],
                            Columns.INFOS: infos[env_index],
                            Columns.TERMINATEDS: terminateds[env_index],
                            Columns.TRUNCATEDS: truncateds[env_index],
                            **extra_model_output,
                        }
                    )
                    # Make the `on_episode_step` and `on_episode_end` callbacks (before
                    # finalizing the episode object).
                    self._make_on_episode_callback("on_episode_step", env_index)

                    # We have to perform an extra env-to-module pass here, just in case
                    # the user's connector pipeline performs (permanent) transforms
                    # on each observation (including this final one here). Without such
                    # a call and in case the structure of the observations change
                    # sufficiently, the following `finalize()` call on the episode will
                    # fail.
                    if self.module is not None:
                        self._env_to_module(
                            episodes=[self._episodes[env_index]],
                            explore=explore,
                            rl_module=self.module,
                            shared_data=self._shared_data,
                        )

                    self._make_on_episode_callback("on_episode_end", env_index)

                    # Then finalize (numpy'ize) the episode.
                    done_episodes_to_return.append(self._episodes[env_index].finalize())
                    done_episodes_to_return_data.extend(self._episodes_data[env_index])

                    # Create a new episode object with already the reset data in it.
                    self._episodes[env_index] = SingleAgentEpisode(
                        observations=[obs[env_index]],
                        infos=[infos[env_index]],
                        observation_space=self.env.single_observation_space,
                        action_space=self.env.single_action_space,
                    )
                    self._episodes_data[env_index] = [
                        {
                            Columns.OBS: obs[env_index],
                        }
                    ]

                    # Make the `on_episode_start` callback.
                    self._make_on_episode_callback("on_episode_start", env_index)

                else:
                    self._episodes[env_index].add_env_step(
                        obs[env_index],
                        actions[env_index],
                        rewards[env_index],
                        infos=infos[env_index],
                        extra_model_outputs=extra_model_output,
                    )
                    self._episodes_data[env_index][-1].update(
                        {
                            Columns.EPS_ID: self._episodes[env_index].id_,
                            Columns.AGENT_ID: self._episodes[env_index].agent_id,
                            Columns.MODULE_ID: self._episodes[env_index].module_id,
                            Columns.ACTIONS: actions[env_index],
                            Columns.REWARDS: rewards[env_index],
                            Columns.NEXT_OBS: obs[env_index],
                            Columns.INFOS: infos[env_index],
                            Columns.TERMINATEDS: terminateds[env_index],
                            Columns.TRUNCATEDS: truncateds[env_index],
                            **extra_model_output,
                        }
                    )
                    self._episodes_data[env_index].append(
                        {
                            Columns.OBS: obs[env_index],
                        }
                    )

                    # Make the `on_episode_step` callback.
                    self._make_on_episode_callback("on_episode_step", env_index)

        # Already perform env-to-module connector call for next call to
        # `_sample_timesteps()`. See comment in c'tor for `self._cached_to_module`.
        if self.module is not None:
            self._cached_to_module = self._env_to_module(
                rl_module=self.module,
                episodes=self._episodes,
                explore=explore,
                shared_data=self._shared_data,
            )

        # Return done episodes ...
        # TODO (simon): Check, how much memory this attribute uses.
        self._done_episodes_for_metrics.extend(done_episodes_to_return)
        # ... and all ongoing episode chunks.

        # Also, make sure we start new episode chunks (continuing the ongoing episodes
        # from the to-be-returned chunks).
        ongoing_episodes_continuations = [
            eps.cut(len_lookback_buffer=self.config.episode_lookback_horizon)
            for eps in self._episodes
        ]

        ongoing_episodes_to_return = []
        ongoing_episodes_to_return_data = []
        for i, eps in enumerate(self._episodes):
            # Just started Episodes do not have to be returned. There is no data
            # in them anyway.
            if eps.t == 0:
                continue
            eps.validate()
            self._ongoing_episodes_for_metrics[eps.id_].append(eps)
            # Return finalized (numpy'ized) Episodes.
            ongoing_episodes_to_return.append(eps.finalize())
            ongoing_episodes_to_return_data.extend(self._episodes_data[i][:-1])

        # Continue collecting into the cut Episode chunks.
        self._episodes = ongoing_episodes_continuations

        self._increase_sampled_metrics(ts)

        # Return collected episode data.
        return (
            done_episodes_to_return + ongoing_episodes_to_return,
            done_episodes_to_return_data + ongoing_episodes_to_return_data,
        )

    def _sample_episodes(
        self,
        num_episodes: int,
        explore: bool,
        random_actions: bool = False,
    ) -> List[SingleAgentEpisode]:
        """Helper method to run n episodes.

        See docstring of `self.sample()` for more details.
        """
        # If user calls sample(num_timesteps=..) after this, we must reset again
        # at the beginning.
        self._needs_initial_reset = True

        done_episodes_to_return: List[SingleAgentEpisode] = []
        done_episodes_to_return_data: List[Dict[str, Any]] = []

        episodes = []
        episodes_data = []
        for env_index in range(self.num_envs):
            episodes.append(self._new_episode())
            # TODO (sven): Add callback `on_episode_created` as soon as
            # `gymnasium-v1.0.0a2` PR is coming.
            episodes_data.append([])
        _shared_data = {}

        # Reset the environment.
        # TODO (simon): Check, if we need here the seed from the config.
        obs, infos = self.env.reset()
        for env_index in range(self.num_envs):
            episodes[env_index].add_env_reset(
                observation=unbatch(obs)[env_index],
                infos=infos[env_index],
            )
            episodes_data[env_index].append(
                {
                    Columns.OBS: obs[env_index],
                }
            )
            self._make_on_episode_callback("on_episode_start", env_index, episodes)

        # Loop over episodes.
        eps = 0
        ts = 0
        while eps < num_episodes:
            # Act randomly.
            if random_actions:
                to_env = {
                    Columns.ACTIONS: self.env.action_space.sample(),
                }
            # Compute an action using the RLModule.
            else:
                # Env-to-module connector.
                to_module = self._env_to_module(
                    rl_module=self.module,
                    episodes=episodes,
                    explore=explore,
                    shared_data=_shared_data,
                )

                # RLModule forward pass: Explore or not.
                if explore:
                    env_steps_lifetime = (
                        self.metrics.peek(NUM_ENV_STEPS_SAMPLED_LIFETIME, default=0)
                        + ts
                    )
                    to_env = self.module.forward_exploration(
                        to_module, t=env_steps_lifetime
                    )
                else:
                    to_env = self.module.forward_inference(to_module)

                # Module-to-env connector.
                to_env = self._module_to_env(
                    rl_module=self.module,
                    data=to_env,
                    episodes=episodes,
                    explore=explore,
                    shared_data=_shared_data,
                )

            # Extract the (vectorized) actions (to be sent to the env) from the
            # module/connector output. Note that these actions are fully ready (e.g.
            # already unsquashed/clipped) to be sent to the environment) and might not
            # be identical to the actions produced by the RLModule/distribution, which
            # are the ones stored permanently in the episode objects.
            actions = to_env.pop(Columns.ACTIONS)
            actions_for_env = to_env.pop(Columns.ACTIONS_FOR_ENV, actions)
            # Step the environment.
            obs, rewards, terminateds, truncateds, infos = self.env.step(
                actions_for_env
            )
            obs, actions = unbatch(obs), unbatch(actions)
            ts += self.num_envs

            for env_index in range(self.num_envs):
                extra_model_output = {k: v[env_index] for k, v in to_env.items()}

                if terminateds[env_index] or truncateds[env_index]:
                    eps += 1

                    episodes[env_index].add_env_step(
                        infos[env_index].pop("final_observation"),
                        actions[env_index],
                        rewards[env_index],
                        infos=infos[env_index].pop("final_info"),
                        terminated=terminateds[env_index],
                        truncated=truncateds[env_index],
                        extra_model_outputs=extra_model_output,
                    )
                    episodes_data[env_index][-1].update(
                        {
                            Columns.EPS_ID: episodes[env_index].id_,
                            Columns.AGENT_ID: episodes[env_index].agent_id,
                            Columns.MODULE_ID: episodes[env_index].module_id,
                            Columns.ACTIONS: actions[env_index],
                            Columns.REWARDS: rewards[env_index],
                            Columns.NEXT_OBS: obs[env_index],
                            Columns.INFOS: infos[env_index]
                            if len(infos[env_index]) > 0
                            else None,
                            Columns.TERMINATEDS: terminateds[env_index],
                            Columns.TRUNCATEDS: truncateds[env_index],
                            **extra_model_output,
                        }
                    )
                    # Make `on_episode_step` and `on_episode_end` callbacks before
                    # finalizing the episode.
                    self._make_on_episode_callback(
                        "on_episode_step", env_index, episodes
                    )

                    # We have to perform an extra env-to-module pass here, just in case
                    # the user's connector pipeline performs (permanent) transforms
                    # on each observation (including this final one here). Without such
                    # a call and in case the structure of the observations change
                    # sufficiently, the following `finalize()` call on the episode will
                    # fail.
                    if self.module is not None:
                        self._env_to_module(
                            episodes=[episodes[env_index]],
                            explore=explore,
                            rl_module=self.module,
                            shared_data=_shared_data,
                        )

                    # Make the `on_episode_end` callback (before finalizing the episode,
                    # but after(!) the last env-to-module connector call has been made.
                    # -> All obs (even the terminal one) should have been processed now
                    # (by the connector, if applicable).
                    self._make_on_episode_callback(
                        "on_episode_end", env_index, episodes
                    )

                    # Finalize (numpy'ize) the episode.
                    done_episodes_to_return.append(episodes[env_index].finalize())
                    done_episodes_to_return_data.append(episodes_data[env_index])

                    # Also early-out if we reach the number of episodes within this
                    # for-loop.
                    if eps == num_episodes:
                        break

                    # Create a new episode object.
                    episodes[env_index] = SingleAgentEpisode(
                        observations=[obs[env_index]],
                        infos=[infos[env_index]],
                        observation_space=self.env.single_observation_space,
                        action_space=self.env.single_action_space,
                    )
                    episodes_data[env_index] = [
                        {
                            Columns.OBS: obs[env_index],
                        }
                    ]
                    # Make `on_episode_start` callback.
                    self._make_on_episode_callback(
                        "on_episode_start", env_index, episodes
                    )
                else:
                    episodes[env_index].add_env_step(
                        obs[env_index],
                        actions[env_index],
                        rewards[env_index],
                        infos=infos[env_index],
                        extra_model_outputs=extra_model_output,
                    )
                    episodes_data[env_index][-1].update(
                        {
                            Columns.EPS_ID: episodes[env_index].id_,
                            Columns.AGENT_ID: episodes[env_index].agent_id,
                            Columns.MODULE_ID: episodes[env_index].module_id,
                            Columns.ACTIONS: actions[env_index],
                            Columns.REWARDS: rewards[env_index],
                            Columns.NEXT_OBS: obs[env_index],
                            Columns.INFOS: infos[env_index]
                            if len(infos[env_index]) > 0
                            else None,
                            Columns.TERMINATEDS: terminateds[env_index],
                            Columns.TRUNCATEDS: truncateds[env_index],
                            **extra_model_output,
                        }
                    )
                    episodes_data[env_index].append(
                        {
                            Columns.OBS: obs[env_index],
                        }
                    )
                    # Make `on_episode_step` callback.
                    self._make_on_episode_callback(
                        "on_episode_step", env_index, episodes
                    )

        self._done_episodes_for_metrics.extend(done_episodes_to_return)

        # Initialized episodes have to be removed as they lack `extra_model_outputs`.
        # samples = [episode for episode in done_episodes_to_return if episode.t > 0]
        samples = []
        samples_data = []
        for i, episode in enumerate(done_episodes_to_return):
            if episode.t > 0:
                samples.append(episode)
                samples_data.extend(done_episodes_to_return_data[i])

        self._increase_sampled_metrics(ts)

        return samples, samples_data

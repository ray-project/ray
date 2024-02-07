import gymnasium as gym
import tree

from collections import defaultdict
from functools import partial
from typing import DefaultDict, List, Optional

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.core.rl_module.rl_module import RLModule, SingleAgentRLModuleSpec
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.env.utils import _gym_env_creator
from ray.rllib.evaluation.metrics import RolloutMetrics
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.annotations import ExperimentalAPI, override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import TensorType
from ray.tune.registry import ENV_CREATOR, _global_registry

_, tf, _ = try_import_tf()


@ExperimentalAPI
class SingleAgentEnvRunner(EnvRunner):
    """The generic environment runner for the single agent case."""

    @override(EnvRunner)
    def __init__(self, config: AlgorithmConfig, **kwargs):
        """Initializes a SingleAgentEnvRunner instance.

        Args:
            config: An `AlgorithmConfig` object containing all settings needed to
                build this `EnvRunner` class.
        """
        super().__init__(config=config)

        # Get the worker index on which this instance is running.
        self.worker_index: int = kwargs.get("worker_index")

        # Create our callbacks object.
        self._callbacks: DefaultCallbacks = self.config.callbacks_class()

        # Create the vectorized gymnasium env.

        # Register env for the local context.
        # Note, `gym.register` has to be called on each worker.
        if isinstance(self.config.env, str) and _global_registry.contains(
            ENV_CREATOR, self.config.env
        ):
            entry_point = partial(
                _global_registry.get(ENV_CREATOR, self.config.env),
                self.config.env_config,
            )

        else:
            entry_point = partial(
                _gym_env_creator,
                env_context=self.config.env_config,
                env_descriptor=self.config.env,
            )
        gym.register("rllib-single-agent-env-runner-v0", entry_point=entry_point)

        # Wrap into `VectorListInfo`` wrapper to get infos as lists.
        self.env: gym.Wrapper = gym.wrappers.VectorListInfo(
            gym.vector.make(
                "rllib-single-agent-env-runner-v0",
                num_envs=self.config.num_envs_per_worker,
                asynchronous=self.config.remote_worker_envs,
            )
        )
        self.num_envs: int = self.env.num_envs
        assert self.num_envs == self.config.num_envs_per_worker

        # Call the `on_environment_created` callback.
        self._callbacks.on_environment_created(
            env_runner=self,
            env=self.env,
            env_config=self.config.env_config,
        )

        # Create the env-to-module connector pipeline.
        self._env_to_module = self.config.build_env_to_module_connector(self.env)
        # Cached env-to-module results taken at the end of a `_sample_timesteps()`
        # call to make sure the final observation (before an episode cut) gets properly
        # processed (and maybe postprocessed and re-stored into the episode).
        # For example, if we had a connector that normalizes observations and directly
        # re-inserts these new obs back into the episode, the last observation in each
        # sample call would NOT be processed, which could be very harmful in cases,
        # in which value function bootstrapping of those (truncation) observations is
        # required in the learning step.
        self._cached_to_module = None

        # Create our own instance of the (single-agent) `RLModule` (which
        # the needs to be weight-synched) each iteration.
        try:
            module_spec: SingleAgentRLModuleSpec = (
                self.config.get_default_rl_module_spec()
            )
            module_spec.observation_space = self._env_to_module.observation_space
            # TODO (simon): The `gym.Wrapper` for `gym.vector.VectorEnv` should
            #  actually hold the spaces for a single env, but for boxes the
            #  shape is (1, 1) which brings a problem with the action dists.
            #  shape=(1,) is expected.
            module_spec.action_space = self.env.envs[0].action_space
            module_spec.model_config_dict = self.config.model
            self.module: RLModule = module_spec.build()
        except NotImplementedError:
            self.module = None

        # Create the two connector pipelines: env-to-module and module-to-env.
        self._module_to_env = self.config.build_module_to_env_connector(self.env)

        # This should be the default.
        self._needs_initial_reset: bool = True
        self._episodes: List[Optional[SingleAgentEpisode]] = [
            None for _ in range(self.num_envs)
        ]

        self._done_episodes_for_metrics: List[SingleAgentEpisode] = []
        self._ongoing_episodes_for_metrics: DefaultDict[List] = defaultdict(list)
        self._weights_seq_no: int = 0

    @override(EnvRunner)
    def sample(
        self,
        *,
        num_timesteps: int = None,
        num_episodes: int = None,
        explore: bool = True,
        random_actions: bool = False,
        with_render_data: bool = False,
    ) -> List[SingleAgentEpisode]:
        """Runs and returns a sample (n timesteps or m episodes) on the env(s).

        Args:
            num_timesteps: int. Number of timesteps to sample during rollout.
                Note, only one of `num_timetseps` or `num_episodes` may be provided.
            num_episodes: int. Number of episodes to sample during rollout.
                Note, only one parameter, `num_timetseps` or `num_episodes`
                    can be provided.
            explore: boolean. If in exploration or inference mode. Exploration
                mode might for some algorithms provide extza model outputs that
                are redundant in inference mode.
            random_actions: boolean. If actions should be sampled from the action
                space. In default mode (i.e. `False`) we sample actions frokm the
                policy.
            with_render_data: If render data from the environment should be collected.
                This is only available when sampling episodes, i.e. `num_episodes` is
                not `None`.
        Returns:
            `Lists of `MultiAgentEpisode` instances, carrying the collected sample data.
        """
        assert not (num_timesteps is not None and num_episodes is not None)

        # If no execution details are provided, use the config to try to infer the
        # desired timesteps/episodes to sample.
        if (
            num_timesteps is None
            and num_episodes is None
            and self.config.batch_mode == "truncate_episodes"
        ):
            num_timesteps = (
                self.config.get_rollout_fragment_length(worker_index=self.worker_index)
                * self.num_envs
            )

        # Sample n timesteps.
        if num_timesteps is not None:
            samples = self._sample_timesteps(
                num_timesteps=num_timesteps,
                explore=explore,
                random_actions=random_actions,
                force_reset=False,
            )
        # Sample m episodes.
        elif num_episodes is not None:
            samples = self._sample_episodes(
                num_episodes=num_episodes,
                explore=explore,
                random_actions=random_actions,
                with_render_data=with_render_data,
            )
        # For complete episodes mode, sample as long as the number of timesteps
        # done is smaller than the `train_batch_size`.
        else:
            total = 0
            samples = []
            while total < self.config.train_batch_size:
                episodes = self._sample_episodes(
                    num_episodes=self.num_envs,
                    explore=explore,
                    random_actions=random_actions,
                    with_render_data=with_render_data,
                )
                total += sum(len(e) for e in episodes)
                samples.extend(episodes)

        # Make the `on_sample_end` callback.
        self._callbacks.on_sample_end(env_runner=self, samples=samples)

        return samples

    def _sample_timesteps(
        self,
        num_timesteps: int,
        explore: bool = True,
        random_actions: bool = False,
        force_reset: bool = False,
    ) -> List[SingleAgentEpisode]:
        """Helper method to sample n timesteps."""

        done_episodes_to_return: List[SingleAgentEpisode] = []

        # Have to reset the env (on all vector sub_envs).
        if force_reset or self._needs_initial_reset:
            # Create n new episodes and make the `on_episode_created` callbacks.
            self._episodes = []
            for env_index in range(self.num_envs):
                self._episodes.append(
                    SingleAgentEpisode(
                        observation_space=self.env.single_observation_space,
                        action_space=self.env.single_action_space,
                    )
                )
                self._make_on_episode_callback("on_episode_created", env_index)

            # Reset the environment.
            # TODO (simon): Check, if we need here the seed from the config.
            obs, infos = self.env.reset()
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

        # Loop through timesteps.
        ts = 0

        while ts < num_timesteps:
            # Act randomly.
            if random_actions:
                to_env = {
                    SampleBatch.ACTIONS: self.env.action_space.sample(),
                }
            # Compute an action using the RLModule.
            else:
                to_module = self._cached_to_module or self._env_to_module(
                    rl_module=self.module,
                    episodes=self._episodes,
                    explore=explore,
                )
                self._cached_to_module = None
                # Explore or not.
                if explore:
                    to_env = self.module.forward_exploration(to_module)
                else:
                    to_env = self.module.forward_inference(to_module)

                to_env = self._module_to_env(
                    rl_module=self.module,
                    data=to_env,
                    episodes=self._episodes,
                    explore=explore,
                )

            actions = to_env.pop(SampleBatch.ACTIONS)

            obs, rewards, terminateds, truncateds, infos = self.env.step(actions)

            ts += self.num_envs

            for env_index in range(self.num_envs):
                # TODO (simon): This might be unfortunate if a user needs to set a
                #  certain env parameter during different episodes (for example for
                #  benchmarking).
                extra_model_output = tree.map_structure(lambda s: s[env_index], to_env)

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

                    # Make the `on_episode_step` callback (before finalizing the
                    # episode object).
                    self._make_on_episode_callback("on_episode_step", env_index)
                    done_episodes_to_return.append(self._episodes[env_index].finalize())

                    # Make the `on_episode_env` callback (after having finalized the
                    # episode object).
                    self._make_on_episode_callback("on_episode_end", env_index)

                    # Create a new episode object with already the reset data in it.
                    self._episodes[env_index] = SingleAgentEpisode(
                        observations=[obs[env_index]],
                        infos=[infos[env_index]],
                        observation_space=self.env.single_observation_space,
                        action_space=self.env.single_action_space,
                    )

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

                    # Make the `on_episode_step` callback.
                    self._make_on_episode_callback("on_episode_step", env_index)

        # Already perform env-to-module connector call for next call to
        # `_sample_timesteps()`. See comment in c'tor for `self._cached_to_module`.
        if self.module is not None:
            self._cached_to_module = self._env_to_module(
                rl_module=self.module,
                episodes=self._episodes,
                explore=explore,
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
        for eps in self._episodes:
            # Just started Episodes do not have to be returned. There is no data
            # in them anyway.
            if eps.t == 0:
                continue
            eps.validate()
            self._ongoing_episodes_for_metrics[eps.id_].append(eps)
            # Return finalized (numpy'ized) Episodes.
            ongoing_episodes_to_return.append(eps.finalize())

        # Continue collecting into the cut Episode chunks.
        self._episodes = ongoing_episodes_continuations

        # Return collected episode data.
        return done_episodes_to_return + ongoing_episodes_to_return

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
        # If user calls sample(num_timesteps=..) after this, we must reset again
        # at the beginning.
        self._needs_initial_reset = True

        done_episodes_to_return: List[SingleAgentEpisode] = []

        # Reset the environment.
        # TODO (simon): Check, if we need here the seed from the config.
        obs, infos = self.env.reset()
        episodes = []
        for env_index in range(self.num_envs):
            episodes.append(
                SingleAgentEpisode(
                    observation_space=self.env.single_observation_space,
                    action_space=self.env.single_action_space,
                )
            )
            self._make_on_episode_callback("on_episode_created", env_index, episodes)

        # Initialize image rendering if needed.
        render_images = [None] * self.num_envs
        if with_render_data:
            render_images = [e.render() for e in self.env.envs]

        for env_index in range(self.num_envs):
            episodes[env_index].add_env_reset(
                observation=obs[env_index],
                infos=infos[env_index],
                render_image=render_images[env_index],
            )
            self._make_on_episode_callback("on_episode_start", env_index, episodes)

        # Loop over episodes.
        eps = 0
        while eps < num_episodes:
            # Act randomly.
            if random_actions:
                to_env = {
                    SampleBatch.ACTIONS: self.env.action_space.sample(),
                }
            # Compute an action using the RLModule.
            else:
                to_module = self._env_to_module(
                    rl_module=self.module,
                    episodes=episodes,
                    explore=explore,
                )
                # Explore or not.
                if explore:
                    to_env = self.module.forward_exploration(to_module)
                else:
                    to_env = self.module.forward_inference(to_module)

                to_env = self._module_to_env(
                    rl_module=self.module,
                    data=to_env,
                    episodes=episodes,
                    explore=explore,
                )

            # Step the environment.
            actions = to_env.pop(SampleBatch.ACTIONS)

            obs, rewards, terminateds, truncateds, infos = self.env.step(actions)

            # Add render data if needed.
            if with_render_data:
                render_images = [e.render() for e in self.env.envs]

            for env_index in range(self.num_envs):
                # Extract info and state for vector sub_env.
                # info = {k: v[i] for k, v in infos.items()}
                # The last entry in self.observations[i] is already the reset
                # obs of the new episode.
                extra_model_output = tree.map_structure(lambda s: s[env_index], to_env)

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
                    # Make `on_episode_step` callback before finalizing the episode.
                    self._make_on_episode_callback(
                        "on_episode_step", env_index, episodes
                    )
                    done_episodes_to_return.append(episodes[env_index].finalize())

                    # Make `on_episode_end` callback after finalizing the episode.
                    self._make_on_episode_callback(
                        "on_episode_end", env_index, episodes
                    )

                    # Also early-out if we reach the number of episodes within this
                    # for-loop.
                    if eps == num_episodes:
                        break

                    # Create a new episode object.
                    episodes[env_index] = SingleAgentEpisode(
                        observations=[obs[env_index]],
                        infos=[infos[env_index]],
                        render_images=None
                        if render_images[env_index] is None
                        else [render_images[env_index]],
                        observation_space=self.env.single_observation_space,
                        action_space=self.env.single_action_space,
                    )
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
                        render_image=render_images[env_index],
                        extra_model_outputs=extra_model_output,
                    )
                    # Make `on_episode_step` callback.
                    self._make_on_episode_callback(
                        "on_episode_step", env_index, episodes
                    )

        self._done_episodes_for_metrics.extend(done_episodes_to_return)

        # Initialized episodes have to be removed as they lack `extra_model_outputs`.
        samples = [episode for episode in done_episodes_to_return if episode.t > 0]

        return samples

    # TODO (sven): Remove the requirement for EnvRunners to have this
    #  API. Instead Algorithm should compile episode metrics itself via its local
    #  buffer.
    def get_metrics(self) -> List[RolloutMetrics]:
        # Compute per-episode metrics (only on already completed episodes).
        metrics = []
        for eps in self._done_episodes_for_metrics:
            assert eps.is_done
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

        return metrics

    # TODO (sven): Remove the requirement for EnvRunners/RolloutWorkers to have this
    #  API. Replace by proper state overriding via `EnvRunner.set_state()`
    def set_weights(self, weights, global_vars=None, weights_seq_no: int = 0):
        """Writes the weights of our (single-agent) RLModule."""

        if isinstance(weights, dict) and DEFAULT_POLICY_ID in weights:
            weights = weights[DEFAULT_POLICY_ID]
        weights = self._convert_to_tensor(weights)
        self.module.set_state(weights)

        # Check, if an update happened since the last call. See
        # `Algorithm._evaluate_async_with_env_runner`.
        # if self._weights_seq_no == 0 or self._weights_seq_no < weights_seq_no:
        #     # In case of a `StateDict` we have to extract the `
        #     # default_policy`.
        #     # TODO (sven): Handle this probably in `RLModule` as the latter
        #     #  does not need a 'StateDict' in its `set_state()` method
        #     #  as the `keras.Model.base_layer` has weights as `List[TensorType]`.
        #     self._weights_seq_no = weights_seq_no
        #     if isinstance(weights, dict) and DEFAULT_POLICY_ID in weights:
        #         weights = weights[DEFAULT_POLICY_ID]
        #     weights = self._convert_to_tensor(weights)
        #     self.module.set_state(weights)
        # # Otherwise ignore.
        # else:
        #     pass

    def get_weights(self, modules=None):
        """Returns the weights of our (single-agent) RLModule."""

        return self.module.get_state()

    @override(EnvRunner)
    def assert_healthy(self):
        """Checks that self.__init__() has been completed properly.

        Ensures that the instances has a `MultiAgentRLModule` and an
        environment defined.

        Raises:
            AssertionError: If the EnvRunner Actor has NOT been properly initialized.
        """
        # Make sure, we have built our gym.vector.Env and RLModule properly.
        assert self.env and self.module

    @override(EnvRunner)
    def stop(self):
        # Close our env object via gymnasium's API.
        self.env.close()

    def _make_on_episode_callback(self, which: str, idx: int, episodes=None):
        episodes = episodes if episodes is not None else self._episodes
        getattr(self._callbacks, which)(
            episode=episodes[idx],
            env_runner=self,
            env=self.env,
            rl_module=self.module,
            env_index=idx,
        )

    def _convert_to_tensor(self, struct) -> TensorType:
        """Converts structs to a framework-specific tensor."""

        if self.config.framework_str == "torch":
            return convert_to_torch_tensor(struct)
        else:
            return tree.map_structure(tf.convert_to_tensor, struct)

import gymnasium as gym
import logging
import tree

from collections import defaultdict
from functools import partial
from typing import DefaultDict, Dict, List, Optional

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.core import DEFAULT_AGENT_ID, DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module import INFERENCE_ONLY
from ray.rllib.core.rl_module.rl_module import RLModule, SingleAgentRLModuleSpec
from ray.rllib.env.env_context import EnvContext
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.env.utils import _gym_env_creator
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_SAMPLED_LIFETIME,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_EPISODES,
    NUM_MODULE_STEPS_SAMPLED,
    NUM_MODULE_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.spaces.space_utils import unbatch
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import EpisodeID, ModelWeights, ResultDict, TensorType
from ray.tune.registry import ENV_CREATOR, _global_registry
from ray.util.annotations import PublicAPI

_, tf, _ = try_import_tf()
logger = logging.getLogger("ray.rllib")


@PublicAPI(stability="alpha")
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

        self.worker_index = kwargs.get("worker_index")

        # Create a MetricsLogger object for logging custom stats.
        self.metrics = MetricsLogger()
        # Initialize lifetime counts.
        self.metrics.log_value(NUM_ENV_STEPS_SAMPLED_LIFETIME, 0, reduce="sum")

        # Create our callbacks object.
        self._callbacks: DefaultCallbacks = self.config.callbacks_class()

        # Create the vectorized gymnasium env.
        self.env: Optional[gym.Wrapper] = None
        self.num_envs: int = 0
        self.make_env()

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
            module_spec: SingleAgentRLModuleSpec = self.config.rl_module_spec
            module_spec.observation_space = self._env_to_module.observation_space
            # TODO (simon): The `gym.Wrapper` for `gym.vector.VectorEnv` should
            #  actually hold the spaces for a single env, but for boxes the
            #  shape is (1, 1) which brings a problem with the action dists.
            #  shape=(1,) is expected.
            module_spec.action_space = self.env.envs[0].action_space
            module_spec.model_config_dict = self.config.model_config
            # Only load a light version of the module, if available. This is useful
            # if the the module has target or critic networks not needed in sampling
            # or inference.
            # TODO (simon): Once we use `get_marl_module_spec` here, we can remove
            # this line here as the function takes care of this flag.
            module_spec.model_config_dict[INFERENCE_ONLY] = True
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
        self._shared_data = None

        self._done_episodes_for_metrics: List[SingleAgentEpisode] = []
        self._ongoing_episodes_for_metrics: DefaultDict[
            EpisodeID, List[SingleAgentEpisode]
        ] = defaultdict(list)
        self._weights_seq_no: int = 0

    @override(EnvRunner)
    def sample(
        self,
        *,
        num_timesteps: int = None,
        num_episodes: int = None,
        explore: bool = None,
        random_actions: bool = False,
        force_reset: bool = False,
    ) -> List[SingleAgentEpisode]:
        """Runs and returns a sample (n timesteps or m episodes) on the env(s).

        Args:
            num_timesteps: The number of timesteps to sample during this call.
                Note that only one of `num_timetseps` or `num_episodes` may be provided.
            num_episodes: The number of episodes to sample during this call.
                Note that only one of `num_timetseps` or `num_episodes` may be provided.
            explore: If True, will use the RLModule's `forward_exploration()`
                method to compute actions. If False, will use the RLModule's
                `forward_inference()` method. If None (default), will use the `explore`
                boolean setting from `self.config` passed into this EnvRunner's
                constructor. You can change this setting in your config via
                `config.env_runners(explore=True|False)`.
            random_actions: If True, actions will be sampled randomly (from the action
                space of the environment). If False (default), actions or action
                distribution parameters are computed by the RLModule.
            force_reset: Whether to force-reset all (vector) environments before
                sampling. Useful if you would like to collect a clean slate of new
                episodes via this call. Note that when sampling n episodes
                (`num_episodes != None`), this is fixed to True.

        Returns:
            A list of `SingleAgentEpisode` instances, carrying the sampled data.
        """
        assert not (num_timesteps is not None and num_episodes is not None)

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
                self.config.get_rollout_fragment_length(worker_index=self.worker_index)
                * self.num_envs
            )

        # Sample n timesteps.
        if num_timesteps is not None:
            samples = self._sample_timesteps(
                num_timesteps=num_timesteps,
                explore=explore,
                random_actions=random_actions,
                force_reset=force_reset,
            )
        # Sample m episodes.
        elif num_episodes is not None:
            samples = self._sample_episodes(
                num_episodes=num_episodes,
                explore=explore,
                random_actions=random_actions,
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
                )
                total += sum(len(e) for e in episodes)
                samples.extend(episodes)

        # Make the `on_sample_end` callback.
        self._callbacks.on_sample_end(
            env_runner=self,
            metrics_logger=self.metrics,
            samples=samples,
        )

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

        # Have to reset the env (on all vector sub_envs).
        if force_reset or self._needs_initial_reset:
            # Create n new episodes and make the `on_episode_created` callbacks.
            self._episodes = []
            for env_index in range(self.num_envs):
                self._episodes.append(self._new_episode())
                self._make_on_episode_callback("on_episode_created", env_index)
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
                    env_steps_lifetime = self.metrics.peek(
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
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

            ts += self._increase_sampled_metrics(self.num_envs)

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
                    # Make the `on_episode_step` and `on_episode_end` callbacks (before
                    # finalizing the episode object).
                    self._make_on_episode_callback("on_episode_step", env_index)
                    self._make_on_episode_callback("on_episode_end", env_index)

                    # Then finalize (numpy'ize) the episode.
                    done_episodes_to_return.append(self._episodes[env_index].finalize())

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

        # Reset the environment.
        # TODO (simon): Check, if we need here the seed from the config.
        obs, infos = self.env.reset()
        episodes = []
        for env_index in range(self.num_envs):
            episodes.append(self._new_episode())
            self._make_on_episode_callback("on_episode_created", env_index, episodes)
        _shared_data = {}

        for env_index in range(self.num_envs):
            episodes[env_index].add_env_reset(
                observation=obs[env_index],
                infos=infos[env_index],
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
                    env_steps_lifetime = self.metrics.peek(
                        NUM_ENV_STEPS_SAMPLED_LIFETIME
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
            ts += self._increase_sampled_metrics(self.num_envs)

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
                    # Make `on_episode_step` and `on_episode_end` callbacks before
                    # finalizing the episode.
                    self._make_on_episode_callback(
                        "on_episode_step", env_index, episodes
                    )
                    self._make_on_episode_callback(
                        "on_episode_end", env_index, episodes
                    )

                    # Finalize (numpy'ize) the episode.
                    done_episodes_to_return.append(episodes[env_index].finalize())

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
                    # Make `on_episode_step` callback.
                    self._make_on_episode_callback(
                        "on_episode_step", env_index, episodes
                    )

        self._done_episodes_for_metrics.extend(done_episodes_to_return)

        # Initialized episodes have to be removed as they lack `extra_model_outputs`.
        samples = [episode for episode in done_episodes_to_return if episode.t > 0]

        return samples

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
                    episode_duration_s += eps2.get_duration_s()
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

    # TODO (sven): Remove the requirement for EnvRunners/RolloutWorkers to have this
    #  API. Replace by proper state overriding via `EnvRunner.set_state()`
    def set_weights(
        self,
        weights: ModelWeights,
        global_vars: Optional[Dict] = None,
        weights_seq_no: int = 0,
    ) -> None:
        """Writes the weights of our (single-agent) RLModule.

        Args:
            weigths: A dictionary mapping `ModuleID`s to the new weigths to
                be used in the `MultiAgentRLModule` stored in this instance.
            global_vars: An optional global vars dictionary to set this
                worker to. If None, do not update the global_vars.
            weights_seq_no: If needed, a sequence number for the weights version
                can be passed into this method. If not None, will store this seq no
                (in self.weights_seq_no) and in future calls - if the seq no did not
                change wrt. the last call - will ignore the call to save on performance.

        """

        # Only update the weigths, if this is the first synchronization or
        # if the weights of this `EnvRunner` lacks behind the actual ones.
        if weights_seq_no == 0 or self._weights_seq_no < weights_seq_no:
            if isinstance(weights, dict) and DEFAULT_MODULE_ID in weights:
                weights = weights[DEFAULT_MODULE_ID]
            weights = self._convert_to_tensor(weights)
            self.module.set_state(weights)

    def get_weights(self, modules=None, inference_only: bool = False):
        """Returns the weights of our (single-agent) RLModule."""

        return self.module.get_state(inference_only=inference_only)

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

    def make_env(self) -> None:
        """Creates a vectorized gymnasium env and stores it in `self.env`.

        Note that users can change the EnvRunner's config (e.g. change
        `self.config.env_config`) and then call this method to create new environments
        with the updated configuration.
        """
        # If an env already exists, try closing it first (to allow it to properly
        # cleanup).
        if self.env is not None:
            try:
                self.env.close()
            except Exception as e:
                logger.warning(
                    "Tried closing the existing env, but failed with error: "
                    f"{e.args[0]}"
                )

        env_ctx = self.config.env_config
        if not isinstance(env_ctx, EnvContext):
            env_ctx = EnvContext(
                env_ctx,
                worker_index=self.worker_index,
                num_workers=self.config.num_env_runners,
                remote=self.config.remote_worker_envs,
            )

        # Register env for the local context.
        # Note, `gym.register` has to be called on each worker.
        if isinstance(self.config.env, str) and _global_registry.contains(
            ENV_CREATOR, self.config.env
        ):
            entry_point = partial(
                _global_registry.get(ENV_CREATOR, self.config.env),
                env_ctx,
            )

        else:
            entry_point = partial(
                _gym_env_creator,
                env_descriptor=self.config.env,
                env_context=env_ctx,
            )
        gym.register("rllib-single-agent-env-v0", entry_point=entry_point)

        # Wrap into `VectorListInfo`` wrapper to get infos as lists.
        self.env: gym.Wrapper = gym.wrappers.VectorListInfo(
            gym.vector.make(
                "rllib-single-agent-env-v0",
                num_envs=self.config.num_envs_per_env_runner,
                asynchronous=self.config.remote_worker_envs,
            )
        )
        self.num_envs: int = self.env.num_envs
        assert self.num_envs == self.config.num_envs_per_env_runner

        # Set the flag to reset all envs upon the next `sample()` call.
        self._needs_initial_reset = True

        # Call the `on_environment_created` callback.
        self._callbacks.on_environment_created(
            env_runner=self,
            metrics_logger=self.metrics,
            env=self.env,
            env_context=env_ctx,
        )

    @override(EnvRunner)
    def stop(self):
        # Close our env object via gymnasium's API.
        self.env.close()

    def _new_episode(self):
        return SingleAgentEpisode(
            observation_space=self.env.single_observation_space,
            action_space=self.env.single_action_space,
        )

    def _make_on_episode_callback(self, which: str, idx: int, episodes=None):
        episodes = episodes if episodes is not None else self._episodes
        getattr(self._callbacks, which)(
            episode=episodes[idx],
            env_runner=self,
            metrics_logger=self.metrics,
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

    def _increase_sampled_metrics(self, num_steps):
        # Per sample cycle stats.
        self.metrics.log_dict(
            {
                NUM_ENV_STEPS_SAMPLED: num_steps,
                NUM_AGENT_STEPS_SAMPLED: {DEFAULT_AGENT_ID: num_steps},
                NUM_MODULE_STEPS_SAMPLED: {DEFAULT_MODULE_ID: num_steps},
            },
            reduce="sum",
            clear_on_reduce=True,
        )
        # Lifetime stats.
        self.metrics.log_dict(
            {
                NUM_ENV_STEPS_SAMPLED_LIFETIME: num_steps,
                NUM_AGENT_STEPS_SAMPLED_LIFETIME: {DEFAULT_AGENT_ID: num_steps},
                NUM_MODULE_STEPS_SAMPLED_LIFETIME: {
                    DEFAULT_MODULE_ID: num_steps,
                },
            },
            reduce="sum",
        )
        return num_steps

    def _log_episode_metrics(self, length, ret, sec):
        # Log general episode metrics.
        self.metrics.log_dict(
            {
                "episode_len_mean": length,
                "episode_return_mean": ret,
                "episode_duration_sec_mean": sec,
                # Per-agent returns.
                "agent_episode_returns_mean": {DEFAULT_AGENT_ID: ret},
                # Per-RLModule returns.
                "module_episode_returns_mean": {DEFAULT_MODULE_ID: ret},
            },
            # To mimick the old API stack behavior, we'll use `window` here for
            # these particular stats (instead of the default EMA).
            window=self.config.metrics_num_episodes_for_smoothing,
        )
        # For some metrics, log min/max as well.
        self.metrics.log_dict(
            {
                "episode_len_min": length,
                "episode_return_min": ret,
            },
            reduce="min",
        )
        self.metrics.log_dict(
            {
                "episode_len_max": length,
                "episode_return_max": ret,
            },
            reduce="max",
        )

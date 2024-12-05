from collections import defaultdict
from functools import partial
import logging
import time
from typing import Collection, DefaultDict, List, Optional, Union

import gymnasium as gym
from gymnasium.wrappers.vector import DictInfoToList
from gymnasium.envs.registration import VectorizeMode

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.core import (
    COMPONENT_ENV_TO_MODULE_CONNECTOR,
    COMPONENT_MODULE_TO_ENV_CONNECTOR,
    COMPONENT_RL_MODULE,
    DEFAULT_AGENT_ID,
    DEFAULT_MODULE_ID,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.env.env_context import EnvContext
from ray.rllib.env.env_runner import EnvRunner, ENV_STEP_FAILURE
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.env.utils import _gym_env_creator
from ray.rllib.utils.annotations import override
from ray.rllib.utils.checkpoints import Checkpointable
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.framework import try_import_tf
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
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_EPISODES,
    NUM_EPISODES_LIFETIME,
    NUM_MODULE_STEPS_SAMPLED,
    NUM_MODULE_STEPS_SAMPLED_LIFETIME,
    SAMPLE_TIMER,
    TIME_BETWEEN_SAMPLING,
    WEIGHTS_SEQ_NO,
)
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.spaces.space_utils import unbatch
from ray.rllib.utils.typing import EpisodeID, ResultDict, StateDict
from ray.tune.registry import ENV_CREATOR, _global_registry
from ray.util.annotations import PublicAPI

_, tf, _ = try_import_tf()
logger = logging.getLogger("ray.rllib")


# TODO (sven): As soon as RolloutWorker is no longer supported, make `EnvRunner` itself
#  a Checkpointable. Currently, only some of its subclasses are Checkpointables.
@PublicAPI(stability="alpha")
class SingleAgentEnvRunner(EnvRunner, Checkpointable):
    """The generic environment runner for the single agent case."""

    @override(EnvRunner)
    def __init__(self, *, config: AlgorithmConfig, **kwargs):
        """Initializes a SingleAgentEnvRunner instance.

        Args:
            config: An `AlgorithmConfig` object containing all settings needed to
                build this `EnvRunner` class.
        """
        super().__init__(config=config)

        self.worker_index: int = kwargs.get("worker_index")
        self.tune_trial_id: str = kwargs.get("tune_trial_id")

        # Create a MetricsLogger object for logging custom stats.
        self.metrics = MetricsLogger()

        # Create our callbacks object.
        self._callbacks: DefaultCallbacks = self.config.callbacks_class()

        # Create the vectorized gymnasium env.
        self.env: Optional[gym.vector.VectorEnvWrapper] = None
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

        # Create the RLModule.
        self.make_module()

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

        self._time_after_sampling = None

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

        if self._time_after_sampling is not None:
            self.metrics.log_value(
                key=TIME_BETWEEN_SAMPLING,
                value=time.perf_counter() - self._time_after_sampling,
            )

        # Log current weight seq no.
        self.metrics.log_value(
            key=WEIGHTS_SEQ_NO,
            value=self._weights_seq_no,
            window=1,
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
                samples = self._sample(
                    num_timesteps=num_timesteps,
                    explore=explore,
                    random_actions=random_actions,
                    force_reset=force_reset,
                )
            # Sample m episodes.
            elif num_episodes is not None:
                samples = self._sample(
                    num_episodes=num_episodes,
                    explore=explore,
                    random_actions=random_actions,
                )
            # For complete episodes mode, sample as long as the number of timesteps
            # done is smaller than the `train_batch_size`.
            else:
                samples = self._sample(
                    num_episodes=self.num_envs,
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

        return samples

    def _sample(
        self,
        *,
        num_timesteps: Optional[int] = None,
        num_episodes: Optional[int] = None,
        explore: bool,
        random_actions: bool = False,
        force_reset: bool = False,
    ) -> List[SingleAgentEpisode]:
        """Helper method to sample n timesteps or m episodes."""

        done_episodes_to_return: List[SingleAgentEpisode] = []

        # Have to reset the env (on all vector sub_envs).
        if force_reset or num_episodes is not None or self._needs_initial_reset:
            episodes = self._episodes = [None for _ in range(self.num_envs)]
            shared_data = self._shared_data = {}
            self._reset_envs(episodes, shared_data, explore)
            # We just reset the env. Don't have to force this again in the next
            # call to `self._sample_timesteps()`.
            self._needs_initial_reset = False
        else:
            episodes = self._episodes
            shared_data = self._shared_data

        if num_episodes is not None:
            self._needs_initial_reset = True

        # Loop through `num_timesteps` timesteps or `num_episodes` episodes.
        ts = 0
        eps = 0
        while (
            (ts < num_timesteps) if num_timesteps is not None else (eps < num_episodes)
        ):
            # Act randomly.
            if random_actions:
                to_env = {
                    Columns.ACTIONS: self.env.action_space.sample(),
                }
            # Compute an action using the RLModule.
            else:
                # Env-to-module connector (already cached).
                to_module = self._cached_to_module
                assert to_module is not None
                self._cached_to_module = None

                # RLModule forward pass: Explore or not.
                if explore:
                    # Global env steps sampled are (roughly) this EnvRunner's lifetime
                    # count times the number of env runners in the algo.
                    global_env_steps_lifetime = (
                        self.metrics.peek(NUM_ENV_STEPS_SAMPLED_LIFETIME, default=0)
                        + ts
                    ) * (self.config.num_env_runners or 1)
                    to_env = self.module.forward_exploration(
                        to_module, t=global_env_steps_lifetime
                    )
                else:
                    to_env = self.module.forward_inference(to_module)

                # Module-to-env connector.
                to_env = self._module_to_env(
                    rl_module=self.module,
                    batch=to_env,
                    episodes=episodes,
                    explore=explore,
                    shared_data=shared_data,
                )

            # Extract the (vectorized) actions (to be sent to the env) from the
            # module/connector output. Note that these actions are fully ready (e.g.
            # already unsquashed/clipped) to be sent to the environment) and might not
            # be identical to the actions produced by the RLModule/distribution, which
            # are the ones stored permanently in the episode objects.
            actions = to_env.pop(Columns.ACTIONS)
            actions_for_env = to_env.pop(Columns.ACTIONS_FOR_ENV, actions)
            # Try stepping the environment.
            results = self._try_env_step(actions_for_env)
            if results == ENV_STEP_FAILURE:
                return self._sample(
                    num_timesteps=num_timesteps,
                    num_episodes=num_episodes,
                    explore=explore,
                    random_actions=random_actions,
                    force_reset=True,
                )
            observations, rewards, terminateds, truncateds, infos = results
            observations, actions = unbatch(observations), unbatch(actions)

            call_on_episode_start = set()
            for env_index in range(self.num_envs):
                extra_model_output = {k: v[env_index] for k, v in to_env.items()}
                extra_model_output[WEIGHTS_SEQ_NO] = self._weights_seq_no

                # Episode has no data in it yet -> Was just reset and needs to be called
                # with its `add_env_reset()` method.
                if not self._episodes[env_index].is_reset:
                    episodes[env_index].add_env_reset(
                        observation=observations[env_index],
                        infos=infos[env_index],
                    )
                    call_on_episode_start.add(env_index)

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
                        extra_model_outputs=extra_model_output,
                    )

            # Env-to-module connector pass (cache results as we will do the RLModule
            # forward pass only in the next `while`-iteration.
            if self.module is not None:
                self._cached_to_module = self._env_to_module(
                    episodes=episodes,
                    explore=explore,
                    rl_module=self.module,
                    shared_data=shared_data,
                )

            for env_index in range(self.num_envs):
                # Call `on_episode_start()` callback (always after reset).
                if env_index in call_on_episode_start:
                    self._make_on_episode_callback(
                        "on_episode_start", env_index, episodes
                    )
                # Make the `on_episode_step` callbacks.
                else:
                    self._make_on_episode_callback(
                        "on_episode_step", env_index, episodes
                    )

                # Episode is done.
                if episodes[env_index].is_done:
                    eps += 1

                    # Make the `on_episode_end` callbacks (before finalizing the episode
                    # object).
                    self._make_on_episode_callback(
                        "on_episode_end", env_index, episodes
                    )

                    # Then finalize (numpy'ize) the episode.
                    done_episodes_to_return.append(episodes[env_index].finalize())

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
        self._done_episodes_for_metrics.extend(done_episodes_to_return)
        # ... and all ongoing episode chunks.

        # Also, make sure we start new episode chunks (continuing the ongoing episodes
        # from the to-be-returned chunks).
        ongoing_episodes_to_return = []
        # Only if we are doing individual timesteps: We have to maybe cut an ongoing
        # episode and continue building it on the next call to `sample()`.
        if num_timesteps is not None:
            ongoing_episodes_continuations = [
                eps.cut(len_lookback_buffer=self.config.episode_lookback_horizon)
                for eps in self._episodes
            ]

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

        self._increase_sampled_metrics(ts, len(done_episodes_to_return))

        # Return collected episode data.
        return done_episodes_to_return + ongoing_episodes_to_return

    @override(EnvRunner)
    def get_spaces(self):
        return {
            INPUT_ENV_SPACES: (self.env.observation_space, self.env.action_space),
            DEFAULT_MODULE_ID: (
                self._env_to_module.observation_space,
                self.env.single_action_space,
            ),
        }

    @override(EnvRunner)
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

        # Now that we have logged everything, clear cache of done episodes.
        self._done_episodes_for_metrics.clear()

        # Return reduced metrics.
        return self.metrics.reduce()

    @override(Checkpointable)
    def get_state(
        self,
        components: Optional[Union[str, Collection[str]]] = None,
        *,
        not_components: Optional[Union[str, Collection[str]]] = None,
        **kwargs,
    ) -> StateDict:
        state = {
            NUM_ENV_STEPS_SAMPLED_LIFETIME: (
                self.metrics.peek(NUM_ENV_STEPS_SAMPLED_LIFETIME, default=0)
            ),
        }

        if self._check_component(COMPONENT_RL_MODULE, components, not_components):
            state[COMPONENT_RL_MODULE] = self.module.get_state(
                components=self._get_subcomponents(COMPONENT_RL_MODULE, components),
                not_components=self._get_subcomponents(
                    COMPONENT_RL_MODULE, not_components
                ),
                **kwargs,
            )
            state[WEIGHTS_SEQ_NO] = self._weights_seq_no
        if self._check_component(
            COMPONENT_ENV_TO_MODULE_CONNECTOR, components, not_components
        ):
            state[COMPONENT_ENV_TO_MODULE_CONNECTOR] = self._env_to_module.get_state()
        if self._check_component(
            COMPONENT_MODULE_TO_ENV_CONNECTOR, components, not_components
        ):
            state[COMPONENT_MODULE_TO_ENV_CONNECTOR] = self._module_to_env.get_state()

        return state

    @override(Checkpointable)
    def set_state(self, state: StateDict) -> None:
        if COMPONENT_ENV_TO_MODULE_CONNECTOR in state:
            self._env_to_module.set_state(state[COMPONENT_ENV_TO_MODULE_CONNECTOR])
        if COMPONENT_MODULE_TO_ENV_CONNECTOR in state:
            self._module_to_env.set_state(state[COMPONENT_MODULE_TO_ENV_CONNECTOR])

        # Update the RLModule state.
        if COMPONENT_RL_MODULE in state:
            # A missing value for WEIGHTS_SEQ_NO or a value of 0 means: Force the
            # update.
            weights_seq_no = state.get(WEIGHTS_SEQ_NO, 0)

            # Only update the weigths, if this is the first synchronization or
            # if the weights of this `EnvRunner` lacks behind the actual ones.
            if weights_seq_no == 0 or self._weights_seq_no < weights_seq_no:
                rl_module_state = state[COMPONENT_RL_MODULE]
                if (
                    isinstance(rl_module_state, dict)
                    and DEFAULT_MODULE_ID in rl_module_state
                ):
                    rl_module_state = rl_module_state[DEFAULT_MODULE_ID]
                self.module.set_state(rl_module_state)

            # Update our weights_seq_no, if the new one is > 0.
            if weights_seq_no > 0:
                self._weights_seq_no = weights_seq_no

        # Update our lifetime counters.
        if NUM_ENV_STEPS_SAMPLED_LIFETIME in state:
            self.metrics.set_value(
                key=NUM_ENV_STEPS_SAMPLED_LIFETIME,
                value=state[NUM_ENV_STEPS_SAMPLED_LIFETIME],
                reduce="sum",
                with_throughput=True,
            )

    @override(Checkpointable)
    def get_ctor_args_and_kwargs(self):
        return (
            (),  # *args
            {"config": self.config},  # **kwargs
        )

    @override(Checkpointable)
    def get_metadata(self):
        metadata = Checkpointable.get_metadata(self)
        metadata.update(
            {
                # TODO (sven): Maybe add serialized (JSON-writable) config here?
            }
        )
        return metadata

    @override(Checkpointable)
    def get_checkpointable_components(self):
        return [
            (COMPONENT_RL_MODULE, self.module),
            (COMPONENT_ENV_TO_MODULE_CONNECTOR, self._env_to_module),
            (COMPONENT_MODULE_TO_ENV_CONNECTOR, self._module_to_env),
        ]

    @override(EnvRunner)
    def assert_healthy(self):
        """Checks that self.__init__() has been completed properly.

        Ensures that the instances has a `MultiRLModule` and an
        environment defined.

        Raises:
            AssertionError: If the EnvRunner Actor has NOT been properly initialized.
        """
        # Make sure, we have built our gym.vector.Env and RLModule properly.
        assert self.env and hasattr(self, "module")

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

        # No env provided -> Error.
        if not self.config.env:
            raise ValueError(
                "`config.env` is not provided! You should provide a valid environment "
                "to your config through `config.environment([env descriptor e.g. "
                "'CartPole-v1'])`."
            )
        # Register env for the local context.
        # Note, `gym.register` has to be called on each worker.
        elif isinstance(self.config.env, str) and _global_registry.contains(
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

        self.env = DictInfoToList(
            gym.make_vec(
                "rllib-single-agent-env-v0",
                num_envs=self.config.num_envs_per_env_runner,
                vectorization_mode=(
                    VectorizeMode.ASYNC
                    if self.config.remote_worker_envs
                    else VectorizeMode.SYNC
                ),
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
            env=self.env.unwrapped,
            env_context=env_ctx,
        )

    @override(EnvRunner)
    def make_module(self):
        try:
            module_spec: RLModuleSpec = self.config.get_rl_module_spec(
                env=self.env.unwrapped, spaces=self.get_spaces(), inference_only=True
            )
            # Build the module from its spec.
            self.module = module_spec.build()
        # If `AlgorithmConfig.get_rl_module_spec()` is not implemented, this env runner
        # will not have an RLModule, but might still be usable with random actions.
        except NotImplementedError:
            self.module = None

    @override(EnvRunner)
    def stop(self):
        # Close our env object via gymnasium's API.
        self.env.close()

    def _reset_envs(self, episodes, shared_data, explore):
        # Create n new episodes and make the `on_episode_created` callbacks.
        for env_index in range(self.num_envs):
            self._new_episode(env_index, episodes)

        # Erase all cached ongoing episodes (these will never be completed and
        # would thus never be returned/cleaned by `get_metrics` and cause a memory
        # leak).
        self._ongoing_episodes_for_metrics.clear()

        # Try resetting the environment.
        # TODO (simon): Check, if we need here the seed from the config.
        observations, infos = self._try_env_reset()
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
            self._cached_to_module = self._env_to_module(
                rl_module=self.module,
                episodes=episodes,
                explore=explore,
                shared_data=shared_data,
            )

        # Call `on_episode_start()` callbacks (always after reset).
        for env_index in range(self.num_envs):
            self._make_on_episode_callback("on_episode_start", env_index, episodes)

    def _new_episode(self, env_index, episodes=None):
        episodes = episodes if episodes is not None else self._episodes
        episodes[env_index] = SingleAgentEpisode(
            observation_space=self.env.single_observation_space,
            action_space=self.env.single_action_space,
        )
        self._make_on_episode_callback("on_episode_created", env_index, episodes)

    def _make_on_episode_callback(self, which: str, idx: int, episodes):
        getattr(self._callbacks, which)(
            episode=episodes[idx],
            env_runner=self,
            metrics_logger=self.metrics,
            env=self.env.unwrapped,
            rl_module=self.module,
            env_index=idx,
        )

    def _increase_sampled_metrics(self, num_steps, num_episodes_completed):
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
        self.metrics.log_value(
            NUM_EPISODES,
            num_episodes_completed,
            reduce="sum",
            clear_on_reduce=True,
        )
        # Lifetime stats.
        self.metrics.log_value(
            NUM_ENV_STEPS_SAMPLED_LIFETIME,
            num_steps,
            reduce="sum",
            with_throughput=True,
        )
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
        self.metrics.log_value(
            NUM_EPISODES_LIFETIME,
            num_episodes_completed,
            reduce="sum",
        )
        return num_steps

    def _log_episode_metrics(self, length, ret, sec):
        # Log general episode metrics.
        # To mimic the old API stack behavior, we'll use `window` here for
        # these particular stats (instead of the default EMA).
        win = self.config.metrics_num_episodes_for_smoothing
        self.metrics.log_value(EPISODE_LEN_MEAN, length, window=win)
        self.metrics.log_value(EPISODE_RETURN_MEAN, ret, window=win)
        self.metrics.log_value(EPISODE_DURATION_SEC_MEAN, sec, window=win)
        # Per-agent returns.
        self.metrics.log_value(
            ("agent_episode_returns_mean", DEFAULT_AGENT_ID), ret, window=win
        )
        # Per-RLModule returns.
        self.metrics.log_value(
            ("module_episode_returns_mean", DEFAULT_MODULE_ID), ret, window=win
        )

        # For some metrics, log min/max as well.
        self.metrics.log_value(EPISODE_LEN_MIN, length, reduce="min", window=win)
        self.metrics.log_value(EPISODE_RETURN_MIN, ret, reduce="min", window=win)
        self.metrics.log_value(EPISODE_LEN_MAX, length, reduce="max", window=win)
        self.metrics.log_value(EPISODE_RETURN_MAX, ret, reduce="max", window=win)

    @Deprecated(
        new="SingleAgentEnvRunner.get_state(components='rl_module')",
        error=True,
    )
    def get_weights(self, *args, **kwargs):
        pass

    @Deprecated(new="SingleAgentEnvRunner.set_state()", error=True)
    def set_weights(self, *args, **kwargs):
        pass

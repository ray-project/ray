import gymnasium as gym

from collections import defaultdict
from functools import partial
from typing import DefaultDict, Dict, List, Optional

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.core.rl_module.marl_module import (
    ModuleID,
    MultiAgentRLModule,
    MultiAgentRLModuleSpec,
)
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.env.utils import _gym_env_creator
from ray.rllib.evaluation.metrics import RolloutMetrics
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import ExperimentalAPI, override
from ray.rllib.utils.typing import ModelWeights
from ray.tune.registry import ENV_CREATOR, _global_registry


@ExperimentalAPI
class MultiAgentEnvRunner(EnvRunner):
    """The genetic environment runner for the multi-agent case."""

    @override(EnvRunner)
    def __init__(self, config: AlgorithmConfig, **kwargs):
        """Initializes a MultiAgentEnvRunner instance.

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
        gym.register(
            "rllib-multi-agent-env-runner-v0",
            entry_point=entry_point,
            disable_env_checker=True,
        )

        # Wrap into `VectorListInfo`` wrapper to get infos as lists.
        self.env = gym.make("rllib-multi-agent-env-runner-v0")

        # Create the vectorized gymnasium env.
        assert isinstance(self.env.unwrapped, MultiAgentEnv), (
            "ERROR: When using the `MultiAgentEnvRunner` the environment needs "
            "to inherit from `ray.rllib.env.multi_agent_env.MultiAgentEnv`."
        )

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

        self._make_module()

        # Create the two connector pipelines: env-to-module and module-to-env.
        self._module_to_env = self.config.build_module_to_env_connector(self.env)

        # This should be the default.
        self._needs_initial_reset: bool = True
        self._episode: Optional[MultiAgentEpisode] = None

        self._done_episodes_for_metrics: List[MultiAgentEpisode] = []
        self._ongoing_episodes_for_metrics: DefaultDict[
            List[MultiAgentEpisode]
        ] = defaultdict(list)
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
    ) -> List[MultiAgentEpisode]:
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

        # If no execution details are provided, use the config.
        if num_timesteps is None and num_episodes is None:
            if self.config.batch_mode == "truncate_episodes":
                num_timesteps = self.config.get_rollout_fragment_length(
                    worker_index=self.worker_index,
                )
            else:
                num_episodes = 1

        # Sample n timesteps.
        if num_timesteps is not None:
            return self._sample_timesteps(
                num_timesteps=num_timesteps,
                explore=explore,
                random_actions=random_actions,
                force_reset=False,
            )
        # Sample m episodes.
        else:
            return self._sample_episodes(
                num_episodes=num_episodes,
                explore=explore,
                random_actions=random_actions,
                with_render_data=with_render_data,
            )

    def _sample_timesteps(
        self,
        num_timesteps: int,
        explore: bool = True,
        random_actions: bool = False,
        force_reset: bool = False,
    ) -> List[MultiAgentEpisode]:
        """Helper method to sample n timesteps.

        Args:
            num_timesteps: int. Number of timesteps to sample during rollout.
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
        done_episodes_to_return: List[MultiAgentEpisode] = []

        # Have to reset the env.
        if force_reset or self._needs_initial_reset:
            # Create n new episodes and make the `on_episode_created` callbacks.
            self._episode = MultiAgentEpisode()
            self._make_on_episode_callback("on_episode_created")

            # Reset the environment.
            # TODO (simon): Check, if we need here the seed from the config.
            obs, infos = self.env.reset()
            self._cached_to_module = None

            # Call `on_episode_start()` callbacks.
            self._make_on_episode_callback("on_episode_start")

            # We just reset the env. Don't have to force this again in the next
            # call to `self._sample_timesteps()`.
            self._needs_initial_reset = False

            # Set the initial observations in the episodes.
            self._episode.add_env_reset(observations=obs, infos=infos)

        # TODO (sven): figure out how often we should reset the shared data dict.
        shared_data = {
            "agent_to_module_mapping_fn": self.config.policy_mapping_fn,
        }

        # Loop through timesteps.
        ts = 0

        while ts < num_timesteps:
            # Act randomly.
            if random_actions:
                # Note, to get sampled actions from all agents' action
                # spaces we need to call `MultiAgentEnv.action_space_sample()`.
                if self.env.unwrapped._action_space_in_preferred_format:
                    actions = self.env.action_space.sample()
                # Otherwise, `action_space_sample()` needs to be implemented.
                else:
                    actions = self.env.action_space_sample()
                # Remove all actions for agents that had no observation.
                to_env = {
                    SampleBatch.ACTIONS: {
                        agent_id: agent_action
                        for agent_id, agent_action in actions.items()
                        if agent_id in self._episode.get_agents_to_act()
                    }
                }
            # Compute an action using the RLModule.
            else:
                to_module = self._cached_to_module or self._env_to_module(
                    rl_module=self.module,
                    episodes=[self._episode],
                    explore=explore,
                    shared_data=shared_data,
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
                    episodes=[self._episode],
                    explore=explore,
                    shared_data=shared_data,
                )

            # Extract the (vectorized) actions from the module/connector output.
            actions = to_env.pop(SampleBatch.ACTIONS)

            # TODO (sven): [0] = actions is vectorized, but env is NOT a vector Env.
            #  Support vectorized multi-agent envs.
            obs, rewards, terminateds, truncateds, infos = self.env.step(actions[0])

            # TODO (sven, simon): We have to record these steps somewhere.
            # TODO: Refactor into multiagent-episode sth. like `get_agent_steps()`.
            # TODO: When env is vectorized, need to change this to:
            #  env_steps += len(obs)  # <- vectorized observations
            #  agent_steps += sum([len(obs) for o in obs])
            # We count by environment steps ..
            if self.config.count_steps_by == "env_steps":
                ts += 1
            # .. or by agent steps.
            else:
                ts += len(obs)

            # TODO (sven): This simple approach to re-map `to_env` from a
            #  dict[col, List[MADict]] to a dict[agentID, MADict] would not work for
            #  a vectorized env.
            extra_model_outputs = defaultdict(dict)
            for col, ma_dict_list in to_env.items():
                # TODO (sven): Support vectorized MA env.
                ma_dict = ma_dict_list[0]
                for agent_id, val in ma_dict.items():
                    extra_model_outputs[agent_id][col] = val
            extra_model_outputs = dict(extra_model_outputs)

            # Record the timestep in the episode instance.
            self._episode.add_env_step(
                obs,
                actions[0],
                rewards,
                infos=infos,
                terminateds=terminateds,
                truncateds=truncateds,
                extra_model_outputs=extra_model_outputs,
            )

            # Make the `on_episode_step` callback.
            self._make_on_episode_callback("on_episode_step")

            # TODO (sven, simon): We have to check, if we need this elaborate
            # function here or if the `MultiAgentEnv` defines the cases that
            # can happen.
            # Right now we have:
            #   1. Most times only agents that step get `terminated`, `truncated`
            #       i.e. the rest we have to check in the episode.
            #   2. There are edge cases like, some agents terminated, all others
            #       truncated and vice versa.
            # See also `MultiAgentEpisode` for handling the `__all__`.
            if self._all_agents_done(terminateds, truncateds):
                # Finalize (numpy'ize) the episode.
                self._episode.finalize()
                done_episodes_to_return.append(self._episode)
                # Create a new episode instance.
                self._episode = MultiAgentEpisode()
                # Reset the environment.
                obs, infos = self.env.reset()
                # Add initial observations and infos.
                self._episode.add_env_reset(observations=obs, infos=infos)

        # Already perform env-to-module connector call for next call to
        # `_sample_timesteps()`. See comment in c'tor for `self._cached_to_module`.
        # TODO (sven): Figure out shared_data problem. We will have to store that as
        #  well (in between `self._sample_timesteps()` calls).
        # if self.module is not None:
        #    self._cached_to_module = self._env_to_module(
        #        rl_module=self.module,
        #        episodes=[self._episode],
        #        explore=explore,
        #        shared_data=shared_data,
        #    )

        # Store done episodes for metrics.
        self._done_episodes_for_metrics.extend(done_episodes_to_return)

        # Also, make sure we start new episode chunks (continuing the ongoing episodes
        # from the to-be-returned chunks).
        ongoing_episode_continuation = self._episode.cut(
            len_lookback_buffer=self.config.episode_lookback_horizon
        )

        ongoing_episodes_to_return = []
        # Just started Episodes do not have to be returned. There is no data
        # in them anyway.
        if self._episode.env_t > 0:
            self._episode.validate()
            self._ongoing_episodes_for_metrics[self._episode.id_].append(self._episode)
            # Return finalized (numpy'ized) Episodes.
            ongoing_episodes_to_return.append(self._episode.finalize())

        # Continue collecting into the cut Episode chunk.
        self._episode = ongoing_episode_continuation

        # Return collected episode data.
        return done_episodes_to_return + ongoing_episodes_to_return

    def _sample_episodes(
        self,
        num_episodes: int,
        explore: bool = True,
        random_actions: bool = False,
        with_render_data: bool = False,
    ) -> List[MultiAgentEpisode]:
        """Helper method to run n episodes.

        See docstring of `self.sample()` for more details.
        """
        # If user calls sample(num_timesteps=..) after this, we must reset again
        # at the beginning.
        self._needs_initial_reset = True

        done_episodes_to_return: List[MultiAgentEpisode] = []

        # Reset the environment.
        # TODO (simon): Check, if we need here the seed from the config.
        obs, infos = self.env.reset()

        # Create a new multi-agent episode.
        _episode = MultiAgentEpisode()
        self._make_on_episode_callback("on_episode_created", _episode)

        # Initialize image rendering if needed.
        render_image = None
        if with_render_data:
            render_image = self.env.render()

        # Set initial obs and infos in the episodes.
        _episode.add_env_reset(observations=obs, infos=infos, render_image=render_image)
        self._make_on_episode_callback("on_episode_start", _episode)

        # TODO (sven): figure out how often we should reset the shared data dict.
        shared_data = {
            "agent_to_module_mapping_fn": self.config.policy_mapping_fn,
        }

        # Loop over episodes.
        eps = 0
        while eps < num_episodes:
            # Act randomly.
            if random_actions:
                # Note, to get sampled actions from all agents' action
                # spaces we need to call `MultiAgentEnv.action_space_sample()`.
                if self.env.unwrapped._action_space_in_preferred_format:
                    actions = self.env.action_space.sample()
                # Otherwise, `action_space_sample()` needs to be implemented.
                else:
                    actions = self.env.action_space_sample()
                # Remove all actions for agents that had no observation.
                to_env = {
                    SampleBatch.ACTIONS: {
                        agent_id: agent_action
                        for agent_id, agent_action in actions.items()
                        if agent_id in _episode.get_agents_to_act()
                    },
                }
            # Compute an action using the RLModule.
            else:
                to_module = self._env_to_module(
                    rl_module=self.module,
                    episodes=[_episode],
                    explore=explore,
                    shared_data=shared_data,
                )

                # Explore or not.
                if explore:
                    to_env = self.module.forward_exploration(to_module)
                else:
                    to_env = self.module.forward_inference(to_module)

                to_env = self._module_to_env(
                    rl_module=self.module,
                    data=to_env,
                    episodes=[_episode],
                    explore=explore,
                    shared_data=shared_data,
                )

            # Step the environment.
            actions = to_env.pop(SampleBatch.ACTIONS)

            obs, rewards, terminateds, truncateds, infos = self.env.step(actions[0])

            # Add render data if needed.
            if with_render_data:
                render_image = self.env.render()

            # TODO (sven): This simple approach to re-map `to_env` from a
            #  dict[col, List[MADict]] to a dict[agentID, MADict] would not work for
            #  a vectorized env.
            extra_model_outputs = defaultdict(dict)
            for col, ma_dict_list in to_env.items():
                # TODO (sven): Support vectorized MA env.
                ma_dict = ma_dict_list[0]
                for agent_id, val in ma_dict.items():
                    extra_model_outputs[agent_id][col] = val
            extra_model_outputs = dict(extra_model_outputs)

            # Record the timestep in the episode instance.
            _episode.add_env_step(
                obs,
                actions[0],
                rewards,
                infos=infos,
                terminateds=terminateds,
                truncateds=truncateds,
                extra_model_outputs=extra_model_outputs,
                render_image=render_image,
            )

            # Make `on_episode_step` callback before finalizing the episode.
            self._make_on_episode_callback("on_episode_step", _episode)

            # TODO (sven, simon): We have to check, if we need this elaborate
            # function here or if the `MultiAgentEnv` defines the cases that
            # can happen.
            # Right now we have:
            #   1. Most times only agents that step get `terminated`, `truncated`
            #       i.e. the rest we have to check in the episode.
            #   2. There are edge cases like, some agents terminated, all others
            #       truncated and vice versa.
            # See also `MultiAgentEpisode` for handling the `__all__`.
            if self._all_agents_done(terminateds, truncateds, episode=_episode):
                # Increase episode count.
                eps += 1

                # Finish the episode.
                done_episodes_to_return.append(_episode.finalize())

                # Make `on_episode_end` callback after finalizing the episode.
                self._make_on_episode_callback("on_episode_end", _episode)

                # Also early-out if we reach the number of episodes within this
                # for-loop.
                if eps == num_episodes:
                    break

                # Create a new episode instance.
                _episode = MultiAgentEpisode()
                self._make_on_episode_callback("on_episode_created", _episode)

                # Reset the environment.
                obs, infos = self.env.reset()
                # Add initial observations and infos.
                _episode.add_env_reset(observations=obs, infos=infos)

                # Make `on_episode_start` callback.
                self._make_on_episode_callback("on_episode_start", _episode)

        self._done_episodes_for_metrics.extend(done_episodes_to_return)

        return done_episodes_to_return

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

    def set_weights(
        self,
        weights: Dict[ModuleID, ModelWeights],
        global_vars: Optional[Dict] = None,
        weights_seq_no: int = 0,
    ) -> None:
        """Writes the weights of our multi-agent `RLModule`

        Args:
            weigths: A dictionary mapping `ModuleID`s to the new weigths to
                be used in the `MultiAgentRLModule` stored in this instance.
            global_vars: An optional global vars dictionary to set this
                worker to. If None, do not update the global_vars.
            weights_seq_no: If needed, a sequence number for the weights version
                can be passed into this method. If not None, will store this seq no
                (in self.weights_seq_no) and in future calls - if the seq no did not
                change wrt. the last call - will ignore the call to save on performance.

        .. testcode::
            :skipif: True

            from ray.rllib.env import MultiAgentEnvRunner
            # Create an `MultiAgentEnvRunner`.
            worker = ...
            weights = worker.get_weights()
            # Set `global_vars` (timestep) as well.
            worker.set_weights(weights, {"timestep": 42})
        """

        self.module.set_state(weights)

    def get_weights(self, modules=None) -> Dict[ModuleID, ModelWeights]:
        """Returns the weights of our multi-agent `RLModule`.

        Args:
            modules: `ModuleID`s for which to return the weights. If `None`
                weigths for all modules are returned. See for details
                `MultiAgentRLModule.get_state()`.

        Returns:
            A dictionary mapping `ModuleID`s to their corresponding weights.
        """

        return self.module.get_state(module_ids=modules)

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
        """Closes this `EnvRunner` by running necessary closing operations."""

        # Note, `MultiAgentEnv` inherits `close()`-method from `gym.Env`.
        self.env.close()

    def _make_module(self):
        # Create our own instance of the (single-agent) `RLModule` (which
        # the needs to be weight-synched) each iteration.
        # TODO (sven, simon): We have to rebuild the `AlgorithmConfig` to work on
        # `RLModule`s and not `Policy`s. Like here `policies`->`modules`
        try:
            policy_dict, _ = self.config.get_multi_agent_setup(env=self.env)
            ma_rlm_spec: MultiAgentRLModuleSpec = self.config.get_marl_module_spec(
                policy_dict=policy_dict
            )

            # Build the module from its spec.
            self.module: MultiAgentRLModule = ma_rlm_spec.build()

        # This error could be thrown, when only random actions are used.
        except NotImplementedError:
            self.module = None

    def _make_on_episode_callback(self, which: str, episode=None):
        episode = episode if episode is not None else self._episode
        getattr(self._callbacks, which)(
            episode=episode,
            env_runner=self,
            env=self.env,
            rl_module=self.module,
            env_index=0,
        )

    def _all_agents_done(self, terminateds, truncateds, episode=None):
        """Determines, if all agents are either terminated or truncated

        Note, this is not determined by the `__all__` in an `MultiAgentEnv`
        as this does not cover the case, if some agents are truncated and
        all the others are terminated and vice versa.

        Args:
            terminateds: dict. A dictionary mapping an agent id to a
                corresponding boolean indicating if the agent is terminated.
            truncateds: dict. A dictionary mapping an agent id to a
                corresponding boolean indicating if the agent is truncated.

        Returns:
            A boolean indicating if all agents are done.
        """
        episode = episode or self._episode

        # CASE 1: all agents are terminated or all are truncated.
        if terminateds["__all__"] or truncateds["__all__"]:
            return True
        # TODO (simon): Refactor into `MultiAgentEpisode`.
        # Find all agents that were done at prior timesteps.
        agents_done = [
            agent_id
            for agent_id, agent_eps in episode.agent_episodes.items()
            if agent_eps.is_done
        ]
        # Add the agents that are done at the present timestep.
        agents_done += [agent_id for agent_id in terminateds if terminateds[agent_id]]
        agents_done += [agent_id for agent_id in truncateds if truncateds[agent_id]]
        # CASE 2: some agents are truncated and the others are terminated.
        if all(agent_id in set(agents_done) for agent_id in episode.agent_ids):
            return True
        # CASE 3: there are still some agents alive.
        else:
            return False

import gymnasium as gym
import numpy as np
import tree

from collections import defaultdict
from functools import partial
from typing import Dict, List, Tuple, TYPE_CHECKING, Union

from ray.rllib.core.models.base import STATE_IN, STATE_OUT
from ray.rllib.core.rl_module.marl_module import (
    MultiAgentRLModule,
    MultiAgentRLModuleSpec,
)
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.utils import _gym_env_creator
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import MultiAgentDict, TensorStructType, TensorType
from ray.tune.registry import ENV_CREATOR, _global_registry

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

    # TODO (sven): This gives a tricky circular import that goes
    #  deep into the library. We have to see, where to dissolve it.
    from ray.rllib.env.multi_agent_episode import MultiAgentEpisode

_, tf, _ = try_import_tf()
torch, nn = try_import_torch()


class MultiAgentEnvRunner(EnvRunner):
    """The genetic environment runner for the multi agent case."""

    @override(EnvRunner)
    def __init__(self, config: "AlgorithmConfig", **kwargs):
        super().__init__(config=config)

        # Get the worker index on which this instance is running.
        self.worker_index: int = kwargs.get("worker_index")

        # TODO (simon): INstantiate an error in `Algorithm.validate()`
        # if `num_envs_per_worker > 1` and `is_multi_agent==True`.

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
        self.env = gym.make(
            "rllib-multi-agent-env-runner-v0",
        )

        # Create the vectorized gymnasium env.
        assert isinstance(self.env.unwrapped, MultiAgentEnv), (
            "ERROR: When using the `MultiAgentEnvRunner` the environment needs "
            "to inherit from `ray.rllib.env.multi_agent_env.MultiAgentEnv`."
        )
        # TODO (sven, simon): Let's decide, if we go with this requirement or instead
        # automatically wrap the environment to get preferred format.
        # assert self.env.unwrapped._obs_space_in_preferred_format, (
        #     "ERROR: When using the `MultiAgentEnvRunner` the environment needs "
        #     "to be in 'preferred format', i.e. the observation and action spaces "
        #     "have to be `gymnasium.spaces.Dict` with all agent ids present in keys."
        # )

        self.agent_ids: List[Union[str, int]] = self.env.get_agent_ids()

        # Create our own instance of the (single-agent) `RLModule` (which
        # the needs to be weight-synched) each iteration.
        # TODO (sven, simon): We have to rebuild the `AlgorithmConfig` to work on
        # `RLModule`s and not `Policy`s. Like here `policies`->`modules`
        module_spec: MultiAgentRLModuleSpec = self.config.get_marl_module_spec(
            policy_dict=config.policies
        )

        # TODO (simon): The `gym.Wrapper` for `gym.vector.VectorEnv` should
        #  actually hold the spaces for a single env, but for boxes the
        #  shape is (1, 1) which brings a problem with the action dists.
        #  shape=(1,) is expected.
        module_spec.action_space = self.env.action_space
        module_spec.observation_space = self.env.observation_space
        # Set action and observation spaces for all module specs.
        for agent_id, agent_module_spec in module_spec.module_specs.items():
            # Note, `MultiAgentEnv` has a preferred format of spaces, i.e.
            # a mapping from agent ids to spaces.
            # If the action space is a mapping from agent ids to spaces.
            if self.env.unwrapped._action_space_in_preferred_format:
                agent_module_spec.action_space = self.env.action_space[agent_id]
            # Otherwise, use the same space for each agent.
            else:
                agent_module_spec.action_space = self.env.action_space
            # Similar to the action spaces, observation spaces could be described
            # by a mapping.
            if self.env.unwrapped._obs_space_in_preferred_format:
                agent_module_spec.observation_space = self.env.observation_space[
                    agent_id
                ]
            # Otherwise, use the same space for all agents.
            else:
                agent_module_spec.observation_space = self.env.observation_space

        # Build the module from its spec.
        self.module: MultiAgentRLModule = module_spec.build()

        # This should be the default.
        self._needs_initial_reset: bool = True
        self._episode: "MultiAgentEpisode" = None

        self._done_episodes_for_metrics: List["MultiAgentEpisode"] = []
        self._ongoing_episodes_for_metrics: Dict[List] = defaultdict(list)
        self._ts_since_last_metrics: int = 0
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
    ) -> List["MultiAgentEpisode"]:
        """Runs and returns a sample (n timesteps or m episodes) on the env(s)."""

        # If npt execution details are provided, use the configf.
        if num_timesteps is None and num_episodes is None:
            if self.config.batch_mode == "truncate_episodes":
                num_timesteps = (
                    self.config.get_rollout_fragment_length(
                        worker_index=self.worker_index,
                    )
                    * self.num_envs
                )
            else:
                num_episodes = self.num_envs

        # Sample n timesteps
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
    ) -> List["MultiAgentEpisode"]:
        """Helper method to sample n timesteps."""

        # TODO (sven): This gives a tricky circular import that goes
        # deep into the library. We have to see, where to dissolve it.
        from ray.rllib.env.multi_agent_episode import MultiAgentEpisode

        done_episodes_to_return: List["MultiAgentEpisode"] = []

        # Get the initial states for all modules. Note, `get_initial_state()`
        # returns an empty dictionary, if no initial states are defined.
        # TODO (sven, simon): We could simply use `MARLModule._run_forward_pass()`
        # to get also an initial state for all modules. Maybe this can be
        # added to the MARLModule.
        initial_states = {
            agent_id: self.module[agent_id].get_initial_state()
            for agent_id in self.module.keys()
        }

        # Have to reset the env (on all vector sub-environments).
        if force_reset or self._needs_initial_reset:
            # Reset the environment.
            obs, info = self.env.reset()

            # We just reset the environment. We do not have to force this again
            # in the next call so `self._sample_timesteps()`.
            self._needs_initial_reset = False

            self._episode = MultiAgentEpisode(agent_ids=self.agent_ids)

            # Set the initial observations in the episodes.
            # TODO (sven): maybe move this into connector pipeline (even
            # if automated).
            self._episode.add_initial_observation(
                initial_observation=obs, initial_info=info
            )

        # Do not reset environments, but instead continue in already started episodes.
        else:
            # Pick up stored observations from previous timesteps.
            obs = self._episode.get_observations()

        # Get the states for all agents.
        states = initial_states

        # TODO (simon): Include also agent_step sampling.
        # Loop through `self.env` in `enumerate(self._episodes)`:
        ts = 0

        while ts < num_timesteps:
            # Act randomly.
            if random_actions:
                # Note, to get sampled actions from all agents' action
                # spaces we need to call `MultiAgentEnv.action_space_sample()`.
                if self.env.unwrapped._action_space_iin_preferred_format:
                    actions = self.env.action_space.sample()
                # Otherwise, `action_space_sample()` needs to be implemented.
                else:
                    actions = self.env.action_space_sample()
                # Remove all actions for agents that had no observation.
                actions = {
                    agent_id: agent_action
                    for agent_id, agent_action in actions.items()
                    if agent_id in obs
                }

            else:
                # TODO (simon): This is not correct `forward()` expects
                # `SampleBatchType`.
                # Note, `RLModule`'s `forward()` methods expect `NestedDict`s.
                # TODO (simon): Check, in which order this has to be: (1) (B, A, C)
                # or (2) (A, B, C).
                # Note, we only consider for states and obs the agents that step.
                batch: MultiAgentDict = {
                    agent_id: {
                        STATE_IN: tree.map_structure(
                            lambda s: self._convert_from_numpy(s),
                            states[agent_id],
                        ),
                        SampleBatch.OBS: self._convert_from_numpy(
                            np.expand_dims(agent_obs, axis=0)
                        ),
                    }
                    for agent_id, agent_obs in obs.items()
                }
                # TODO (Sven, Simon): The `RLModule` has `SampleBatchType` as input
                # type. Only the _forward_x()` methods have a `NestedDict`. Shall we
                # compile to `SampleBatchType` here and in `SingleAgentEnvRunner`?
                from ray.rllib.utils.nested_dict import NestedDict

                batch = NestedDict(batch)

                # Explore or not.
                if explore:
                    fwd_out = self.module.forward_exploration(batch)
                else:
                    fwd_out = self.module.forward_inference(batch)

                actions, action_logps = self._sample_actions_if_necessary(
                    fwd_out,
                    explore=explore,
                )

                # Convert to numpy.
                fwd_out = tree.map_structure(convert_to_numpy, fwd_out)

                # Assign the new states for the agents that stepped.
                # TODO (simon): These states are different in each timestep
                # as different agents step - like this they get overriden.
                if STATE_OUT in fwd_out:
                    states.update(tree.map_structure(lambda s: s[STATE_OUT], fwd_out))

            obs, rewards, terminateds, truncateds, infos = self.env.step(actions)

            # TODO (simon): If agent steps add len(obs).
            ts += 1

            extra_model_output = {
                agent_id: {
                    k: v for k, v in agent_fwd_out.items() if k != SampleBatch.ACTIONS
                }
                for agent_id, agent_fwd_out in fwd_out.items()
            }
            for agent_id, agent_extra_model_output in extra_model_output.items():
                agent_extra_model_output[SampleBatch.ACTION_LOGP] = action_logps[
                    agent_id
                ]

            # In inference we have only the action logits.
            # TODO (simon): Refactor.
            # TODO (simon): CHeck, if "__all__"  is always in `MultiAgentEnv`.
            if not terminateds["__all__"]:
                agents_terminated = {
                    agent_id: agent_eps.is_terminated
                    for agent_id, agent_eps in self._episode.agent_episodes.items()
                }
                agents_terminated.update(terminateds)
                agents_terminated = agents_terminated.values()
            else:
                agents_terminated = [terminateds["__all__"]]
            if not truncateds["__all__"]:
                agents_truncated = {
                    agent_id: agent_eps.is_truncated
                    for agent_id, agent_eps in self._episode.agent_episodes.items()
                }
                agents_truncated.update(truncateds)
                agents_truncated = agents_truncated.values()
            else:
                agents_truncated = [truncateds["__all__"]]

            if all(agents_terminated) or all(agents_truncated):
                # Finish the episode with the actual terminal observation stored in
                # the info dict.
                self._episode.add_timestep(
                    # Gym vector env provides the `"final_observation"`.
                    infos["final_observation"],
                    actions,
                    rewards,
                    info=infos["final_info"],
                    is_terminated=terminateds,
                    is_truncated=truncateds,
                    extra_model_output=extra_model_output,
                )

                # Reset all h-states to the model's initial ones b/c we are starting
                # a new episode.
                states = initial_states

                done_episodes_to_return.append(self._episode)
                # Create a new episode instance.
                self._episode = MultiAgentEpisode(agent_ids=self.agent_ids)
            else:
                self._episode.add_timestep(
                    obs,
                    actions,
                    rewards,
                    info=infos,
                    is_terminated=terminateds,
                    is_truncated=truncateds,
                    extra_model_output=extra_model_output,
                )

        return done_episodes_to_return + [self._episode]

    def _sample_episodes(
        self,
        num_episodes: int,
        explore: bool = True,
        random_actions: bool = False,
        with_render_data: bool = False,
    ) -> List["MultiAgentEpisode"]:
        """Helper method to run n episodes.

        See docstring of `self.sample()` for more details.
        """

        # TODO (sven): This gives a tricky circular import that goes
        # deep into the library. We have to see, where to dissolve it.
        # from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
        pass

    @override(EnvRunner)
    def assert_healthy(self):
        # Make sure, we have built our gym.vector.Env and RLModule properly.
        assert self.env and self.module

    def _convert_from_numpy(self, array: np.array) -> TensorType:
        """Converts a numpy array to a framework-specific tensor."""

        if self.config.framework_str == "torch":
            return torch.from_numpy(array)
        else:
            return tf.convert_to_tensor(array)

        # TODO (sven): Replace by default "to-env" connector.

    def _sample_actions_if_necessary(
        self, fwd_out: TensorStructType, explore: bool = True
    ) -> Tuple[np.array, np.array]:
        """Samples actions from action distribution if necessary."""

        # TODO (sven): Move this into connector pipeline (if no
        # "actions" key in returned dict, sample automatically as
        # the last piece of the connector pipeline; basically do
        # the same thing that the Policy is currently doing, but
        # using connectors)
        actions = {}
        action_logps = {}
        for agent_id, agent_fwd_out in fwd_out.items():
            # If actions are provided just load them.
            if SampleBatch.ACTIONS in agent_fwd_out.keys():
                actions[agent_id] = convert_to_numpy(agent_fwd_out[SampleBatch.ACTIONS])
                # TODO (simon, sven): Some algos do not return logps.
                if SampleBatch.ACTION_LOGP in agent_fwd_out:
                    action_logps[agent_id] = convert_to_numpy(
                        agent_fwd_out[SampleBatch.ACTION_LOGP]
                    )
            # If no actions are provided we need to sample them.
            else:
                # Explore or not.
                if explore:
                    action_dist_cls = self.module[
                        agent_id
                    ].get_exploration_action_dist_cls()
                else:
                    action_dist_cls = self.module[
                        agent_id
                    ].get_inference_action_dist_cls()
                # Generate action distribution and sample actions.
                action_dist = action_dist_cls.from_logits(
                    agent_fwd_out[SampleBatch.ACTION_DIST_INPUTS]
                )
                action = action_dist.sample()
                # We need numpy actions for gym environments.
                action_logps[agent_id] = convert_to_numpy(action_dist.logp(action))
                actions[agent_id] = convert_to_numpy(action)
                # Squeeze for the last dimension if necessary.
                # TODO (sven, simon): This is not optimal here. But there seems
                # to be some differences between MultiDiscrete action spaces
                # and Box action spaces for `gym.VectorEnv`.
                # For the former we have to squeeze away the last action
                # dimension delivered from the action_dist and for the latter
                # we should not. This might be connected to the way how the
                # `action_space` is defined for the `RLModule` in the
                # `__init__()` of this class here.
                # if actions.ndim > len(self.env.action_space.shape):
                #    actions = actions.squeeze(axis=-1)

        return actions, action_logps

    def _convert_agent_actions_for_vector_env(
        self, actions: MultiAgentDict
    ) -> List[MultiAgentDict]:
        """Converts multi-agent batch actions to a list of multi-agent actions."""
        return [
            {agent_id: agent_action[i] for agent_id, agent_action in actions.items()}
            for i in range(self.num_envs)
        ]

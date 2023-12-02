import gymnasium as gym
import numpy as np
import tree

from collections import defaultdict
from functools import partial
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

from ray.rllib.core.models.base import STATE_IN, STATE_OUT
from ray.rllib.core.rl_module.rl_module import RLModule, SingleAgentRLModuleSpec
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.env.utils import _gym_env_creator
from ray.rllib.evaluation.metrics import RolloutMetrics
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.annotations import ExperimentalAPI, override
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import TensorStructType, TensorType
from ray.tune.registry import ENV_CREATOR, _global_registry

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig


_, tf, _ = try_import_tf()
torch, nn = try_import_torch()


@ExperimentalAPI
class SingleAgentEnvRunner(EnvRunner):
    """The generic environment runner for the single agent case."""

    @override(EnvRunner)
    def __init__(self, config: "AlgorithmConfig", **kwargs):
        super().__init__(config=config)

        # Get the worker index on which this instance is running.
        self.worker_index: int = kwargs.get("worker_index")

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

        # Create our own instance of the (single-agent) `RLModule` (which
        # the needs to be weight-synched) each iteration.
        try:
            module_spec: SingleAgentRLModuleSpec = (
                self.config.get_default_rl_module_spec()
            )
            module_spec.observation_space = self.env.envs[0].observation_space
            # TODO (simon): The `gym.Wrapper` for `gym.vector.VectorEnv` should
            #  actually hold the spaces for a single env, but for boxes the
            #  shape is (1, 1) which brings a problem with the action dists.
            #  shape=(1,) is expected.
            module_spec.action_space = self.env.envs[0].action_space
            module_spec.model_config_dict = self.config.model
            self.module: RLModule = module_spec.build()
        except NotImplementedError:
            self.module = None

        # This should be the default.
        self._needs_initial_reset: bool = True
        self._episodes: List[Optional["SingleAgentEpisode"]] = [
            None for _ in range(self.num_envs)
        ]

        self._done_episodes_for_metrics: List["SingleAgentEpisode"] = []
        self._ongoing_episodes_for_metrics: Dict[List] = defaultdict(list)
        self._ts_since_last_metrics: int = 0
        self._weights_seq_no: int = 0

        # TODO (sven): This is a temporary solution. STATE_OUTs
        #  will be resolved entirely as `extra_model_outputs` and
        #  not be stored separately inside Episodes.
        self._states = [None for _ in range(self.num_envs)]

    @override(EnvRunner)
    def sample(
        self,
        *,
        num_timesteps: int = None,
        num_episodes: int = None,
        explore: bool = True,
        random_actions: bool = False,
        with_render_data: bool = False,
    ) -> List["SingleAgentEpisode"]:
        """Runs and returns a sample (n timesteps or m episodes) on the env(s)."""
        assert not (num_timesteps is not None and num_episodes is not None)

        # If not execution details are provided, use the config.
        if num_timesteps is None and num_episodes is None:
            if self.config.batch_mode == "truncate_episodes":
                num_timesteps = (
                    self.config.get_rollout_fragment_length(
                        worker_index=self.worker_index
                    )
                    * self.num_envs
                )
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
    ) -> List["SingleAgentEpisode"]:
        """Helper method to sample n timesteps."""

        # TODO (sven): This gives a tricky circular import that goes
        # deep into the library. We have to see, where to dissolve it.
        from ray.rllib.env.single_agent_episode import SingleAgentEpisode

        done_episodes_to_return: List["SingleAgentEpisode"] = []

        # Get initial states for all 'batch_size_B` rows in the forward batch,
        # i.e. for all vector sub_envs.
        if hasattr(self.module, "get_initial_state"):
            initial_states = tree.map_structure(
                lambda s: np.repeat(s, self.num_envs, axis=0),
                self.module.get_initial_state(),
            )
        else:
            initial_states = {}

        # Have to reset the env (on all vector sub_envs).
        if force_reset or self._needs_initial_reset:
            obs, infos = self.env.reset()

            # We just reset the env. Don't have to force this again in the next
            # call to `self._sample_timesteps()`.
            self._needs_initial_reset = False

            self._episodes = [SingleAgentEpisode() for _ in range(self.num_envs)]
            states = initial_states

            # Set initial obs and states in the episodes.
            for i in range(self.num_envs):
                # TODO (sven): Maybe move this into connector pipeline
                # (even if automated).
                self._episodes[i].add_env_reset(
                    observation=obs[i],
                    infos=infos[i],
                )
                self._states[i] = {k: s[i] for k, s in states.items()}
        # Do not reset envs, but instead continue in already started episodes.
        else:
            # Pick up stored observations and states from previous timesteps.
            obs = np.stack([eps.observations[-1] for eps in self._episodes])
            # Compile the initial state for each batch row (vector sub_env):
            # If episode just started, use the model's initial state, in the
            # other case use the state stored last in the Episode.
            states = {
                k: np.stack(
                    [
                        initial_states[k][i] if state is None else state[k]
                        for i, state in enumerate(self._states)
                    ]
                )
                for k in initial_states.keys()
            }

        # Loop through env in enumerate.(self._episodes):
        ts = 0

        while ts < num_timesteps:
            # Act randomly.
            if random_actions:
                actions = self.env.action_space.sample()
                action_logp = np.zeros(shape=(actions.shape[0],))
                fwd_out = {}
            # Compute an action using the RLModule.
            else:
                # Note, RLModule `forward()` methods expect `NestedDict`s.
                batch = {
                    STATE_IN: tree.map_structure(
                        lambda s: self._convert_from_numpy(s),
                        states,
                    ),
                    SampleBatch.OBS: self._convert_from_numpy(obs),
                }
                from ray.rllib.utils.nested_dict import NestedDict

                batch = NestedDict(batch)

                # Explore or not.
                if explore:
                    fwd_out = self.module.forward_exploration(batch)
                else:
                    fwd_out = self.module.forward_inference(batch)

                # TODO (sven): Will be completely replaced by connector logic in
                #  upcoming PR.
                actions, action_logp = self._sample_actions_if_necessary(
                    fwd_out, explore
                )

                fwd_out = convert_to_numpy(fwd_out)

                if STATE_OUT in fwd_out:
                    states = fwd_out[STATE_OUT]

            obs, rewards, terminateds, truncateds, infos = self.env.step(actions)

            ts += self.num_envs

            for i in range(self.num_envs):
                # Extract state for vector sub_env.
                s = {k: s[i] for k, s in states.items()}
                # The last entry in self.observations[i] is already the reset
                # obs of the new episode.
                # TODO (simon): This might be unfortunate if a user needs to set a
                # certain env parameter during different episodes (for example for
                # benchmarking).
                extra_model_output = {}
                for k, v in fwd_out.items():
                    if SampleBatch.ACTIONS != k:
                        extra_model_output[k] = v[i]
                # TODO (simon, sven): Some algos do not have logps.
                extra_model_output[SampleBatch.ACTION_LOGP] = action_logp[i]

                # In inference we have only the action logits.
                if terminateds[i] or truncateds[i]:
                    # Finish the episode with the actual terminal observation stored in
                    # the info dict.
                    self._episodes[i].add_env_step(
                        # Gym vector env provides the `"final_observation"`.
                        infos[i]["final_observation"],
                        actions[i],
                        rewards[i],
                        infos=infos[i]["final_info"],
                        terminated=terminateds[i],
                        truncated=truncateds[i],
                        extra_model_outputs=extra_model_output,
                    )
                    self._states[i] = s

                    # Reset h-states to nthe model's intiial ones b/c we are starting a
                    # new episode.
                    if hasattr(self.module, "get_initial_state"):
                        for k, v in self.module.get_initial_state().items():
                            states[k][i] = convert_to_numpy(v)

                    done_episodes_to_return.append(self._episodes[i].finalize())
                    # Create a new episode object with already the reset data in it.
                    self._episodes[i] = SingleAgentEpisode(
                        observations=[obs[i]], infos=[infos[i]]
                    )
                    self._states[i] = s
                else:
                    self._episodes[i].add_env_step(
                        obs[i],
                        actions[i],
                        rewards[i],
                        infos=infos[i],
                        extra_model_outputs=extra_model_output,
                    )
                    self._states[i] = s

        # Return done episodes ...
        self._done_episodes_for_metrics.extend(done_episodes_to_return)
        # Also, make sure, we return a copy and start new chunks so that callers
        # of this function do not alter the ongoing and returned Episode objects.
        new_episodes = [eps.cut() for eps in self._episodes]

        # ... and all ongoing episode chunks.
        # Initialized episodes do not have recorded any step and lack
        # `extra_model_outputs`.
        ongoing_episodes_to_return = [
            episode.finalize() for episode in self._episodes if episode.t > 0
        ]
        for eps in ongoing_episodes_to_return:
            self._ongoing_episodes_for_metrics[eps.id_].append(eps)

        # Record last metrics collection.
        self._ts_since_last_metrics += ts

        self._episodes = new_episodes

        return done_episodes_to_return + ongoing_episodes_to_return

    def _sample_episodes(
        self,
        num_episodes: int,
        explore: bool = True,
        random_actions: bool = False,
        with_render_data: bool = False,
    ) -> List["SingleAgentEpisode"]:
        """Helper method to run n episodes.

        See docstring of `self.sample()` for more details.
        """
        # TODO (sven): This gives a tricky circular import that goes
        # deep into the library. We have to see, where to dissolve it.
        from ray.rllib.env.single_agent_episode import SingleAgentEpisode

        # If user calls sample(num_timesteps=..) after this, we must reset again
        # at the beginning.
        self._needs_initial_reset = True

        done_episodes_to_return: List["SingleAgentEpisode"] = []

        obs, infos = self.env.reset()
        episodes = [SingleAgentEpisode() for _ in range(self.num_envs)]

        # Get initial states for all 'batch_size_B` rows in the forward batch,
        # i.e. for all vector sub_envs.
        if hasattr(self.module, "get_initial_state"):
            states = tree.map_structure(
                lambda s: np.repeat(s, self.num_envs, axis=0),
                self.module.get_initial_state(),
            )
        else:
            states = {}

        render_images = [None] * self.num_envs
        if with_render_data:
            render_images = [e.render() for e in self.env.envs]

        for i in range(self.num_envs):
            episodes[i].add_env_reset(
                observation=obs[i],
                infos=infos[i],
                render_image=render_images[i],
            )

        eps = 0
        while eps < num_episodes:
            if random_actions:
                actions = self.env.action_space.sample()
                action_logp = np.zeros(shape=(actions.shape[0]))
                fwd_out = {}
            else:
                batch = {
                    # TODO (sven): This will move entirely into connector logic in
                    #  upcoming PR.
                    STATE_IN: tree.map_structure(
                        lambda s: self._convert_from_numpy(s), states
                    ),
                    SampleBatch.OBS: self._convert_from_numpy(obs),
                }

                # Explore or not.
                if explore:
                    fwd_out = self.module.forward_exploration(batch)
                else:
                    fwd_out = self.module.forward_inference(batch)

                # TODO (sven): This will move entirely into connector logic in upcoming
                # PR.
                actions, action_logp = self._sample_actions_if_necessary(
                    fwd_out, explore
                )

                fwd_out = convert_to_numpy(fwd_out)

                # TODO (sven): This will move entirely into connector logic in upcoming
                # PR.
                if STATE_OUT in fwd_out:
                    states = convert_to_numpy(fwd_out[STATE_OUT])

            obs, rewards, terminateds, truncateds, infos = self.env.step(actions)
            if with_render_data:
                render_images = [e.render() for e in self.env.envs]

            for i in range(self.num_envs):
                # Extract info and state for vector sub_env.
                # info = {k: v[i] for k, v in infos.items()}
                # The last entry in self.observations[i] is already the reset
                # obs of the new episode.
                extra_model_output = {}
                for k, v in fwd_out.items():
                    if SampleBatch.ACTIONS not in k:
                        extra_model_output[k] = v[i]
                # TODO (sven): This will move entirely into connector logic in upcoming
                #  PR.
                extra_model_output[SampleBatch.ACTION_LOGP] = action_logp[i]

                if terminateds[i] or truncateds[i]:
                    eps += 1

                    episodes[i].add_env_step(
                        infos[i]["final_observation"],
                        actions[i],
                        rewards[i],
                        infos=infos[i]["final_info"],
                        terminated=terminateds[i],
                        truncated=truncateds[i],
                        extra_model_outputs=extra_model_output,
                    )

                    done_episodes_to_return.append(episodes[i])

                    # Also early-out if we reach the number of episodes within this
                    # for-loop.
                    if eps == num_episodes:
                        break

                    # TODO (sven): This will move entirely into connector logic in
                    #  upcoming PR.
                    if hasattr(self.module, "get_initial_state"):
                        for k, v in self.module.get_initial_state().items():
                            states[k][i] = (convert_to_numpy(v),)

                    # Create a new episode object.
                    episodes[i] = SingleAgentEpisode(
                        observations=[obs[i]],
                        infos=[infos[i]],
                        render_images=None
                        if render_images[i] is None
                        else [render_images[i]],
                    )
                else:
                    episodes[i].add_env_step(
                        obs[i],
                        actions[i],
                        rewards[i],
                        infos=infos[i],
                        render_image=render_images[i],
                        extra_model_outputs=extra_model_output,
                    )

        self._done_episodes_for_metrics.extend(done_episodes_to_return)
        self._ts_since_last_metrics += sum(len(eps) for eps in done_episodes_to_return)

        # Initialized episodes have to be removed as they lack `extra_model_outputs`.
        return [episode for episode in done_episodes_to_return if episode.t > 0]

    # TODO (sven): Remove the requirement for EnvRunners/RolloutWorkers to have this
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
        self._ts_since_last_metrics = 0

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
        # Make sure, we have built our gym.vector.Env and RLModule properly.
        assert self.env and self.module

    @override(EnvRunner)
    def stop(self):
        # Close our env object via gymnasium's API.
        self.env.close()

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
        # If actions are provided just load them.
        if SampleBatch.ACTIONS in fwd_out.keys():
            actions = convert_to_numpy(fwd_out[SampleBatch.ACTIONS])
            # TODO (simon, sven): Some algos do not return logps.
            action_logp = convert_to_numpy(fwd_out[SampleBatch.ACTION_LOGP])
        # If no actions are provided we need to sample them.
        else:
            # Explore or not.
            if explore:
                action_dist_cls = self.module.get_exploration_action_dist_cls()
            else:
                action_dist_cls = self.module.get_inference_action_dist_cls()
            # Generate action distribution and sample actions.
            action_dist = action_dist_cls.from_logits(
                fwd_out[SampleBatch.ACTION_DIST_INPUTS]
            )
            actions = action_dist.sample()
            # We need numpy actions for gym environments.
            action_logp = convert_to_numpy(action_dist.logp(actions))
            actions = convert_to_numpy(actions)
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

        return actions, action_logp

    def _convert_from_numpy(self, array: np.array) -> TensorType:
        """Converts a numpy array to a framework-specific tensor."""

        if self.config.framework_str == "torch":
            return torch.from_numpy(array)
        else:
            return tf.convert_to_tensor(array)

    def _convert_to_tensor(self, struct) -> TensorType:
        """Converts structs to a framework-specific tensor."""

        if self.config.framework_str == "torch":
            return convert_to_torch_tensor(struct)
        else:
            # `tf.convert_to_tensor` cannot deal with tensors as inputs.
            # if not tf.is_tensor(struct):
            #     return tree.map_structure(tf.convert_to_tensor, struct)
            # else:
            #     return struct
            return tree.map_structure(tf.convert_to_tensor, struct)

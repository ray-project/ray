import abc
import gymnasium as gym
import numpy as np
import tree

from collections import defaultdict
from functools import partial
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from ray.experimental.tqdm_ray import tqdm
from ray.rllib.core.models.base import STATE_IN, STATE_OUT
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.utils import _gym_env_creator
from ray.rllib.evaluation.metrics import RolloutMetrics
from ray.rllib.evaluation.postprocessing_v2 import compute_gae_for_episode
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.actor_manager import FaultAwareApply
from ray.rllib.utils.annotations import ExperimentalAPI, override
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.replay_buffers.episode_replay_buffer import _Episode as Episode
from ray.rllib.utils.typing import TensorStructType, TensorType

tf1, tf, _ = try_import_tf()
torch, _ = try_import_torch()

from ray.tune.registry import ENV_CREATOR, _global_registry

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig


@ExperimentalAPI
class EnvRunner(FaultAwareApply, metaclass=abc.ABCMeta):
    """Base class for distributed RL-style data collection from an environment.

    The EnvRunner API's core functionalities can be summarized as:
    - Gets configured via passing a AlgorithmConfig object to the constructor.
    Normally, subclasses of EnvRunner then construct their own environment (possibly
    vectorized) copies and RLModules/Policies and use the latter to step through the
    environment in order to collect training data.
    - Clients of EnvRunner can use the `sample()` method to collect data for training
    from the environment(s).
    - EnvRunner offers parallelism via creating n remote Ray Actors based on this class.
    Use `ray.remote([resources])(EnvRunner)` method to create the corresponding Ray
    remote class. Then instantiate n Actors using the Ray `[ctor].remote(...)` syntax.
    - EnvRunner clients can get information about the server/node on which the
    individual Actors are running.
    """

    def __init__(self, *, config: "AlgorithmConfig", **kwargs):
        """Initializes an EnvRunner instance.

        Args:
            config: The config to use to setup this EnvRunner.
            **kwargs: Forward compatibility kwargs.
        """
        self.config = config
        super().__init__(**kwargs)

        # Get the framework string.
        self.framework_str = self.config.framework_str
        # This eager check is necessary for certain all-framework tests
        # that use tf's eager_mode() context generator.
        if (
            tf1
            and (self.framework_str == "tf2" or config.enable_tf1_exec_eagerly)
            and not tf1.executing_eagerly()
        ):
            tf1.enable_eager_execution()

    @abc.abstractmethod
    def assert_healthy(self):
        """Checks that self.__init__() has been completed properly.

        Useful in case an `EnvRunner` is run as @ray.remote (Actor) and the owner
        would like to make sure the Ray Actor has been properly initialized.

        Raises:
            AssertionError: If the EnvRunner Actor has NOT been properly initialized.
        """

    @abc.abstractmethod
    def sample(self, **kwargs) -> Any:
        """Returns experiences (of any form) sampled from this EnvRunner.

        The exact nature and size of collected data are defined via the EnvRunner's
        config and may be overridden by the given arguments.

        Args:
            **kwargs: Forward compatibility kwargs.

        Returns:
            The collected experience in any form.
        """

    def get_state(self) -> Dict[str, Any]:
        """Returns this EnvRunner's (possibly serialized) current state as a dict.

        Returns:
            The current state of this EnvRunner.
        """
        return {}

    def set_state(self, state: Dict[str, Any]) -> None:
        """Restores this EnvRunner's state from the given state dict.

        Args:
            state: The state dict to restore the state from.

        Examples:
            >>> from ray.rllib.env.env_runner import EnvRunner
            >>> env_runner = ... # doctest: +SKIP
            >>> state = env_runner.get_state() # doctest: +SKIP
            >>> new_runner = EnvRunner(...) # doctest: +SKIP
            >>> new_runner.set_state(state) # doctest: +SKIP
        """
        pass

    def stop(self) -> None:
        """Releases all resources used by this EnvRunner."""
        pass

    def __del__(self) -> None:
        """If this Actor is deleted, clears all resources used by it."""
        pass


@ExperimentalAPI
class SingleAgentEnvRunner(EnvRunner):
    """The generic environment runner for the single agent case."""

    @override(EnvRunner)
    def __init__(self, config: "AlgorithmConfig", **kwargs):
        super().__init__(config=config)

        # Get the worker index on which this instance is running.
        self.worker_index = kwargs.get("worker_index")

        # Register env for the local context.
        # Note, `gym.register` has to be called on each worker.
        gym.register(
            "custom-env-v0",
            partial(
                _global_registry.get(ENV_CREATOR, self.config.env),
                self.config.env_config,
            )
            if _global_registry.contains(ENV_CREATOR, self.config.env)
            else partial(
                _gym_env_creator,
                env_context=self.config.env_config,
                env_descriptor=self.config.env,
            ),
        )

        # Create the vectorized gymnasium env.
        # Wrap into `VectorListInfo`` wrapper to get infos as lists.
        self.env: gym.Wrapper = gym.wrappers.VectorListInfo(
            gym.vector.make(
                "custom-env-v0",
                num_envs=self.config.num_envs_per_worker,
                asynchronous=self.config.remote_worker_envs,
            )
        )

        self.num_envs: int = self.env.num_envs
        assert self.num_envs == self.config.num_envs_per_worker

        # Create our own instance of the single-agent `RLModule` (which
        # the needs to be weight-synched) each iteration.
        module_dict: dict = {DEFAULT_POLICY_ID: None}
        # TODO (sven): By time the `AlgorithmConfig` will get rid of `PolicyDict`
        # as well. Then we have to change this function parameter.
        module_spec: MultiAgentRLModuleSpec = self.config.get_marl_module_spec(
            policy_dict=module_dict
        )
        self.module: RLModule = module_spec.build(module_id=DEFAULT_POLICY_ID)

        # This should be the default.
        self._needs_initial_reset: bool = True
        self._episodes: List[Optional["Episode"]] = [None for _ in range(self.num_envs)]

        self._done_episodes_for_metrics: List[Episode] = []
        self._ongoing_episodes_for_metrics: Dict[List] = defaultdict(list)
        self._ts_since_last_metrics: int = 0

    @override(EnvRunner)
    def sample(
        self,
        *,
        num_timesteps: int = None,
        num_episodes: int = None,
        explore: bool = True,
        random_actions: bool = False,
        with_render_data: bool = False,
    ) -> List["Episode"]:
        """Runs and returns a sample (n timesteps or m episodes) on the env(s)."""

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
    ) -> List["Episode"]:
        """Helper method to sample n timesteps."""

        done_episodes_to_return = []

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

            self._episodes = [Episode() for _ in range(self.num_envs)]
            states = initial_states

            # Set initial obs and states in the episodes.
            for i in range(self.num_envs):
                # Extract info for the vector sub_env.
                self._episodes[i].add_initial_observation(
                    initial_observation=obs[i],
                    initial_info=infos[i],
                    # TODO (simon): Check, if this works for the default
                    # stateful encoders.
                    initial_state={k: s[i] for k, s in states.items()},
                )
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
                        initial_states[k][i] if eps.states is None else eps.states[k]
                        for i, eps in enumerate(self._episodes)
                    ]
                )
                for k in initial_states.keys()
            }

        # Loop through env in enumerate.(self._episodes):
        ts = 0
        pbar = tqdm(total=num_timesteps, desc=f"Sampling {num_timesteps} timesteps ...")

        while ts < num_timesteps:
            # Act randomly.
            if random_actions:
                actions = self.env.action_space.sample()
            # Compute an action using the RLModule.
            else:
                # Note, RLModule `forward()` methods expect `NestedDict`s.
                # TODO (simon): Framework-agnostic.
                batch = {
                    STATE_IN: tree.map_structure(
                        lambda s: self._convert_from_numpy(s),
                        states,
                    ),
                    SampleBatch.OBS: self._convert_from_numpy(obs),
                }

                # Explore or not.
                if explore:
                    fwd_out = self.module.forward_exploration(batch)
                else:
                    fwd_out = self.module.forward_inference(batch)

                actions, action_logp = self._sample_actions_if_necessary(
                    fwd_out, explore
                )

                fwd_out = convert_to_numpy(fwd_out)

                if STATE_OUT in fwd_out:
                    # TODO (simon): Check if still needed when full `fwd_out`
                    # is converted.
                    states = convert_to_numpy(fwd_out[STATE_OUT])

            obs, rewards, terminateds, truncateds, infos = self.env.step(actions)
            ts += self.num_envs

            # Record only every `10 * self.num_envs`` as otherwise
            # logging is too fast for `tqdm_ray`.
            if ts % (self.num_envs * 10) == 0:
                pbar.update(self.num_envs * 10)

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
                    if not SampleBatch.ACTIONS in k:
                        extra_model_output[k] = v[i]
                # TODO (simon, sven): Some algos do not have logps.
                extra_model_output[SampleBatch.ACTION_LOGP] = action_logp

                # In inference we have only the action logits.
                if terminateds[i] or truncateds[i]:
                    # Finish the episode with the actual terminal observation stored in
                    # the info dict.
                    self._episodes[i].add_timestep(
                        # Gym vector env provides the `"final_observation"`.
                        infos[i]["final_observation"],
                        actions[i],
                        rewards[i],
                        infos[i]["final_info"],
                        state=s,
                        is_terminated=terminateds[i],
                        is_truncated=truncateds[i],
                        extra_model_output=extra_model_output,
                    )
                    if explore and self.config.use_gae:
                        # Make postprocessing here. Calculate advantages and value targets.
                        self._episodes[i] = compute_gae_for_episode(
                            self._episodes[i],
                            self.config,
                            self.marl_module,
                        )
                    # Reset h-states to nthe model's intiial ones b/c we are starting a
                    # new episode.
                    for k, v in self.module.get_initial_state().items():
                        states[k][i] = convert_to_numpy(v)

                    done_episodes_to_return.append(self._episodes[i])
                    # Create a new episode object.
                    self._episodes[i] = Episode(
                        observations=[obs[i]], infos=[infos[i]], states=s
                    )
                else:
                    self._episodes[i].add_timestep(
                        obs[i],
                        actions[i],
                        rewards[i],
                        infos[i],
                        state=s,
                        extra_model_output=extra_model_output,
                    )

        # Return done episodes ...
        self._done_episodes_for_metrics.extend(done_episodes_to_return)
        # ... and all ongoing episode chunks. Also, make sure, we return
        # a copy and start new chunks so that callers of this function
        # do not alter the ongoing and returned Episode objects.
        ongoing_episodes = self._episodes
        self._episodes = [eps.create_successor() for eps in self._episodes]
        for eps in ongoing_episodes:
            self._ongoing_episodes_for_metrics[eps.id_].append(eps)
        # Make postprocessing here for ongoing episodes. Compute
        # advantages and value targets.
        if explore and self.config.use_gae:
            ongoing_episodes = [
                compute_gae_for_episode(eps, self.config, self.marl_module)
                for eps in ongoing_episodes
            ]

        # Record last metrics collection.
        self._ts_since_last_metrics += ts

        return done_episodes_to_return + ongoing_episodes
    
    def _sample_episodes(
        self,
        num_episodes: int,
        explore: bool = True,
        random_actions: bool = False,
        with_render_data: bool = False,
    ) -> List[Episode]:
        """Helper method to run n episodes.

        See docstring of `self.sample()` for more details.
        """
        done_episodes_to_return = []

        obs, infos = self.env.reset()
        episodes = [Episode() for _ in range(self.num_envs)]

        # Multiply states n times according to our vector env batch size (num_envs).
        states = tree.map_structure(
            lambda s: np.repeat(s, self.num_envs, axis=0),
            self.module.get_initial_state(),
        )

        render_images = [None] * self.num_envs
        if with_render_data:
            render_images = [e.render() for e in self.env.envs]

        for i in range(self.num_envs):
            # Extract info for vector sub_env.
            # info = {k: v[i] for k, v in infos.items()}
            episodes[i].add_initial_observation(
                initial_observation=obs[i],
                initial_info=infos[i],
                initial_state={k: s[i] for k, s in states.items()},
                initial_render_image=render_images[i],
            )

        eps = 0
        pbar = tqdm(total=num_episodes, desc=f"Sampling {num_episodes} episodes ...")
        while eps < num_episodes:
            if random_actions:
                actions = self.env.action_space.sample()
            else:
                batch = {
                    STATE_IN: tree.map_structure(
                        lambda s: self._convert_from_numpy(s), states
                    ),
                    SampleBatch.OBS: self._convert_from_numpy(obs),
                }

                # Explore or not.
                if explore:                    
                    fwd_out = self.marl_module[DEFAULT_POLICY_ID].forward_exploration(
                        batch
                    )
                else:
                    fwd_out = self.marl_module[DEFAULT_POLICY_ID].forward_inference(
                        batch
                    )
                    
                actions, action_logp = self._sample_actions_if_necessary(fwd_out, explore)

                if STATE_OUT in fwd_out:
                    states = convert_to_numpy(fwd_out[STATE_OUT])
                    # states = tree.map_structure(
                    #     lambda s: s.numpy(), fwd_out[STATE_OUT]
                    # )

            obs, rewards, terminateds, truncateds, infos = self.env.step(actions)
            if with_render_data:
                render_images = [e.render() for e in self.env.envs]

            for i in range(self.num_envs):
                # Extract info and state for vector sub_env.
                # info = {k: v[i] for k, v in infos.items()}
                s = {k: s[i] for k, s in states.items()}
                # The last entry in self.observations[i] is already the reset
                # obs of the new episode.
                extra_model_output = {}
                for k, v in fwd_out.items():
                    if not SampleBatch.ACTIONS in k:
                        extra_model_output[k] = v[i]
                # TODO (simon, sven): Some algos do not have logps.
                extra_model_output[SampleBatch.ACTION_LOGP] = action_logp
        
                if terminateds[i] or truncateds[i]:
                    eps += 1
                    pbar.update(1)

                    episodes[i].add_timestep(
                        infos[i]["final_observation"],
                        actions[i],
                        rewards[i],
                        infos[i]["final_info"],
                        state=s,
                        is_terminated=terminateds[i],
                        is_truncated=truncateds[i],
                        extra_model_output=extra_model_output,
                    )

                    if explore and self.config.use_gae:
                        # Make postprocessing here.Calculate advantages and
                        # value targets.
                        episodes[i] = compute_gae_for_episode(
                            episodes[i],
                            self.config,
                            self.marl_module,
                        )

                    done_episodes_to_return.append(episodes[i])

                    # Also early-out if we reach the number of episodes within this
                    # for-loop.
                    if eps == num_episodes:
                        break

                    # Reset h-states to the model's initial ones b/c we are starting
                    # a new episode.
                    for k, v in (
                        self.module.get_initial_state().items()
                    ):
                        states[k][i] = convert_to_numpy(v),

                    # Create a new episode object.
                    episodes[i] = Episode(
                        observations=[obs[i]],
                        infos=[infos[i]],
                        states=s,
                        render_images=None
                        if render_images[i] is None
                        else [render_images[i]],
                    )
                else:
                    episodes[i].add_timestep(
                        obs[i],
                        actions[i],
                        rewards[i],
                        infos[i],
                        state=s,
                        render_image=render_images[i],
                        extra_model_output=extra_model_output,
                    )

        self._done_episodes_for_metrics.extend(done_episodes_to_return)
        self._ts_since_last_metrics += sum(len(eps) for eps in done_episodes_to_return)

        # If user calls sample(num_timesteps=..) after this, we must reset again
        # at the beginning.
        self._needs_initial_reset = True

        return done_episodes_to_return
    
    # TODO (sven): Remove the requirement for EnvRunners/RolloutWorkers to have this
    #  API. Instead Algorithm should compile episode metrics itself via its local
    #  buffer.
    def get_metrics(self) -> List[RolloutMetrics]:
        # Compute per-episode metrics (only on already completed episodes).
        metrics = []
        for eps in self._done_episodes_for_metrics:
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
    def set_weights(self, weights, global_vars=None):
        """Writes the weights of our (single-agent) RLModule."""
        self.module.set_state(weights)

    def get_weights(self, modules=None):
        """Returns the weights of our (single-agent) RLModule."""
        return self.module.get_state(modules)

    @override(EnvRunner)
    def assert_healthy(self):
        # Make sure, we have built our gym.vector.Env and RLModule properly.
        assert self.env and self.module

    @override(EnvRunner)
    def stop(self):
        # Close our env object via gymnasium's API.
        self.env.close()

    def _sample_actions_if_necessary(
        self, fwd_out: TensorStructType, explore: bool = True
    ) -> Tuple[np.array, np.array]:
        """Samples actions from action distribution if necessary."""

        # If actions are provided just load them.
        if SampleBatch.ACTIONS in fwd_out:
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

        return actions, action_logp

    def _convert_from_numpy(self, array: np.array) -> TensorType:
        """Converts a numpy array to a framework-specific tensor."""

        if self.framework_str == "torch":
            return torch.from_numpy(array)
        else:
            return tf.convert_to_tensor(array)

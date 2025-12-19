import json
import logging
import os
import platform
from abc import ABCMeta, abstractmethod
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree
from gymnasium.spaces import Box
from packaging import version

import ray
import ray.cloudpickle as pickle
from ray._common.deprecation import (
    DEPRECATED_VALUE,
    deprecation_warning,
)
from ray.actor import ActorHandle
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.annotations import (
    OldAPIStack,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
    is_overridden,
)
from ray.rllib.utils.checkpoints import (
    CHECKPOINT_VERSION,
    get_checkpoint_info,
    try_import_msgpack,
)
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.serialization import (
    deserialize_type,
    space_from_dict,
    space_to_dict,
)
from ray.rllib.utils.spaces.space_utils import (
    get_base_struct_from_space,
    get_dummy_batch_for_space,
    unbatch,
)
from ray.rllib.utils.tensor_dtype import get_np_dtype
from ray.rllib.utils.tf_utils import get_tf_eager_cls_if_necessary
from ray.rllib.utils.typing import (
    AgentID,
    AlgorithmConfigDict,
    ModelGradients,
    ModelWeights,
    PolicyID,
    PolicyState,
    T,
    TensorStructType,
    TensorType,
)
from ray.tune import Checkpoint

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


logger = logging.getLogger(__name__)


@OldAPIStack
class PolicySpec:
    """A policy spec used in the "config.multiagent.policies" specification dict.

    As values (keys are the policy IDs (str)). E.g.:
    config:
        multiagent:
            policies: {
                "pol1": PolicySpec(None, Box, Discrete(2), {"lr": 0.0001}),
                "pol2": PolicySpec(config={"lr": 0.001}),
            }
    """

    def __init__(
        self, policy_class=None, observation_space=None, action_space=None, config=None
    ):
        # If None, use the Algorithm's default policy class stored under
        # `Algorithm._policy_class`.
        self.policy_class = policy_class
        # If None, use the env's observation space. If None and there is no Env
        # (e.g. offline RL), an error is thrown.
        self.observation_space = observation_space
        # If None, use the env's action space. If None and there is no Env
        # (e.g. offline RL), an error is thrown.
        self.action_space = action_space
        # Overrides defined keys in the main Algorithm config.
        # If None, use {}.
        self.config = config

    def __eq__(self, other: "PolicySpec"):
        return (
            self.policy_class == other.policy_class
            and self.observation_space == other.observation_space
            and self.action_space == other.action_space
            and self.config == other.config
        )

    def get_state(self) -> Dict[str, Any]:
        """Returns the state of a `PolicyDict` as a dict."""
        return (
            self.policy_class,
            self.observation_space,
            self.action_space,
            self.config,
        )

    @classmethod
    def from_state(cls, state: Dict[str, Any]) -> "PolicySpec":
        """Builds a `PolicySpec` from a state."""
        policy_spec = PolicySpec()
        policy_spec.__dict__.update(state)

        return policy_spec

    def serialize(self) -> Dict:
        from ray.rllib.algorithms.registry import get_policy_class_name

        # Try to figure out a durable name for this policy.
        cls = get_policy_class_name(self.policy_class)
        if cls is None:
            logger.warning(
                f"Can not figure out a durable policy name for {self.policy_class}. "
                f"You are probably trying to checkpoint a custom policy. "
                f"Raw policy class may cause problems when the checkpoint needs to "
                "be loaded in the future. To fix this, make sure you add your "
                "custom policy in rllib.algorithms.registry.POLICIES."
            )
            cls = self.policy_class

        return {
            "policy_class": cls,
            "observation_space": space_to_dict(self.observation_space),
            "action_space": space_to_dict(self.action_space),
            # TODO(jungong) : try making the config dict durable by maybe
            #  getting rid of all the fields that are not JSON serializable.
            "config": self.config,
        }

    @classmethod
    def deserialize(cls, spec: Dict) -> "PolicySpec":
        if isinstance(spec["policy_class"], str):
            # Try to recover the actual policy class from durable name.
            from ray.rllib.algorithms.registry import get_policy_class

            policy_class = get_policy_class(spec["policy_class"])
        elif isinstance(spec["policy_class"], type):
            # Policy spec is already a class type. Simply use it.
            policy_class = spec["policy_class"]
        else:
            raise AttributeError(f"Unknown policy class spec {spec['policy_class']}")

        return cls(
            policy_class=policy_class,
            observation_space=space_from_dict(spec["observation_space"]),
            action_space=space_from_dict(spec["action_space"]),
            config=spec["config"],
        )


@OldAPIStack
class Policy(metaclass=ABCMeta):
    """RLlib's base class for all Policy implementations.

    Policy is the abstract superclass for all DL-framework specific sub-classes
    (e.g. TFPolicy or TorchPolicy). It exposes APIs to

    1. Compute actions from observation (and possibly other) inputs.

    2. Manage the Policy's NN model(s), like exporting and loading their weights.

    3. Postprocess a given trajectory from the environment or other input via the
        `postprocess_trajectory` method.

    4. Compute losses from a train batch.

    5. Perform updates from a train batch on the NN-models (this normally includes loss
        calculations) either:

        a. in one monolithic step (`learn_on_batch`)

        b. via batch pre-loading, then n steps of actual loss computations  and updates
            (`load_batch_into_buffer` + `learn_on_loaded_batch`).
    """

    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        config: AlgorithmConfigDict,
    ):
        """Initializes a Policy instance.

        Args:
            observation_space: Observation space of the policy.
            action_space: Action space of the policy.
            config: A complete Algorithm/Policy config dict. For the default
                config keys and values, see rllib/algorithm/algorithm.py.
        """
        self.observation_space: gym.Space = observation_space
        self.action_space: gym.Space = action_space
        # the policy id in the global context.
        self.__policy_id = config.get("__policy_id")
        # The base struct of the observation/action spaces.
        # E.g. action-space = gym.spaces.Dict({"a": Discrete(2)}) ->
        # action_space_struct = {"a": Discrete(2)}
        self.observation_space_struct = get_base_struct_from_space(observation_space)
        self.action_space_struct = get_base_struct_from_space(action_space)

        self.config: AlgorithmConfigDict = config
        self.framework = self.config.get("framework")

        # Create the callbacks object to use for handling custom callbacks.
        from ray.rllib.callbacks.callbacks import RLlibCallback

        callbacks = self.config.get("callbacks")
        if isinstance(callbacks, RLlibCallback):
            self.callbacks = callbacks()
        elif isinstance(callbacks, (str, type)):
            try:
                self.callbacks: "RLlibCallback" = deserialize_type(
                    self.config.get("callbacks")
                )()
            except Exception:
                pass  # TEST
        else:
            self.callbacks: "RLlibCallback" = RLlibCallback()

        # The global timestep, broadcast down from time to time from the
        # local worker to all remote workers.
        self.global_timestep: int = 0
        # The number of gradient updates this policy has undergone.
        self.num_grad_updates: int = 0

        # The action distribution class to use for action sampling, if any.
        # Child classes may set this.
        self.dist_class: Optional[Type] = None

        # Initialize view requirements.
        self.init_view_requirements()

        # Whether the Model's initial state (method) has been added
        # automatically based on the given view requirements of the model.
        self._model_init_state_automatically_added = False

        # Connectors.
        self.agent_connectors = None
        self.action_connectors = None

    @staticmethod
    def from_checkpoint(
        checkpoint: Union[str, Checkpoint],
        policy_ids: Optional[Collection[PolicyID]] = None,
    ) -> Union["Policy", Dict[PolicyID, "Policy"]]:
        """Creates new Policy instance(s) from a given Policy or Algorithm checkpoint.

        Note: This method must remain backward compatible from 2.1.0 on, wrt.
        checkpoints created with Ray 2.0.0 or later.

        Args:
            checkpoint: The path (str) to a Policy or Algorithm checkpoint directory
                or an AIR Checkpoint (Policy or Algorithm) instance to restore
                from.
                If checkpoint is a Policy checkpoint, `policy_ids` must be None
                and only the Policy in that checkpoint is restored and returned.
                If checkpoint is an Algorithm checkpoint and `policy_ids` is None,
                will return a list of all Policy objects found in
                the checkpoint, otherwise a list of those policies in `policy_ids`.
            policy_ids: List of policy IDs to extract from a given Algorithm checkpoint.
                If None and an Algorithm checkpoint is provided, will restore all
                policies found in that checkpoint. If a Policy checkpoint is given,
                this arg must be None.

        Returns:
            An instantiated Policy, if `checkpoint` is a Policy checkpoint. A dict
            mapping PolicyID to Policies, if `checkpoint` is an Algorithm checkpoint.
            In the latter case, returns all policies within the Algorithm if
            `policy_ids` is None, else a dict of only those Policies that are in
            `policy_ids`.
        """
        checkpoint_info = get_checkpoint_info(checkpoint)

        # Algorithm checkpoint: Extract one or more policies from it and return them
        # in a dict (mapping PolicyID to Policy instances).
        if checkpoint_info["type"] == "Algorithm":
            from ray.rllib.algorithms.algorithm import Algorithm

            policies = {}

            # Old Algorithm checkpoints: State must be completely retrieved from:
            # algo state file -> worker -> "state".
            if checkpoint_info["checkpoint_version"] < version.Version("1.0"):
                with open(checkpoint_info["state_file"], "rb") as f:
                    state = pickle.load(f)
                # In older checkpoint versions, the policy states are stored under
                # "state" within the worker state (which is pickled in itself).
                worker_state = pickle.loads(state["worker"])
                policy_states = worker_state["state"]
                for pid, policy_state in policy_states.items():
                    # Get spec and config, merge config with
                    serialized_policy_spec = worker_state["policy_specs"][pid]
                    policy_config = Algorithm.merge_algorithm_configs(
                        worker_state["policy_config"], serialized_policy_spec["config"]
                    )
                    serialized_policy_spec.update({"config": policy_config})
                    policy_state.update({"policy_spec": serialized_policy_spec})
                    policies[pid] = Policy.from_state(policy_state)
            # Newer versions: Get policy states from "policies/" sub-dirs.
            elif checkpoint_info["policy_ids"] is not None:
                for policy_id in checkpoint_info["policy_ids"]:
                    if policy_ids is None or policy_id in policy_ids:
                        policy_checkpoint_info = get_checkpoint_info(
                            os.path.join(
                                checkpoint_info["checkpoint_dir"],
                                "policies",
                                policy_id,
                            )
                        )
                        assert policy_checkpoint_info["type"] == "Policy"
                        with open(policy_checkpoint_info["state_file"], "rb") as f:
                            policy_state = pickle.load(f)
                        policies[policy_id] = Policy.from_state(policy_state)
            return policies

        # Policy checkpoint: Return a single Policy instance.
        else:
            msgpack = None
            if checkpoint_info.get("format") == "msgpack":
                msgpack = try_import_msgpack(error=True)

            with open(checkpoint_info["state_file"], "rb") as f:
                if msgpack is not None:
                    state = msgpack.load(f)
                else:
                    state = pickle.load(f)
            return Policy.from_state(state)

    @staticmethod
    def from_state(state: PolicyState) -> "Policy":
        """Recovers a Policy from a state object.

        The `state` of an instantiated Policy can be retrieved by calling its
        `get_state` method. This only works for the V2 Policy classes (EagerTFPolicyV2,
        SynamicTFPolicyV2, and TorchPolicyV2). It contains all information necessary
        to create the Policy. No access to the original code (e.g. configs, knowledge of
        the policy's class, etc..) is needed.

        Args:
            state: The state to recover a new Policy instance from.

        Returns:
            A new Policy instance.
        """
        serialized_pol_spec: Optional[dict] = state.get("policy_spec")
        if serialized_pol_spec is None:
            raise ValueError(
                "No `policy_spec` key was found in given `state`! "
                "Cannot create new Policy."
            )
        pol_spec = PolicySpec.deserialize(serialized_pol_spec)
        actual_class = get_tf_eager_cls_if_necessary(
            pol_spec.policy_class,
            pol_spec.config,
        )

        if pol_spec.config["framework"] == "tf":
            from ray.rllib.policy.tf_policy import TFPolicy

            return TFPolicy._tf1_from_state_helper(state)

        # Create the new policy.
        new_policy = actual_class(
            # Note(jungong) : we are intentionally not using keyward arguments here
            # because some policies name the observation space parameter obs_space,
            # and some others name it observation_space.
            pol_spec.observation_space,
            pol_spec.action_space,
            pol_spec.config,
        )

        # Set the new policy's state (weights, optimizer vars, exploration state,
        # etc..).
        new_policy.set_state(state)
        # Return the new policy.
        return new_policy

    def init_view_requirements(self):
        """Maximal view requirements dict for `learn_on_batch()` and
        `compute_actions` calls.
        Specific policies can override this function to provide custom
        list of view requirements.
        """
        # Maximal view requirements dict for `learn_on_batch()` and
        # `compute_actions` calls.
        # View requirements will be automatically filtered out later based
        # on the postprocessing and loss functions to ensure optimal data
        # collection and transfer performance.
        view_reqs = self._get_default_view_requirements()
        if not hasattr(self, "view_requirements"):
            self.view_requirements = view_reqs
        else:
            for k, v in view_reqs.items():
                if k not in self.view_requirements:
                    self.view_requirements[k] = v

    def get_connector_metrics(self) -> Dict:
        """Get metrics on timing from connectors."""
        return {
            "agent_connectors": {
                name + "_ms": 1000 * timer.mean
                for name, timer in self.agent_connectors.timers.items()
            },
            "action_connectors": {
                name + "_ms": 1000 * timer.mean
                for name, timer in self.agent_connectors.timers.items()
            },
        }

    def reset_connectors(self, env_id) -> None:
        """Reset action- and agent-connectors for this policy."""
        self.agent_connectors.reset(env_id=env_id)
        self.action_connectors.reset(env_id=env_id)

    def compute_single_action(
        self,
        obs: Optional[TensorStructType] = None,
        state: Optional[List[TensorType]] = None,
        *,
        prev_action: Optional[TensorStructType] = None,
        prev_reward: Optional[TensorStructType] = None,
        info: dict = None,
        input_dict: Optional[SampleBatch] = None,
        episode=None,
        explore: Optional[bool] = None,
        timestep: Optional[int] = None,
        # Kwars placeholder for future compatibility.
        **kwargs,
    ) -> Tuple[TensorStructType, List[TensorType], Dict[str, TensorType]]:
        """Computes and returns a single (B=1) action value.

        Takes an input dict (usually a SampleBatch) as its main data input.
        This allows for using this method in case a more complex input pattern
        (view requirements) is needed, for example when the Model requires the
        last n observations, the last m actions/rewards, or a combination
        of any of these.
        Alternatively, in case no complex inputs are required, takes a single
        `obs` values (and possibly single state values, prev-action/reward
        values, etc..).

        Args:
            obs: Single observation.
            state: List of RNN state inputs, if any.
            prev_action: Previous action value, if any.
            prev_reward: Previous reward, if any.
            info: Info object, if any.
            input_dict: A SampleBatch or input dict containing the
                single (unbatched) Tensors to compute actions. If given, it'll
                be used instead of `obs`, `state`, `prev_action|reward`, and
                `info`.
            episode: This provides access to all of the internal episode state,
                which may be useful for model-based or multi-agent algorithms.
            explore: Whether to pick an exploitation or
                exploration action
                (default: None -> use self.config["explore"]).
            timestep: The current (sampling) time step.

        Keyword Args:
            kwargs: Forward compatibility placeholder.

        Returns:
            Tuple consisting of the action, the list of RNN state outputs (if
            any), and a dictionary of extra features (if any).
        """
        # Build the input-dict used for the call to
        # `self.compute_actions_from_input_dict()`.
        if input_dict is None:
            input_dict = {SampleBatch.OBS: obs}
            if state is not None:
                for i, s in enumerate(state):
                    input_dict[f"state_in_{i}"] = s
            if prev_action is not None:
                input_dict[SampleBatch.PREV_ACTIONS] = prev_action
            if prev_reward is not None:
                input_dict[SampleBatch.PREV_REWARDS] = prev_reward
            if info is not None:
                input_dict[SampleBatch.INFOS] = info

        # Batch all data in input dict.
        input_dict = tree.map_structure_with_path(
            lambda p, s: (
                s
                if p == "seq_lens"
                else s.unsqueeze(0)
                if torch and isinstance(s, torch.Tensor)
                else np.expand_dims(s, 0)
            ),
            input_dict,
        )

        episodes = None
        if episode is not None:
            episodes = [episode]

        out = self.compute_actions_from_input_dict(
            input_dict=SampleBatch(input_dict),
            episodes=episodes,
            explore=explore,
            timestep=timestep,
        )

        # Some policies don't return a tuple, but always just a single action.
        # E.g. ES and ARS.
        if not isinstance(out, tuple):
            single_action = out
            state_out = []
            info = {}
        # Normal case: Policy should return (action, state, info) tuple.
        else:
            batched_action, state_out, info = out
            single_action = unbatch(batched_action)
        assert len(single_action) == 1
        single_action = single_action[0]

        # Return action, internal state(s), infos.
        return (
            single_action,
            tree.map_structure(lambda x: x[0], state_out),
            tree.map_structure(lambda x: x[0], info),
        )

    def compute_actions_from_input_dict(
        self,
        input_dict: Union[SampleBatch, Dict[str, TensorStructType]],
        explore: Optional[bool] = None,
        timestep: Optional[int] = None,
        episodes=None,
        **kwargs,
    ) -> Tuple[TensorType, List[TensorType], Dict[str, TensorType]]:
        """Computes actions from collected samples (across multiple-agents).

        Takes an input dict (usually a SampleBatch) as its main data input.
        This allows for using this method in case a more complex input pattern
        (view requirements) is needed, for example when the Model requires the
        last n observations, the last m actions/rewards, or a combination
        of any of these.

        Args:
            input_dict: A SampleBatch or input dict containing the Tensors
                to compute actions. `input_dict` already abides to the
                Policy's as well as the Model's view requirements and can
                thus be passed to the Model as-is.
            explore: Whether to pick an exploitation or exploration
                action (default: None -> use self.config["explore"]).
            timestep: The current (sampling) time step.
            episodes: This provides access to all of the internal episodes'
                state, which may be useful for model-based or multi-agent
                algorithms.

        Keyword Args:
            kwargs: Forward compatibility placeholder.

        Returns:
            actions: Batch of output actions, with shape like
                [BATCH_SIZE, ACTION_SHAPE].
            state_outs: List of RNN state output
                batches, if any, each with shape [BATCH_SIZE, STATE_SIZE].
            info: Dictionary of extra feature batches, if any, with shape like
                {"f1": [BATCH_SIZE, ...], "f2": [BATCH_SIZE, ...]}.
        """
        # Default implementation just passes obs, prev-a/r, and states on to
        # `self.compute_actions()`.
        state_batches = [s for k, s in input_dict.items() if k.startswith("state_in")]
        return self.compute_actions(
            input_dict[SampleBatch.OBS],
            state_batches,
            prev_action_batch=input_dict.get(SampleBatch.PREV_ACTIONS),
            prev_reward_batch=input_dict.get(SampleBatch.PREV_REWARDS),
            info_batch=input_dict.get(SampleBatch.INFOS),
            explore=explore,
            timestep=timestep,
            episodes=episodes,
            **kwargs,
        )

    @abstractmethod
    def compute_actions(
        self,
        obs_batch: Union[List[TensorStructType], TensorStructType],
        state_batches: Optional[List[TensorType]] = None,
        prev_action_batch: Union[List[TensorStructType], TensorStructType] = None,
        prev_reward_batch: Union[List[TensorStructType], TensorStructType] = None,
        info_batch: Optional[Dict[str, list]] = None,
        episodes: Optional[List] = None,
        explore: Optional[bool] = None,
        timestep: Optional[int] = None,
        **kwargs,
    ) -> Tuple[TensorType, List[TensorType], Dict[str, TensorType]]:
        """Computes actions for the current policy.

        Args:
            obs_batch: Batch of observations.
            state_batches: List of RNN state input batches, if any.
            prev_action_batch: Batch of previous action values.
            prev_reward_batch: Batch of previous rewards.
            info_batch: Batch of info objects.
            episodes: List of Episode objects, one for each obs in
                obs_batch. This provides access to all of the internal
                episode state, which may be useful for model-based or
                multi-agent algorithms.
            explore: Whether to pick an exploitation or exploration action.
                Set to None (default) for using the value of
                `self.config["explore"]`.
            timestep: The current (sampling) time step.

        Keyword Args:
            kwargs: Forward compatibility placeholder

        Returns:
            actions: Batch of output actions, with shape like
                [BATCH_SIZE, ACTION_SHAPE].
            state_outs (List[TensorType]): List of RNN state output
                batches, if any, each with shape [BATCH_SIZE, STATE_SIZE].
            info (List[dict]): Dictionary of extra feature batches, if any,
                with shape like
                {"f1": [BATCH_SIZE, ...], "f2": [BATCH_SIZE, ...]}.
        """
        raise NotImplementedError

    def compute_log_likelihoods(
        self,
        actions: Union[List[TensorType], TensorType],
        obs_batch: Union[List[TensorType], TensorType],
        state_batches: Optional[List[TensorType]] = None,
        prev_action_batch: Optional[Union[List[TensorType], TensorType]] = None,
        prev_reward_batch: Optional[Union[List[TensorType], TensorType]] = None,
        actions_normalized: bool = True,
        in_training: bool = True,
    ) -> TensorType:
        """Computes the log-prob/likelihood for a given action and observation.

        The log-likelihood is calculated using this Policy's action
        distribution class (self.dist_class).

        Args:
            actions: Batch of actions, for which to retrieve the
                log-probs/likelihoods (given all other inputs: obs,
                states, ..).
            obs_batch: Batch of observations.
            state_batches: List of RNN state input batches, if any.
            prev_action_batch: Batch of previous action values.
            prev_reward_batch: Batch of previous rewards.
            actions_normalized: Is the given `actions` already normalized
                (between -1.0 and 1.0) or not? If not and
                `normalize_actions=True`, we need to normalize the given
                actions first, before calculating log likelihoods.
            in_training: Whether to use the forward_train() or forward_exploration() of
                the underlying RLModule.
        Returns:
            Batch of log probs/likelihoods, with shape: [BATCH_SIZE].
        """
        raise NotImplementedError

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def postprocess_trajectory(
        self,
        sample_batch: SampleBatch,
        other_agent_batches: Optional[
            Dict[AgentID, Tuple["Policy", SampleBatch]]
        ] = None,
        episode=None,
    ) -> SampleBatch:
        """Implements algorithm-specific trajectory postprocessing.

        This will be called on each trajectory fragment computed during policy
        evaluation. Each fragment is guaranteed to be only from one episode.
        The given fragment may or may not contain the end of this episode,
        depending on the `batch_mode=truncate_episodes|complete_episodes`,
        `rollout_fragment_length`, and other settings.

        Args:
            sample_batch: batch of experiences for the policy,
                which will contain at most one episode trajectory.
            other_agent_batches: In a multi-agent env, this contains a
                mapping of agent ids to (policy, agent_batch) tuples
                containing the policy and experiences of the other agents.
            episode: An optional multi-agent episode object to provide
                access to all of the internal episode state, which may
                be useful for model-based or multi-agent algorithms.

        Returns:
            The postprocessed sample batch.
        """
        # The default implementation just returns the same, unaltered batch.
        return sample_batch

    @OverrideToImplementCustomLogic
    def loss(
        self, model: ModelV2, dist_class: ActionDistribution, train_batch: SampleBatch
    ) -> Union[TensorType, List[TensorType]]:
        """Loss function for this Policy.

        Override this method in order to implement custom loss computations.

        Args:
            model: The model to calculate the loss(es).
            dist_class: The action distribution class to sample actions
                from the model's outputs.
            train_batch: The input batch on which to calculate the loss.

        Returns:
            Either a single loss tensor or a list of loss tensors.
        """
        raise NotImplementedError

    def learn_on_batch(self, samples: SampleBatch) -> Dict[str, TensorType]:
        """Perform one learning update, given `samples`.

        Either this method or the combination of `compute_gradients` and
        `apply_gradients` must be implemented by subclasses.

        Args:
            samples: The SampleBatch object to learn from.

        Returns:
            Dictionary of extra metadata from `compute_gradients()`.

        .. testcode::
            :skipif: True

            policy, sample_batch = ...
            policy.learn_on_batch(sample_batch)
        """
        # The default implementation is simply a fused `compute_gradients` plus
        # `apply_gradients` call.
        grads, grad_info = self.compute_gradients(samples)
        self.apply_gradients(grads)
        return grad_info

    def learn_on_batch_from_replay_buffer(
        self, replay_actor: ActorHandle, policy_id: PolicyID
    ) -> Dict[str, TensorType]:
        """Samples a batch from given replay actor and performs an update.

        Args:
            replay_actor: The replay buffer actor to sample from.
            policy_id: The ID of this policy.

        Returns:
            Dictionary of extra metadata from `compute_gradients()`.
        """
        # Sample a batch from the given replay actor.
        # Note that for better performance (less data sent through the
        # network), this policy should be co-located on the same node
        # as `replay_actor`. Such a co-location step is usually done during
        # the Algorithm's `setup()` phase.
        batch = ray.get(replay_actor.replay.remote(policy_id=policy_id))
        if batch is None:
            return {}

        # Send to own learn_on_batch method for updating.
        # TODO: hack w/ `hasattr`
        if hasattr(self, "devices") and len(self.devices) > 1:
            self.load_batch_into_buffer(batch, buffer_index=0)
            return self.learn_on_loaded_batch(offset=0, buffer_index=0)
        else:
            return self.learn_on_batch(batch)

    def load_batch_into_buffer(self, batch: SampleBatch, buffer_index: int = 0) -> int:
        """Bulk-loads the given SampleBatch into the devices' memories.

        The data is split equally across all the Policy's devices.
        If the data is not evenly divisible by the batch size, excess data
        should be discarded.

        Args:
            batch: The SampleBatch to load.
            buffer_index: The index of the buffer (a MultiGPUTowerStack) to use
                on the devices. The number of buffers on each device depends
                on the value of the `num_multi_gpu_tower_stacks` config key.

        Returns:
            The number of tuples loaded per device.
        """
        raise NotImplementedError

    def get_num_samples_loaded_into_buffer(self, buffer_index: int = 0) -> int:
        """Returns the number of currently loaded samples in the given buffer.

        Args:
            buffer_index: The index of the buffer (a MultiGPUTowerStack)
                to use on the devices. The number of buffers on each device
                depends on the value of the `num_multi_gpu_tower_stacks` config
                key.

        Returns:
            The number of tuples loaded per device.
        """
        raise NotImplementedError

    def learn_on_loaded_batch(self, offset: int = 0, buffer_index: int = 0):
        """Runs a single step of SGD on an already loaded data in a buffer.

        Runs an SGD step over a slice of the pre-loaded batch, offset by
        the `offset` argument (useful for performing n minibatch SGD
        updates repeatedly on the same, already pre-loaded data).

        Updates the model weights based on the averaged per-device gradients.

        Args:
            offset: Offset into the preloaded data. Used for pre-loading
                a train-batch once to a device, then iterating over
                (subsampling through) this batch n times doing minibatch SGD.
            buffer_index: The index of the buffer (a MultiGPUTowerStack)
                to take the already pre-loaded data from. The number of buffers
                on each device depends on the value of the
                `num_multi_gpu_tower_stacks` config key.

        Returns:
            The outputs of extra_ops evaluated over the batch.
        """
        raise NotImplementedError

    def compute_gradients(
        self, postprocessed_batch: SampleBatch
    ) -> Tuple[ModelGradients, Dict[str, TensorType]]:
        """Computes gradients given a batch of experiences.

        Either this in combination with `apply_gradients()` or
        `learn_on_batch()` must be implemented by subclasses.

        Args:
            postprocessed_batch: The SampleBatch object to use
                for calculating gradients.

        Returns:
            grads: List of gradient output values.
            grad_info: Extra policy-specific info values.
        """
        raise NotImplementedError

    def apply_gradients(self, gradients: ModelGradients) -> None:
        """Applies the (previously) computed gradients.

        Either this in combination with `compute_gradients()` or
        `learn_on_batch()` must be implemented by subclasses.

        Args:
            gradients: The already calculated gradients to apply to this
                Policy.
        """
        raise NotImplementedError

    def get_weights(self) -> ModelWeights:
        """Returns model weights.

        Note: The return value of this method will reside under the "weights"
        key in the return value of Policy.get_state(). Model weights are only
        one part of a Policy's state. Other state information contains:
        optimizer variables, exploration state, and global state vars such as
        the sampling timestep.

        Returns:
            Serializable copy or view of model weights.
        """
        raise NotImplementedError

    def set_weights(self, weights: ModelWeights) -> None:
        """Sets this Policy's model's weights.

        Note: Model weights are only one part of a Policy's state. Other
        state information contains: optimizer variables, exploration state,
        and global state vars such as the sampling timestep.

        Args:
            weights: Serializable copy or view of model weights.
        """
        raise NotImplementedError

    def get_exploration_state(self) -> Dict[str, TensorType]:
        """Returns the state of this Policy's exploration component.

        Returns:
            Serializable information on the `self.exploration` object.
        """
        return self.exploration.get_state()

    def is_recurrent(self) -> bool:
        """Whether this Policy holds a recurrent Model.

        Returns:
            True if this Policy has an RNN-based Model.
        """
        return False

    def num_state_tensors(self) -> int:
        """The number of internal states needed by the RNN-Model of the Policy.

        Returns:
            int: The number of RNN internal states kept by this Policy's Model.
        """
        return 0

    def get_initial_state(self) -> List[TensorType]:
        """Returns initial RNN state for the current policy.

        Returns:
            List[TensorType]: Initial RNN state for the current policy.
        """
        return []

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def get_state(self) -> PolicyState:
        """Returns the entire current state of this Policy.

        Note: Not to be confused with an RNN model's internal state.
        State includes the Model(s)' weights, optimizer weights,
        the exploration component's state, as well as global variables, such
        as sampling timesteps.

        Note that the state may contain references to the original variables.
        This means that you may need to deepcopy() the state before mutating it.

        Returns:
            Serialized local state.
        """
        state = {
            # All the policy's weights.
            "weights": self.get_weights(),
            # The current global timestep.
            "global_timestep": self.global_timestep,
            # The current num_grad_updates counter.
            "num_grad_updates": self.num_grad_updates,
        }

        # Add this Policy's spec so it can be retreived w/o access to the original
        # code.
        policy_spec = PolicySpec(
            policy_class=type(self),
            observation_space=self.observation_space,
            action_space=self.action_space,
            config=self.config,
        )
        state["policy_spec"] = policy_spec.serialize()

        # Checkpoint connectors state as well if enabled.
        connector_configs = {}
        if self.agent_connectors:
            connector_configs["agent"] = self.agent_connectors.to_state()
        if self.action_connectors:
            connector_configs["action"] = self.action_connectors.to_state()
        state["connector_configs"] = connector_configs

        return state

    def restore_connectors(self, state: PolicyState):
        """Restore agent and action connectors if configs available.

        Args:
            state: The new state to set this policy to. Can be
                obtained by calling `self.get_state()`.
        """
        # To avoid a circular dependency problem cause by SampleBatch.
        from ray.rllib.connectors.util import restore_connectors_for_policy

        connector_configs = state.get("connector_configs", {})
        if "agent" in connector_configs:
            self.agent_connectors = restore_connectors_for_policy(
                self, connector_configs["agent"]
            )
            logger.debug("restoring agent connectors:")
            logger.debug(self.agent_connectors.__str__(indentation=4))
        if "action" in connector_configs:
            self.action_connectors = restore_connectors_for_policy(
                self, connector_configs["action"]
            )
            logger.debug("restoring action connectors:")
            logger.debug(self.action_connectors.__str__(indentation=4))

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def set_state(self, state: PolicyState) -> None:
        """Restores the entire current state of this Policy from `state`.

        Args:
            state: The new state to set this policy to. Can be
                obtained by calling `self.get_state()`.
        """
        if "policy_spec" in state:
            policy_spec = PolicySpec.deserialize(state["policy_spec"])
            # Assert spaces remained the same.
            if (
                policy_spec.observation_space is not None
                and policy_spec.observation_space != self.observation_space
            ):
                logger.warning(
                    "`observation_space` in given policy state ("
                    f"{policy_spec.observation_space}) does not match this Policy's "
                    f"observation space ({self.observation_space})."
                )
            if (
                policy_spec.action_space is not None
                and policy_spec.action_space != self.action_space
            ):
                logger.warning(
                    "`action_space` in given policy state ("
                    f"{policy_spec.action_space}) does not match this Policy's "
                    f"action space ({self.action_space})."
                )
            # Override config, if part of the spec.
            if policy_spec.config:
                self.config = policy_spec.config

        # Override NN weights.
        self.set_weights(state["weights"])
        self.restore_connectors(state)

    def apply(
        self,
        func: Callable[["Policy", Optional[Any], Optional[Any]], T],
        *args,
        **kwargs,
    ) -> T:
        """Calls the given function with this Policy instance.

        Useful for when the Policy class has been converted into a ActorHandle
        and the user needs to execute some functionality (e.g. add a property)
        on the underlying policy object.

        Args:
            func: The function to call, with this Policy as first
                argument, followed by args, and kwargs.
            args: Optional additional args to pass to the function call.
            kwargs: Optional additional kwargs to pass to the function call.

        Returns:
            The return value of the function call.
        """
        return func(self, *args, **kwargs)

    def on_global_var_update(self, global_vars: Dict[str, TensorType]) -> None:
        """Called on an update to global vars.

        Args:
            global_vars: Global variables by str key, broadcast from the
                driver.
        """
        # Store the current global time step (sum over all policies' sample
        # steps).
        # Make sure, we keep global_timestep as a Tensor for tf-eager
        # (leads to memory leaks if not doing so).
        if self.framework == "tf2":
            self.global_timestep.assign(global_vars["timestep"])
        else:
            self.global_timestep = global_vars["timestep"]
        # Update our lifetime gradient update counter.
        num_grad_updates = global_vars.get("num_grad_updates")
        if num_grad_updates is not None:
            self.num_grad_updates = num_grad_updates

    def export_checkpoint(
        self,
        export_dir: str,
        filename_prefix=DEPRECATED_VALUE,
        *,
        policy_state: Optional[PolicyState] = None,
        checkpoint_format: str = "cloudpickle",
    ) -> None:
        """Exports Policy checkpoint to a local directory and returns an AIR Checkpoint.

        Args:
            export_dir: Local writable directory to store the AIR Checkpoint
                information into.
            policy_state: An optional PolicyState to write to disk. Used by
                `Algorithm.save_checkpoint()` to save on the additional
                `self.get_state()` calls of its different Policies.
            checkpoint_format: Either one of 'cloudpickle' or 'msgpack'.

        .. testcode::
            :skipif: True

            from ray.rllib.algorithms.ppo import PPOTorchPolicy
            policy = PPOTorchPolicy(...)
            policy.export_checkpoint("/tmp/export_dir")
        """
        # `filename_prefix` should not longer be used as new Policy checkpoints
        # contain more than one file with a fixed filename structure.
        if filename_prefix != DEPRECATED_VALUE:
            deprecation_warning(
                old="Policy.export_checkpoint(filename_prefix=...)",
                error=True,
            )
        if checkpoint_format not in ["cloudpickle", "msgpack"]:
            raise ValueError(
                f"Value of `checkpoint_format` ({checkpoint_format}) must either be "
                "'cloudpickle' or 'msgpack'!"
            )

        if policy_state is None:
            policy_state = self.get_state()

        # Write main policy state file.
        os.makedirs(export_dir, exist_ok=True)
        if checkpoint_format == "cloudpickle":
            policy_state["checkpoint_version"] = CHECKPOINT_VERSION
            state_file = "policy_state.pkl"
            with open(os.path.join(export_dir, state_file), "w+b") as f:
                pickle.dump(policy_state, f)
        else:
            from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

            msgpack = try_import_msgpack(error=True)
            policy_state["checkpoint_version"] = str(CHECKPOINT_VERSION)
            # Serialize the config for msgpack dump'ing.
            policy_state["policy_spec"]["config"] = AlgorithmConfig._serialize_dict(
                policy_state["policy_spec"]["config"]
            )
            state_file = "policy_state.msgpck"
            with open(os.path.join(export_dir, state_file), "w+b") as f:
                msgpack.dump(policy_state, f)

        # Write RLlib checkpoint json.
        with open(os.path.join(export_dir, "rllib_checkpoint.json"), "w") as f:
            json.dump(
                {
                    "type": "Policy",
                    "checkpoint_version": str(policy_state["checkpoint_version"]),
                    "format": checkpoint_format,
                    "state_file": state_file,
                    "ray_version": ray.__version__,
                    "ray_commit": ray.__commit__,
                },
                f,
            )

        # Add external model files, if required.
        if self.config["export_native_model_files"]:
            self.export_model(os.path.join(export_dir, "model"))

    def export_model(self, export_dir: str, onnx: Optional[int] = None) -> None:
        """Exports the Policy's Model to local directory for serving.

        Note: The file format will depend on the deep learning framework used.
        See the child classed of Policy and their `export_model`
        implementations for more details.

        Args:
            export_dir: Local writable directory.
            onnx: If given, will export model in ONNX format. The
                value of this parameter set the ONNX OpSet version to use.

        Raises:
            ValueError: If a native DL-framework based model (e.g. a keras Model)
                cannot be saved to disk for various reasons.
        """
        raise NotImplementedError

    def import_model_from_h5(self, import_file: str) -> None:
        """Imports Policy from local file.

        Args:
            import_file: Local readable file.
        """
        raise NotImplementedError

    def get_session(self) -> Optional["tf1.Session"]:
        """Returns tf.Session object to use for computing actions or None.

        Note: This method only applies to TFPolicy sub-classes. All other
        sub-classes should expect a None to be returned from this method.

        Returns:
            The tf Session to use for computing actions and losses with
                this policy or None.
        """
        return None

    def get_host(self) -> str:
        """Returns the computer's network name.

        Returns:
            The computer's networks name or an empty string, if the network
            name could not be determined.
        """
        return platform.node()

    def _get_num_gpus_for_policy(self) -> int:
        """Decide on the number of CPU/GPU nodes this policy should run on.

        Return:
            0 if policy should run on CPU. >0 if policy should run on 1 or
            more GPUs.
        """
        worker_idx = self.config.get("worker_index", 0)
        fake_gpus = self.config.get("_fake_gpus", False)

        if (
            ray._private.worker._mode() == ray._private.worker.LOCAL_MODE
            and not fake_gpus
        ):
            # If in local debugging mode, and _fake_gpus is not on.
            num_gpus = 0
        elif worker_idx == 0:
            # If head node, take num_gpus.
            num_gpus = self.config["num_gpus"]
        else:
            # If worker node, take `num_gpus_per_env_runner`.
            num_gpus = self.config["num_gpus_per_env_runner"]

        if num_gpus == 0:
            dev = "CPU"
        else:
            dev = "{} {}".format(num_gpus, "fake-GPUs" if fake_gpus else "GPUs")

        logger.info(
            "Policy (worker={}) running on {}.".format(
                worker_idx if worker_idx > 0 else "local", dev
            )
        )

        return num_gpus

    def _create_exploration(self) -> Exploration:
        """Creates the Policy's Exploration object.

        This method only exists b/c some Algorithms do not use TfPolicy nor
        TorchPolicy, but inherit directly from Policy. Others inherit from
        TfPolicy w/o using DynamicTFPolicy.

        Returns:
            Exploration: The Exploration object to be used by this Policy.
        """
        if getattr(self, "exploration", None) is not None:
            return self.exploration

        exploration = from_config(
            Exploration,
            self.config.get("exploration_config", {"type": "StochasticSampling"}),
            action_space=self.action_space,
            policy_config=self.config,
            model=getattr(self, "model", None),
            num_workers=self.config.get("num_env_runners", 0),
            worker_index=self.config.get("worker_index", 0),
            framework=getattr(self, "framework", self.config.get("framework", "tf")),
        )
        return exploration

    def _get_default_view_requirements(self):
        """Returns a default ViewRequirements dict.

        Note: This is the base/maximum requirement dict, from which later
        some requirements will be subtracted again automatically to streamline
        data collection, batch creation, and data transfer.

        Returns:
            ViewReqDict: The default view requirements dict.
        """

        # Default view requirements (equal to those that we would use before
        # the trajectory view API was introduced).
        return {
            SampleBatch.OBS: ViewRequirement(space=self.observation_space),
            SampleBatch.NEXT_OBS: ViewRequirement(
                data_col=SampleBatch.OBS,
                shift=1,
                space=self.observation_space,
                used_for_compute_actions=False,
            ),
            SampleBatch.ACTIONS: ViewRequirement(
                space=self.action_space, used_for_compute_actions=False
            ),
            # For backward compatibility with custom Models that don't specify
            # these explicitly (will be removed by Policy if not used).
            SampleBatch.PREV_ACTIONS: ViewRequirement(
                data_col=SampleBatch.ACTIONS, shift=-1, space=self.action_space
            ),
            SampleBatch.REWARDS: ViewRequirement(),
            # For backward compatibility with custom Models that don't specify
            # these explicitly (will be removed by Policy if not used).
            SampleBatch.PREV_REWARDS: ViewRequirement(
                data_col=SampleBatch.REWARDS, shift=-1
            ),
            SampleBatch.TERMINATEDS: ViewRequirement(),
            SampleBatch.TRUNCATEDS: ViewRequirement(),
            SampleBatch.INFOS: ViewRequirement(used_for_compute_actions=False),
            SampleBatch.EPS_ID: ViewRequirement(),
            SampleBatch.UNROLL_ID: ViewRequirement(),
            SampleBatch.AGENT_INDEX: ViewRequirement(),
            SampleBatch.T: ViewRequirement(),
        }

    def _initialize_loss_from_dummy_batch(
        self,
        auto_remove_unneeded_view_reqs: bool = True,
        stats_fn=None,
    ) -> None:
        """Performs test calls through policy's model and loss.

        NOTE: This base method should work for define-by-run Policies such as
        torch and tf-eager policies.

        If required, will thereby detect automatically, which data views are
        required by a) the forward pass, b) the postprocessing, and c) the loss
        functions, and remove those from self.view_requirements that are not
        necessary for these computations (to save data storage and transfer).

        Args:
            auto_remove_unneeded_view_reqs: Whether to automatically
                remove those ViewRequirements records from
                self.view_requirements that are not needed.
                stats_fn (Optional[Callable[[Policy, SampleBatch], Dict[str,
                    TensorType]]]): An optional stats function to be called after
                    the loss.
        """

        if self.config.get("_disable_initialize_loss_from_dummy_batch", False):
            return
        # Signal Policy that currently we do not like to eager/jit trace
        # any function calls. This is to be able to track, which columns
        # in the dummy batch are accessed by the different function (e.g.
        # loss) such that we can then adjust our view requirements.
        self._no_tracing = True
        # Save for later so that loss init does not change global timestep
        global_ts_before_init = int(convert_to_numpy(self.global_timestep))

        sample_batch_size = min(
            max(self.batch_divisibility_req * 4, 32),
            self.config["train_batch_size"],  # Don't go over the asked batch size.
        )
        self._dummy_batch = self._get_dummy_batch_from_view_requirements(
            sample_batch_size
        )
        self._lazy_tensor_dict(self._dummy_batch)
        explore = False
        actions, state_outs, extra_outs = self.compute_actions_from_input_dict(
            self._dummy_batch, explore=explore
        )
        for key, view_req in self.view_requirements.items():
            if key not in self._dummy_batch.accessed_keys:
                view_req.used_for_compute_actions = False
        # Add all extra action outputs to view reqirements (these may be
        # filtered out later again, if not needed for postprocessing or loss).
        for key, value in extra_outs.items():
            self._dummy_batch[key] = value
            if key not in self.view_requirements:
                if isinstance(value, (dict, np.ndarray)):
                    # the assumption is that value is a nested_dict of np.arrays leaves
                    space = get_gym_space_from_struct_of_tensors(value)
                    self.view_requirements[key] = ViewRequirement(
                        space=space, used_for_compute_actions=False
                    )
                else:
                    raise ValueError(
                        "policy.compute_actions_from_input_dict() returns an "
                        "extra action output that is neither a numpy array nor a dict."
                    )

        for key in self._dummy_batch.accessed_keys:
            if key not in self.view_requirements:
                self.view_requirements[key] = ViewRequirement()
                self.view_requirements[key].used_for_compute_actions = False
            # TODO (kourosh) Why did we use to make used_for_compute_actions True here?
        new_batch = self._get_dummy_batch_from_view_requirements(sample_batch_size)
        # Make sure the dummy_batch will return numpy arrays when accessed
        self._dummy_batch.set_get_interceptor(None)

        # try to re-use the output of the previous run to avoid overriding things that
        # would break (e.g. scale = 0 of Normal distribution cannot be zero)
        for k in new_batch:
            if k not in self._dummy_batch:
                self._dummy_batch[k] = new_batch[k]

        # Make sure the book-keeping of dummy_batch keys are reset to correcly track
        # what is accessed, what is added and what's deleted from now on.
        self._dummy_batch.accessed_keys.clear()
        self._dummy_batch.deleted_keys.clear()
        self._dummy_batch.added_keys.clear()

        if self.exploration:
            # Policies with RLModules don't have an exploration object.
            self.exploration.postprocess_trajectory(self, self._dummy_batch)

        postprocessed_batch = self.postprocess_trajectory(self._dummy_batch)
        seq_lens = None
        if state_outs:
            B = 4  # For RNNs, have B=4, T=[depends on sample_batch_size]
            i = 0
            while "state_in_{}".format(i) in postprocessed_batch:
                postprocessed_batch["state_in_{}".format(i)] = postprocessed_batch[
                    "state_in_{}".format(i)
                ][:B]
                if "state_out_{}".format(i) in postprocessed_batch:
                    postprocessed_batch["state_out_{}".format(i)] = postprocessed_batch[
                        "state_out_{}".format(i)
                    ][:B]
                i += 1

            seq_len = sample_batch_size // B
            seq_lens = np.array([seq_len for _ in range(B)], dtype=np.int32)
            postprocessed_batch[SampleBatch.SEQ_LENS] = seq_lens

        # Switch on lazy to-tensor conversion on `postprocessed_batch`.
        train_batch = self._lazy_tensor_dict(postprocessed_batch)
        # Calling loss, so set `is_training` to True.
        train_batch.set_training(True)
        if seq_lens is not None:
            train_batch[SampleBatch.SEQ_LENS] = seq_lens
        train_batch.count = self._dummy_batch.count

        # Call the loss function, if it exists.
        # TODO(jungong) : clean up after all agents get migrated.
        # We should simply do self.loss(...) here.
        if self._loss is not None:
            self._loss(self, self.model, self.dist_class, train_batch)
        elif is_overridden(self.loss) and not self.config["in_evaluation"]:
            self.loss(self.model, self.dist_class, train_batch)
        # Call the stats fn, if given.
        # TODO(jungong) : clean up after all agents get migrated.
        # We should simply do self.stats_fn(train_batch) here.
        if stats_fn is not None:
            stats_fn(self, train_batch)
        if hasattr(self, "stats_fn") and not self.config["in_evaluation"]:
            self.stats_fn(train_batch)

        # Re-enable tracing.
        self._no_tracing = False

        # Add new columns automatically to view-reqs.
        if auto_remove_unneeded_view_reqs:
            # Add those needed for postprocessing and training.
            all_accessed_keys = (
                train_batch.accessed_keys
                | self._dummy_batch.accessed_keys
                | self._dummy_batch.added_keys
            )
            for key in all_accessed_keys:
                if key not in self.view_requirements and key != SampleBatch.SEQ_LENS:
                    self.view_requirements[key] = ViewRequirement(
                        used_for_compute_actions=False
                    )
            if self._loss or is_overridden(self.loss):
                # Tag those only needed for post-processing (with some
                # exceptions).
                for key in self._dummy_batch.accessed_keys:
                    if (
                        key not in train_batch.accessed_keys
                        and key in self.view_requirements
                        and key not in self.model.view_requirements
                        and key
                        not in [
                            SampleBatch.EPS_ID,
                            SampleBatch.AGENT_INDEX,
                            SampleBatch.UNROLL_ID,
                            SampleBatch.TERMINATEDS,
                            SampleBatch.TRUNCATEDS,
                            SampleBatch.REWARDS,
                            SampleBatch.INFOS,
                            SampleBatch.T,
                        ]
                    ):
                        self.view_requirements[key].used_for_training = False
                # Remove those not needed at all (leave those that are needed
                # by Sampler to properly execute sample collection). Also always leave
                # TERMINATEDS, TRUNCATEDS, REWARDS, INFOS, no matter what.
                for key in list(self.view_requirements.keys()):
                    if (
                        key not in all_accessed_keys
                        and key
                        not in [
                            SampleBatch.EPS_ID,
                            SampleBatch.AGENT_INDEX,
                            SampleBatch.UNROLL_ID,
                            SampleBatch.TERMINATEDS,
                            SampleBatch.TRUNCATEDS,
                            SampleBatch.REWARDS,
                            SampleBatch.INFOS,
                            SampleBatch.T,
                        ]
                        and key not in self.model.view_requirements
                    ):
                        # If user deleted this key manually in postprocessing
                        # fn, warn about it and do not remove from
                        # view-requirements.
                        if key in self._dummy_batch.deleted_keys:
                            logger.warning(
                                "SampleBatch key '{}' was deleted manually in "
                                "postprocessing function! RLlib will "
                                "automatically remove non-used items from the "
                                "data stream. Remove the `del` from your "
                                "postprocessing function.".format(key)
                            )
                        # If we are not writing output to disk, save to erase
                        # this key to save space in the sample batch.
                        elif self.config["output"] is None:
                            del self.view_requirements[key]

        if type(self.global_timestep) is int:
            self.global_timestep = global_ts_before_init
        elif isinstance(self.global_timestep, tf.Variable):
            self.global_timestep.assign(global_ts_before_init)
        else:
            raise ValueError(
                "Variable self.global_timestep of policy {} needs to be "
                "either of type `int` or `tf.Variable`, "
                "but is of type {}.".format(self, type(self.global_timestep))
            )

    def maybe_remove_time_dimension(self, input_dict: Dict[str, TensorType]):
        """Removes a time dimension for recurrent RLModules.

        Args:
            input_dict: The input dict.

        Returns:
            The input dict with a possibly removed time dimension.
        """
        raise NotImplementedError

    def _get_dummy_batch_from_view_requirements(
        self, batch_size: int = 1
    ) -> SampleBatch:
        """Creates a numpy dummy batch based on the Policy's view requirements.

        Args:
            batch_size: The size of the batch to create.

        Returns:
            Dict[str, TensorType]: The dummy batch containing all zero values.
        """
        ret = {}
        for view_col, view_req in self.view_requirements.items():
            data_col = view_req.data_col or view_col
            # Flattened dummy batch.
            if (isinstance(view_req.space, (gym.spaces.Tuple, gym.spaces.Dict))) and (
                (
                    data_col == SampleBatch.OBS
                    and not self.config["_disable_preprocessor_api"]
                )
                or (
                    data_col == SampleBatch.ACTIONS
                    and not self.config.get("_disable_action_flattening")
                )
            ):
                _, shape = ModelCatalog.get_action_shape(
                    view_req.space, framework=self.config["framework"]
                )
                ret[view_col] = np.zeros((batch_size,) + shape[1:], np.float32)
            # Non-flattened dummy batch.
            else:
                # Range of indices on time-axis, e.g. "-50:-1".
                if isinstance(view_req.space, gym.spaces.Space):
                    time_size = (
                        len(view_req.shift_arr) if len(view_req.shift_arr) > 1 else None
                    )
                    ret[view_col] = get_dummy_batch_for_space(
                        view_req.space, batch_size=batch_size, time_size=time_size
                    )
                else:
                    ret[view_col] = [view_req.space for _ in range(batch_size)]

        # Due to different view requirements for the different columns,
        # columns in the resulting batch may not all have the same batch size.
        return SampleBatch(ret)

    def _update_model_view_requirements_from_init_state(self):
        """Uses Model's (or this Policy's) init state to add needed ViewReqs.

        Can be called from within a Policy to make sure RNNs automatically
        update their internal state-related view requirements.
        Changes the `self.view_requirements` dict.
        """
        self._model_init_state_automatically_added = True
        model = getattr(self, "model", None)

        obj = model or self
        if model and not hasattr(model, "view_requirements"):
            model.view_requirements = {
                SampleBatch.OBS: ViewRequirement(space=self.observation_space)
            }
        view_reqs = obj.view_requirements
        # Add state-ins to this model's view.
        init_state = []
        if hasattr(obj, "get_initial_state") and callable(obj.get_initial_state):
            init_state = obj.get_initial_state()
        else:
            # Add this functionality automatically for new native model API.
            if (
                tf
                and isinstance(model, tf.keras.Model)
                and "state_in_0" not in view_reqs
            ):
                obj.get_initial_state = lambda: [
                    np.zeros_like(view_req.space.sample())
                    for k, view_req in model.view_requirements.items()
                    if k.startswith("state_in_")
                ]
            else:
                obj.get_initial_state = lambda: []
                if "state_in_0" in view_reqs:
                    self.is_recurrent = lambda: True

        # Make sure auto-generated init-state view requirements get added
        # to both Policy and Model, no matter what.
        view_reqs = [view_reqs] + (
            [self.view_requirements] if hasattr(self, "view_requirements") else []
        )

        for i, state in enumerate(init_state):
            # Allow `state` to be either a Space (use zeros as initial values)
            # or any value (e.g. a dict or a non-zero tensor).
            fw = (
                np
                if isinstance(state, np.ndarray)
                else torch
                if torch and torch.is_tensor(state)
                else None
            )
            if fw:
                space = (
                    Box(-1.0, 1.0, shape=state.shape) if fw.all(state == 0.0) else state
                )
            else:
                space = state
            for vr in view_reqs:
                # Only override if user has not already provided
                # custom view-requirements for state_in_n.
                if "state_in_{}".format(i) not in vr:
                    vr["state_in_{}".format(i)] = ViewRequirement(
                        "state_out_{}".format(i),
                        shift=-1,
                        used_for_compute_actions=True,
                        batch_repeat_value=self.config.get("model", {}).get(
                            "max_seq_len", 1
                        ),
                        space=space,
                    )
                # Only override if user has not already provided
                # custom view-requirements for state_out_n.
                if "state_out_{}".format(i) not in vr:
                    vr["state_out_{}".format(i)] = ViewRequirement(
                        space=space, used_for_training=True
                    )

    def __repr__(self):
        return type(self).__name__


@OldAPIStack
def get_gym_space_from_struct_of_tensors(
    value: Union[Dict, Tuple, List, TensorType],
    batched_input=True,
) -> gym.Space:
    start_idx = 1 if batched_input else 0
    struct = tree.map_structure(
        lambda x: gym.spaces.Box(
            -1.0, 1.0, shape=x.shape[start_idx:], dtype=get_np_dtype(x)
        ),
        value,
    )
    space = get_gym_space_from_struct_of_spaces(struct)
    return space


@OldAPIStack
def get_gym_space_from_struct_of_spaces(value: Union[Dict, Tuple]) -> gym.spaces.Dict:
    if isinstance(value, dict):
        return gym.spaces.Dict(
            {k: get_gym_space_from_struct_of_spaces(v) for k, v in value.items()}
        )
    elif isinstance(value, (tuple, list)):
        return gym.spaces.Tuple([get_gym_space_from_struct_of_spaces(v) for v in value])
    else:
        assert isinstance(value, gym.spaces.Space), (
            f"The struct of spaces should only contain dicts, tiples and primitive "
            f"gym spaces. Space is of type {type(value)}"
        )
        return value

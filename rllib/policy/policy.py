from abc import ABCMeta, abstractmethod
from collections import namedtuple
import gym
from gym.spaces import Box
import logging
import numpy as np
import platform
import tree  # pip install dm_tree
from typing import Any, Callable, Dict, List, Optional, Type, TYPE_CHECKING

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.annotations import DeveloperAPI, ExperimentalAPI, \
    OverrideToImplementCustomLogic
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space, \
    get_dummy_batch_for_space, unbatch
from ray.rllib.utils.typing import AgentID, ModelGradients, ModelWeights, \
    T, TensorType, TensorStructType, TrainerConfigDict, Tuple, Union

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()

if TYPE_CHECKING:
    from ray.rllib.evaluation import Episode

logger = logging.getLogger(__name__)

# A policy spec used in the "config.multiagent.policies" specification dict
# as values (keys are the policy IDs (str)). E.g.:
# config:
#   multiagent:
#     policies: {
#       "pol1": PolicySpec(None, Box, Discrete(2), {"lr": 0.0001}),
#       "pol2": PolicySpec(config={"lr": 0.001}),
#     }
PolicySpec = namedtuple(
    "PolicySpec",
    [
        # If None, use the Trainer's default policy class stored under
        # `Trainer._policy_class`.
        "policy_class",
        # If None, use the env's observation space. If None and there is no Env
        # (e.g. offline RL), an error is thrown.
        "observation_space",
        # If None, use the env's action space. If None and there is no Env
        # (e.g. offline RL), an error is thrown.
        "action_space",
        # Overrides defined keys in the main Trainer config.
        # If None, use {}.
        "config",
    ])  # defaults=(None, None, None, None)
# TODO: From 3.7 on, we could pass `defaults` into the above constructor.
#  We still support py3.6.
PolicySpec.__new__.__defaults__ = (None, None, None, None)


@DeveloperAPI
class Policy(metaclass=ABCMeta):
    """Policy base class: Calculates actions, losses, and holds NN models.

    Policy is the abstract superclass for all DL-framework specific sub-classes
    (e.g. TFPolicy or TorchPolicy). It exposes APIs to

    1) Compute actions from observation (and possibly other) inputs.
    2) Manage the Policy's NN model(s), like exporting and loading their
       weights.
    3) Postprocess a given trajectory from the environment or other input
       via the `postprocess_trajectory` method.
    4) Compute losses from a train batch.
    5) Perform updates from a train batch on the NN-models (this normally
       includes loss calculations) either a) in one monolithic step
       (`train_on_batch`) or b) via batch pre-loading, then n steps of actual
       loss computations and updates (`load_batch_into_buffer` +
       `learn_on_loaded_batch`).

    Note: It is not recommended to sub-class Policy directly, but rather use
    one of the following two convenience methods:
    `rllib.policy.policy_template::build_policy_class` (PyTorch) or
    `rllib.policy.tf_policy_template::build_tf_policy_class` (TF).
    """

    @DeveloperAPI
    def __init__(self, observation_space: gym.Space, action_space: gym.Space,
                 config: TrainerConfigDict):
        """Initializes a Policy instance.

        Args:
            observation_space: Observation space of the policy.
            action_space: Action space of the policy.
            config: A complete Trainer/Policy config dict. For the default
                config keys and values, see rllib/trainer/trainer.py.
        """
        self.observation_space: gym.Space = observation_space
        self.action_space: gym.Space = action_space
        # The base struct of the observation/action spaces.
        # E.g. action-space = gym.spaces.Dict({"a": Discrete(2)}) ->
        # action_space_struct = {"a": Discrete(2)}
        self.observation_space_struct = get_base_struct_from_space(
            observation_space)
        self.action_space_struct = get_base_struct_from_space(action_space)

        self.config: TrainerConfigDict = config
        self.framework = self.config.get("framework")
        # Create the callbacks object to use for handling custom callbacks.
        if self.config.get("callbacks"):
            self.callbacks: "DefaultCallbacks" = self.config.get("callbacks")()
        else:
            from ray.rllib.agents.callbacks import DefaultCallbacks
            self.callbacks: "DefaultCallbacks" = DefaultCallbacks()

        # The global timestep, broadcast down from time to time from the
        # local worker to all remote workers.
        self.global_timestep: int = 0

        # The action distribution class to use for action sampling, if any.
        # Child classes may set this.
        self.dist_class: Optional[Type] = None

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
        # Whether the Model's initial state (method) has been added
        # automatically based on the given view requirements of the model.
        self._model_init_state_automatically_added = False

    @DeveloperAPI
    def compute_single_action(
            self,
            obs: Optional[TensorStructType] = None,
            state: Optional[List[TensorType]] = None,
            *,
            prev_action: Optional[TensorStructType] = None,
            prev_reward: Optional[TensorStructType] = None,
            info: dict = None,
            input_dict: Optional[SampleBatch] = None,
            episode: Optional["Episode"] = None,
            explore: Optional[bool] = None,
            timestep: Optional[int] = None,
            # Kwars placeholder for future compatibility.
            **kwargs) -> \
            Tuple[TensorStructType, List[TensorType], Dict[str, TensorType]]:
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
            lambda p, s: (s if p == "seq_lens" else s.unsqueeze(0) if
                          torch and isinstance(s, torch.Tensor) else
                          np.expand_dims(s, 0)),
            input_dict)

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
        return single_action, [s[0] for s in state_out], \
            {k: v[0] for k, v in info.items()}

    @DeveloperAPI
    def compute_actions_from_input_dict(
            self,
            input_dict: Union[SampleBatch, Dict[str, TensorStructType]],
            explore: bool = None,
            timestep: Optional[int] = None,
            episodes: Optional[List["Episode"]] = None,
            **kwargs) -> \
            Tuple[TensorType, List[TensorType], Dict[str, TensorType]]:
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
        state_batches = [
            s for k, s in input_dict.items() if k[:9] == "state_in_"
        ]
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
    @DeveloperAPI
    def compute_actions(
            self,
            obs_batch: Union[List[TensorStructType], TensorStructType],
            state_batches: Optional[List[TensorType]] = None,
            prev_action_batch: Union[List[TensorStructType],
                                     TensorStructType] = None,
            prev_reward_batch: Union[List[TensorStructType],
                                     TensorStructType] = None,
            info_batch: Optional[Dict[str, list]] = None,
            episodes: Optional[List["Episode"]] = None,
            explore: Optional[bool] = None,
            timestep: Optional[int] = None,
            **kwargs) -> \
            Tuple[TensorType, List[TensorType], Dict[str, TensorType]]:
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
            actions (TensorType): Batch of output actions, with shape like
                [BATCH_SIZE, ACTION_SHAPE].
            state_outs (List[TensorType]): List of RNN state output
                batches, if any, each with shape [BATCH_SIZE, STATE_SIZE].
            info (List[dict]): Dictionary of extra feature batches, if any,
                with shape like
                {"f1": [BATCH_SIZE, ...], "f2": [BATCH_SIZE, ...]}.
        """
        raise NotImplementedError

    @DeveloperAPI
    def compute_log_likelihoods(
            self,
            actions: Union[List[TensorType], TensorType],
            obs_batch: Union[List[TensorType], TensorType],
            state_batches: Optional[List[TensorType]] = None,
            prev_action_batch: Optional[Union[List[TensorType],
                                              TensorType]] = None,
            prev_reward_batch: Optional[Union[List[TensorType],
                                              TensorType]] = None,
            actions_normalized: bool = True,
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

        Returns:
            Batch of log probs/likelihoods, with shape: [BATCH_SIZE].
        """
        raise NotImplementedError

    @DeveloperAPI
    def postprocess_trajectory(
            self,
            sample_batch: SampleBatch,
            other_agent_batches: Optional[Dict[AgentID, Tuple[
                "Policy", SampleBatch]]] = None,
            episode: Optional["Episode"] = None) -> SampleBatch:
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

    @ExperimentalAPI
    @OverrideToImplementCustomLogic
    def loss(self, model: ModelV2, dist_class: ActionDistribution,
             train_batch: SampleBatch) -> Union[TensorType, List[TensorType]]:
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

    @DeveloperAPI
    def learn_on_batch(self, samples: SampleBatch) -> Dict[str, TensorType]:
        """Perform one learning update, given `samples`.

        Either this method or the combination of `compute_gradients` and
        `apply_gradients` must be implemented by subclasses.

        Args:
            samples: The SampleBatch object to learn from.

        Returns:
            Dictionary of extra metadata from `compute_gradients()`.

        Examples:
            >>> sample_batch = ev.sample()
            >>> ev.learn_on_batch(sample_batch)
        """
        # The default implementation is simply a fused `compute_gradients` plus
        # `apply_gradients` call.
        grads, grad_info = self.compute_gradients(samples)
        self.apply_gradients(grads)
        return grad_info

    @DeveloperAPI
    def load_batch_into_buffer(self, batch: SampleBatch,
                               buffer_index: int = 0) -> int:
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

    @DeveloperAPI
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

    @DeveloperAPI
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

    @DeveloperAPI
    def compute_gradients(self, postprocessed_batch: SampleBatch) -> \
            Tuple[ModelGradients, Dict[str, TensorType]]:
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

    @DeveloperAPI
    def apply_gradients(self, gradients: ModelGradients) -> None:
        """Applies the (previously) computed gradients.

        Either this in combination with `compute_gradients()` or
        `learn_on_batch()` must be implemented by subclasses.

        Args:
            gradients: The already calculated gradients to apply to this
                Policy.
        """
        raise NotImplementedError

    @DeveloperAPI
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

    @DeveloperAPI
    def set_weights(self, weights: ModelWeights) -> None:
        """Sets this Policy's model's weights.

        Note: Model weights are only one part of a Policy's state. Other
        state information contains: optimizer variables, exploration state,
        and global state vars such as the sampling timestep.

        Args:
            weights: Serializable copy or view of model weights.
        """
        raise NotImplementedError

    @DeveloperAPI
    def get_exploration_state(self) -> Dict[str, TensorType]:
        """Returns the state of this Policy's exploration component.

        Returns:
            Serializable information on the `self.exploration` object.
        """
        return self.exploration.get_state()

    @DeveloperAPI
    def is_recurrent(self) -> bool:
        """Whether this Policy holds a recurrent Model.

        Returns:
            True if this Policy has-a RNN-based Model.
        """
        return False

    @DeveloperAPI
    def num_state_tensors(self) -> int:
        """The number of internal states needed by the RNN-Model of the Policy.

        Returns:
            int: The number of RNN internal states kept by this Policy's Model.
        """
        return 0

    @DeveloperAPI
    def get_initial_state(self) -> List[TensorType]:
        """Returns initial RNN state for the current policy.

        Returns:
            List[TensorType]: Initial RNN state for the current policy.
        """
        return []

    @DeveloperAPI
    def get_state(self) -> Union[Dict[str, TensorType], List[TensorType]]:
        """Returns the entire current state of this Policy.

        Note: Not to be confused with an RNN model's internal state.
        State includes the Model(s)' weights, optimizer weights,
        the exploration component's state, as well as global variables, such
        as sampling timesteps.

        Returns:
            Serialized local state.
        """
        state = {
            # All the policy's weights.
            "weights": self.get_weights(),
            # The current global timestep.
            "global_timestep": self.global_timestep,
        }
        return state

    @DeveloperAPI
    def set_state(
            self,
            state: Union[Dict[str, TensorType], List[TensorType]],
    ) -> None:
        """Restores the entire current state of this Policy from `state`.

        Args:
            state: The new state to set this policy to. Can be
                obtained by calling `self.get_state()`.
        """
        self.set_weights(state["weights"])
        self.global_timestep = state["global_timestep"]

    @ExperimentalAPI
    def apply(self,
              func: Callable[["Policy", Optional[Any], Optional[Any]], T],
              *args, **kwargs) -> T:
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

    @DeveloperAPI
    def on_global_var_update(self, global_vars: Dict[str, TensorType]) -> None:
        """Called on an update to global vars.

        Args:
            global_vars: Global variables by str key, broadcast from the
                driver.
        """
        # Store the current global time step (sum over all policies' sample
        # steps).
        self.global_timestep = global_vars["timestep"]

    @DeveloperAPI
    def export_checkpoint(self, export_dir: str) -> None:
        """Export Policy checkpoint to local directory.

        Args:
            export_dir: Local writable directory.
        """
        raise NotImplementedError

    @DeveloperAPI
    def export_model(self, export_dir: str,
                     onnx: Optional[int] = None) -> None:
        """Exports the Policy's Model to local directory for serving.

        Note: The file format will depend on the deep learning framework used.
        See the child classed of Policy and their `export_model`
        implementations for more details.

        Args:
            export_dir: Local writable directory.
            onnx: If given, will export model in ONNX format. The
                value of this parameter set the ONNX OpSet version to use.
        """
        raise NotImplementedError

    @DeveloperAPI
    def import_model_from_h5(self, import_file: str) -> None:
        """Imports Policy from local file.

        Args:
            import_file (str): Local readable file.
        """
        raise NotImplementedError

    @DeveloperAPI
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

    def _create_exploration(self) -> Exploration:
        """Creates the Policy's Exploration object.

        This method only exists b/c some Trainers do not use TfPolicy nor
        TorchPolicy, but inherit directly from Policy. Others inherit from
        TfPolicy w/o using DynamicTfPolicy.
        TODO(sven): unify these cases.

        Returns:
            Exploration: The Exploration object to be used by this Policy.
        """
        if getattr(self, "exploration", None) is not None:
            return self.exploration

        exploration = from_config(
            Exploration,
            self.config.get("exploration_config",
                            {"type": "StochasticSampling"}),
            action_space=self.action_space,
            policy_config=self.config,
            model=getattr(self, "model", None),
            num_workers=self.config.get("num_workers", 0),
            worker_index=self.config.get("worker_index", 0),
            framework=getattr(self, "framework",
                              self.config.get("framework", "tf")))
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
                space=self.observation_space),
            SampleBatch.ACTIONS: ViewRequirement(
                space=self.action_space, used_for_compute_actions=False),
            # For backward compatibility with custom Models that don't specify
            # these explicitly (will be removed by Policy if not used).
            SampleBatch.PREV_ACTIONS: ViewRequirement(
                data_col=SampleBatch.ACTIONS,
                shift=-1,
                space=self.action_space),
            SampleBatch.REWARDS: ViewRequirement(),
            # For backward compatibility with custom Models that don't specify
            # these explicitly (will be removed by Policy if not used).
            SampleBatch.PREV_REWARDS: ViewRequirement(
                data_col=SampleBatch.REWARDS, shift=-1),
            SampleBatch.DONES: ViewRequirement(),
            SampleBatch.INFOS: ViewRequirement(),
            SampleBatch.EPS_ID: ViewRequirement(),
            SampleBatch.UNROLL_ID: ViewRequirement(),
            SampleBatch.AGENT_INDEX: ViewRequirement(),
            "t": ViewRequirement(),
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
            auto_remove_unneeded_view_reqs (bool): Whether to automatically
                remove those ViewRequirements records from
                self.view_requirements that are not needed.
            stats_fn (Optional[Callable[[Policy, SampleBatch], Dict[str,
                TensorType]]]): An optional stats function to be called after
                the loss.
        """
        # Signal Policy that currently we do not like to eager/jit trace
        # any function calls. This is to be able to track, which columns
        # in the dummy batch are accessed by the different function (e.g.
        # loss) such that we can then adjust our view requirements.
        self._no_tracing = True

        sample_batch_size = max(self.batch_divisibility_req * 4, 32)
        self._dummy_batch = self._get_dummy_batch_from_view_requirements(
            sample_batch_size)
        self._lazy_tensor_dict(self._dummy_batch)
        actions, state_outs, extra_outs = \
            self.compute_actions_from_input_dict(
                self._dummy_batch, explore=False)
        for key, view_req in self.view_requirements.items():
            if key not in self._dummy_batch.accessed_keys:
                view_req.used_for_compute_actions = False
        # Add all extra action outputs to view reqirements (these may be
        # filtered out later again, if not needed for postprocessing or loss).
        for key, value in extra_outs.items():
            self._dummy_batch[key] = value
            if key not in self.view_requirements:
                self.view_requirements[key] = \
                    ViewRequirement(space=gym.spaces.Box(
                        -1.0, 1.0, shape=value.shape[1:], dtype=value.dtype),
                    used_for_compute_actions=False)
        for key in self._dummy_batch.accessed_keys:
            if key not in self.view_requirements:
                self.view_requirements[key] = ViewRequirement()
            self.view_requirements[key].used_for_compute_actions = True
        self._dummy_batch = self._get_dummy_batch_from_view_requirements(
            sample_batch_size)
        self._dummy_batch.set_get_interceptor(None)
        self.exploration.postprocess_trajectory(self, self._dummy_batch)
        postprocessed_batch = self.postprocess_trajectory(self._dummy_batch)
        seq_lens = None
        if state_outs:
            B = 4  # For RNNs, have B=4, T=[depends on sample_batch_size]
            i = 0
            while "state_in_{}".format(i) in postprocessed_batch:
                postprocessed_batch["state_in_{}".format(i)] = \
                    postprocessed_batch["state_in_{}".format(i)][:B]
                if "state_out_{}".format(i) in postprocessed_batch:
                    postprocessed_batch["state_out_{}".format(i)] = \
                        postprocessed_batch["state_out_{}".format(i)][:B]
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
        if self._loss is not None:
            self._loss(self, self.model, self.dist_class, train_batch)
        # Call the stats fn, if given.
        if stats_fn is not None:
            stats_fn(self, train_batch)

        # Re-enable tracing.
        self._no_tracing = False

        # Add new columns automatically to view-reqs.
        if auto_remove_unneeded_view_reqs:
            # Add those needed for postprocessing and training.
            all_accessed_keys = train_batch.accessed_keys | \
                                self._dummy_batch.accessed_keys | \
                                self._dummy_batch.added_keys
            for key in all_accessed_keys:
                if key not in self.view_requirements and \
                        key != SampleBatch.SEQ_LENS:
                    self.view_requirements[key] = ViewRequirement(
                        used_for_compute_actions=False)
            if self._loss:
                # Tag those only needed for post-processing (with some
                # exceptions).
                for key in self._dummy_batch.accessed_keys:
                    if key not in train_batch.accessed_keys and \
                            key in self.view_requirements and \
                            key not in self.model.view_requirements and \
                            key not in [
                                SampleBatch.EPS_ID, SampleBatch.AGENT_INDEX,
                                SampleBatch.UNROLL_ID, SampleBatch.DONES,
                                SampleBatch.REWARDS, SampleBatch.INFOS]:
                        self.view_requirements[key].used_for_training = False
                # Remove those not needed at all (leave those that are needed
                # by Sampler to properly execute sample collection).
                # Also always leave DONES, REWARDS, INFOS, no matter what.
                for key in list(self.view_requirements.keys()):
                    if key not in all_accessed_keys and key not in [
                        SampleBatch.EPS_ID, SampleBatch.AGENT_INDEX,
                        SampleBatch.UNROLL_ID, SampleBatch.DONES,
                        SampleBatch.REWARDS, SampleBatch.INFOS] and \
                            key not in self.model.view_requirements:
                        # If user deleted this key manually in postprocessing
                        # fn, warn about it and do not remove from
                        # view-requirements.
                        if key in self._dummy_batch.deleted_keys:
                            logger.warning(
                                "SampleBatch key '{}' was deleted manually in "
                                "postprocessing function! RLlib will "
                                "automatically remove non-used items from the "
                                "data stream. Remove the `del` from your "
                                "postprocessing function.".format(key))
                        # If we are not writing output to disk, save to erase
                        # this key to save space in the sample batch.
                        elif self.config["output"] is None:
                            del self.view_requirements[key]

    def _get_dummy_batch_from_view_requirements(
            self, batch_size: int = 1) -> SampleBatch:
        """Creates a numpy dummy batch based on the Policy's view requirements.

        Args:
            batch_size (int): The size of the batch to create.

        Returns:
            Dict[str, TensorType]: The dummy batch containing all zero values.
        """
        ret = {}
        for view_col, view_req in self.view_requirements.items():
            data_col = view_req.data_col or view_col
            # Flattened dummy batch.
            if (isinstance(view_req.space,
                           (gym.spaces.Tuple, gym.spaces.Dict))) and \
                    ((data_col == SampleBatch.OBS and
                      not self.config["_disable_preprocessor_api"]) or
                     (data_col == SampleBatch.ACTIONS and
                      not self.config.get("_disable_action_flattening"))):
                _, shape = ModelCatalog.get_action_shape(
                    view_req.space, framework=self.config["framework"])
                ret[view_col] = \
                    np.zeros((batch_size, ) + shape[1:], np.float32)
            # Non-flattened dummy batch.
            else:
                # Range of indices on time-axis, e.g. "-50:-1".
                if view_req.shift_from is not None:
                    ret[view_col] = get_dummy_batch_for_space(
                        view_req.space,
                        batch_size=batch_size,
                        time_size=view_req.shift_to - view_req.shift_from + 1)
                # Sequence of (probably non-consecutive) indices.
                elif isinstance(view_req.shift, (list, tuple)):
                    ret[view_col] = get_dummy_batch_for_space(
                        view_req.space,
                        batch_size=batch_size,
                        time_size=len(view_req.shift))
                # Single shift int value.
                else:
                    if isinstance(view_req.space, gym.spaces.Space):
                        ret[view_col] = get_dummy_batch_for_space(
                            view_req.space,
                            batch_size=batch_size,
                            fill_value=0.0)
                    else:
                        ret[view_col] = [
                            view_req.space for _ in range(batch_size)
                        ]

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
        if hasattr(obj, "get_initial_state") and callable(
                obj.get_initial_state):
            init_state = obj.get_initial_state()
        else:
            # Add this functionality automatically for new native model API.
            if tf and isinstance(model, tf.keras.Model) and \
                    "state_in_0" not in view_reqs:
                obj.get_initial_state = lambda: [
                    np.zeros_like(view_req.space.sample())
                    for k, view_req in model.view_requirements.items()
                    if k.startswith("state_in_")]
            else:
                obj.get_initial_state = lambda: []
                if "state_in_0" in view_reqs:
                    self.is_recurrent = lambda: True

        # Make sure auto-generated init-state view requirements get added
        # to both Policy and Model, no matter what.
        view_reqs = [view_reqs] + ([self.view_requirements] if hasattr(
            self, "view_requirements") else [])

        for i, state in enumerate(init_state):
            # Allow `state` to be either a Space (use zeros as initial values)
            # or any value (e.g. a dict or a non-zero tensor).
            fw = np if isinstance(state, np.ndarray) else torch if \
                torch and torch.is_tensor(state) else None
            if fw:
                space = Box(-1.0, 1.0, shape=state.shape) if \
                    fw.all(state == 0.0) else state
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
                            "max_seq_len", 1),
                        space=space)
                # Only override if user has not already provided
                # custom view-requirements for state_out_n.
                if "state_out_{}".format(i) not in vr:
                    vr["state_out_{}".format(i)] = ViewRequirement(
                        space=space, used_for_training=True)

    @DeveloperAPI
    def __repr__(self):
        return type(self).__name__

    @Deprecated(new="get_exploration_state", error=False)
    def get_exploration_info(self) -> Dict[str, TensorType]:
        return self.get_exploration_state()

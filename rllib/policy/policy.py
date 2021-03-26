from abc import ABCMeta, abstractmethod
import gym
from gym.spaces import Box
import logging
import numpy as np
import tree
from typing import Dict, List, Optional

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space, \
    unbatch
from ray.rllib.utils.typing import AgentID, ModelGradients, ModelWeights, \
    TensorType, TrainerConfigDict, Tuple, Union

torch, _ = try_import_torch()

logger = logging.getLogger(__name__)

# By convention, metrics from optimizing the loss can be reported in the
# `grad_info` dict returned by learn_on_batch() / compute_grads() via this key.
LEARNER_STATS_KEY = "learner_stats"


@DeveloperAPI
class Policy(metaclass=ABCMeta):
    """An agent policy and loss, i.e., a TFPolicy or other subclass.

    This object defines how to act in the environment, and also losses used to
    improve the policy based on its experiences. Note that both policy and
    loss are defined together for convenience, though the policy itself is
    logically separate.

    All policies can directly extend Policy, however TensorFlow users may
    find TFPolicy simpler to implement. TFPolicy also enables RLlib
    to apply TensorFlow-specific optimizations such as fusing multiple policy
    graphs and multi-GPU support.

    Attributes:
        observation_space (gym.Space): Observation space of the policy. For
            complex spaces (e.g., Dict), this will be flattened version of the
            space, and you can access the original space via
            ``observation_space.original_space``.
        action_space (gym.Space): Action space of the policy.
        exploration (Exploration): The exploration object to use for
            computing actions, or None.
    """

    @DeveloperAPI
    def __init__(self, observation_space: gym.spaces.Space,
                 action_space: gym.spaces.Space, config: TrainerConfigDict):
        """Initialize the graph.

        This is the standard constructor for policies. The policy
        class you pass into RolloutWorker will be constructed with
        these arguments.

        Args:
            observation_space (gym.spaces.Space): Observation space of the
                policy.
            action_space (gym.spaces.Space): Action space of the policy.
            config (TrainerConfigDict): Policy-specific configuration data.
        """
        self.observation_space = observation_space
        self.action_space = action_space
        self.action_space_struct = get_base_struct_from_space(action_space)
        self.config = config
        if self.config.get("callbacks"):
            self.callbacks: "DefaultCallbacks" = self.config.get("callbacks")()
        else:
            from ray.rllib.agents.callbacks import DefaultCallbacks
            self.callbacks: "DefaultCallbacks" = DefaultCallbacks()
        # The global timestep, broadcast down from time to time from the
        # driver.
        self.global_timestep = 0
        # The action distribution class to use for action sampling, if any.
        # Child classes may set this.
        self.dist_class = None
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
        self._model_init_state_automatically_added = False

    @abstractmethod
    @DeveloperAPI
    def compute_actions(
            self,
            obs_batch: Union[List[TensorType], TensorType],
            state_batches: Optional[List[TensorType]] = None,
            prev_action_batch: Union[List[TensorType], TensorType] = None,
            prev_reward_batch: Union[List[TensorType], TensorType] = None,
            info_batch: Optional[Dict[str, list]] = None,
            episodes: Optional[List["MultiAgentEpisode"]] = None,
            explore: Optional[bool] = None,
            timestep: Optional[int] = None,
            **kwargs) -> \
            Tuple[TensorType, List[TensorType], Dict[str, TensorType]]:
        """Computes actions for the current policy.

        Args:
            obs_batch (Union[List[TensorType], TensorType]): Batch of
                observations.
            state_batches (Optional[List[TensorType]]): List of RNN state input
                batches, if any.
            prev_action_batch (Union[List[TensorType], TensorType]): Batch of
                previous action values.
            prev_reward_batch (Union[List[TensorType], TensorType]): Batch of
                previous rewards.
            info_batch (Optional[Dict[str, list]]): Batch of info objects.
            episodes (Optional[List[MultiAgentEpisode]] ): List of
                MultiAgentEpisode, one for each obs in obs_batch. This provides
                access to all of the internal episode state, which may be
                useful for model-based or multiagent algorithms.
            explore (Optional[bool]): Whether to pick an exploitation or
                exploration action. Set to None (default) for using the
                value of `self.config["explore"]`.
            timestep (Optional[int]): The current (sampling) time step.

        Keyword Args:
            kwargs: forward compatibility placeholder

        Returns:
            Tuple:
                actions (TensorType): Batch of output actions, with shape like
                    [BATCH_SIZE, ACTION_SHAPE].
                state_outs (List[TensorType]): List of RNN state output
                    batches, if any, with shape like [STATE_SIZE, BATCH_SIZE].
                info (List[dict]): Dictionary of extra feature batches, if any,
                    with shape like
                    {"f1": [BATCH_SIZE, ...], "f2": [BATCH_SIZE, ...]}.
        """
        raise NotImplementedError

    @DeveloperAPI
    def compute_single_action(
            self,
            obs: TensorType,
            state: Optional[List[TensorType]] = None,
            prev_action: Optional[TensorType] = None,
            prev_reward: Optional[TensorType] = None,
            info: dict = None,
            episode: Optional["MultiAgentEpisode"] = None,
            clip_actions: bool = False,
            explore: Optional[bool] = None,
            timestep: Optional[int] = None,
            **kwargs) -> \
            Tuple[TensorType, List[TensorType], Dict[str, TensorType]]:
        """Unbatched version of compute_actions.

        Args:
            obs (TensorType): Single observation.
            state (Optional[List[TensorType]]): List of RNN state inputs, if
                any.
            prev_action (Optional[TensorType]): Previous action value, if any.
            prev_reward (Optional[TensorType]): Previous reward, if any.
            info (dict): Info object, if any.
            episode (Optional[MultiAgentEpisode]): this provides access to all
                of the internal episode state, which may be useful for
                model-based or multi-agent algorithms.
            clip_actions (bool): Should actions be clipped?
            explore (Optional[bool]): Whether to pick an exploitation or
                exploration action
                (default: None -> use self.config["explore"]).
            timestep (Optional[int]): The current (sampling) time step.

        Keyword Args:
            kwargs: Forward compatibility.

        Returns:
            Tuple:
                - actions (TensorType): Single action.
                - state_outs (List[TensorType]): List of RNN state outputs,
                    if any.
                - info (dict): Dictionary of extra features, if any.
        """
        prev_action_batch = None
        prev_reward_batch = None
        info_batch = None
        episodes = None
        state_batch = None
        if prev_action is not None:
            prev_action_batch = [prev_action]
        if prev_reward is not None:
            prev_reward_batch = [prev_reward]
        if info is not None:
            info_batch = [info]
        if episode is not None:
            episodes = [episode]
        if state is not None:
            state_batch = [
                s.unsqueeze(0)
                if torch and isinstance(s, torch.Tensor) else np.expand_dims(
                    s, 0) for s in state
            ]

        out = self.compute_actions(
            [obs],
            state_batch,
            prev_action_batch=prev_action_batch,
            prev_reward_batch=prev_reward_batch,
            info_batch=info_batch,
            episodes=episodes,
            explore=explore,
            timestep=timestep)

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

        if clip_actions:
            single_action = clip_action(single_action,
                                        self.action_space_struct)

        # Return action, internal state(s), infos.
        return single_action, [s[0] for s in state_out], \
            {k: v[0] for k, v in info.items()}

    @DeveloperAPI
    def compute_actions_from_input_dict(
            self,
            input_dict: Dict[str, TensorType],
            explore: bool = None,
            timestep: Optional[int] = None,
            episodes: Optional[List["MultiAgentEpisode"]] = None,
            **kwargs) -> \
            Tuple[TensorType, List[TensorType], Dict[str, TensorType]]:
        """Computes actions from collected samples (across multiple-agents).

        Uses the currently "forward-pass-registered" samples from the collector
        to construct the input_dict for the Model.

        Args:
            input_dict (Dict[str, TensorType]): An input dict mapping str
                keys to Tensors. `input_dict` already abides to the Policy's
                as well as the Model's view requirements and can be passed
                to the Model as-is.
            explore (bool): Whether to pick an exploitation or exploration
                action (default: None -> use self.config["explore"]).
            timestep (Optional[int]): The current (sampling) time step.
            kwargs: forward compatibility placeholder

        Returns:
            Tuple:
                actions (TensorType): Batch of output actions, with shape
                    like [BATCH_SIZE, ACTION_SHAPE].
                state_outs (List[TensorType]): List of RNN state output
                    batches, if any, with shape like [STATE_SIZE, BATCH_SIZE].
                info (dict): Dictionary of extra feature batches, if any, with
                    shape like
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

    @DeveloperAPI
    def compute_log_likelihoods(
            self,
            actions: Union[List[TensorType], TensorType],
            obs_batch: Union[List[TensorType], TensorType],
            state_batches: Optional[List[TensorType]] = None,
            prev_action_batch: Optional[Union[List[TensorType],
                                              TensorType]] = None,
            prev_reward_batch: Optional[Union[List[
                TensorType], TensorType]] = None) -> TensorType:
        """Computes the log-prob/likelihood for a given action and observation.

        Args:
            actions (Union[List[TensorType], TensorType]): Batch of actions,
                for which to retrieve the log-probs/likelihoods (given all
                other inputs: obs, states, ..).
            obs_batch (Union[List[TensorType], TensorType]): Batch of
                observations.
            state_batches (Optional[List[TensorType]]): List of RNN state input
                batches, if any.
            prev_action_batch (Optional[Union[List[TensorType], TensorType]]):
                Batch of previous action values.
            prev_reward_batch (Optional[Union[List[TensorType], TensorType]]):
                Batch of previous rewards.

        Returns:
            TensorType: Batch of log probs/likelihoods, with shape:
                [BATCH_SIZE].
        """
        raise NotImplementedError

    @DeveloperAPI
    def postprocess_trajectory(
            self,
            sample_batch: SampleBatch,
            other_agent_batches: Optional[Dict[AgentID, Tuple[
                "Policy", SampleBatch]]] = None,
            episode: Optional["MultiAgentEpisode"] = None) -> SampleBatch:
        """Implements algorithm-specific trajectory postprocessing.

        This will be called on each trajectory fragment computed during policy
        evaluation. Each fragment is guaranteed to be only from one episode.

        Args:
            sample_batch (SampleBatch): batch of experiences for the policy,
                which will contain at most one episode trajectory.
            other_agent_batches (dict): In a multi-agent env, this contains a
                mapping of agent ids to (policy, agent_batch) tuples
                containing the policy and experiences of the other agents.
            episode (Optional[MultiAgentEpisode]): An optional multi-agent
                episode object to provide access to all of the
                internal episode state, which may be useful for model-based or
                multi-agent algorithms.

        Returns:
            SampleBatch: Postprocessed sample batch.
        """
        return sample_batch

    @DeveloperAPI
    def learn_on_batch(self, samples: SampleBatch) -> Dict[str, TensorType]:
        """Fused compute gradients and apply gradients call.

        Either this or the combination of compute/apply grads must be
        implemented by subclasses.

        Args:
            samples (SampleBatch): The SampleBatch object to learn from.

        Returns:
            Dict[str, TensorType]: Dictionary of extra metadata from
                compute_gradients().

        Examples:
            >>> sample_batch = ev.sample()
            >>> ev.learn_on_batch(sample_batch)
        """

        grads, grad_info = self.compute_gradients(samples)
        self.apply_gradients(grads)
        return grad_info

    @DeveloperAPI
    def compute_gradients(self, postprocessed_batch: SampleBatch) -> \
            Tuple[ModelGradients, Dict[str, TensorType]]:
        """Computes gradients against a batch of experiences.

        Either this or learn_on_batch() must be implemented by subclasses.

        Args:
            postprocessed_batch (SampleBatch): The SampleBatch object to use
                for calculating gradients.

        Returns:
            Tuple[ModelGradients, Dict[str, TensorType]]:
                - List of gradient output values.
                - Extra policy-specific info values.
        """
        raise NotImplementedError

    @DeveloperAPI
    def apply_gradients(self, gradients: ModelGradients) -> None:
        """Applies previously computed gradients.

        Either this or learn_on_batch() must be implemented by subclasses.

        Args:
            gradients (ModelGradients): The already calculated gradients to
                apply to this Policy.
        """
        raise NotImplementedError

    @DeveloperAPI
    def get_weights(self) -> ModelWeights:
        """Returns model weights.

        Returns:
            ModelWeights: Serializable copy or view of model weights.
        """
        raise NotImplementedError

    @DeveloperAPI
    def set_weights(self, weights: ModelWeights) -> None:
        """Sets model weights.

        Args:
            weights (ModelWeights): Serializable copy or view of model weights.
        """
        raise NotImplementedError

    @DeveloperAPI
    def get_exploration_info(self) -> Dict[str, TensorType]:
        """Returns the current exploration information of this policy.

        This information depends on the policy's Exploration object.

        Returns:
            Dict[str, TensorType]: Serializable information on the
                `self.exploration` object.
        """
        return self.exploration.get_info()

    @DeveloperAPI
    def is_recurrent(self) -> bool:
        """Whether this Policy holds a recurrent Model.

        Returns:
            bool: True if this Policy has-a RNN-based Model.
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
        """Saves all local state.

        Returns:
            Union[Dict[str, TensorType], List[TensorType]]: Serialized local
                state.
        """
        return self.get_weights()

    @DeveloperAPI
    def set_state(self, state: object) -> None:
        """Restores all local state.

        Args:
            state (obj): Serialized local state.
        """
        self.set_weights(state)

    @DeveloperAPI
    def on_global_var_update(self, global_vars: Dict[str, TensorType]) -> None:
        """Called on an update to global vars.

        Args:
            global_vars (Dict[str, TensorType]): Global variables by str key,
                broadcast from the driver.
        """
        # Store the current global time step (sum over all policies' sample
        # steps).
        self.global_timestep = global_vars["timestep"]

    @DeveloperAPI
    def export_model(self, export_dir: str) -> None:
        """Exports the Policy's Model to local directory for serving.

        Note: The file format will depend on the deep learning framework used.
        See the child classed of Policy and their `export_model`
        implementations for more details.

        Args:
            export_dir (str): Local writable directory.
        """
        raise NotImplementedError

    @DeveloperAPI
    def export_checkpoint(self, export_dir: str) -> None:
        """Export Policy checkpoint to local directory.

        Args:
            export_dir (str): Local writable directory.
        """
        raise NotImplementedError

    @DeveloperAPI
    def import_model_from_h5(self, import_file: str) -> None:
        """Imports Policy from local file.

        Args:
            import_file (str): Local readable file.
        """
        raise NotImplementedError

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
            SampleBatch.ACTIONS: ViewRequirement(space=self.action_space),
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
        sample_batch_size = max(self.batch_divisibility_req * 4, 32)
        self._dummy_batch = self._get_dummy_batch_from_view_requirements(
            sample_batch_size)
        input_dict = self._lazy_tensor_dict(
            {k: v
             for k, v in self._dummy_batch.items()})
        actions, state_outs, extra_outs = \
            self.compute_actions_from_input_dict(input_dict, explore=False)
        # Add all extra action outputs to view reqirements (these may be
        # filtered out later again, if not needed for postprocessing or loss).
        for key, value in extra_outs.items():
            self._dummy_batch[key] = np.zeros_like(value)
            if key not in self.view_requirements:
                self.view_requirements[key] = \
                    ViewRequirement(space=gym.spaces.Box(
                        -1.0, 1.0, shape=value.shape[1:], dtype=value.dtype),
                    used_for_compute_actions=False)
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
        # Switch on lazy to-tensor conversion on `postprocessed_batch`.
        train_batch = self._lazy_tensor_dict(postprocessed_batch)
        if seq_lens is not None:
            train_batch["seq_lens"] = seq_lens
        train_batch.count = self._dummy_batch.count
        # Call the loss function, if it exists.
        if self._loss is not None:
            self._loss(self, self.model, self.dist_class, train_batch)
        # Call the stats fn, if given.
        if stats_fn is not None:
            stats_fn(self, train_batch)

        # Add new columns automatically to view-reqs.
        if auto_remove_unneeded_view_reqs:
            # Add those needed for postprocessing and training.
            all_accessed_keys = train_batch.accessed_keys | \
                                self._dummy_batch.accessed_keys | \
                                self._dummy_batch.added_keys
            for key in all_accessed_keys:
                if key not in self.view_requirements:
                    self.view_requirements[key] = ViewRequirement()
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
                        else:
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
            if isinstance(view_req.space, (gym.spaces.Dict, gym.spaces.Tuple)):
                _, shape = ModelCatalog.get_action_shape(
                    view_req.space, framework=self.config["framework"])
                ret[view_col] = \
                    np.zeros((batch_size, ) + shape[1:], np.float32)
            else:
                # Range of indices on time-axis, e.g. "-50:-1".
                if view_req.shift_from is not None:
                    ret[view_col] = np.zeros_like([[
                        view_req.space.sample()
                        for _ in range(view_req.shift_to -
                                       view_req.shift_from + 1)
                    ] for _ in range(batch_size)])
                # Set of (probably non-consecutive) indices.
                elif isinstance(view_req.shift, (list, tuple)):
                    ret[view_col] = np.zeros_like([[
                        view_req.space.sample()
                        for t in range(len(view_req.shift))
                    ] for _ in range(batch_size)])
                # Single shift int value.
                else:
                    if isinstance(view_req.space, gym.spaces.Space):
                        ret[view_col] = np.zeros_like([
                            view_req.space.sample() for _ in range(batch_size)
                        ])
                    else:
                        ret[view_col] = [
                            view_req.space for _ in range(batch_size)
                        ]

        # Due to different view requirements for the different columns,
        # columns in the resulting batch may not all have the same batch size.
        return SampleBatch(ret, _dont_check_lens=True)

    def _update_model_view_requirements_from_init_state(self):
        """Uses Model's (or this Policy's) init state to add needed ViewReqs.

        Can be called from within a Policy to make sure RNNs automatically
        update their internal state-related view requirements.
        Changes the `self.view_requirements` dict.
        """
        self._model_init_state_automatically_added = True
        model = getattr(self, "model", None)
        obj = model or self
        # Add state-ins to this model's view.
        for i, state in enumerate(obj.get_initial_state()):
            space = Box(-1.0, 1.0, shape=state.shape) if \
                hasattr(state, "shape") else state
            view_reqs = model.view_requirements if model else \
                self.view_requirements
            view_reqs["state_in_{}".format(i)] = ViewRequirement(
                "state_out_{}".format(i),
                shift=-1,
                batch_repeat_value=self.config.get("model", {}).get(
                    "max_seq_len", 1),
                space=space)
            view_reqs["state_out_{}".format(i)] = ViewRequirement(
                space=space, used_for_training=True)


def clip_action(action, action_space):
    """Clips all actions in `flat_actions` according to the given Spaces.

    Args:
        flat_actions (List[np.ndarray]): The (flattened) list of single action
            components. List will have len=1 for "primitive" action Spaces.
        flat_space (List[Space]): The (flattened) list of single action Space
            objects. Has to be of same length as `flat_actions`.

    Returns:
        List[np.ndarray]: Flattened list of single clipped "primitive" actions.
    """

    def map_(a, s):
        if isinstance(s, gym.spaces.Box):
            a = np.clip(a, s.low, s.high)
        return a

    return tree.map_structure(map_, action, action_space)

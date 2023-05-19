import abc
import json
import logging
import pathlib
from collections import defaultdict
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Hashable,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
    TYPE_CHECKING,
)

import ray
from ray.rllib.core.learner.reduce_result_dict_fn import _reduce_mean_results
from ray.rllib.core.learner.scaling_config import LearnerGroupScalingConfig
from ray.rllib.core.rl_module.marl_module import (
    MultiAgentRLModule,
    MultiAgentRLModuleSpec,
)
from ray.rllib.core.rl_module.rl_module import (
    RLModule,
    ModuleID,
    SingleAgentRLModuleSpec,
)
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.metrics import ALL_MODULES
from ray.rllib.utils.minibatch_utils import (
    MiniBatchDummyIterator,
    MiniBatchCyclicIterator,
)
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.serialization import serialize_type
from ray.rllib.utils.typing import TensorType, ResultDict

if TYPE_CHECKING:
    from ray.rllib.core.rl_module.torch.torch_compile_config import TorchCompileConfig

torch, _ = try_import_torch()
tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)

Optimizer = Union["torch.optim.Optimizer", "tf.keras.optimizers.Optimizer"]
ParamType = Union["torch.Tensor", "tf.Variable"]
ParamOptimizerPair = Tuple[Sequence[ParamType], Optimizer]
ParamOptimizerPairs = List[ParamOptimizerPair]
NamedParamOptimizerPairs = Dict[str, ParamOptimizerPair]
ParamRef = Hashable
ParamDictType = Dict[ParamRef, ParamType]
# A single learning rate or a learning rate schedule (list of sub-lists, each of
# the format: [ts (int), lr_to_reach_by_ts (float)]).
LearningRateType = Union[float, List[List[Union[int, float]]]]

# COMMON LEARNER LOSS_KEYS
POLICY_LOSS_KEY = "policy_loss"
VF_LOSS_KEY = "vf_loss"
ENTROPY_KEY = "entropy"

# Additional update keys
LEARNER_RESULTS_CURR_LR_KEY = "curr_lr"


@dataclass
class FrameworkHyperparameters:
    """The framework specific hyper-parameters.

    Args:
        eager_tracing: Whether to trace the model in eager mode. This enables tf
            tracing mode by wrapping the loss function computation in a tf.function.
            This is useful for speeding up the training loop. However, it is not
            compatible with all tf operations. For example, tf.print is not supported
            in tf.function.
        troch_compile_config: The TorchCompileConfig to use for compiling the RL
            Module in Torch.
    """

    eager_tracing: bool = False
    torch_compile_cfg: Optional["TorchCompileConfig"] = None


@dataclass
class LearnerHyperparameters:
    """Hyperparameters for a Learner, derived from a subset of AlgorithmConfig values.

    Instances of this class should only be created via calling
    `get_learner_hyperparameters()` on a frozen AlgorithmConfig object and should always
    considered read-only.

    When creating a new Learner, you should also define a new sub-class of this class
    and make sure the respective AlgorithmConfig sub-class has a proper implementation
    of the `get_learner_hyperparameters` method.

    Validation of the values of these hyperparameters should be done by the
    respective AlgorithmConfig class.

    For configuring different learning behaviors for different (single-agent) RLModules
    within the Learner, RLlib uses the `per_module_overrides` property (dict), mapping
    ModuleID to a "sub-version" of a `LearnerHyperparameters` instance, in which the
    module-specific override settings are applied AND the `per_module_overrides` is set
    to None.
    """

    # Parameters used for gradient postprocessing (clipping) and gradient application.
    optimizer_type: Union[str, Dict[str, str]] = None
    learning_rate: Union[LearningRateType, Dict[str, LearningRateType]] = None
    grad_clip: Union[float, Dict[str, float]] = None
    grad_clip_by: Union[str, Dict[str, str]] = None

    # Holds hyperparameters per module. This is not None only in the top-level
    # Learner's self.hps (whose self.module is a `MARLModule`) and then contains the correct
    # mappings from ModuleID to the .
    _per_module_overrides: Optional[Dict[ModuleID, "LearnerHyperparameters"]] = None

    def get_hps_for_module(self, module_id: ModuleID) -> "LearnerHyperparameters":
        """Returns a LearnerHyperparameter instance, given a `module_id`.

        Args:
            module_id: The module ID for which to return a specific
                LearnerHyperparameter instance.

        Returns:
            The module specific LearnerHyperparameter instance.
        """
        if (
            self._per_module_overrides is not None
            and module_id in self._per_module_overrides
        ):
            return self._per_module_overrides[module_id]
        else:
            return self


class Learner:
    """Base class for learners.

    This class will be used to train RLModules. It is responsible for defining the loss
    function, and updating the neural network weights that it owns. It also provides a
    way to add/remove modules to/from RLModules in a multi-agent scenario, in the
    middle of training (This is useful for league based training).

    TF and Torch specific implementation of this class fills in the framework-specific
    implementation details for distributed training, and for computing and applying
    gradients. User should not need to sub-class this class, but instead inherit from
    the TF or Torch specific sub-classes to implement their algorithm-specific update
    logic.

    Args:
        module_spec: The module specification for the RLModule that is being trained.
            If the module is a single agent module, after building the module it will
            be converted to a multi-agent module with a default key. Can be none if the
            module is provided directly via the `module` argument. Refer to
            ray.rllib.core.rl_module.SingleAgentRLModuleSpec
            or ray.rllib.core.rl_module.MultiAgentRLModuleSpec for more info.
        module: If learner is being used stand-alone, the RLModule can be optionally
            passed in directly instead of the through the `module_spec`.
        #optimizer_config: The deep learning gradient optimizer configuration to be
        #    used. For example lr=0.0001, momentum=0.9, etc.
        scaling_config: Configuration for scaling the learner actors.
            Refer to ray.rllib.core.learner.scaling_config.LearnerGroupScalingConfig
            for more info.
        learner_hyperparameters: The hyper-parameters for the Learner.
            Algorithm specific learner hyper-parameters will passed in via this
            argument. For example in PPO the `vf_loss_coeff` hyper-parameter will be
            passed in via this argument. Refer to
            ray.rllib.core.learner.learner.LearnerHyperparameters for more info.
        framework_hps: The framework specific hyper-parameters. This will be used to
            pass in any framework specific hyper-parameter that will impact the module
            creation. For example `eager_tracing` in TF or `torch.compile()` in Torch.
            Refer to ray.rllib.core.learner.learner.FrameworkHyperparameters for
            more info.


    Usage pattern:

        .. code-block:: python

        # create a single agent RL module spec.
        module_spec = SingleAgentRLModuleSpec(
            module_class=MyModule,
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config_dict = {"hidden": [128, 128]}
        )

        # create a learner instance that will train the module
        learner = MyLearner(module_spec=module_spec)

        # Note: the learner should be built before it can be used.
        learner.build()

        # take one gradient update on the module and report the results
        results = learner.update(batch)

        # add a new module, perhaps for league based training
        learner.add_module(
            module_id="new_player",
            module_spec=SingleAgentRLModuleSpec(
                module_class=NewPlayerModule,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config_dict = {"hidden": [128, 128]}
            )
        )

        # Take another gradient update with both previous and new modules.
        results = learner.update(batch)

        # remove a module
        learner.remove_module("new_player")

        # will train previous modules only.
        results = learner.update(batch)

        # get the state of the learner
        state = learner.get_state()

        # set the state of the learner
        learner.set_state(state)

        # get the weights of the underly multi-agent RLModule
        weights = learner.get_weights()

        # set the weights of the underly multi-agent RLModule
        learner.set_weights(weights)


    Extension pattern:

        .. code-block:: python

        class MyLearner(TorchLearner):

            def compute_loss(self, fwd_out, batch):
                # compute the loss based on batch and output of the forward pass
                # to access the learner hyper-parameters use `self._hps`

                return {ALL_MODULES: loss}
    """

    framework: str = None
    TOTAL_LOSS_KEY: str = "total_loss"

    def __init__(
        self,
        *,
        module_spec: Optional[
            Union[SingleAgentRLModuleSpec, MultiAgentRLModuleSpec]
        ] = None,
        module: Optional[RLModule] = None,
        #optimizer_config: Mapping[str, Any] = None,
        learner_group_scaling_config: Optional[LearnerGroupScalingConfig] = None,
        learner_hyperparameters: Optional[LearnerHyperparameters] = None,
        framework_hyperparameters: Optional[FrameworkHyperparameters] = None,
    ):
        # TODO (Kourosh): convert optimizer configs to dataclasses
        if module_spec is not None and module is not None:
            raise ValueError(
                "Only one of module spec or module can be provided to Learner."
            )

        if module_spec is None and module is None:
            raise ValueError(
                "Either module_spec or module should be provided to Learner."
            )

        self._module_spec = module_spec
        self._module_obj = module
        #self._optimizer_config = optimizer_config
        self._hps = learner_hyperparameters or LearnerHyperparameters()
        self._device = None

        # pick the configs that we need for the learner from scaling config
        self._learner_group_scaling_config = (
            learner_group_scaling_config or LearnerGroupScalingConfig()
        )
        self._distributed = self._learner_group_scaling_config.num_workers > 1
        self._use_gpu = self._learner_group_scaling_config.num_gpus_per_worker > 0
        # if we are using gpu but we are not distributed, use this gpu for training
        self._local_gpu_idx = self._learner_group_scaling_config.local_gpu_idx

        self._framework_hyperparameters = (
            framework_hyperparameters or FrameworkHyperparameters()
        )

        # whether self.build has already been called
        self._is_built = False

        # These are the attributes that are set during build.

        # The actual MARLModule used by this Learner.
        self._module: Optional[MultiAgentRLModule] = None
        # These are set for properly applying optimizers and adding or removing modules.
        self._optimizer_parameters: Dict[Optimizer, List[ParamRef]] = {}
        self._optimizer_lr_schedules: Dict[Optimizer, Scheduler] = {}
        self._named_optimizers: Dict[str, Optimizer] = {}
        self._params: ParamDictType = {}
        # Dict mapping ModuleID to a list of optimizer names. Note that the optimizer
        # name includes the ModuleID as a prefix: optimizer_name=`[ModuleID]_[.. rest]`.
        self._module_optimizers: Dict[ModuleID, List[str]] = defaultdict(list)

        # Registered metrics (one sub-dict per module ID) to be returned from
        # `Learner.update()`. These metrics will be "compiled" automatically into
        # the final results dict in the `self.compile_results()` method.
        self._metrics = defaultdict(dict)

    @property
    def distributed(self) -> bool:
        """Whether the learner is running in distributed mode."""
        return self._distributed

    @property
    def module(self) -> MultiAgentRLModule:
        """The multi-agent RLModule that is being trained."""
        return self._module

    @property
    def hps(self) -> LearnerHyperparameters:
        """The hyper-parameters for the learner."""
        return self._hps

    def configure_optimizers(self) -> ParamOptimizerPairs:
        """Configures the optimizers for the Learner.

        This method is responsible for setting up the optimizers that will be used to
        train the model. The optimizers are responsible for updating the model's
        parameters during training, based on the computed gradients. The method should
        return a list of tuples, where each tuple consists of a list of model
        parameters and a deep learning optimizer that should be used to optimize those
        parameters. To support both tf and torch, we must explicitly return the
        parameters as the first element of the tuple regardless of whether those
        exist in the optimizer objects or not. This method is called once at
        initialization.

        Returns:
            A list of tuples (parameters, optimizer), where parameters is a list of
            model parameters and optimizer is a deep learning optimizer.
        """
        param_optimizer_pairs = []
        name_to_optim = {}
        for module_id in self.module.keys():
            if self._is_module_compatible_with_learner(self.module[module_id]):
                (
                    module_param_optimizer_pairs,
                    module_named_optims,
                ) = self._configure_optimizers_for_module_helper(module_id)
                param_optimizer_pairs.extend(module_param_optimizer_pairs)
                name_to_optim.update(module_named_optims)
                self._module_optimizers[module_id].extend(
                    list(module_named_optims.keys())
                )
        self._named_optimizers = name_to_optim
        return param_optimizer_pairs

    def _configure_optimizers_for_module_helper(
        self, module_id: ModuleID
    ) -> Tuple[ParamOptimizerPairs, Dict[str, Optimizer]]:
        """Configures the optimizers for the given module_id.

        This method is a helper method for processing the output of
        configure_optimizers_for_module into a dictionary of names mapping to optimizers
        and a list of ParamOptimizerPairs. Developers should call this method
        instead of configure_optimizers_for_module, but users should still override
        configure_optimizers_for_module.

        Args:
            module_id: The module_id of the module to configure optimizers for.

        Returns:
            A tuple consisting of: A list of ParamOptimizerPairs and a dict of names
            mapping from optimizer names to optimizers.

        """
        hps = self.hps.get_hps_for_module(module_id)

        pairs = []
        name_to_optim = {}
        pair_or_pairs: Union[
            ParamOptimizerPair, NamedParamOptimizerPairs
        ] = self.configure_optimizers_for_module(module_id, hps=hps)
        if isinstance(pair_or_pairs, tuple):
            # pair_or_pairs is a single ParamOptimizerPair
            pair = pair_or_pairs
            self._check_structure_param_optim_pair(pair)
            _, optim = pair
            name = f"{module_id}"
            name_to_optim[name] = optim
            pairs.append(pair)
            self._pair_optim_with_lr_scheduler(hps, name, optim)
        elif isinstance(pair_or_pairs, dict):
            # pair_or_pairs is a NamedParamOptimizerPairs
            for name, pair in pair_or_pairs.items():
                self._check_structure_param_optim_pair(pair)
                _, optim = pair
                if not isinstance(name, str):
                    raise ValueError(
                        "The output of configure_optimizers_for_module must be a "
                        "NamedParamOptimizerPairs. The key of a "
                        "NamedParamOptimizerPairs must be a string."
                    )
                name = f"{module_id}_{name}"
                name_to_optim[name] = optim
                pairs.append(pair)
                self._pair_optim_with_lr_scheduler(hps, name, optim)
        else:
            raise ValueError(
                "The output of configure_optimizers_for_module must be a "
                "ParamOptimizerPair or NamedParamOptimizerPairs."
            )
        return pairs, name_to_optim

    def _check_structure_param_optim_pair(self, param_optim_pair: Any) -> None:
        """Checks that the given param_optim_pair is a valid ParamOptimizerPair.

        Args:
            param_optim_pair: The param_optim_pair to check.

        """
        if not isinstance(param_optim_pair, tuple):
            raise ValueError(
                "ParamOptimizerPair must be a tuple of (parameters, optim)."
                f"Got a {type(param_optim_pair)} instead."
            )
        if len(param_optim_pair) != 2:
            raise ValueError(
                "ParamOptimizerPair must be a tuple of length 2 (parameters, optim)."
                f"This tuple has a length of {len(param_optim_pair)}."
            )
        params, _ = param_optim_pair
        if not isinstance(params, list):
            raise ValueError(
                "The first element of a ParamOptimizerPair must be a list of "
                "parameters."
            )

    @OverrideToImplementCustomLogic
    @abc.abstractmethod
    def configure_optimizers_for_module(
        self, module_id: ModuleID, hps: LearnerHyperparameters
    ) -> Union[ParamOptimizerPair, NamedParamOptimizerPairs]:
        """Configures an optimizer for the given module_id.

        This method is called for each RLModule in the Multi-Agent RLModule being
        trained by the Learner, as well as any new module added during training via
        add_module. It should construct a ParamOptimizerPair or
        NamedParamOptimizerPairs.

        In order to construct one optimizer for the entire RLModule, it should return a
        ParamOptimizerPair. A ParamOptimzierPair is a tuple (parameters, optimizer),
        where parameters is a list of model parameters and optimizer is a deep learning
        framework optimizer The parameter module_id can be used to determine which
        module to configure the optimizer for.

        Alternatively, for a module with different optimizers for policy and value
        networks, it should return a NamedParamOptimizerPairs, which is a dictionary
        mapping from optimizer name to a ParamOptimizerPair. e.g.
        {"policy_optim" : policy_param_optim_pair, "value_optim" : ...}

        Args:
            module_id: The module_id of the RLModule that is being configured.
            hps: The LearnerHyperparameters specific to the given `module_id`.

        Returns:
            A ParamOptimizerPair or NamedParamOptimizerPairs for this module_id.
        """

    @OverrideToImplementCustomLogic
    @abc.abstractmethod
    def compute_gradients(
        self, loss_per_module: Mapping[str, TensorType], **kwargs
    ) -> ParamDictType:
        """Computes the gradients based on the given losses.

        Args:
            loss_per_module: Dict mapping module IDs to their individual total loss
                terms, computed by the individual `compute_loss_for_module()` calls.
                The overall total loss (sum of loss terms over all modules) is stored
                under the `ALL_MODULES` key.
            **kwargs: Forward compatibility kwargs.

        Returns:
            The gradients in the same (flat) format as self._params. Note that all
            top-level structures, such as module IDs, will not be present anymore in
            the returned dict. It will merely map gradient tensor references to gradient
            tensors.
        """

    @OverrideToImplementCustomLogic
    def postprocess_gradients(self, gradients_dict: ParamDictType) -> ParamDictType:
        """Applies potential postprocessing operations on the gradients.

        This method is called after gradients have been computed and modifies them
        before they are applied to the respective module(s) by the optimizer(s).
        This might include grad clipping by value, norm, or global-norm, or other
        algorithm specific gradient postprocessing steps.

        This default implementation calls `self.postprocess_gradients_for_module()`
        on each of the sub-modules in our MultiAgentRLModule: `self.module` and
        returns the accumulated gradients dicts.

        Args:
            gradients_dict: A dictionary of gradients in the same (flat) format as
                self._params. Note that top-level structures, such as module IDs,
                will not be present anymore in this dict. It will merely map gradient
                tensor references to gradient tensors.

        Returns:
            A dictionary with the updated gradients and the exact same (flat) structure
            as the incoming `gradients_dict` arg.
        """

        # The flat gradients dict (mapping param refs to params), returned by this
        # method.
        postprocessed_gradients = {}

        for module_id in self.module.keys():
            # Send a sub-gradients dict to the `postprocess_gradients_for_module`
            # method.
            module_grads_dict = {}
            for name in self._module_optimizers[module_id]:
                optimizer = self._named_optimizers[name]
                module_grads_dict.update({
                    ref: gradients_dict[ref]
                    for ref in self._optimizer_parameters[optimizer]
                    if ref in gradients_dict and gradients_dict[ref] is not None
                })
            module_grads_dict = self.postprocess_gradients_for_module(
                module_id=module_id,
                hps=self.hps.get_hps_for_module(module_id),
                module_gradients_dict=module_grads_dict,
            )
            assert isinstance(module_grads_dict, dict)

            # Update our return dict.
            postprocessed_gradients.update(module_grads_dict)

        return postprocessed_gradients

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def postprocess_gradients_for_module(
        self,
        *,
        module_id: ModuleID,
        hps: LearnerHyperparameters,
        module_gradients_dict: ParamDictType,
    ) -> ParamDictType:
        """

        Args:
            module_id: The module ID for which we will postprocess computed gradients.
                Note that `module_gradients_dict` already only carries those gradient
                tensors that belong to this `module_id`. Other `module_id`'s gradients
                are not available in this call.
            hps: The LearnerHyperparameters specific to the given `module_id`.
            module_gradients_dict: A dictionary of gradients in the same (flat) format
                as self._params, mapping gradient refs to gradient tensors, which are to
                be postprocessed. You may alter these tensors in place or create new
                ones and return these in a new dict.

        Returns:
            A dictionary with the updated gradients and the exact same (flat) structure
            as the incoming `module_gradients_dict` arg.
        """
        clip_gradients = self._get_clip_function()

        postprocessed_grads = {}

        # Loop through all optimizers of this `module_id`.
        for name, optimizer in self._named_optimizers.items():
            optim_name = name[len(module_id) + 1:]
            grad_clip: Optional[float] = (
                hps.grad_clip.get(optim_name) if isinstance(hps.grad_clip, dict)
                else hps.grad_clip
            )
            grad_clip_by: Optional[str] = (
                hps.grad_clip_by.get(optim_name) if isinstance(hps.grad_clip_by,
                                                               dict)
                else hps.grad_clip_by
            )

            if grad_clip is not None:
                grad_dict = {
                    ref: module_gradients_dict[ref]
                    for ref in self._optimizer_parameters[optimizer]
                    if (
                        ref in module_gradients_dict
                        and module_gradients_dict[ref] is not None
                    )
                }

                # Perform gradient clipping, if necessary.
                global_norm = clip_gradients(
                    grad_dict,
                    grad_clip=grad_clip,
                    grad_clip_by=grad_clip_by,
                )
                if grad_clip_by == "global_norm":
                    self.register_metric(
                        module_id,
                        f"gradients_{optim_name}_global_norm",
                        global_norm,
                    )
                postprocessed_grads.update(grad_dict)

        return postprocessed_grads

    @OverrideToImplementCustomLogic
    @abc.abstractmethod
    def apply_gradients(self, gradients: ParamDictType) -> None:
        """Applies the gradients to the MultiAgentRLModule parameters.

        Args:
            gradients: A dictionary of gradients  in the same (flat) format as
                self._params. Note that top-level structures, such as module IDs,
                will not be present anymore in this dict. It will merely map gradient
                tensor references to gradient tensors.
        """

    def register_metric(self, module_id: str, key: str, value: Any) -> None:
        """Registers a single key/value metric pair for loss and gradient stats.

        Args:
            module_id: The module_id to register the metric under. This may be
                ALL_MODULES.
            key: The name of the metric to register (below the given `module_id`).
            value: The actual value of the metric. This might also be a tensor var (e.g.
                from within a traced tf2 function).
        """
        self._metrics[module_id][key] = value

    def register_metrics(self, module_id: str, metrics_dict: Dict[str, Any]) -> None:
        """Registers a several key/value metric pairs for loss and gradient stats.

        Args:
            module_id: The module_id to register the metrics under. This may be
                ALL_MODULES.
            metrics_dict: A dict mapping names of metrics to be registered (below the
                given `module_id`) to the actual values of these metrics. Values might
                also be tensor vars (e.g. from within a traced tf2 function).
        """
        for key, value in metrics_dict.items():
            self.register_metric(module_id, key, value)

    def get_weights(self, module_ids: Optional[Set[str]] = None) -> Mapping[str, Any]:
        """Returns the weights of the underlying MultiAgentRLModule.

        The output should be numpy-friendly for easy serialization, not framework
        specific tensors.

        Args:
            module_ids: The ids of the modules to get the weights for. If None, all
                modules will be returned.

        Returns:
            A dictionary that holds the weights of the modules in a numpy-friendly
            format.
        """
        module_states = self.module.get_state(module_ids)
        return convert_to_numpy({k: v for k, v in module_states.items()})

    @abc.abstractmethod
    def set_weights(self, weights: Mapping[str, Any]) -> None:
        """Sets the weights of the underlying MultiAgentRLModule"""

    @abc.abstractmethod
    def get_param_ref(self, param: ParamType) -> Hashable:
        """Returns a hashable reference to a trainable parameter.

        This should be overriden in framework specific specialization. For example in
        torch it will return the parameter itself, while in tf it returns the .ref() of
        the variable. The purpose is to retrieve a unique reference to the parameters.

        Args:
            param: The parameter to get the reference to.

        Returns:
            A reference to the parameter.
        """

    @abc.abstractmethod
    def get_parameters(self, module: RLModule) -> Sequence[ParamType]:
        """Returns the list of parameters of a module.

        This should be overriden in framework specific learner. For example in torch it
        will return .parameters(), while in tf it returns .trainable_variables.

        Args:
            module: The module to get the parameters from.

        Returns:
            The parameters of the module.
        """

    @abc.abstractmethod
    def _convert_batch_type(self, batch: MultiAgentBatch) -> NestedDict[TensorType]:
        """Converts a MultiAgentBatch to a NestedDict of Tensors.

        This should convert the input batch from a MultiAgentBatch format to framework
        specific tensor format located on the correct device.

        Args:
            batch: A MultiAgentBatch.

        Returns:
            A NestedDict.
        """

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def compile_results(
        self,
        *,
        batch: MultiAgentBatch,
        fwd_out: Mapping[str, Any],
        loss_per_module: Mapping[str, TensorType],
        postprocessed_gradients: ParamDictType,
    ) -> Mapping[str, Any]:
        """Compile results from the update in a numpy-friendly format.

        Args:
            batch: The batch that was used for the update.
            fwd_out: The output of the forward train pass.
            loss_per_module: A dict mapping module IDs (including ALL_MODULES) to the
                individual loss tensors as returned by calls to
                `compute_loss_for_module(module_id=...)`.
            postprocessed_gradients: The postprocessed gradients dict, (flat) mapping
                gradient tensor refs to the already postprocessed gradient tensors.

        Returns:
            A dictionary of results sub-dicts per module (including ALL_MODULES).
        """
        if not isinstance(batch, MultiAgentBatch):
            raise ValueError(
                f"batch must be a MultiAgentBatch, but got {type(batch)} instead."
            )

        loss_per_module_numpy = convert_to_numpy(loss_per_module)

        # We compile the metrics to have the structure:
        # top-leve key: module_id -> [key, e.g. self.TOTAL_LOSS_KEY] -> [value].
        # Results will include all registered metrics under the respective module ID
        # top-level key.
        module_learner_stats = {}
        for module_id in list(batch.policy_batches.keys()) + [ALL_MODULES]:
            module_learner_stats[module_id] = dict(
                {self.TOTAL_LOSS_KEY: loss_per_module_numpy[module_id]},
                **self._metrics[module_id],
            )
        return module_learner_stats

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def add_module(
        self,
        *,
        module_id: ModuleID,
        module_spec: SingleAgentRLModuleSpec,
    ) -> None:
        """Add a module to the underlying MultiAgentRLModule and the Learner.

        Args:
            module_id: The id of the module to add.
            module_spec: The module spec of the module to add.
        """
        self._check_is_built()
        module = module_spec.build()

        self.module.add_module(module_id, module)

        (
            param_optimizer_pair,
            name_to_optimizer,
        ) = self._configure_optimizers_for_module_helper(module_id)

        for (param_seq, optimizer) in param_optimizer_pair:
            self._optimizer_parameters[optimizer] = []
            for param in param_seq:
                param_ref = self.get_param_ref(param)
                self._optimizer_parameters[optimizer].append(param_ref)
                self._params[param_ref] = param
        self._named_optimizers.update(name_to_optimizer)
        # Register optimizer name and create a lr scheduler for it.
        for name, optim in name_to_optimizer.items():
            self._module_optimizers[module_id].append(name)

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def remove_module(self, module_id: ModuleID) -> None:
        """Remove a module from the Learner.

        Args:
            module_id: The id of the module to remove.
        """
        self._check_is_built()
        module = self.module[module_id]

        if self._is_module_compatible_with_learner(module):
            parameters = self.get_parameters(module)
            for param in parameters:
                param_ref = self.get_param_ref(param)
                if param_ref in self._params:
                    del self._params[param_ref]
            optim_names = self._module_optimizers[module_id]
            for optim_name in optim_names:
                optim = self._named_optimizers[optim_name]
                del self._optimizer_parameters[optim]
                del self._named_optimizers[optim_name]
                del self._optimizer_lr_schedules[optim]
            del self._module_optimizers[module_id]

        self.module.remove_module(module_id)

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def build(self) -> None:
        """Builds the Learner.

        This method should be called before the learner is used. It is responsible for
        setting up the RLModule, optimizers, and their lr-schedulers.
        """
        if self._is_built:
            logger.debug("Learner already built. Skipping build.")
            return
        self._is_built = True

        self._module = self._make_module()

        for param_seq, optimizer in self.configure_optimizers():
            self._optimizer_parameters[optimizer] = []
            for param in param_seq:
                param_ref = self.get_param_ref(param)
                self._optimizer_parameters[optimizer].append(param_ref)
                self._params[param_ref] = param

    @OverrideToImplementCustomLogic
    def compute_loss(
        self,
        *,
        fwd_out: Union[MultiAgentBatch, NestedDict],
        batch: Union[MultiAgentBatch, NestedDict],
    ) -> Union[TensorType, Mapping[str, Any]]:
        """Computes the loss for the module being optimized.

        This method must be overridden multiagent-specific algorithm learners to
        specify the specific loss computation logic. If the algorithm is single agent
        `compute_loss_for_module()` should be overriden instead.
        The input "fwd_out" is the output "forward_train" method of the underlying
        MultiAgentRLModule. The input "batch" is the data that was used to compute
        "fwd_out". The returned dictionary must contain a key called "total_loss",
        which will be used to compute gradients. It is recommended to not compute any
        forward passes within this method, and to use the "forward_train" outputs to
        compute the required tensors for loss calculation.

        Args:
            fwd_out: Output from a call to `forward_train` on self.module during
                training.
            batch: The data that was used to compute fwd_out.

        Returns:
            A dictionary of losses. The dictionary
            must contain one protected key "total_loss" which will be used for
            computing gradients through.
        """
        loss_total = None
        loss_per_module = {}
        for module_id in fwd_out:
            module_batch = batch[module_id]
            module_fwd_out = fwd_out[module_id]

            loss = self.compute_loss_for_module(
                module_id=module_id,
                hps=self.hps.get_hps_for_module(module_id),
                batch=module_batch,
                fwd_out=module_fwd_out,
            )
            loss_per_module[module_id] = loss

            if loss_total is None:
                loss_total = loss
            else:
                loss_total += loss

        loss_per_module[ALL_MODULES] = loss_total

        return loss_per_module

    @OverrideToImplementCustomLogic
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        hps: LearnerHyperparameters,
        batch: SampleBatch,
        fwd_out: Mapping[str, TensorType],
    ) -> TensorType:
        """Computes the loss for a single module.

        Think of this as computing loss for a single agent. For multi-agent use-cases
        that require more complicated computation for loss, consider overriding the
        `compute_loss` method instead.

        Args:
            module_id: The id of the module.
            hps: The LearnerHyperparameters specific to the given `module_id`.
            batch: The sample batch for this particular module.
            fwd_out: The output of the forward pass for this particular module.

        Returns:
            A single total loss tensor. If you have more than one optimizer on the
            provided `module_id` and would like to compute gradients separately using
            these different optimizers, simply add up the individual loss terms for
            each optimizer and return the sum. Also, for tracking the individual loss
            terms, you can use the `Learner.register_metric(s)` APIs.
        """
        raise NotImplementedError

    @OverrideToImplementCustomLogic
    def additional_update(
        self,
        *,
        module_ids_to_update: Sequence[ModuleID] = None,
        timestep: int,
        **kwargs,
    ) -> Mapping[ModuleID, Any]:
        """Apply additional non-gradient based updates to this Trainer.

        For example, this could be used to do a polyak averaging update
        of a target network in off policy algorithms like SAC or DQN.

        Example:

        .. code-block:: python

            class DQNLearner(TorchLearner):

                def additional_update_for_module(self, module_id: ModuleID, tau: float):
                    # perform polyak averaging update
                    main = self.module[module_id].main
                    target = self.module[module_id].target
                    for param, target_param in zip(
                        main.parameters(), target.parameters()
                    ):
                        target_param.data.copy_(
                            tau * param.data + (1.0 - tau) * target_param.data
                        )

        And inside a training loop:

        .. code-block:: python

            for _ in range(100):
                sample = ...
                self.learner.update(sample)
                if self.learner.global_step % 10 == 0:
                    self.learner.additional_update(tau=0.01)

        Args:
            module_ids_to_update: The ids of the modules to update. If None, all
                modules will be updated.
            timestep: The current timestep.
            **kwargs: Keyword arguments to use for the additional update.

        Returns:
            A dictionary of results from the update
        """
        results_all_modules = {}
        module_ids = module_ids_to_update or self.module.keys()
        for module_id in module_ids:
            module_results = self.additional_update_for_module(
                module_id=module_id,
                hps=self.hps.get_hps_for_module(module_id),
                timestep=timestep,
                **kwargs,
            )
            results_all_modules[module_id] = module_results

        return results_all_modules

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def additional_update_for_module(
        self,
        *,
        module_id: ModuleID,
        hps: LearnerHyperparameters,
        timestep: int, **kwargs,
    ) -> Dict[str, Any]:
        """Apply additional non-gradient based updates for a single module.

        See `additional_update` for more details.

        Args:
            module_id: The id of the module to update.
            hps: The LearnerHyperparameters specific to the given `module_id`.
            timestep: The current global timestep (to be used with schedulers).
            **kwargs: Keyword arguments to use for the additional update.

        Returns:
            A dictionary of results from the update
        """
        results = {}

        # Handle lr scheduling updates and apply new learning rates to the optimizers.
        for name, optimizer in self._named_optimizers.items():
            # Only cover optimizers for this particular module.
            if name not in self._module_optimizers[module_id]:
                continue

            new_lr = self._optimizer_lr_schedules[optimizer].update(timestep=timestep)
            self._set_optimizer_lr(optimizer, lr=new_lr)
            results.update({name: new_lr})
        # In the simple case (only one optimizer for this module), publish current lr
        # directly under LEARNER_RESULTS_CURR_LR_KEY, otherwise, return new lrs by
        # optimizer name.
        results = {
            LEARNER_RESULTS_CURR_LR_KEY: (
                results if len(results) > 1 else list(results.values())[0]
            )
        }
        return results

    def update(
        self,
        batch: MultiAgentBatch,
        *,
        minibatch_size: Optional[int] = None,
        num_iters: int = 1,
        reduce_fn: Callable[[List[Mapping[str, Any]]], ResultDict] = (
            _reduce_mean_results
        ),
    ) -> Mapping[str, Any]:
        """Do `num_iters` minibatch updates given the original batch.

        Given a batch of episodes you can use this method to take more
        than one backward pass on the batch. The same minibatch_size and num_iters
        will be used for all module ids in MultiAgentRLModule.

        Args:
            batch: A batch of data.
            minibatch_size: The size of the minibatch to use for each update.
            num_iters: The number of complete passes over all the sub-batches
                in the input multi-agent batch.
            reduce_fn: reduce_fn: A function to reduce the results from a list of
                minibatch updates. This can be any arbitrary function that takes a
                list of dictionaries and returns a single dictionary. For example you
                can either take an average (default) or concatenate the results (for
                example for metrics) or be more selective about you want to report back
                to the algorithm's training_step. If None is passed, the results will
                not get reduced.
        Returns:
            A dictionary of results, in numpy format.
        """
        self._check_is_built()

        missing_module_ids = set(batch.policy_batches.keys()) - set(self.module.keys())
        if len(missing_module_ids) > 0:
            raise ValueError(
                "Batch contains module ids that are not in the learner: "
                f"{missing_module_ids}"
            )

        if num_iters < 1:
            # we must do at least one pass on the batch for training
            raise ValueError("num_iters must be >= 1")

        if minibatch_size:
            batch_iter = MiniBatchCyclicIterator
        elif num_iters > 1:
            # minibatch size was not set but num_iters > 1
            # Under the old training stack, users could do multiple sgd passes
            # over a batch without specifying a minibatch size. We enable
            # this behavior here by setting the minibatch size to be the size
            # of the batch (e.g. 1 minibatch of size batch.count)
            minibatch_size = batch.count
            batch_iter = MiniBatchCyclicIterator
        else:
            # minibatch_size and num_iters are not set by the user
            batch_iter = MiniBatchDummyIterator

        results = []
        for minibatch in batch_iter(batch, minibatch_size, num_iters):

            result = self._update(minibatch)
            results.append(result)

        # Reduce results across all minibatches, if necessary.
        if len(results) == 1:
            return results[0]
        else:
            if reduce_fn is None:
                return results
            return reduce_fn(results)

    def set_state(self, state: Mapping[str, Any]) -> None:
        """Set the state of the learner.

        Args:
            state: The state of the optimizer and module. Can be obtained
                from `get_state`. State is a dictionary with two keys:
                "module_state" and "optimizer_state". The value of each key
                is a dictionary that can be passed to `set_weights` and
                `set_optimizer_weights` respectively.

        """
        # TODO (Kourosh): We have both get(set)_state and get(set)_weights. I think
        # having both can become confusing. Can we simplify this API requirement?
        self._check_is_built()
        # TODO: once we figure out the optimizer format, we can set/get the state
        if "module_state" not in state:
            raise ValueError(
                "state must have a key 'module_state' for the module weights"
            )
        if "optimizer_state" not in state:
            raise ValueError(
                "state must have a key 'optimizer_state' for the optimizer weights"
            )

        module_state = state.get("module_state")
        optimizer_state = state.get("optimizer_state")
        self.set_weights(module_state)
        self.set_optimizer_weights(optimizer_state)

    def get_state(self) -> Mapping[str, Any]:
        """Get the state of the learner.

        Returns:
            The state of the optimizer and module.

        """
        self._check_is_built()
        # TODO: once we figure out the optimizer format, we can set/get the state
        return {
            "module_state": self.get_weights(),
            "optimizer_state": self.get_optimizer_weights(),
        }
        # return {"module_state": self.get_weights(), "optimizer_state": {}}

    def set_optimizer_weights(self, weights: Mapping[str, Any]) -> None:
        """Set the weights of the optimizer.

        Args:
            weights: The weights of the optimizer.

        """
        raise NotImplementedError

    def get_optimizer_weights(self) -> Mapping[str, Any]:
        """Get the weights of the optimizer.

        Returns:
            The weights of the optimizer.

        """
        raise NotImplementedError

    def _get_metadata(self) -> Dict[str, Any]:
        metadata = {
            "learner_class": serialize_type(self.__class__),
            "ray_version": ray.__version__,
            "ray_commit": ray.__commit__,
            "module_state_dir": "module_state",
            "optimizer_state_dir": "optimizer_state",
        }
        return metadata

    def _save_optimizers(self, path: Union[str, pathlib.Path]) -> None:
        """Save the state of the optimizer to path

        NOTE: if path doesn't exist, then a new directory will be created. otherwise, it
        will be appended to.

        Args:
            path: The path to the directory to save the state to.

        """
        pass

    def _load_optimizers(self, path: Union[str, pathlib.Path]) -> None:
        """Load the state of the optimizer from path

        Args:
            path: The path to the directory to load the state from.

        """
        pass

    def save_state(self, path: Union[str, pathlib.Path]) -> None:
        """Save the state of the learner to path

        NOTE: if path doesn't exist, then a new directory will be created. otherwise, it
        will be appended to.

        the state of the learner is saved in the following format:

        checkpoint_dir/
            learner_state.json
            module_state/
                module_1/
                    ...
            optimizer_state/
                optimizers_module_1/
                    ...

        Args:
            path: The path to the directory to save the state to.

        """
        self._check_is_built()
        path = pathlib.Path(path)
        path.mkdir(parents=True, exist_ok=True)
        self.module.save_to_checkpoint(path / "module_state")
        self._save_optimizers(path / "optimizer_state")
        with open(path / "learner_state.json", "w") as f:
            metadata = self._get_metadata()
            json.dump(metadata, f)

    def load_state(
        self,
        path: Union[str, pathlib.Path],
    ) -> None:
        """Load the state of the learner from path

        Note: The learner must be constructed ahead of time before its state is loaded.

        Args:
            path: The path to the directory to load the state from.
        """
        self._check_is_built()
        path = pathlib.Path(path)
        del self._module
        # TODO(avnishn) from checkpoint doesn't currently support modules_to_load,
        #  but it should, so we will add it later.
        self._module_obj = MultiAgentRLModule.from_checkpoint(path / "module_state")
        self._reset()
        self.build()
        self._load_optimizers(path / "optimizer_state")

    @abc.abstractmethod
    def _is_module_compatible_with_learner(self, module: RLModule) -> bool:
        """Check whether the module is compatible with the learner.

        For example, if there is a random RLModule, it will not be a torch or tf
        module, but rather it is a numpy module. Therefore we should not consider it
        during gradient based optimization.

        Args:
            module: The module to check.

        Returns:
            True if the module is compatible with the learner.
        """

    def _make_module(self) -> MultiAgentRLModule:
        """Construct the multi-agent RL module for the learner.

        This method uses `self._module_specs` or `self._module_obj` to construct the
        module. If the module_class is a single agent RL module it will be wrapped to a
        multi-agent RL module. Override this method if there are other things that
        need to happen for instantiation of the module.

        Returns:
            A constructed MultiAgentRLModule.
        """
        if self._module_obj is not None:
            module = self._module_obj
        else:
            module = self._module_spec.build()
        # If not already, convert to MultiAgentRLModule.
        module = module.as_multi_agent()
        return module

    def _check_result(self, result: Mapping[str, Any]) -> None:
        """Checks whether the result has the correct format.

        All the keys should be referencing the module ids that got updated. There is a
        special key `ALL_MODULES` that hold any extra information that is not specific
        to a module.

        Args:
            result: The result of the update.

        Raises:
            ValueError: If the result are not in the correct format.
        """
        if not isinstance(result, dict):
            raise ValueError(
                f"The result of the update must be a dictionary. Got: {type(result)}"
            )

        if ALL_MODULES not in result:
            raise ValueError(
                f"The result of the update must have a key {ALL_MODULES} "
                "that holds any extra information that is not specific to a module."
            )

        for key in result:
            if key != ALL_MODULES:
                if key not in self.module.keys():
                    raise ValueError(
                        f"The key {key} in the result of the update is not a valid "
                        f"module id. Valid module ids are: {list(self.module.keys())}."
                    )

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def _update(
        self,
        batch: MultiAgentBatch,
    ) -> Mapping[str, Any]:
        """Performs a single update given a batch of data."""
        # TODO (Kourosh): remove the MultiAgentBatch from the type, it should be
        #  NestedDict from the base class.
        tensorbatch = self._convert_batch_type(batch)
        fwd_out = self.module.forward_train(tensorbatch)
        loss_per_module = self.compute_loss(fwd_out=fwd_out, batch=tensorbatch)

        gradients = self.compute_gradients(loss_per_module)
        postprocessed_gradients = self.postprocess_gradients(gradients)
        self.apply_gradients(postprocessed_gradients)
        results = self.compile_results(
            batch=batch,
            fwd_out=fwd_out,
            loss_per_module=loss_per_module,
            postprocessed_gradients=postprocessed_gradients,
        )
        self._check_result(results)
        return convert_to_numpy(results)

    def _check_is_built(self):
        if self.module is None:
            raise ValueError(
                "Learner.build() must be called after constructing a "
                "Learner and before calling any methods on it."
            )

    def _reset(self):
        self._params = {}
        self._optimizer_parameters = {}
        self._named_optimizers = {}
        self._module_optimizers = defaultdict(list)
        self._is_built = False

    def apply(self, func, *_args, **_kwargs):
        return func(self, *_args, **_kwargs)

    def _pair_optim_with_lr_scheduler(self, hps, name, optim):
        # Figure out the correct setting to use for this optimizer.
        fixed_value_or_schedule = (
            hps.learning_rate.get(name) if isinstance(hps.learning_rate, dict)
            else hps.learning_rate
        )

        # Build learning rate scheduling tools (one Schedule per optimizer).
        self._optimizer_lr_schedules[optim] = Scheduler(
            fixed_value_or_schedule=fixed_value_or_schedule,
            framework=self.framework,
            device=self._device,
        )
        # Set the optimizer to the current (first) learning rate.
        lr = self._optimizer_lr_schedules[optim].get_current_value()
        self._set_optimizer_lr(optimizer=optim, lr=lr)

    @abc.abstractmethod
    def _get_tensor_variable(
        self,
        value: Any,
        dtype: Any = None,
        trainable: bool = False,
    ) -> TensorType:
        """Returns a framework-specific tensor variable with the initial given value.

        This is a framework specific method that should be implemented by the
        framework specific sub-class.

        Args:
            value: The initial value for the tensor variable variable.

        Returns:
            The framework specific tensor variable of the given initial value,
            dtype and trainable/requires_grad property.
        """

    @staticmethod
    @abc.abstractmethod
    def _set_optimizer_lr(optimizer: Optimizer, lr: float) -> None:
        """Updates the learning rate of the given local optimizer.

        Args:
            optimizer: The local optimizer to update the learning rate for.
            lr: The new learning rate.
        """

    @staticmethod
    @abc.abstractmethod
    def _get_clip_function() -> Callable:
        """Returns the gradient clipping function to use, given the framework."""


@dataclass
class LearnerSpec:
    """The spec for constructing Learner actors.

    Args:
        learner_class: The Learner class to use.
        module_spec: The underlying (MA)RLModule spec to completely define the module.
        module: Alternatively the RLModule instance can be passed in directly. This
            only works if the Learner is not an actor.
        backend_config: The backend config for properly distributing the RLModule.
        #optimizer_config: The optimizer setting to apply during training.
        learner_hyperparameters: The extra config for the loss/additional update. This
            should be a subclass of LearnerHyperparameters. This is useful for passing
            in algorithm configs that contains the hyper-parameters for loss
            computation, change of training behaviors, etc. e.g lr, entropy_coeff.
    """

    learner_class: Type["Learner"]
    module_spec: Union["SingleAgentRLModuleSpec", "MultiAgentRLModuleSpec"] = None
    module: Optional["RLModule"] = None
    learner_group_scaling_config: LearnerGroupScalingConfig = field(
        default_factory=LearnerGroupScalingConfig
    )
    #optimizer_config: Dict[str, Any] = field(default_factory=dict)
    learner_hyperparameters: LearnerHyperparameters = field(
        default_factory=LearnerHyperparameters
    )
    framework_hyperparameters: FrameworkHyperparameters = field(
        default_factory=FrameworkHyperparameters
    )

    def get_params_dict(self) -> Dict[str, Any]:
        """Returns the parameters than be passed to the Learner constructor."""
        return {
            "module": self.module,
            "module_spec": self.module_spec,
            "learner_group_scaling_config": self.learner_group_scaling_config,
            #"optimizer_config": self.optimizer_config,
            "learner_hyperparameters": self.learner_hyperparameters,
            "framework_hyperparameters": self.framework_hyperparameters,
        }

    def build(self) -> "Learner":
        """Builds the Learner instance."""
        return self.learner_class(**self.get_params_dict())

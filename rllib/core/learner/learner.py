import abc
import json
import logging
import pathlib
from collections import defaultdict
from enum import Enum
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    DefaultDict,
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
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, MultiAgentBatch
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.debug import update_global_seed_if_necessary
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED,
)
from ray.rllib.utils.minibatch_utils import (
    MiniBatchDummyIterator,
    MiniBatchCyclicIterator,
)
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.serialization import serialize_type
from ray.rllib.utils.typing import (
    LearningRateOrSchedule,
    Optimizer,
    Param,
    ParamRef,
    ParamDict,
    ResultDict,
    TensorType,
)

if TYPE_CHECKING:
    from ray.rllib.core.rl_module.torch.torch_compile_config import TorchCompileConfig

torch, _ = try_import_torch()
tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)

DEFAULT_OPTIMIZER = "default_optimizer"

# COMMON LEARNER LOSS_KEYS
POLICY_LOSS_KEY = "policy_loss"
VF_LOSS_KEY = "vf_loss"
ENTROPY_KEY = "entropy"

# Additional update keys
LEARNER_RESULTS_CURR_LR_KEY = "curr_lr"


class TorchCompileWhatToCompile(str, Enum):
    """Enumerates schemes of what parts of the TorchLearner can be compiled.

    This can be either the entire update step of the learner or only the forward
    methods (and therein the forward_train method) of the RLModule.

    .. note::
        - torch.compiled code can become slow on graph breaks or even raise
            errors on unsupported operations. Empirically, compiling
            `forward_train` should introduce little graph breaks, raise no
            errors but result in a speedup comparable to compiling the
            complete update.
        - Using `complete_update` is experimental and may result in errors.
    """

    # Compile the entire update step of the learner.
    # This includes the forward pass of the RLModule, the loss computation, and the
    # optimizer step.
    COMPLETE_UPDATE = "complete_update"
    # Only compile the forward methods (and therein the forward_train method) of the
    # RLModule.
    FORWARD_TRAIN = "forward_train"


@dataclass
class FrameworkHyperparameters:
    """The framework specific hyper-parameters.

    Args:
        eager_tracing: Whether to trace the model in eager mode. This enables tf
            tracing mode by wrapping the loss function computation in a tf.function.
            This is useful for speeding up the training loop. However, it is not
            compatible with all tf operations. For example, tf.print is not supported
            in tf.function.
        torch_compile: Whether to use torch.compile() within the context of a given
            learner.
        what_to_compile: What to compile when using torch.compile(). Can be one of
            [TorchCompileWhatToCompile.complete_update,
            TorchCompileWhatToCompile.forward_train].
            If `complete_update`, the update step of the learner will be compiled. This
            includes the forward pass of the RLModule, the loss computation, and the
            optimizer step.
            If `forward_train`, only the forward methods (and therein the
            forward_train method) of the RLModule will be compiled.
            Either of the two may lead to different performance gains in different
            settings.
            `complete_update` promises the highest performance gains, but may not work
            in some settings. By compiling only forward_train, you may already get
            some speedups and avoid issues that arise from compiling the entire update.
        troch_compile_config: The TorchCompileConfig to use for compiling the RL
            Module in Torch.
    """

    eager_tracing: bool = True
    torch_compile: bool = False
    what_to_compile: str = TorchCompileWhatToCompile.FORWARD_TRAIN
    torch_compile_cfg: Optional["TorchCompileConfig"] = None

    def validate(self):
        if self.torch_compile:
            if self.what_to_compile not in [
                TorchCompileWhatToCompile.FORWARD_TRAIN,
                TorchCompileWhatToCompile.COMPLETE_UPDATE,
            ]:
                raise ValueError(
                    f"what_to_compile must be one of ["
                    f"TorchCompileWhatToCompile.forward_train, "
                    f"TorchCompileWhatToCompile.complete_update] but is"
                    f" {self.what_to_compile}"
                )
            if self.torch_compile_cfg is None:
                raise ValueError(
                    "torch_compile_cfg must be set when torch_compile is True."
                )


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
    within the Learner, RLlib uses the `_per_module_overrides` property (dict), mapping
    ModuleID to a overridden version of self, in which the module-specific override
    settings are applied.
    """

    # Parameters used for gradient postprocessing (clipping) and gradient application.
    learning_rate: LearningRateOrSchedule = None
    grad_clip: float = None
    grad_clip_by: str = None
    seed: int = None

    # Maps ModuleIDs to LearnerHyperparameters that are to be used for that particular
    # module.
    # You can access the module-specific `LearnerHyperparameters` object for a given
    # module_id by using the `get_hps_for_module(module_id=..)` API.
    _per_module_overrides: Optional[Dict[ModuleID, "LearnerHyperparameters"]] = None

    def get_hps_for_module(self, module_id: ModuleID) -> "LearnerHyperparameters":
        """Returns a LearnerHyperparameter instance, given a `module_id`.

        This is useful for passing these module-specific HPs to a Learner's
        `..._for_module(module_id=.., hps=..)` methods. Individual modules within
        a MultiAgentRLModule can then override certain AlgorithmConfig settings
        of the main config, e.g. the learning rate.

        Args:
            module_id: The module ID for which to return a specific
                LearnerHyperparameter instance.

        Returns:
            The module specific LearnerHyperparameter instance.
        """
        # ModuleID found in our overrides dict. Return module specific HPs.
        if (
            self._per_module_overrides is not None
            and module_id in self._per_module_overrides
        ):
            # In case, the per-module sub-HPs object is still a dict, convert
            # it to a fully qualified LearnerHyperparameter object here first.
            if isinstance(self._per_module_overrides[module_id], dict):
                self._per_module_overrides[module_id] = type(self)(
                    **self._per_module_overrides[module_id]
                )
            # Return the module specific version of self.
            return self._per_module_overrides[module_id]
        # ModuleID not found in overrides or the overrides dict is None
        # -> return self.
        else:
            return self


class Learner:
    """Base class for Learners.

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

        Note: We use PPO and torch as an example here because many of the showcased
        components need implementations to come together. However, the same
        pattern is generally applicable.

        .. testcode::

            from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import (
                PPOTorchRLModule
            )
            from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
            from ray.rllib.core.learner.torch.torch_learner import TorchLearner
            import gymnasium as gym

            env = gym.make("CartPole-v1")

            # Create a single agent RL module spec.
            module_spec = SingleAgentRLModuleSpec(
                module_class=PPOTorchRLModule,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config_dict = {"hidden": [128, 128]},
                catalog_class = PPOCatalog,
            )

            # Create a learner instance that will train the module
            learner = TorchLearner(module_spec=module_spec)

            # Note: the learner should be built before it can be used.
            learner.build()

            # Take one gradient update on the module and report the results
            # results = learner.update(...)

            # Add a new module, perhaps for league based training
            learner.add_module(
                module_id="new_player",
                module_spec=SingleAgentRLModuleSpec(
                    module_class=PPOTorchRLModule,
                    observation_space=env.observation_space,
                    action_space=env.action_space,
                    model_config_dict = {"hidden": [128, 128]},
                    catalog_class = PPOCatalog,
                )
            )

            # Take another gradient update with both previous and new modules.
            # results = learner.update(...)

            # Remove a module
            learner.remove_module("new_player")

            # Will train previous modules only.
            # results = learner.update(...)

            # Get the state of the learner
            state = learner.get_state()

            # Set the state of the learner
            learner.set_state(state)

            # Get the weights of the underly multi-agent RLModule
            weights = learner.get_module_state()

            # Set the weights of the underly multi-agent RLModule
            learner.set_module_state(weights)


    Extension pattern:

        .. testcode::

            from ray.rllib.core.learner.torch.torch_learner import TorchLearner

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
        learner_group_scaling_config: Optional[LearnerGroupScalingConfig] = None,
        learner_hyperparameters: Optional[LearnerHyperparameters] = None,
        framework_hyperparameters: Optional[FrameworkHyperparameters] = None,
    ):
        # We first set seeds
        if learner_hyperparameters and learner_hyperparameters.seed is not None:
            update_global_seed_if_necessary(
                self.framework, learner_hyperparameters.seed
            )

        if (module_spec is None) is (module is None):
            raise ValueError(
                "Exactly one of `module_spec` or `module` must be provided to Learner!"
            )

        self._module_spec = module_spec
        self._module_obj = module
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
        self._framework_hyperparameters.validate()

        # whether self.build has already been called
        self._is_built = False

        # These are the attributes that are set during build.

        # The actual MARLModule used by this Learner.
        self._module: Optional[MultiAgentRLModule] = None
        # These are set for properly applying optimizers and adding or removing modules.
        self._optimizer_parameters: Dict[Optimizer, List[ParamRef]] = {}
        self._named_optimizers: Dict[str, Optimizer] = {}
        self._params: ParamDict = {}
        # Dict mapping ModuleID to a list of optimizer names. Note that the optimizer
        # name includes the ModuleID as a prefix: optimizer_name=`[ModuleID]_[.. rest]`.
        self._module_optimizers: Dict[ModuleID, List[str]] = defaultdict(list)

        # Only manage optimizer's learning rate if user has NOT overridden
        # the `configure_optimizers_for_module` method. Otherwise, leave responsibility
        # to handle lr-updates entirely in user's hands.
        self._optimizer_lr_schedules: Dict[Optimizer, Scheduler] = {}

        # Registered metrics (one sub-dict per module ID) to be returned from
        # `Learner.update()`. These metrics will be "compiled" automatically into
        # the final results dict in the `self.compile_update_results()` method.
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

    def register_optimizer(
        self,
        *,
        module_id: ModuleID = ALL_MODULES,
        optimizer_name: str = DEFAULT_OPTIMIZER,
        optimizer: Optimizer,
        params: Sequence[Param],
        lr_or_lr_schedule: Optional[LearningRateOrSchedule] = None,
    ) -> None:
        """Registers an optimizer with a ModuleID, name, param list and lr-scheduler.

        Use this method in your custom implementations of either
        `self.configure_optimizers()` or `self.configure_optimzers_for_module()` (you
        should only override one of these!). If you register a learning rate Scheduler
        setting together with an optimizer, RLlib will automatically keep this
        optimizer's learning rate updated throughout the training process.
        Alternatively, you can construct your optimizers directly with a learning rate
        and manage learning rate scheduling or updating yourself.

        Args:
            module_id: The `module_id` under which to register the optimizer. If not
                provided, will assume ALL_MODULES.
            optimizer_name: The name (str) of the optimizer. If not provided, will
                assume DEFAULT_OPTIMIZER.
            optimizer: The already instantiated optimizer object to register.
            params: A list of parameters (framework-specific variables) that will be
                trained/updated
            lr_or_lr_schedule: An optional fixed learning rate or learning rate schedule
                setup. If provided, RLlib will automatically keep the optimizer's
                learning rate updated.
        """
        # Validate optimizer instance and its param list.
        self._check_registered_optimizer(optimizer, params)

        full_registration_name = module_id + "_" + optimizer_name

        # Store the given optimizer under the given `module_id`.
        self._module_optimizers[module_id].append(full_registration_name)

        # Store the optimizer instance under its full `module_id`_`optimizer_name`
        # key.
        self._named_optimizers[full_registration_name] = optimizer

        # Store all given parameters under the given optimizer.
        self._optimizer_parameters[optimizer] = []
        for param in params:
            param_ref = self.get_param_ref(param)
            self._optimizer_parameters[optimizer].append(param_ref)
            self._params[param_ref] = param

        # Optionally, store a scheduler object along with this optimizer. If such a
        # setting is provided, RLlib will handle updating the optimizer's learning rate
        # over time.
        if lr_or_lr_schedule is not None:
            # Validate the given setting.
            Scheduler.validate(
                fixed_value_or_schedule=lr_or_lr_schedule,
                setting_name="lr_or_lr_schedule",
                description="learning rate or schedule",
            )
            # Create the scheduler object for this optimizer.
            scheduler = Scheduler(
                fixed_value_or_schedule=lr_or_lr_schedule,
                framework=self.framework,
                device=self._device,
            )
            self._optimizer_lr_schedules[optimizer] = scheduler
            # Set the optimizer to the current (first) learning rate.
            self._set_optimizer_lr(
                optimizer=optimizer,
                lr=scheduler.get_current_value(),
            )

    @OverrideToImplementCustomLogic
    def configure_optimizers(self) -> None:
        """Configures, creates, and registers the optimizers for this Learner.

        Optimizers are responsible for updating the model's parameters during training,
        based on the computed gradients.

        Normally, you should not override this method for your custom algorithms
        (which require certain optimizers), but rather override the
        `self.configure_optimizers_for_module(module_id=..)` method and register those
        optimizers in there that you need for the given `module_id`.

        You can register an optimizer for any RLModule within `self.module` (or for
        the ALL_MODULES ID) by calling `self.register_optimizer()` and passing the
        module_id, optimizer_name (only in case you would like to register more than
        one optimizer for a given module), the optimizer instane itself, a list
        of all the optimizer's parameters (to be updated by the optimizer), and
        an optional learning rate or learning rate schedule setting.

        This method is called once during building (`self.build()`).
        """

        # The default implementation simply calls `self.configure_optimizers_for_module`
        # on each RLModule within `self.module`.
        for module_id in self.module.keys():
            if self._is_module_compatible_with_learner(self.module[module_id]):
                hps = self.hps.get_hps_for_module(module_id)
                self.configure_optimizers_for_module(module_id=module_id, hps=hps)

    @OverrideToImplementCustomLogic
    @abc.abstractmethod
    def configure_optimizers_for_module(
        self, module_id: ModuleID, hps: LearnerHyperparameters
    ) -> None:
        """Configures an optimizer for the given module_id.

        This method is called for each RLModule in the Multi-Agent RLModule being
        trained by the Learner, as well as any new module added during training via
        `self.add_module()`. It should configure and construct one or more optimizers
        and register them via calls to `self.register_optimizer()` along with the
        `module_id`, an optional optimizer name (str), a list of the optimizer's
        framework specific parameters (variables), and an optional learning rate value
        or -schedule.

        Args:
            module_id: The module_id of the RLModule that is being configured.
            hps: The LearnerHyperparameters specific to the given `module_id`.
        """

    @OverrideToImplementCustomLogic
    @abc.abstractmethod
    def compute_gradients(
        self, loss_per_module: Mapping[str, TensorType], **kwargs
    ) -> ParamDict:
        """Computes the gradients based on the given losses.

        Args:
            loss_per_module: Dict mapping module IDs to their individual total loss
                terms, computed by the individual `compute_loss_for_module()` calls.
                The overall total loss (sum of loss terms over all modules) is stored
                under `loss_per_module[ALL_MODULES]`.
            **kwargs: Forward compatibility kwargs.

        Returns:
            The gradients in the same (flat) format as self._params. Note that all
            top-level structures, such as module IDs, will not be present anymore in
            the returned dict. It will merely map parameter tensor references to their
            respective gradient tensors.
        """

    @OverrideToImplementCustomLogic
    def postprocess_gradients(self, gradients_dict: ParamDict) -> ParamDict:
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
            # Send a gradients dict for only this `module_id` to the
            # `self.postprocess_gradients_for_module()` method.
            module_grads_dict = {}
            for optimizer_name, optimizer in self.get_optimizers_for_module(module_id):
                module_grads_dict.update(
                    self.filter_param_dict_for_optimizer(gradients_dict, optimizer)
                )

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
        module_gradients_dict: ParamDict,
    ) -> ParamDict:
        """Applies postprocessing operations on the gradients of the given module.

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
        postprocessed_grads = {}

        if hps.grad_clip is None:
            postprocessed_grads.update(module_gradients_dict)
            return postprocessed_grads

        for optimizer_name, optimizer in self.get_optimizers_for_module(module_id):
            grad_dict_to_clip = self.filter_param_dict_for_optimizer(
                param_dict=module_gradients_dict,
                optimizer=optimizer,
            )
            # Perform gradient clipping, if configured.
            global_norm = self._get_clip_function()(
                grad_dict_to_clip,
                grad_clip=hps.grad_clip,
                grad_clip_by=hps.grad_clip_by,
            )
            if hps.grad_clip_by == "global_norm":
                self.register_metric(
                    module_id,
                    f"gradients_{optimizer_name}_global_norm",
                    global_norm,
                )
            postprocessed_grads.update(grad_dict_to_clip)

        return postprocessed_grads

    @OverrideToImplementCustomLogic
    @abc.abstractmethod
    def apply_gradients(self, gradients_dict: ParamDict) -> None:
        """Applies the gradients to the MultiAgentRLModule parameters.

        Args:
            gradients_dict: A dictionary of gradients in the same (flat) format as
                self._params. Note that top-level structures, such as module IDs,
                will not be present anymore in this dict. It will merely map gradient
                tensor references to gradient tensors.
        """

    def register_metric(self, module_id: str, key: str, value: Any) -> None:
        """Registers a single key/value metric pair for loss- and gradient stats.

        Args:
            module_id: The module_id to register the metric under. This may be
                ALL_MODULES.
            key: The name of the metric to register (below the given `module_id`).
            value: The actual value of the metric. This might also be a tensor var (e.g.
                from within a traced tf2 function).
        """
        self._metrics[module_id][key] = value

    def register_metrics(self, module_id: str, metrics_dict: Dict[str, Any]) -> None:
        """Registers several key/value metric pairs for loss- and gradient stats.

        Args:
            module_id: The module_id to register the metrics under. This may be
                ALL_MODULES.
            metrics_dict: A dict mapping names of metrics to be registered (below the
                given `module_id`) to the actual values of these metrics. Values might
                also be tensor vars (e.g. from within a traced tf2 function).
                These will be automatically converted to numpy values.
        """
        for key, value in metrics_dict.items():
            self.register_metric(module_id, key, value)

    def get_optimizer(
        self,
        module_id: ModuleID = DEFAULT_POLICY_ID,
        optimizer_name: str = DEFAULT_OPTIMIZER,
    ) -> Optimizer:
        """Returns the optimizer object, configured under the given module_id and name.

        If only one optimizer was registered under `module_id` (or ALL_MODULES)
        via the `self.register_optimizer` method, `optimizer_name` is assumed to be
        DEFAULT_OPTIMIZER.

        Args:
            module_id: The ModuleID for which to return the configured optimizer.
                If not provided, will assume DEFAULT_POLICY_ID.
            optimizer_name: The name of the optimizer (registered under `module_id` via
                `self.register_optimizer()`) to return. If not provided, will assume
                DEFAULT_OPTIMIZER.

        Returns:
            The optimizer object, configured under the given `module_id` and
            `optimizer_name`.
        """
        full_registration_name = module_id + "_" + optimizer_name
        assert full_registration_name in self._named_optimizers
        return self._named_optimizers[full_registration_name]

    def get_optimizers_for_module(
        self, module_id: ModuleID = ALL_MODULES
    ) -> List[Tuple[str, Optimizer]]:
        """Returns a list of (optimizer_name, optimizer instance)-tuples for module_id.

        Args:
            module_id: The ModuleID for which to return the configured
                (optimizer name, optimizer)-pairs. If not provided, will return
                optimizers registered under ALL_MODULES.

        Returns:
            A list of tuples of the format: ([optimizer_name], [optimizer object]),
            where optimizer_name is the name under which the optimizer was registered
            in `self.register_optimizer`. If only a single optimizer was
            configured for `module_id`, [optimizer_name] will be DEFAULT_OPTIMIZER.
        """
        named_optimizers = []
        for full_registration_name in self._module_optimizers[module_id]:
            optimizer = self._named_optimizers[full_registration_name]
            # TODO (sven): How can we avoid registering optimziers under this
            #  constructed `[module_id]_[optim_name]` format?
            optim_name = full_registration_name[len(module_id) + 1 :]
            named_optimizers.append((optim_name, optimizer))
        return named_optimizers

    def filter_param_dict_for_optimizer(
        self, param_dict: ParamDict, optimizer: Optimizer
    ) -> ParamDict:
        """Reduces the given ParamDict to contain only parameters for given optimizer.

        Args:
            param_dict: The ParamDict to reduce/filter down to the given `optimizer`.
                The returned dict will be a subset of `param_dict` only containing keys
                (param refs) that were registered together with `optimizer` (and thus
                that `optimizer` is responsible for applying gradients to).
            optimizer: The optimizer object to whose parameter refs the given
                `param_dict` should be reduced.

        Returns:
            A new ParamDict only containing param ref keys that belong to `optimizer`.
        """
        # Return a sub-dict only containing those param_ref keys (and their values)
        # that belong to the `optimizer`.
        return {
            ref: param_dict[ref]
            for ref in self._optimizer_parameters[optimizer]
            if ref in param_dict and param_dict[ref] is not None
        }

    def get_module_state(
        self, module_ids: Optional[Set[str]] = None
    ) -> Mapping[str, Any]:
        """Returns the state of the underlying MultiAgentRLModule.

        The output should be numpy-friendly for easy serialization, not framework
        specific tensors.

        Args:
            module_ids: The ids of the modules to get the weights for. If None, all
                modules will be returned.

        Returns:
            A dictionary that holds the state of the modules in a numpy-friendly
            format.
        """
        module_states = self.module.get_state(module_ids)
        return convert_to_numpy({k: v for k, v in module_states.items()})

    @abc.abstractmethod
    def set_module_state(self, state: Mapping[str, Any]) -> None:
        """Sets the state of the underlying MultiAgentRLModule"""

    @abc.abstractmethod
    def get_param_ref(self, param: Param) -> Hashable:
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
    def get_parameters(self, module: RLModule) -> Sequence[Param]:
        """Returns the list of parameters of a module.

        This should be overriden in framework specific learner. For example in torch it
        will return .parameters(), while in tf it returns .trainable_variables.

        Args:
            module: The module to get the parameters from.

        Returns:
            The parameters of the module.
        """

    @abc.abstractmethod
    def _convert_batch_type(self, batch: MultiAgentBatch) -> MultiAgentBatch:
        """Converts the elements of a MultiAgentBatch to Tensors on the correct device.

        Args:
            batch: The MultiAgentBatch object to convert.

        Returns:
            The resulting MultiAgentBatch with framework-specific tensor values placed
            on the correct device.
        """

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def compile_results(
        self,
        *,
        batch: MultiAgentBatch,
        fwd_out: Mapping[str, Any],
        loss_per_module: Mapping[str, TensorType],
        metrics_per_module: DefaultDict[ModuleID, Dict[str, Any]],
    ) -> Mapping[str, Any]:
        """Compile results from the update in a numpy-friendly format.

        Args:
            batch: The batch that was used for the update.
            fwd_out: The output of the forward train pass.
            loss_per_module: A dict mapping module IDs (including ALL_MODULES) to the
                individual loss tensors as returned by calls to
                `compute_loss_for_module(module_id=...)`.
            metrics_per_module: The collected metrics defaultdict mapping ModuleIDs to
                metrics dicts. These metrics are collected during loss- and
                gradient computation, gradient postprocessing, and gradient application.

        Returns:
            A dictionary of results sub-dicts per module (including ALL_MODULES).
        """
        if not isinstance(batch, MultiAgentBatch):
            raise ValueError(
                f"batch must be a MultiAgentBatch, but got {type(batch)} instead."
            )

        # We compile the metrics to have the structure:
        # top-leve key: module_id -> [key, e.g. self.TOTAL_LOSS_KEY] -> [value].
        # Results will include all registered metrics under the respective module ID
        # top-level key.
        module_learner_stats = defaultdict(dict)
        # Add the num agent|env steps trained counts for all modules.
        module_learner_stats[ALL_MODULES][NUM_AGENT_STEPS_TRAINED] = batch.agent_steps()
        module_learner_stats[ALL_MODULES][NUM_ENV_STEPS_TRAINED] = batch.env_steps()

        loss_per_module_numpy = convert_to_numpy(loss_per_module)

        for module_id in list(batch.policy_batches.keys()) + [ALL_MODULES]:
            # Report total loss per module and other registered metrics.
            module_learner_stats[module_id].update(
                {
                    self.TOTAL_LOSS_KEY: loss_per_module_numpy[module_id],
                    **convert_to_numpy(metrics_per_module[module_id]),
                }
            )
            # Report registered optimizers' learning rates.
            module_learner_stats[module_id].update(
                {
                    f"{optim_name}_lr": convert_to_numpy(
                        self._get_optimizer_lr(optimizer)
                    )
                    for optim_name, optimizer in (
                        self.get_optimizers_for_module(module_id=module_id)
                    )
                }
            )

        return dict(module_learner_stats)

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

        # Allow the user to configure one or more optimizers for this new module.
        self.configure_optimizers_for_module(
            module_id=module_id,
            hps=self.hps.get_hps_for_module(module_id),
        )

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def remove_module(self, module_id: ModuleID) -> None:
        """Remove a module from the Learner.

        Args:
            module_id: The id of the module to remove.
        """
        self._check_is_built()
        module = self.module[module_id]

        if self._is_module_compatible_with_learner(module):
            # Delete the removed module's parameters.
            parameters = self.get_parameters(module)
            for param in parameters:
                param_ref = self.get_param_ref(param)
                if param_ref in self._params:
                    del self._params[param_ref]
            # Delete the removed module's registered optimizers.
            for optimizer_name, optimizer in self.get_optimizers_for_module(module_id):
                del self._optimizer_parameters[optimizer]
                name = module_id + "_" + optimizer_name
                del self._named_optimizers[name]
                if optimizer in self._optimizer_lr_schedules:
                    del self._optimizer_lr_schedules[optimizer]
            del self._module_optimizers[module_id]

        self.module.remove_module(module_id)

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def build(self) -> None:
        """Builds the Learner.

        This method should be called before the learner is used. It is responsible for
        setting up the RLModule, optimizers, and (optionally) their lr-schedulers.
        """
        if self._is_built:
            logger.debug("Learner already built. Skipping build.")
            return
        self._is_built = True

        # Build the module to be trained by this learner.
        self._module = self._make_module()

        # Configure, construct, and register all optimizers needed to train
        # `self.module`.
        self.configure_optimizers()

    @OverrideToImplementCustomLogic
    def compute_loss(
        self,
        *,
        fwd_out: Union[MultiAgentBatch, NestedDict],
        batch: Union[MultiAgentBatch, NestedDict],
    ) -> Union[TensorType, Mapping[str, Any]]:
        """Computes the loss for the module being optimized.

        This method must be overridden by multiagent-specific algorithm learners to
        specify the specific loss computation logic. If the algorithm is single agent
        `compute_loss_for_module()` should be overridden instead.
        `fwd_out` is the output of the `forward_train()` method of the underlying
        MultiAgentRLModule. `batch` is the data that was used to compute `fwd_out`.
        The returned dictionary must contain a key called
        ALL_MODULES, which will be used to compute gradients. It is recommended
        to not compute any forward passes within this method, and to use the
        `forward_train()` outputs of the RLModule(s) to compute the required tensors for
        loss calculations.

        Args:
            fwd_out: Output from a call to the `forward_train()` method of self.module
                during training (`self.update()`).
            batch: The training batch that was used to compute `fwd_out`.

        Returns:
            A dictionary mapping module IDs to individual loss terms. The dictionary
            must contain one protected key ALL_MODULES which will be used for computing
            gradients through.
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
    @abc.abstractmethod
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        hps: LearnerHyperparameters,
        batch: NestedDict,
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

    @OverrideToImplementCustomLogic
    def additional_update(
        self,
        *,
        module_ids_to_update: Sequence[ModuleID] = None,
        timestep: int,
        **kwargs,
    ) -> Mapping[ModuleID, Any]:
        """Apply additional non-gradient based updates to this Algorithm.

        For example, this could be used to do a polyak averaging update
        of a target network in off policy algorithms like SAC or DQN.

        Example:

        .. testcode::

            from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import (
                PPOTorchRLModule
            )
            from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
            from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import (
                PPOTorchLearner
            )
            from ray.rllib.algorithms.ppo.ppo_learner import (
                LEARNER_RESULTS_CURR_KL_COEFF_KEY
            )
            from ray.rllib.algorithms.ppo.ppo_learner import PPOLearnerHyperparameters
            import gymnasium as gym

            env = gym.make("CartPole-v1")
            hps = PPOLearnerHyperparameters(
                use_kl_loss=True,
                kl_coeff=0.2,
                kl_target=0.01,
                use_critic=True,
                clip_param=0.3,
                vf_clip_param=10.0,
                entropy_coeff=0.01,
                entropy_coeff_schedule = [
                    [0, 0.01],
                    [20000000, 0.0],
                ],
                vf_loss_coeff=0.5,
            )

            # Create a single agent RL module spec.
            module_spec = SingleAgentRLModuleSpec(
                module_class=PPOTorchRLModule,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config_dict = {"hidden": [128, 128]},
                catalog_class = PPOCatalog,
            )

            class CustomPPOLearner(PPOTorchLearner):
                def additional_update_for_module(
                    self, *, module_id, hps, timestep, sampled_kl_values
                ):

                    results = super().additional_update_for_module(
                        module_id=module_id,
                        hps=hps,
                        timestep=timestep,
                        sampled_kl_values=sampled_kl_values,
                    )

                    # Try something else than the PPO paper here.
                    sampled_kl = sampled_kl_values[module_id]
                    curr_var = self.curr_kl_coeffs_per_module[module_id]
                    if sampled_kl > 1.2 * self.hps.kl_target:
                        curr_var.data *= 1.2
                    elif sampled_kl < 0.8 * self.hps.kl_target:
                        curr_var.data *= 0.4
                    results.update({LEARNER_RESULTS_CURR_KL_COEFF_KEY: curr_var.item()})


            learner = CustomPPOLearner(
                module_spec=module_spec,
                learner_hyperparameters=hps,
            )

            # Note: the learner should be built before it can be used.
            learner.build()

            # Inside a training loop, we can now call the additional update as we like:
            for i in range(100):
                # sample = ...
                # learner.update(sample)
                if i % 10 == 0:
                    learner.additional_update(
                        timestep=i,
                        sampled_kl_values={"default_policy": 0.5}
                    )

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
        timestep: int,
        **kwargs,
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

        # Only cover the optimizer mapped to this particular module.
        for optimizer_name, optimizer in self.get_optimizers_for_module(module_id):
            # Only update this optimizer's lr, if a scheduler has been registered
            # along with it.
            if optimizer in self._optimizer_lr_schedules:
                new_lr = self._optimizer_lr_schedules[optimizer].update(
                    timestep=timestep
                )
                self._set_optimizer_lr(optimizer, lr=new_lr)
                # Make sure our returned results differentiate by optimizer name
                # (if not the default name).
                stats_name = LEARNER_RESULTS_CURR_LR_KEY
                if optimizer_name != DEFAULT_OPTIMIZER:
                    stats_name += "_" + optimizer_name
                results.update({stats_name: new_lr})

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
    ) -> Union[Mapping[str, Any], List[Mapping[str, Any]]]:
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
            A dictionary of results, in numpy format or a list of such dictionaries in
            case `reduce_fn` is None and we have more than one minibatch pass.
        """
        self._check_is_built()

        missing_module_ids = set(batch.policy_batches.keys()) - set(self.module.keys())
        if len(missing_module_ids) > 0:
            raise ValueError(
                "Batch contains module ids that are not in the learner: "
                f"{missing_module_ids}"
            )

        if num_iters < 1:
            # We must do at least one pass on the batch for training.
            raise ValueError("`num_iters` must be >= 1")

        if minibatch_size:
            batch_iter = MiniBatchCyclicIterator
        elif num_iters > 1:
            # `minibatch_size` was not set but `num_iters` > 1.
            # Under the old training stack, users could do multiple sgd passes
            # over a batch without specifying a minibatch size. We enable
            # this behavior here by setting the minibatch size to be the size
            # of the batch (e.g. 1 minibatch of size batch.count)
            minibatch_size = batch.count
            batch_iter = MiniBatchCyclicIterator
        else:
            # `minibatch_size` and `num_iters` are not set by the user.
            batch_iter = MiniBatchDummyIterator

        results = []
        # Convert input batch into a tensor batch (MultiAgentBatch) on the correct
        # device (e.g. GPU). We move the batch already here to avoid having to move
        # every single minibatch that is created in the `batch_iter` below.
        batch = self._convert_batch_type(batch)
        batch = self._set_slicing_by_batch_id(batch, value=True)

        for tensor_minibatch in batch_iter(batch, minibatch_size, num_iters):
            # Make the actual in-graph/traced `_update` call. This should return
            # all tensor values (no numpy).
            nested_tensor_minibatch = NestedDict(tensor_minibatch.policy_batches)
            (
                fwd_out,
                loss_per_module,
                metrics_per_module,
            ) = self._update(nested_tensor_minibatch)

            result = self.compile_results(
                batch=tensor_minibatch,
                fwd_out=fwd_out,
                loss_per_module=loss_per_module,
                metrics_per_module=defaultdict(dict, **metrics_per_module),
            )
            self._check_result(result)
            # TODO (sven): Figure out whether `compile_metrics` should be forced
            #  to return all numpy/python data, then we can skip this conversion
            #  step here.
            results.append(convert_to_numpy(result))

        batch = self._set_slicing_by_batch_id(batch, value=False)

        # Reduce results across all minibatches, if necessary.

        # If we only have one result anyways, then the user will not expect a list
        # to be reduced here (and might not provide a `reduce_fn` therefore) ->
        # Return single results dict.
        if len(results) == 1:
            return results[0]
        # If no `reduce_fn` provided, return list of results dicts.
        elif reduce_fn is None:
            return results
        # Pass list of results dicts through `reduce_fn` and return a single results
        # dict.
        return reduce_fn(results)

    @OverrideToImplementCustomLogic
    @abc.abstractmethod
    def _update(
        self,
        batch: NestedDict,
        **kwargs,
    ) -> Tuple[Any, Any, Any]:
        """Contains all logic for an in-graph/traceable update step.

        Framework specific subclasses must implement this method. This should include
        calls to the RLModule's `forward_train`, `compute_loss`, compute_gradients`,
        `postprocess_gradients`, and `apply_gradients` methods and return a tuple
        with all the individual results.

        Args:
            batch: The train batch already converted in to a (tensor) NestedDict.
            kwargs: Forward compatibility kwargs.

        Returns:
            A tuple consisting of:
                1) The `forward_train()` output of the RLModule,
                2) the loss_per_module dictionary mapping module IDs to individual loss
                    tensors
                3) a metrics dict mapping module IDs to metrics key/value pairs.

        """

    def set_state(self, state: Mapping[str, Any]) -> None:
        """Set the state of the learner.

        Args:
            state: The state of the optimizer and module. Can be obtained
                from `get_state`. State is a dictionary with two keys:
                "module_state" and "optimizer_state". The value of each key
                is a dictionary that can be passed to `set_module_state` and
                `set_optimizer_state` respectively.

        """
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
        self.set_module_state(module_state)
        self.set_optimizer_state(optimizer_state)

    def get_state(self) -> Mapping[str, Any]:
        """Get the state of the learner.

        Returns:
            The state of the optimizer and module.

        """
        self._check_is_built()
        # TODO: once we figure out the optimizer format, we can set/get the state
        return {
            "module_state": self.get_module_state(),
            "optimizer_state": self.get_optimizer_state(),
        }
        # return {"module_state": self.get_module_state(), "optimizer_state": {}}

    def set_optimizer_state(self, state: Mapping[str, Any]) -> None:
        """Sets the state of all optimizers currently registered in this Learner.

        Args:
            state: The state of the optimizers.
        """
        raise NotImplementedError

    def get_optimizer_state(self) -> Mapping[str, Any]:
        """Returns the state of all optimizers currently registered in this Learner.

        Returns:
            The current state of all optimizers currently registered in this Learner.
        """
        raise NotImplementedError

    def _set_slicing_by_batch_id(
        self, batch: MultiAgentBatch, *, value: bool
    ) -> MultiAgentBatch:
        """Enables slicing by batch id in the given batch.

        If the input batch contains batches of sequences we need to make sure when
        slicing happens it is sliced via batch id and not timestamp. Calling this
        method enables the same flag on each SampleBatch within the input
        MultiAgentBatch.

        Args:
            batch: The MultiAgentBatch to enable slicing by batch id on.
            value: The value to set the flag to.

        Returns:
            The input MultiAgentBatch with the indexing flag is enabled / disabled on.
        """

        for pid, policy_batch in batch.policy_batches.items():
            if self.module[pid].is_stateful():
                # We assume that arriving batches for recurrent modules are already
                # padded to the max sequence length and have tensors of shape
                # [B, T, ...]. Therefore, we slice sequence lengths in B. See
                # SampleBatch for more information.
                if value:
                    policy_batch.enable_slicing_by_batch_id()
                else:
                    policy_batch.disable_slicing_by_batch_id()

        return batch

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

        .. testcode::
            :skipif: True

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

    def _check_registered_optimizer(
        self,
        optimizer: Optimizer,
        params: Sequence[Param],
    ) -> None:
        """Checks that the given optimizer and parameters are valid for the framework.

        Args:
            optimizer: The optimizer object to check.
            params: The list of parameters to check.
        """
        if not isinstance(params, list):
            raise ValueError(
                f"`params` ({params}) must be a list of framework-specific parameters "
                "(variables)!"
            )

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
        self._optimizer_lr_schedules = {}
        self._metrics = defaultdict(dict)
        self._is_built = False

    def apply(self, func, *_args, **_kwargs):
        return func(self, *_args, **_kwargs)

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
    def _get_optimizer_lr(optimizer: Optimizer) -> float:
        """Returns the current learning rate of the given local optimizer.

        Args:
            optimizer: The local optimizer to get the current learning rate for.

        Returns:
            The learning rate value (float) of the given optimizer.
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
            "learner_hyperparameters": self.learner_hyperparameters,
            "framework_hyperparameters": self.framework_hyperparameters,
        }

    def build(self) -> "Learner":
        """Builds the Learner instance."""
        return self.learner_class(**self.get_params_dict())

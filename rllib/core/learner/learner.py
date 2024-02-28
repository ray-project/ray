import abc
from collections import defaultdict
from dataclasses import dataclass
from functools import partial
import json
import logging
import pathlib
from typing import (
    Any,
    Callable,
    DefaultDict,
    Dict,
    List,
    Hashable,
    Optional,
    Sequence,
    Set,
    Tuple,
    TYPE_CHECKING,
    Union,
)

import ray
from ray.rllib.connectors.learner.learner_connector_pipeline import (
    LearnerConnectorPipeline,
)
from ray.rllib.core.learner.reduce_result_dict_fn import _reduce_mean_results
from ray.rllib.core.rl_module.marl_module import (
    MultiAgentRLModule,
    MultiAgentRLModuleSpec,
)
from ray.rllib.core.rl_module.rl_module import RLModule, SingleAgentRLModuleSpec
from ray.rllib.policy.sample_batch import (
    DEFAULT_POLICY_ID,
    MultiAgentBatch,
)
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.debug import update_global_seed_if_necessary
from ray.rllib.utils.deprecation import Deprecated, deprecation_warning
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
    EpisodeType,
    LearningRateOrSchedule,
    ModuleID,
    Optimizer,
    Param,
    ParamRef,
    ParamDict,
    ResultDict,
    TensorType,
)
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig


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


@dataclass
class LearnerHyperparameters:
    def __init_subclass__(cls, **kwargs):
        raise ValueError(
            "All `LearnerHyperparameters` classes have been deprecated! Instead, "
            "the AlgorithmConfig object of your experiment is passed into the `Learner`"
            " constructor as the `config` arg. Make sure that your custom `Learner` "
            "class(es) can read information from this config object (e.g. instead of "
            "using `LearnerHyperparameters.entropy_coeff`, now read the value from "
            "`config.entropy_coeff`). All `Learner` methods that used to accept a "
            "(module specific) `hps` argument, now take a (module specific) `config` "
            "argument instead."
        )


@PublicAPI(stability="alpha")
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
        config: The AlgorithmConfig object from which to derive most of the settings
            needed to build the Learner.
        module_spec: The module specification for the RLModule that is being trained.
            If the module is a single agent module, after building the module it will
            be converted to a multi-agent module with a default key. Can be none if the
            module is provided directly via the `module` argument. Refer to
            ray.rllib.core.rl_module.SingleAgentRLModuleSpec
            or ray.rllib.core.rl_module.MultiAgentRLModuleSpec for more info.
        module: If learner is being used stand-alone, the RLModule can be optionally
            passed in directly instead of the through the `module_spec`.

    Note: We use PPO and torch as an example here because many of the showcased
    components need implementations to come together. However, the same
    pattern is generally applicable.

        .. testcode::

            import gymnasium as gym

            from ray.rllib.algorithms.ppo.ppo import PPOConfig
            from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
            from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import (
                PPOTorchRLModule
            )
            from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec

            env = gym.make("CartPole-v1")

            # Create a PPO config object first.
            config = (
                PPOConfig()
                .framework("torch")
                .training(model={"fcnet_hiddens": [128, 128]})
            )

            # Create a learner instance directly from our config. All we need as
            # extra information here is the env to be able to extract space information
            # (needed to construct the RLModule inside the Learner).
            learner = config.build_learner(env=env)

            # Take one gradient update on the module and report the results.
            # results = learner.update(...)

            # Add a new module, perhaps for league based training.
            learner.add_module(
                module_id="new_player",
                module_spec=SingleAgentRLModuleSpec(
                    module_class=PPOTorchRLModule,
                    observation_space=env.observation_space,
                    action_space=env.action_space,
                    model_config_dict={"fcnet_hiddens": [64, 64]},
                    catalog_class=PPOCatalog,
                )
            )

            # Take another gradient update with both previous and new modules.
            # results = learner.update(...)

            # Remove a module.
            learner.remove_module("new_player")

            # Will train previous modules only.
            # results = learner.update(...)

            # Get the state of the learner.
            state = learner.get_state()

            # Set the state of the learner.
            learner.set_state(state)

            # Get the weights of the underly multi-agent RLModule.
            weights = learner.get_module_state()

            # Set the weights of the underly multi-agent RLModule.
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
        config: "AlgorithmConfig",
        module_spec: Optional[
            Union[SingleAgentRLModuleSpec, MultiAgentRLModuleSpec]
        ] = None,
        module: Optional[RLModule] = None,
        # Deprecated args.
        learner_group_scaling_config=None,
        learner_hyperparameters=None,
        framework_hyperparameters=None,
    ):
        if learner_group_scaling_config is not None:
            deprecation_warning(
                old="Learner(.., learner_group_scaling_config=..)",
                help="Deprecated argument. Use `config` (AlgorithmConfig) instead.",
                error=True,
            )
        if learner_hyperparameters is not None:
            deprecation_warning(
                old="Learner(.., learner_hyperparameters=..)",
                help="Deprecated argument. Use `config` (AlgorithmConfig) instead.",
                error=True,
            )
        if framework_hyperparameters is not None:
            deprecation_warning(
                old="Learner(.., framework_hyperparameters=..)",
                help="Deprecated argument. Use `config` (AlgorithmConfig) instead.",
                error=True,
            )
        # TODO (sven): Figure out how to do this
        self.config = config.copy(copy_frozen=False)
        self._module_spec = module_spec
        self._module_obj = module
        self._device = None

        # Set a seed, if necessary.
        if self.config.seed is not None:
            update_global_seed_if_necessary(self.framework, self.config.seed)

        self._distributed = self.config.num_learner_workers > 1
        self._use_gpu = self.config.num_gpus_per_learner_worker > 0
        # If we are using gpu but we are not distributed, use this gpu for training.
        self._local_gpu_idx = self.config.local_gpu_idx

        # whether self.build has already been called
        self._is_built = False

        # These are the attributes that are set during build.

        # The actual MARLModule used by this Learner.
        self._module: Optional[MultiAgentRLModule] = None
        # Our Learner connector pipeline.
        self._learner_connector: Optional[LearnerConnectorPipeline] = None
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

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def build(self) -> None:
        """Builds the Learner.

        This method should be called before the learner is used. It is responsible for
        setting up the LearnerConnectorPipeline, the RLModule, optimizer(s), and
        (optionally) the optimizers' learning rate schedulers.
        """
        if self._is_built:
            logger.debug("Learner already built. Skipping build.")
            return

        # Build learner connector pipeline used on this Learner worker.
        if self.config.uses_new_env_runners:
            # TODO (sven): Figure out which space to provide here. For now,
            #  it doesn't matter, as the default connector piece doesn't use
            #  this information anyway.
            #  module_spec = self._module_spec.as_multi_agent()
            self._learner_connector = self.config.build_learner_connector(
                input_observation_space=None,
                input_action_space=None,
            )

        # Build the module to be trained by this learner.
        self._module = self._make_module()

        # Configure, construct, and register all optimizers needed to train
        # `self.module`.
        self.configure_optimizers()

        self._is_built = True

    @property
    def distributed(self) -> bool:
        """Whether the learner is running in distributed mode."""
        return self._distributed

    @property
    def module(self) -> MultiAgentRLModule:
        """The multi-agent RLModule that is being trained."""
        return self._module

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
                config = self.config.get_config_for_module(module_id)
                self.configure_optimizers_for_module(module_id=module_id, config=config)

    @OverrideToImplementCustomLogic
    @abc.abstractmethod
    def configure_optimizers_for_module(
        self, module_id: ModuleID, config: "AlgorithmConfig" = None, hps=None
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
            config: The AlgorithmConfig specific to the given `module_id`.
        """

    @OverrideToImplementCustomLogic
    @abc.abstractmethod
    def compute_gradients(
        self, loss_per_module: Dict[str, TensorType], **kwargs
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
                config=self.config.get_config_for_module(module_id),
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
        config: Optional["AlgorithmConfig"] = None,
        module_gradients_dict: ParamDict,
        hps=None,
    ) -> ParamDict:
        """Applies postprocessing operations on the gradients of the given module.

        Args:
            module_id: The module ID for which we will postprocess computed gradients.
                Note that `module_gradients_dict` already only carries those gradient
                tensors that belong to this `module_id`. Other `module_id`'s gradients
                are not available in this call.
            config: The AlgorithmConfig specific to the given `module_id`.
            module_gradients_dict: A dictionary of gradients in the same (flat) format
                as self._params, mapping gradient refs to gradient tensors, which are to
                be postprocessed. You may alter these tensors in place or create new
                ones and return these in a new dict.

        Returns:
            A dictionary with the updated gradients and the exact same (flat) structure
            as the incoming `module_gradients_dict` arg.
        """
        if hps is not None:
            deprecation_warning(
                old="Learner.postprocess_gradients_for_module(.., hps=..)",
                help="Deprecated argument. Use `config` (AlgorithmConfig) instead.",
                error=True,
            )

        postprocessed_grads = {}

        if config.grad_clip is None:
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
                grad_clip=config.grad_clip,
                grad_clip_by=config.grad_clip_by,
            )
            if config.grad_clip_by == "global_norm":
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

    def get_module_state(self, module_ids: Optional[Set[str]] = None) -> Dict[str, Any]:
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
    def set_module_state(self, state: Dict[str, Any]) -> None:
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
        fwd_out: Dict[str, Any],
        loss_per_module: Dict[str, TensorType],
        metrics_per_module: DefaultDict[ModuleID, Dict[str, Any]],
    ) -> Dict[str, Any]:
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
            config=self.config.get_config_for_module(module_id),
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

    @OverrideToImplementCustomLogic
    def should_module_be_updated(self, module_id, multi_agent_batch=None):
        """Returns whether a module should be updated or not based on `self.config`.

        Args:
            module_id: The ModuleID that we want to query on whether this module
                should be updated or not.
            multi_agent_batch: An optional MultiAgentBatch to possibly provide further
                information on the decision on whether the RLModule should be updated
                or not.
        """
        should_module_be_updated_fn = self.config.policies_to_train
        # If None, return True (by default, all modules should be updated).
        if should_module_be_updated_fn is None:
            return True
        # If container given, return whether `module_id` is in that container.
        elif not callable(should_module_be_updated_fn):
            return module_id in set(should_module_be_updated_fn)

        return should_module_be_updated_fn(module_id, multi_agent_batch)

    @OverrideToImplementCustomLogic
    def compute_loss(
        self,
        *,
        fwd_out: Union[MultiAgentBatch, NestedDict],
        batch: Union[MultiAgentBatch, NestedDict],
    ) -> Union[TensorType, Dict[str, Any]]:
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
                config=self.config.get_config_for_module(module_id),
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
        config: Optional["AlgorithmConfig"] = None,
        batch: NestedDict,
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:
        """Computes the loss for a single module.

        Think of this as computing loss for a single agent. For multi-agent use-cases
        that require more complicated computation for loss, consider overriding the
        `compute_loss` method instead.

        Args:
            module_id: The id of the module.
            config: The AlgorithmConfig specific to the given `module_id`.
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
        module_ids_to_update: Optional[Sequence[ModuleID]] = None,
        timestep: int,
        **kwargs,
    ) -> Dict[ModuleID, Any]:
        """Apply additional non-gradient based updates to this Algorithm.

        For example, this could be used to do a polyak averaging update
        of a target network in off policy algorithms like SAC or DQN.

        Example:

        .. testcode::

            import gymnasium as gym

            from ray.rllib.algorithms.ppo.ppo import (
                LEARNER_RESULTS_CURR_KL_COEFF_KEY,
                PPOConfig,
            )
            from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
            from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import (
                PPOTorchLearner
            )
            from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import (
                PPOTorchRLModule
            )
            from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec

            env = gym.make("CartPole-v1")
            config = (
                PPOConfig()
                .training(
                    kl_coeff=0.2,
                    kl_target=0.01,
                    clip_param=0.3,
                    vf_clip_param=10.0,
                    # Taper down entropy coeff. from 0.01 to 0.0 over 20M ts.
                    entropy_coeff=[
                        [0, 0.01],
                        [20000000, 0.0],
                    ],
                    vf_loss_coeff=0.5,
                )
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
                    self, *, module_id, config, timestep, sampled_kl_values
                ):

                    results = super().additional_update_for_module(
                        module_id=module_id,
                        config=config,
                        timestep=timestep,
                        sampled_kl_values=sampled_kl_values,
                    )

                    # Try something else than the PPO paper here.
                    sampled_kl = sampled_kl_values[module_id]
                    curr_var = self.curr_kl_coeffs_per_module[module_id]
                    if sampled_kl > 1.2 * self.config.kl_target:
                        curr_var.data *= 1.2
                    elif sampled_kl < 0.8 * self.config.kl_target:
                        curr_var.data *= 0.4
                    results.update({LEARNER_RESULTS_CURR_KL_COEFF_KEY: curr_var.item()})

            # Construct the Learner object.
            learner = CustomPPOLearner(
                config=config,
                module_spec=module_spec,
            )
            # Note: Learners need to be built before they can be used.
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
        module_ids = (
            module_ids_to_update
            if module_ids_to_update is not None
            else self.module.keys()
        )
        for module_id in module_ids:
            module_results = self.additional_update_for_module(
                module_id=module_id,
                config=self.config.get_config_for_module(module_id),
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
        config: Optional["AlgorithmConfig"] = None,
        timestep: int,
        hps=None,
        **kwargs,
    ) -> Dict[str, Any]:
        """Apply additional non-gradient based updates for a single module.

        See `additional_update` for more details.

        Args:
            module_id: The id of the module to update.
            config: The AlgorithmConfig specific to the given `module_id`.
            timestep: The current global timestep (to be used with schedulers).
            **kwargs: Keyword arguments to use for the additional update.

        Returns:
            A dictionary of results from the update
        """
        if hps is not None:
            deprecation_warning(
                old="Learner.additional_update_for_module(.., hps=..)",
                help="Deprecated argument. Use `config` (AlgorithmConfig) instead.",
                error=True,
            )

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

    def update_from_batch(
        self,
        batch: MultiAgentBatch,
        *,
        reduce_fn: Callable[[List[Dict[str, Any]]], ResultDict] = (
            _reduce_mean_results
        ),
        # TODO (sven): Deprecate these in favor of config attributes for only those
        #  algos that actually need (and know how) to do minibatching.
        minibatch_size: Optional[int] = None,
        num_iters: int = 1,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Do `num_iters` minibatch updates given a train batch.

        You can use this method to take more than one backward pass on the batch.
        The same `minibatch_size` and `num_iters` will be used for all module ids in
        MultiAgentRLModule.

        Args:
            batch: A batch of training data to update from.
            reduce_fn: reduce_fn: A function to reduce the results from a list of
                minibatch updates. This can be any arbitrary function that takes a
                list of dictionaries and returns a single dictionary. For example you
                can either take an average (default) or concatenate the results (for
                example for metrics) or be more selective about you want to report back
                to the algorithm's training_step. If None is passed, the results will
                not get reduced.
            minibatch_size: The size of the minibatch to use for each update.
            num_iters: The number of complete passes over all the sub-batches
                in the input multi-agent batch.

        Returns:
            A dictionary of results, in numpy format or a list of such dictionaries in
            case `reduce_fn` is None and we have more than one minibatch pass.
        """
        return self._update_from_batch_or_episodes(
            batch=batch,
            episodes=None,
            reduce_fn=reduce_fn,
            minibatch_size=minibatch_size,
            num_iters=num_iters,
        )

    def update_from_episodes(
        self,
        episodes: List[EpisodeType],
        *,
        reduce_fn: Callable[[List[Dict[str, Any]]], ResultDict] = (
            _reduce_mean_results
        ),
        # TODO (sven): Deprecate these in favor of config attributes for only those
        #  algos that actually need (and know how) to do minibatching.
        minibatch_size: Optional[int] = None,
        num_iters: int = 1,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Do `num_iters` minibatch updates given a list of episodes.

        You can use this method to take more than one backward pass on the batch.
        The same `minibatch_size` and `num_iters` will be used for all module ids in
        MultiAgentRLModule.

        Args:
            episodes: An list of episode objects to update from.
            reduce_fn: reduce_fn: A function to reduce the results from a list of
                minibatch updates. This can be any arbitrary function that takes a
                list of dictionaries and returns a single dictionary. For example you
                can either take an average (default) or concatenate the results (for
                example for metrics) or be more selective about you want to report back
                to the algorithm's training_step. If None is passed, the results will
                not get reduced.
            minibatch_size: The size of the minibatch to use for each update.
            num_iters: The number of complete passes over all the sub-batches
                in the input multi-agent batch.

        Returns:
            A dictionary of results, in numpy format or a list of such dictionaries in
            case `reduce_fn` is None and we have more than one minibatch pass.
        """
        return self._update_from_batch_or_episodes(
            batch=None,
            episodes=episodes,
            reduce_fn=reduce_fn,
            minibatch_size=minibatch_size,
            num_iters=num_iters,
        )

    @OverrideToImplementCustomLogic
    def _preprocess_train_data(
        self,
        *,
        batch: Optional[MultiAgentBatch] = None,
        episodes: Optional[List[EpisodeType]] = None,
    ) -> Tuple[Optional[MultiAgentBatch], Optional[List[EpisodeType]]]:
        """Allows custom preprocessing of batch/episode data before the actual update.

        The higher level order, in which this method is called from within
        `Learner.update(batch, episodes)` is:
        * batch, episodes = self._preprocess_train_data(batch, episodes)
        * batch = self._learner_connector(batch, episodes)
        * results = self._update(batch)

        The default implementation does not do any processing and is a mere pass
        through. However, specific algorithms should override this method to implement
        their specific training data preprocessing needs. It is possible to perform
        preliminary RLModule forward passes (besides the main "forward_train()" call
        during `self._update`) in this method and custom algorithms might also want to
        use this Learner's `self._learner_connector` to prepare the data
        (batch/episodes) for such extra forward calls.

        Args:
            batch: An optional batch of training data to preprocess.
            episodes: An optional list of episodes objects to preprocess.

        Returns:
            A tuple consisting of the processed `batch` and the processed list of
            `episodes`.
        """
        return batch, episodes

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

    def set_state(self, state: Dict[str, Any]) -> None:
        """Set the state of the learner.

        Args:
            state: The state of the optimizer and module. Can be obtained
                from `get_state`. State is a dictionary with two keys:
                "module_state" and "optimizer_state". The value of each key
                is a dictionary that can be passed to `set_module_state` and
                `set_optimizer_state` respectively.

        """
        self._check_is_built()

        module_state = state.get("module_state")
        # TODO: once we figure out the optimizer format, we can set/get the state
        if module_state is None:
            raise ValueError(
                "state must have a key 'module_state' for the module weights"
            )
        self.set_module_state(module_state)

        optimizer_state = state.get("optimizer_state")
        if optimizer_state is None:
            raise ValueError(
                "state must have a key 'optimizer_state' for the optimizer weights"
            )
        self.set_optimizer_state(optimizer_state)

        # Update our trainable Modules information/function via our config.
        # If not provided in state (None), all Modules will be trained by default.
        self.config.multi_agent(policies_to_train=state.get("modules_to_train"))

    def get_state(self) -> Dict[str, Any]:
        """Get the state of the learner.

        Returns:
            The state of the optimizer and module.

        """
        self._check_is_built()
        return {
            "module_state": self.get_module_state(),
            "optimizer_state": self.get_optimizer_state(),
            "modules_to_train": self.config.policies_to_train,
        }

    def set_optimizer_state(self, state: Dict[str, Any]) -> None:
        """Sets the state of all optimizers currently registered in this Learner.

        Args:
            state: The state of the optimizers.
        """
        raise NotImplementedError

    def get_optimizer_state(self) -> Dict[str, Any]:
        """Returns the state of all optimizers currently registered in this Learner.

        Returns:
            The current state of all optimizers currently registered in this Learner.
        """
        raise NotImplementedError

    def _update_from_batch_or_episodes(
        self,
        *,
        # TODO (sven): We should allow passing in a single agent batch here
        #  as well for simplicity.
        batch: Optional[MultiAgentBatch] = None,
        episodes: Optional[List[EpisodeType]] = None,
        reduce_fn: Callable[[List[Dict[str, Any]]], ResultDict] = (
            _reduce_mean_results
        ),
        # TODO (sven): Deprecate these in favor of config attributes for only those
        #  algos that actually need (and know how) to do minibatching.
        minibatch_size: Optional[int] = None,
        num_iters: int = 1,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        self._check_is_built()

        # If a (multi-agent) batch is provided, check, whether our RLModule
        # contains all ModuleIDs found in this batch. If not, throw an error.
        if batch is not None:
            unknown_module_ids = set(batch.policy_batches.keys()) - set(
                self.module.keys()
            )
            if len(unknown_module_ids) > 0:
                raise ValueError(
                    "Batch contains module ids that are not in the learner: "
                    f"{unknown_module_ids}"
                )

        if num_iters < 1:
            # We must do at least one pass on the batch for training.
            raise ValueError("`num_iters` must be >= 1")

        # Call the learner connector.
        if self._learner_connector is not None:
            # Call the train data preprocessor.
            batch, episodes = self._preprocess_train_data(
                batch=batch, episodes=episodes
            )
            batch = self._learner_connector(
                rl_module=self.module,
                data=batch,
                episodes=episodes,
            )

        # Filter out those RLModules from the final train batch that should not be
        # updated.
        for module_id in list(batch.policy_batches.keys()):
            if not self.should_module_be_updated(module_id, batch):
                del batch.policy_batches[module_id]

        if minibatch_size and self._learner_connector is not None:
            batch_iter = partial(MiniBatchCyclicIterator, uses_new_env_runners=True)
        elif minibatch_size:
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
            # TODO (sven): Figure out whether `compile_results` should be forced
            #  to return all numpy/python data, then we can skip this conversion
            #  step here.
            results.append(result)

        self._set_slicing_by_batch_id(batch, value=False)

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
            # We assume that arriving batches for recurrent modules OR batches that
            # have a SEQ_LENS column are already zero-padded to the max sequence length
            # and have tensors of shape [B, T, ...]. Therefore, we slice sequence
            # lengths in B. See SampleBatch for more information.
            if (
                self.module[pid].is_stateful()
                or policy_batch.get("seq_lens") is not None
            ):
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
        # TODO (avnishn): from checkpoint doesn't currently support modules_to_load,
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
        # Module was provided directly through constructor -> Use as-is.
        if self._module_obj is not None:
            module = self._module_obj
        # RLModuleSpec was provided directly through constructor -> Use it to build the
        # RLModule.
        elif self._module_spec is not None:
            module = self._module_spec.build()
        # Try using our config object. Note that this would only work if the config
        # object has all the necessary space information already in it.
        else:
            module = self.config.get_multi_agent_module_spec().build()

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

    def _check_result(self, result: Dict[str, Any]) -> None:
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

    @Deprecated(
        help="Use `config` (AlgorithmConfig) everywhere where you would have used "
        "Learner.hps instead.",
        error=True,
    )
    @property
    def hps(self):
        pass

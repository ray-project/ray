import abc
from collections import defaultdict
import copy
import logging
import numpy
import platform
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    List,
    Hashable,
    Optional,
    Sequence,
    Tuple,
    TYPE_CHECKING,
    Union,
)

import tree  # pip install dm_tree

import ray
from ray.data.iterator import DataIterator
from ray.rllib.connectors.learner.learner_connector_pipeline import (
    LearnerConnectorPipeline,
)
from ray.rllib.core import (
    COMPONENT_METRICS_LOGGER,
    COMPONENT_OPTIMIZER,
    COMPONENT_RL_MODULE,
    DEFAULT_MODULE_ID,
)
from ray.rllib.core.rl_module.apis import SelfSupervisedLossAPI
from ray.rllib.core.rl_module import validate_module_id
from ray.rllib.core.rl_module.multi_rl_module import (
    MultiRLModule,
    MultiRLModuleSpec,
)
from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleSpec
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.checkpoints import Checkpointable
from ray.rllib.utils.debug import update_global_seed_if_necessary
from ray.rllib.utils.deprecation import (
    Deprecated,
    DEPRECATED_VALUE,
    deprecation_warning,
)
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    DATASET_NUM_ITERS_TRAINED,
    DATASET_NUM_ITERS_TRAINED_LIFETIME,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_ENV_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED_LIFETIME,
    NUM_MODULE_STEPS_TRAINED,
    NUM_MODULE_STEPS_TRAINED_LIFETIME,
    MODULE_TRAIN_BATCH_SIZE_MEAN,
    WEIGHTS_SEQ_NO,
)
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.minibatch_utils import (
    MiniBatchDummyIterator,
    MiniBatchCyclicIterator,
)
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import (
    EpisodeType,
    LearningRateOrSchedule,
    ModuleID,
    Optimizer,
    Param,
    ParamRef,
    ParamDict,
    ResultDict,
    ShouldModuleBeUpdatedFn,
    StateDict,
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
LR_KEY = "learning_rate"


@PublicAPI(stability="alpha")
class Learner(Checkpointable):
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
            ray.rllib.core.rl_module.RLModuleSpec
            or ray.rllib.core.rl_module.MultiRLModuleSpec for more info.
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
            from ray.rllib.core import COMPONENT_RL_MODULE, DEFAULT_MODULE_ID
            from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
            from ray.rllib.core.rl_module.rl_module import RLModuleSpec

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
                module_spec=RLModuleSpec(
                    module_class=PPOTorchRLModule,
                    observation_space=env.observation_space,
                    action_space=env.action_space,
                    model_config=DefaultModelConfig(fcnet_hiddens=[64, 64]),
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

            # Get the weights of the underlying MultiRLModule.
            weights = learner.get_state(components=COMPONENT_RL_MODULE)

            # Set the weights of the underlying MultiRLModule.
            learner.set_state({COMPONENT_RL_MODULE: weights})


    Extension pattern:

        .. testcode::

            from ray.rllib.core.learner.torch.torch_learner import TorchLearner

            class MyLearner(TorchLearner):

               def compute_losses(self, fwd_out, batch):
                   # Compute the losses per module based on `batch` and output of the
                   # forward pass (`fwd_out`). To access the (algorithm) config for a
                   # specific RLModule, do:
                   # `self.config.get_config_for_module([moduleID])`.
                   return {DEFAULT_MODULE_ID: module_loss}
    """

    framework: str = None
    TOTAL_LOSS_KEY: str = "total_loss"

    def __init__(
        self,
        *,
        config: "AlgorithmConfig",
        module_spec: Optional[Union[RLModuleSpec, MultiRLModuleSpec]] = None,
        module: Optional[RLModule] = None,
    ):
        # TODO (sven): Figure out how to do this
        self.config = config.copy(copy_frozen=False)
        self._module_spec: Optional[MultiRLModuleSpec] = module_spec
        self._module_obj: Optional[MultiRLModule] = module

        # Make node and device of this Learner available.
        self._node = platform.node()
        self._device = None

        # Set a seed, if necessary.
        if self.config.seed is not None:
            update_global_seed_if_necessary(self.framework, self.config.seed)

        # Whether self.build has already been called.
        self._is_built = False

        # These are the attributes that are set during build.

        # The actual MultiRLModule used by this Learner.
        self._module: Optional[MultiRLModule] = None
        self._weights_seq_no = 0
        # Our Learner connector pipeline.
        self._learner_connector: Optional[LearnerConnectorPipeline] = None
        # These are set for properly applying optimizers and adding or removing modules.
        self._optimizer_parameters: Dict[Optimizer, List[ParamRef]] = {}
        self._named_optimizers: Dict[str, Optimizer] = {}
        self._params: ParamDict = {}
        # Dict mapping ModuleID to a list of optimizer names. Note that the optimizer
        # name includes the ModuleID as a prefix: optimizer_name=`[ModuleID]_[.. rest]`.
        self._module_optimizers: Dict[ModuleID, List[str]] = defaultdict(list)
        self._optimizer_name_to_module: Dict[str, ModuleID] = {}

        # Only manage optimizer's learning rate if user has NOT overridden
        # the `configure_optimizers_for_module` method. Otherwise, leave responsibility
        # to handle lr-updates entirely in user's hands.
        self._optimizer_lr_schedules: Dict[Optimizer, Scheduler] = {}

        # The Learner's own MetricsLogger to be used to log RLlib's built-in metrics or
        # custom user-defined ones (e.g. custom loss values). When returning from an
        # `update_from_...()` method call, the Learner will do a `self.metrics.reduce()`
        # and return the resulting (reduced) dict.
        self.metrics = MetricsLogger()

        # In case of offline learning and multiple learners, each learner receives a
        # repeatable iterator that iterates over a split of the streamed data.
        self.iterator: DataIterator = None

    # TODO (sven): Do we really need this API? It seems like LearnerGroup constructs
    #  all Learner workers and then immediately builds them any ways? Unless there is
    #  a reason related to Train worker group setup.
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
        self._learner_connector = None
        # If the Algorithm uses aggregation actors to run episodes through the learner
        # connector, its Learners don't need a connector pipelines and instead learn
        # directly from pre-loaded batches already on the GPU.
        if self.config.num_aggregator_actors_per_learner == 0:
            # TODO (sven): Figure out which space to provide here. For now,
            #  it doesn't matter, as the default connector piece doesn't use
            #  this information anyway.
            #  module_spec = self._module_spec.as_multi_rl_module_spec()
            self._learner_connector = self.config.build_learner_connector(
                input_observation_space=None,
                input_action_space=None,
                device=self._device,
            )

        # Build the module to be trained by this learner.
        self._module = self._make_module()

        # Configure, construct, and register all optimizers needed to train
        # `self.module`.
        self.configure_optimizers()

        # Log the number of trainable/non-trainable parameters.
        self._log_trainable_parameters()

        self._is_built = True

    @property
    def distributed(self) -> bool:
        """Whether the learner is running in distributed mode."""
        return self.config.num_learners > 1

    @property
    def module(self) -> MultiRLModule:
        """The MultiRLModule that is being trained."""
        return self._module

    @property
    def node(self) -> Any:
        return self._node

    @property
    def device(self) -> Any:
        return self._device

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
        self._optimizer_name_to_module[full_registration_name] = module_id

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
            if self.rl_module_is_compatible(self.module[module_id]):
                config = self.config.get_config_for_module(module_id)
                self.configure_optimizers_for_module(module_id=module_id, config=config)

    @OverrideToImplementCustomLogic
    @abc.abstractmethod
    def configure_optimizers_for_module(
        self, module_id: ModuleID, config: "AlgorithmConfig" = None
    ) -> None:
        """Configures an optimizer for the given module_id.

        This method is called for each RLModule in the MultiRLModule being
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
        self, loss_per_module: Dict[ModuleID, TensorType], **kwargs
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
        on each of the sub-modules in our MultiRLModule: `self.module` and
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
        postprocessed_grads = {}

        if config.grad_clip is None and not config.log_gradients:
            postprocessed_grads.update(module_gradients_dict)
            return postprocessed_grads

        for optimizer_name, optimizer in self.get_optimizers_for_module(module_id):
            grad_dict_to_clip = self.filter_param_dict_for_optimizer(
                param_dict=module_gradients_dict,
                optimizer=optimizer,
            )
            if config.grad_clip:
                # Perform gradient clipping, if configured.
                global_norm = self._get_clip_function()(
                    grad_dict_to_clip,
                    grad_clip=config.grad_clip,
                    grad_clip_by=config.grad_clip_by,
                )
                if config.grad_clip_by == "global_norm" or config.log_gradients:
                    # If we want to log gradients, but do not use the global norm
                    # for clipping compute it here.
                    if config.log_gradients and config.grad_clip_by != "global_norm":
                        # Compute the global norm of gradients.
                        global_norm = self._get_global_norm_function()(
                            # Note, `tf.linalg.global_norm` needs a list of tensors.
                            list(grad_dict_to_clip.values()),
                        )
                    self.metrics.log_value(
                        key=(module_id, f"gradients_{optimizer_name}_global_norm"),
                        value=global_norm,
                        window=1,
                    )
                postprocessed_grads.update(grad_dict_to_clip)
            # In the other case check, if we want to log gradients only.
            elif config.log_gradients:
                # Compute the global norm of gradients and log it.
                global_norm = self._get_global_norm_function()(
                    # Note, `tf.linalg.global_norm` needs a list of tensors.
                    list(grad_dict_to_clip.values()),
                )
                self.metrics.log_value(
                    key=(module_id, f"gradients_{optimizer_name}_global_norm"),
                    value=global_norm,
                    window=1,
                )

        return postprocessed_grads

    @OverrideToImplementCustomLogic
    @abc.abstractmethod
    def apply_gradients(self, gradients_dict: ParamDict) -> None:
        """Applies the gradients to the MultiRLModule parameters.

        Args:
            gradients_dict: A dictionary of gradients in the same (flat) format as
                self._params. Note that top-level structures, such as module IDs,
                will not be present anymore in this dict. It will merely map gradient
                tensor references to gradient tensors.
        """

    def get_optimizer(
        self,
        module_id: ModuleID = DEFAULT_MODULE_ID,
        optimizer_name: str = DEFAULT_OPTIMIZER,
    ) -> Optimizer:
        """Returns the optimizer object, configured under the given module_id and name.

        If only one optimizer was registered under `module_id` (or ALL_MODULES)
        via the `self.register_optimizer` method, `optimizer_name` is assumed to be
        DEFAULT_OPTIMIZER.

        Args:
            module_id: The ModuleID for which to return the configured optimizer.
                If not provided, will assume DEFAULT_MODULE_ID.
            optimizer_name: The name of the optimizer (registered under `module_id` via
                `self.register_optimizer()`) to return. If not provided, will assume
                DEFAULT_OPTIMIZER.

        Returns:
            The optimizer object, configured under the given `module_id` and
            `optimizer_name`.
        """
        # `optimizer_name` could possibly be the full optimizer name (including the
        # module_id under which it is registered).
        if optimizer_name in self._named_optimizers:
            return self._named_optimizers[optimizer_name]

        # Normally, `optimizer_name` is just the optimizer's name, not including the
        # `module_id`.
        full_registration_name = module_id + "_" + optimizer_name
        if full_registration_name in self._named_optimizers:
            return self._named_optimizers[full_registration_name]

        # No optimizer found.
        raise KeyError(
            f"Optimizer not found! module_id={module_id} "
            f"optimizer_name={optimizer_name}"
        )

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

    @abc.abstractmethod
    def get_param_ref(self, param: Param) -> Hashable:
        """Returns a hashable reference to a trainable parameter.

        This should be overridden in framework specific specialization. For example in
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

        This should be overridden in framework specific learner. For example in torch it
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
    def add_module(
        self,
        *,
        module_id: ModuleID,
        module_spec: RLModuleSpec,
        config_overrides: Optional[Dict] = None,
        new_should_module_be_updated: Optional[ShouldModuleBeUpdatedFn] = None,
    ) -> MultiRLModuleSpec:
        """Adds a module to the underlying MultiRLModule.

        Changes this Learner's config in order to make this architectural change
        permanent wrt. to checkpointing.

        Args:
            module_id: The ModuleID of the module to be added.
            module_spec: The ModuleSpec of the module to be added.
            config_overrides: The `AlgorithmConfig` overrides that should apply to
                the new Module, if any.
            new_should_module_be_updated: An optional sequence of ModuleIDs or a
                callable taking ModuleID and SampleBatchType and returning whether the
                ModuleID should be updated (trained).
                If None, will keep the existing setup in place. RLModules,
                whose IDs are not in the list (or for which the callable
                returns False) will not be updated.

        Returns:
            The new MultiRLModuleSpec (after the RLModule has been added).
        """
        validate_module_id(module_id, error=True)
        self._check_is_built()

        # Force-set inference-only = False.
        module_spec = copy.deepcopy(module_spec)
        module_spec.inference_only = False

        # Build the new RLModule and add it to self.module.
        module = module_spec.build()
        self.module.add_module(module_id, module)

        # Change our config (AlgorithmConfig) to contain the new Module.
        # TODO (sven): This is a hack to manipulate the AlgorithmConfig directly,
        #  but we'll deprecate config.policies soon anyway.
        self.config.policies[module_id] = PolicySpec()
        if config_overrides is not None:
            self.config.multi_agent(
                algorithm_config_overrides_per_module={module_id: config_overrides}
            )
        self.config.rl_module(rl_module_spec=MultiRLModuleSpec.from_module(self.module))
        self._module_spec = self.config.rl_module_spec
        if new_should_module_be_updated is not None:
            self.config.multi_agent(policies_to_train=new_should_module_be_updated)

        # Allow the user to configure one or more optimizers for this new module.
        self.configure_optimizers_for_module(
            module_id=module_id,
            config=self.config.get_config_for_module(module_id),
        )
        return self.config.rl_module_spec

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def remove_module(
        self,
        module_id: ModuleID,
        *,
        new_should_module_be_updated: Optional[ShouldModuleBeUpdatedFn] = None,
    ) -> MultiRLModuleSpec:
        """Removes a module from the Learner.

        Args:
            module_id: The ModuleID of the module to be removed.
            new_should_module_be_updated: An optional sequence of ModuleIDs or a
                callable taking ModuleID and SampleBatchType and returning whether the
                ModuleID should be updated (trained).
                If None, will keep the existing setup in place. RLModules,
                whose IDs are not in the list (or for which the callable
                returns False) will not be updated.

        Returns:
            The new MultiRLModuleSpec (after the RLModule has been removed).
        """
        self._check_is_built()
        module = self.module[module_id]

        # Delete the removed module's parameters and optimizers.
        if self.rl_module_is_compatible(module):
            parameters = self.get_parameters(module)
            for param in parameters:
                param_ref = self.get_param_ref(param)
                if param_ref in self._params:
                    del self._params[param_ref]
            for optimizer_name, optimizer in self.get_optimizers_for_module(module_id):
                del self._optimizer_parameters[optimizer]
                name = module_id + "_" + optimizer_name
                del self._named_optimizers[name]
                if optimizer in self._optimizer_lr_schedules:
                    del self._optimizer_lr_schedules[optimizer]
            del self._module_optimizers[module_id]

        # Remove the module from the MultiRLModule.
        self.module.remove_module(module_id)

        # Change self.config to reflect the new architecture.
        # TODO (sven): This is a hack to manipulate the AlgorithmConfig directly,
        #  but we'll deprecate config.policies soon anyway.
        del self.config.policies[module_id]
        self.config.algorithm_config_overrides_per_module.pop(module_id, None)
        if new_should_module_be_updated is not None:
            self.config.multi_agent(policies_to_train=new_should_module_be_updated)
        self.config.rl_module(rl_module_spec=MultiRLModuleSpec.from_module(self.module))

        # Remove all stats from the module from our metrics logger, so we don't report
        # results from this module again.
        if module_id in self.metrics.stats:
            del self.metrics.stats[module_id]

        return self.config.rl_module_spec

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
        # If collection given, return whether `module_id` is in that container.
        elif not callable(should_module_be_updated_fn):
            return module_id in set(should_module_be_updated_fn)

        return should_module_be_updated_fn(module_id, multi_agent_batch)

    @OverrideToImplementCustomLogic
    def compute_losses(
        self, *, fwd_out: Dict[str, Any], batch: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Computes the loss(es) for the module being optimized.

        This method must be overridden by MultiRLModule-specific Learners in order to
        define the specific loss computation logic. If the algorithm is single-agent,
        only `compute_loss_for_module()` should be overridden instead. If the algorithm
        uses independent multi-agent learning (default behavior for RLlib's multi-agent
        setups), also only `compute_loss_for_module()` should be overridden, but it will
        be called for each individual RLModule inside the MultiRLModule.
        It is recommended to not compute any forward passes within this method, and to
        use the `forward_train()` outputs of the RLModule(s) to compute the required
        loss tensors.
        See here for a custom loss function example script:
        https://github.com/ray-project/ray/blob/master/rllib/examples/learners/ppo_with_custom_loss_fn.py  # noqa

        Args:
            fwd_out: Output from a call to the `forward_train()` method of the
                underlying MultiRLModule (`self.module`) during training
                (`self.update()`).
            batch: The train batch that was used to compute `fwd_out`.

        Returns:
            A dictionary mapping module IDs to individual loss terms.
        """
        loss_per_module = {}
        for module_id in fwd_out:
            module_batch = batch[module_id]
            module_fwd_out = fwd_out[module_id]

            module = self.module[module_id].unwrapped()
            if isinstance(module, SelfSupervisedLossAPI):
                loss = module.compute_self_supervised_loss(
                    learner=self,
                    module_id=module_id,
                    config=self.config.get_config_for_module(module_id),
                    batch=module_batch,
                    fwd_out=module_fwd_out,
                )
            else:
                loss = self.compute_loss_for_module(
                    module_id=module_id,
                    config=self.config.get_config_for_module(module_id),
                    batch=module_batch,
                    fwd_out=module_fwd_out,
                )
            loss_per_module[module_id] = loss

        return loss_per_module

    @OverrideToImplementCustomLogic
    @abc.abstractmethod
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: "AlgorithmConfig",
        batch: Dict[str, Any],
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:
        """Computes the loss for a single module.

        Think of this as computing loss for a single agent. For multi-agent use-cases
        that require more complicated computation for loss, consider overriding the
        `compute_losses` method instead.

        Args:
            module_id: The id of the module.
            config: The AlgorithmConfig specific to the given `module_id`.
            batch: The train batch for this particular module.
            fwd_out: The output of the forward pass for this particular module.

        Returns:
            A single total loss tensor. If you have more than one optimizer on the
            provided `module_id` and would like to compute gradients separately using
            these different optimizers, simply add up the individual loss terms for
            each optimizer and return the sum. Also, for recording/logging any
            individual loss terms, you can use the `Learner.metrics.log_value(
            key=..., value=...)` or `Learner.metrics.log_dict()` APIs. See:
            :py:class:`~ray.rllib.utils.metrics.metrics_logger.MetricsLogger` for more
            information.
        """

    def update_from_batch(
        self,
        batch: MultiAgentBatch,
        *,
        # TODO (sven): Make this a more formal structure with its own type.
        timesteps: Optional[Dict[str, Any]] = None,
        num_epochs: int = 1,
        minibatch_size: Optional[int] = None,
        shuffle_batch_per_epoch: bool = False,
        # Deprecated args.
        num_iters=DEPRECATED_VALUE,
        **kwargs,
    ) -> ResultDict:
        """Run `num_epochs` epochs over the given train batch.

        You can use this method to take more than one backward pass on the batch.
        The same `minibatch_size` and `num_epochs` will be used for all module ids in
        MultiRLModule.

        Args:
            batch: A batch of training data to update from.
            timesteps: Timesteps dict, which must have the key
                `NUM_ENV_STEPS_SAMPLED_LIFETIME`.
                # TODO (sven): Make this a more formal structure with its own type.
            num_epochs: The number of complete passes over the entire train batch. Each
                pass might be further split into n minibatches (if `minibatch_size`
                provided).
            minibatch_size: The size of minibatches to use to further split the train
                `batch` into sub-batches. The `batch` is then iterated over n times
                where n is `len(batch) // minibatch_size`.
            shuffle_batch_per_epoch: Whether to shuffle the train batch once per epoch.
                If the train batch has a time rank (axis=1), shuffling will only take
                place along the batch axis to not disturb any intact (episode)
                trajectories. Also, shuffling is always skipped if `minibatch_size` is
                None, meaning the entire train batch is processed each epoch, making it
                unnecessary to shuffle.

        Returns:
            A `ResultDict` object produced by a call to `self.metrics.reduce()`. The
            returned dict may be arbitrarily nested and must have `Stats` objects at
            all its leafs, allowing components further downstream (i.e. a user of this
            Learner) to further reduce these results (for example over n parallel
            Learners).
        """
        if num_iters != DEPRECATED_VALUE:
            deprecation_warning(
                old="Learner.update_from_episodes(num_iters=...)",
                new="Learner.update_from_episodes(num_epochs=...)",
                error=True,
            )
        self._update_from_batch_or_episodes(
            batch=batch,
            timesteps=timesteps,
            num_epochs=num_epochs,
            minibatch_size=minibatch_size,
            shuffle_batch_per_epoch=shuffle_batch_per_epoch,
        )
        return self.metrics.reduce()

    def update_from_episodes(
        self,
        episodes: List[EpisodeType],
        *,
        # TODO (sven): Make this a more formal structure with its own type.
        timesteps: Optional[Dict[str, Any]] = None,
        num_epochs: int = 1,
        minibatch_size: Optional[int] = None,
        shuffle_batch_per_epoch: bool = False,
        num_total_minibatches: int = 0,
        # Deprecated args.
        num_iters=DEPRECATED_VALUE,
    ) -> ResultDict:
        """Run `num_epochs` epochs over the train batch generated from `episodes`.

        You can use this method to take more than one backward pass on the batch.
        The same `minibatch_size` and `num_epochs` will be used for all module ids in
        MultiRLModule.

        Args:
            episodes: An list of episode objects to update from.
            timesteps: Timesteps dict, which must have the key
                `NUM_ENV_STEPS_SAMPLED_LIFETIME`.
                # TODO (sven): Make this a more formal structure with its own type.
            num_epochs: The number of complete passes over the entire train batch. Each
                pass might be further split into n minibatches (if `minibatch_size`
                provided). The train batch is generated from the given `episodes`
                through the Learner connector pipeline.
            minibatch_size: The size of minibatches to use to further split the train
                `batch` into sub-batches. The `batch` is then iterated over n times
                where n is `len(batch) // minibatch_size`. The train batch is generated
                from the given `episodes` through the Learner connector pipeline.
            shuffle_batch_per_epoch: Whether to shuffle the train batch once per epoch.
                If the train batch has a time rank (axis=1), shuffling will only take
                place along the batch axis to not disturb any intact (episode)
                trajectories. Also, shuffling is always skipped if `minibatch_size` is
                None, meaning the entire train batch is processed each epoch, making it
                unnecessary to shuffle. The train batch is generated from the given
                `episodes` through the Learner connector pipeline.
            num_total_minibatches: The total number of minibatches to loop through
                (over all `num_epochs` epochs). It's only required to set this to != 0
                in multi-agent + multi-GPU situations, in which the MultiAgentEpisodes
                themselves are roughly sharded equally, however, they might contain
                SingleAgentEpisodes with very lopsided length distributions. Thus,
                without this fixed, pre-computed value, one Learner might go through a
                different number of minibatche passes than others causing a deadlock.

        Returns:
            A `ResultDict` object produced by a call to `self.metrics.reduce()`. The
            returned dict may be arbitrarily nested and must have `Stats` objects at
            all its leafs, allowing components further downstream (i.e. a user of this
            Learner) to further reduce these results (for example over n parallel
            Learners).
        """
        if num_iters != DEPRECATED_VALUE:
            deprecation_warning(
                old="Learner.update_from_episodes(num_iters=...)",
                new="Learner.update_from_episodes(num_epochs=...)",
                error=True,
            )
        self._update_from_batch_or_episodes(
            episodes=episodes,
            timesteps=timesteps,
            num_epochs=num_epochs,
            minibatch_size=minibatch_size,
            shuffle_batch_per_epoch=shuffle_batch_per_epoch,
            num_total_minibatches=num_total_minibatches,
        )
        return self.metrics.reduce()

    def update_from_iterator(
        self,
        iterator,
        *,
        timesteps: Optional[Dict[str, Any]] = None,
        minibatch_size: Optional[int] = None,
        num_iters: int = None,
        **kwargs,
    ):
        if "num_epochs" in kwargs:
            raise ValueError(
                "`num_epochs` arg NOT supported by Learner.update_from_iterator! Use "
                "`num_iters` instead."
            )

        if not self.iterator:
            self.iterator = iterator

        self._check_is_built()

        # Call `before_gradient_based_update` to allow for non-gradient based
        # preparations-, logging-, and update logic to happen.
        self.before_gradient_based_update(timesteps=timesteps or {})

        def _finalize_fn(batch: Dict[str, numpy.ndarray]) -> Dict[str, Any]:
            # Note, the incoming batch is a dictionary with a numpy array
            # holding the `MultiAgentBatch`.
            batch = self._convert_batch_type(batch["batch"][0])
            return {"batch": self._set_slicing_by_batch_id(batch, value=True)}

        i = 0
        logger.debug(f"===> [Learner {id(self)}]: Looping through batches ... ")
        while num_iters is None or i < num_iters:
            for batch in self.iterator.iter_batches(
                # Note, this needs to be one b/c data is already mapped to
                # `MultiAgentBatch`es of `minibatch_size`.
                batch_size=1,
                _finalize_fn=_finalize_fn,
                **kwargs,
            ):
                # TODO (simon): Add metrics for the `dataset_num_iter`.
                # Update the iteration counter.
                i += 1

                # Note, `_finalize_fn`  must return a dictionary.
                batch = batch["batch"]
                logger.debug(
                    f"===> [Learner {id(self)}]: batch {i} with {batch.env_steps()} rows."
                )
                # Check the MultiAgentBatch, whether our RLModule contains all ModuleIDs
                # found in this batch. If not, throw an error.
                unknown_module_ids = set(batch.policy_batches.keys()) - set(
                    self.module.keys()
                )
                if len(unknown_module_ids) > 0:
                    raise ValueError(
                        "Batch contains one or more ModuleIDs that are not in this "
                        f"Learner! Found IDs: {unknown_module_ids}"
                    )

                # Log metrics.
                self._log_steps_trained_metrics(batch)

                # Make the actual in-graph/traced `_update` call. This should return
                # all tensor values (no numpy).
                fwd_out, loss_per_module, tensor_metrics = self._update(
                    batch.policy_batches
                )
                # Convert logged tensor metrics (logged during tensor-mode of
                # MetricsLogger) to actual (numpy) values.
                self.metrics.tensors_to_numpy(tensor_metrics)

                self._set_slicing_by_batch_id(batch, value=False)
                # If `num_iters` is reached break and return.
                if num_iters and i == num_iters:
                    break

        logger.debug(
            f"===> [Learner {id(self)}] number of iterations run in this epoch: {i}"
        )

        # Log all individual RLModules' loss terms and its registered optimizers'
        # current learning rates.
        for mid, loss in convert_to_numpy(loss_per_module).items():
            self.metrics.log_value(
                key=(mid, self.TOTAL_LOSS_KEY),
                value=loss,
                window=1,
            )
        # Record the number of batches pulled from the dataset in this RLlib iteration.
        self.metrics.log_value(
            DATASET_NUM_ITERS_TRAINED,
            i,
            reduce="sum",
            clear_on_reduce=True,
        )
        self.metrics.log_value(
            DATASET_NUM_ITERS_TRAINED_LIFETIME,
            i,
            reduce="sum",
        )
        # Call `after_gradient_based_update` to allow for non-gradient based
        # cleanups-, logging-, and update logic to happen.
        # TODO (simon): Check, if this should stay here, when running multiple
        # gradient steps inside the iterator loop above (could be a complete epoch)
        # the target networks might need to be updated earlier.
        self.after_gradient_based_update(timesteps=timesteps or {})

        # Reduce results across all minibatch update steps.
        return self.metrics.reduce()

    @OverrideToImplementCustomLogic
    @abc.abstractmethod
    def _update(
        self,
        batch: Dict[str, Any],
        **kwargs,
    ) -> Tuple[Any, Any, Any]:
        """Contains all logic for an in-graph/traceable update step.

        Framework specific subclasses must implement this method. This should include
        calls to the RLModule's `forward_train`, `compute_loss`, compute_gradients`,
        `postprocess_gradients`, and `apply_gradients` methods and return a tuple
        with all the individual results.

        Args:
            batch: The train batch already converted to a Dict mapping str to (possibly
                nested) tensors.
            kwargs: Forward compatibility kwargs.

        Returns:
            A tuple consisting of:
                1) The `forward_train()` output of the RLModule,
                2) the loss_per_module dictionary mapping module IDs to individual loss
                    tensors
                3) a metrics dict mapping module IDs to metrics key/value pairs.

        """

    @override(Checkpointable)
    def get_state(
        self,
        components: Optional[Union[str, Collection[str]]] = None,
        *,
        not_components: Optional[Union[str, Collection[str]]] = None,
        **kwargs,
    ) -> StateDict:
        self._check_is_built()

        state = {
            "should_module_be_updated": self.config.policies_to_train,
            WEIGHTS_SEQ_NO: self._weights_seq_no,
        }

        if self._check_component(COMPONENT_RL_MODULE, components, not_components):
            state[COMPONENT_RL_MODULE] = self.module.get_state(
                components=self._get_subcomponents(COMPONENT_RL_MODULE, components),
                not_components=self._get_subcomponents(
                    COMPONENT_RL_MODULE, not_components
                ),
                **kwargs,
            )
        if self._check_component(COMPONENT_OPTIMIZER, components, not_components):
            state[COMPONENT_OPTIMIZER] = self._get_optimizer_state()

        if self._check_component(COMPONENT_METRICS_LOGGER, components, not_components):
            # TODO (sven): Make `MetricsLogger` a Checkpointable.
            state[COMPONENT_METRICS_LOGGER] = self.metrics.get_state()

        return state

    @override(Checkpointable)
    def set_state(self, state: StateDict) -> None:
        self._check_is_built()

        weights_seq_no = state.get(WEIGHTS_SEQ_NO, 0)

        if COMPONENT_RL_MODULE in state:
            if weights_seq_no == 0 or self._weights_seq_no < weights_seq_no:
                self.module.set_state(state[COMPONENT_RL_MODULE])

        if COMPONENT_OPTIMIZER in state:
            self._set_optimizer_state(state[COMPONENT_OPTIMIZER])

        # Update our weights_seq_no, if the new one is > 0.
        if weights_seq_no > 0:
            self._weights_seq_no = weights_seq_no

        # Update our trainable Modules information/function via our config.
        # If not provided in state (None), all Modules will be trained by default.
        if "should_module_be_updated" in state:
            self.config.multi_agent(policies_to_train=state["should_module_be_updated"])

        # TODO (sven): Make `MetricsLogger` a Checkpointable.
        if COMPONENT_METRICS_LOGGER in state:
            self.metrics.set_state(state[COMPONENT_METRICS_LOGGER])

    @override(Checkpointable)
    def get_ctor_args_and_kwargs(self):
        return (
            (),  # *args,
            {
                "config": self.config,
                "module_spec": self._module_spec,
                "module": self._module_obj,
            },  # **kwargs
        )

    @override(Checkpointable)
    def get_checkpointable_components(self):
        if not self._check_is_built(error=False):
            self.build()
        return [
            (COMPONENT_RL_MODULE, self.module),
        ]

    def _get_optimizer_state(self) -> StateDict:
        """Returns the state of all optimizers currently registered in this Learner.

        Returns:
            The current state of all optimizers currently registered in this Learner.
        """
        raise NotImplementedError

    def _set_optimizer_state(self, state: StateDict) -> None:
        """Sets the state of all optimizers currently registered in this Learner.

        Args:
            state: The state of the optimizers.
        """
        raise NotImplementedError

    def _update_from_batch_or_episodes(
        self,
        *,
        # TODO (sven): We should allow passing in a single agent batch here
        #  as well for simplicity.
        batch: Optional[MultiAgentBatch] = None,
        episodes: Optional[List[EpisodeType]] = None,
        # TODO (sven): Make this a more formal structure with its own type.
        timesteps: Optional[Dict[str, Any]] = None,
        # TODO (sven): Deprecate these in favor of config attributes for only those
        #  algos that actually need (and know how) to do minibatching.
        num_epochs: int = 1,
        minibatch_size: Optional[int] = None,
        shuffle_batch_per_epoch: bool = False,
        num_total_minibatches: int = 0,
    ) -> None:

        self._check_is_built()

        # Call `before_gradient_based_update` to allow for non-gradient based
        # preparations-, logging-, and update logic to happen.
        self.before_gradient_based_update(timesteps=timesteps or {})

        # Resolve batch/episodes being ray object refs (instead of
        # actual batch/episodes objects).
        if isinstance(batch, ray.ObjectRef):
            batch = ray.get(batch)
        if isinstance(episodes, ray.ObjectRef):
            episodes = ray.get(episodes)
        elif isinstance(episodes, list) and isinstance(episodes[0], ray.ObjectRef):
            # It's possible that individual refs are invalid due to the EnvRunner
            # that produced the ref has crashed or had its entire node go down.
            # In this case, try each ref individually and collect only valid results.
            try:
                episodes = tree.flatten(ray.get(episodes))
            except ray.exceptions.OwnerDiedError:
                episode_refs = episodes
                episodes = []
                for ref in episode_refs:
                    try:
                        episodes.extend(ray.get(ref))
                    except ray.exceptions.OwnerDiedError:
                        pass

        # Call the learner connector on the given `episodes` (if we have one).
        if episodes is not None and self._learner_connector is not None:
            # Call the learner connector pipeline.
            shared_data = {}
            batch = self._learner_connector(
                rl_module=self.module,
                batch=batch if batch is not None else {},
                episodes=episodes,
                shared_data=shared_data,
                metrics=self.metrics,
            )
            # Convert to a batch.
            # TODO (sven): Try to not require MultiAgentBatch anymore.
            batch = MultiAgentBatch(
                {
                    module_id: (
                        SampleBatch(module_data, _zero_padded=True)
                        if shared_data.get(f"_zero_padded_for_mid={module_id}")
                        else SampleBatch(module_data)
                    )
                    for module_id, module_data in batch.items()
                },
                env_steps=sum(len(e) for e in episodes),
            )
        # Single-agent SampleBatch: Have to convert to MultiAgentBatch.
        elif isinstance(batch, SampleBatch):
            assert len(self.module) == 1
            batch = MultiAgentBatch(
                {next(iter(self.module.keys())): batch}, env_steps=len(batch)
            )
        # If we have already an `MultiAgentBatch` but with `numpy` array, convert to
        # tensors.
        elif (
            isinstance(batch, MultiAgentBatch)
            and batch.policy_batches
            and isinstance(
                next(iter(batch.policy_batches.values()))["obs"], numpy.ndarray
            )
        ):
            batch = self._convert_batch_type(batch)

        # Check the MultiAgentBatch, whether our RLModule contains all ModuleIDs
        # found in this batch. If not, throw an error.
        unknown_module_ids = set(batch.policy_batches.keys()) - set(self.module.keys())
        if len(unknown_module_ids) > 0:
            raise ValueError(
                "Batch contains one or more ModuleIDs that are not in this Learner! "
                f"Found IDs: {unknown_module_ids}"
            )

        # TODO: Move this into LearnerConnector pipeline?
        # Filter out those RLModules from the final train batch that should not be
        # updated.
        for module_id in list(batch.policy_batches.keys()):
            if not self.should_module_be_updated(module_id, batch):
                del batch.policy_batches[module_id]
        if not batch.policy_batches:
            return

        # Log all timesteps (env, agent, modules) based on given episodes/batch.
        self._log_steps_trained_metrics(batch)

        if minibatch_size:
            batch_iter = MiniBatchCyclicIterator
        elif num_epochs > 1:
            # `minibatch_size` was not set but `num_epochs` > 1.
            # Under the old training stack, users could do multiple epochs
            # over a batch without specifying a minibatch size. We enable
            # this behavior here by setting the minibatch size to be the size
            # of the batch (e.g. 1 minibatch of size batch.count)
            minibatch_size = batch.count
            # Note that there is no need to shuffle here, b/c we don't have minibatches.
            batch_iter = MiniBatchCyclicIterator
        else:
            # `minibatch_size` and `num_epochs` are not set by the user.
            batch_iter = MiniBatchDummyIterator

        batch = self._set_slicing_by_batch_id(batch, value=True)

        for tensor_minibatch in batch_iter(
            batch,
            num_epochs=num_epochs,
            minibatch_size=minibatch_size,
            shuffle_batch_per_epoch=shuffle_batch_per_epoch and (num_epochs > 1),
            num_total_minibatches=num_total_minibatches,
        ):
            # Make the actual in-graph/traced `_update` call. This should return
            # all tensor values (no numpy).
            fwd_out, loss_per_module, tensor_metrics = self._update(
                tensor_minibatch.policy_batches
            )

            # Convert logged tensor metrics (logged during tensor-mode of MetricsLogger)
            # to actual (numpy) values.
            self.metrics.tensors_to_numpy(tensor_metrics)

            # Log all individual RLModules' loss terms and its registered optimizers'
            # current learning rates.
            for mid, loss in convert_to_numpy(loss_per_module).items():
                self.metrics.log_value(
                    key=(mid, self.TOTAL_LOSS_KEY),
                    value=loss,
                    window=1,
                )

        self._weights_seq_no += 1
        self.metrics.log_dict(
            {
                (mid, WEIGHTS_SEQ_NO): self._weights_seq_no
                for mid in batch.policy_batches.keys()
            },
            window=1,
        )

        self._set_slicing_by_batch_id(batch, value=False)

        # Call `after_gradient_based_update` to allow for non-gradient based
        # cleanups-, logging-, and update logic to happen.
        self.after_gradient_based_update(timesteps=timesteps or {})

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def before_gradient_based_update(self, *, timesteps: Dict[str, Any]) -> None:
        """Called before gradient-based updates are completed.

        Should be overridden to implement custom preparation-, logging-, or
        non-gradient-based Learner/RLModule update logic before(!) gradient-based
        updates are performed.

        Args:
            timesteps: Timesteps dict, which must have the key
                `NUM_ENV_STEPS_SAMPLED_LIFETIME`.
                # TODO (sven): Make this a more formal structure with its own type.
        """

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def after_gradient_based_update(self, *, timesteps: Dict[str, Any]) -> None:
        """Called after gradient-based updates are completed.

        Should be overridden to implement custom cleanup-, logging-, or non-gradient-
        based Learner/RLModule update logic after(!) gradient-based updates have been
        completed.

        Args:
            timesteps: Timesteps dict, which must have the key
                `NUM_ENV_STEPS_SAMPLED_LIFETIME`.
                # TODO (sven): Make this a more formal structure with its own type.
        """
        # Only update this optimizer's lr, if a scheduler has been registered
        # along with it.
        for module_id, optimizer_names in self._module_optimizers.items():
            for optimizer_name in optimizer_names:
                optimizer = self._named_optimizers[optimizer_name]
                # Update and log learning rate of this optimizer.
                lr_schedule = self._optimizer_lr_schedules.get(optimizer)
                if lr_schedule is not None:
                    new_lr = lr_schedule.update(
                        timestep=timesteps.get(NUM_ENV_STEPS_SAMPLED_LIFETIME, 0)
                    )
                    self._set_optimizer_lr(optimizer, lr=new_lr)
                self.metrics.log_value(
                    # Cut out the module ID from the beginning since it's already part
                    # of the key sequence: (ModuleID, "[optim name]_lr").
                    key=(module_id, f"{optimizer_name[len(module_id) + 1:]}_{LR_KEY}"),
                    value=convert_to_numpy(self._get_optimizer_lr(optimizer)),
                    window=1,
                )

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

    def _make_module(self) -> MultiRLModule:
        """Construct the multi-agent RL module for the learner.

        This method uses `self._module_specs` or `self._module_obj` to construct the
        module. If the module_class is a single agent RL module it will be wrapped to a
        multi-agent RL module. Override this method if there are other things that
        need to happen for instantiation of the module.

        Returns:
            A constructed MultiRLModule.
        """
        # Module was provided directly through constructor -> Use as-is.
        if self._module_obj is not None:
            module = self._module_obj
            self._module_spec = MultiRLModuleSpec.from_module(module)
        # RLModuleSpec was provided directly through constructor -> Use it to build the
        # RLModule.
        elif self._module_spec is not None:
            module = self._module_spec.build()
        # Try using our config object. Note that this would only work if the config
        # object has all the necessary space information already in it.
        else:
            module = self.config.get_multi_rl_module_spec().build()

        # If not already, convert to MultiRLModule.
        module = module.as_multi_rl_module()

        return module

    def rl_module_is_compatible(self, module: RLModule) -> bool:
        """Check whether the given `module` is compatible with this Learner.

        The default implementation checks the Learner-required APIs and whether the
        given `module` implements all of them (if not, returns False).

        Args:
            module: The RLModule to check.

        Returns:
            True if the module is compatible with this Learner.
        """
        return all(isinstance(module, api) for api in self.rl_module_required_apis())

    @classmethod
    def rl_module_required_apis(cls) -> list[type]:
        """Returns the required APIs for an RLModule to be compatible with this Learner.

        The returned values may or may not be used inside the `rl_module_is_compatible`
        method.

        Args:
            module: The RLModule to check.

        Returns:
            A list of RLModule API classes that an RLModule must implement in order
            to be compatible with this Learner.
        """
        return []

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

    def _log_trainable_parameters(self) -> None:
        """Logs the number of trainable and non-trainable parameters to self.metrics.

        Use MetricsLogger (self.metrics) tuple-keys:
        (ALL_MODULES, NUM_TRAINABLE_PARAMETERS) and
        (ALL_MODULES, NUM_NON_TRAINABLE_PARAMETERS) with EMA.
        """
        pass

    def _check_is_built(self, error: bool = True) -> bool:
        if self.module is None:
            if error:
                raise ValueError(
                    "Learner.build() must be called after constructing a "
                    "Learner and before calling any methods on it."
                )
            return False
        return True

    def _reset(self):
        self._params = {}
        self._optimizer_parameters = {}
        self._named_optimizers = {}
        self._module_optimizers = defaultdict(list)
        self._optimizer_lr_schedules = {}
        self.metrics = MetricsLogger()
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
        framework specific sub-classes.

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
        """Returns the gradient clipping function to use."""

    @staticmethod
    @abc.abstractmethod
    def _get_global_norm_function() -> Callable:
        """Returns the global norm function to use, given the framework."""

    def _log_steps_trained_metrics(self, batch: MultiAgentBatch):
        """Logs this iteration's steps trained, based on given `batch`."""
        for mid, module_batch in batch.policy_batches.items():
            module_batch_size = len(module_batch)
            # Log average batch size (for each module).
            self.metrics.log_value(
                key=(mid, MODULE_TRAIN_BATCH_SIZE_MEAN),
                value=module_batch_size,
            )
            # Log module steps (for each module).
            self.metrics.log_value(
                key=(mid, NUM_MODULE_STEPS_TRAINED),
                value=module_batch_size,
                reduce="sum",
                clear_on_reduce=True,
            )
            self.metrics.log_value(
                key=(mid, NUM_MODULE_STEPS_TRAINED_LIFETIME),
                value=module_batch_size,
                reduce="sum",
            )
            # Log module steps (sum of all modules).
            self.metrics.log_value(
                key=(ALL_MODULES, NUM_MODULE_STEPS_TRAINED),
                value=module_batch_size,
                reduce="sum",
                clear_on_reduce=True,
            )
            self.metrics.log_value(
                key=(ALL_MODULES, NUM_MODULE_STEPS_TRAINED_LIFETIME),
                value=module_batch_size,
                reduce="sum",
            )
        # Log env steps (all modules).
        self.metrics.log_value(
            (ALL_MODULES, NUM_ENV_STEPS_TRAINED),
            batch.env_steps(),
            reduce="sum",
            clear_on_reduce=True,
        )
        self.metrics.log_value(
            (ALL_MODULES, NUM_ENV_STEPS_TRAINED_LIFETIME),
            batch.env_steps(),
            reduce="sum",
            with_throughput=True,
        )

    @Deprecated(
        new="Learner.before_gradient_based_update("
        "timesteps={'num_env_steps_sampled_lifetime': ...}) and/or "
        "Learner.after_gradient_based_update("
        "timesteps={'num_env_steps_sampled_lifetime': ...})",
        error=True,
    )
    def additional_update_for_module(self, *args, **kwargs):
        pass

    @Deprecated(new="Learner.save_to_path(...)", error=True)
    def save_state(self, *args, **kwargs):
        pass

    @Deprecated(new="Learner.restore_from_path(...)", error=True)
    def load_state(self, *args, **kwargs):
        pass

    @Deprecated(new="Learner.module.get_state()", error=True)
    def get_module_state(self, *args, **kwargs):
        pass

    @Deprecated(new="Learner.module.set_state()", error=True)
    def set_module_state(self, *args, **kwargs):
        pass

    @Deprecated(new="Learner._get_optimizer_state()", error=True)
    def get_optimizer_state(self, *args, **kwargs):
        pass

    @Deprecated(new="Learner._set_optimizer_state()", error=True)
    def set_optimizer_state(self, *args, **kwargs):
        pass

    @Deprecated(new="Learner.compute_losses(...)", error=False)
    def compute_loss(self, *args, **kwargs):
        losses_per_module = self.compute_losses(*args, **kwargs)
        # To continue supporting the old `compute_loss` behavior (instead of
        # the new `compute_losses`, add the ALL_MODULES key here holding the sum
        # of all individual loss terms.
        if ALL_MODULES not in losses_per_module:
            losses_per_module[ALL_MODULES] = sum(losses_per_module.values())
        return losses_per_module

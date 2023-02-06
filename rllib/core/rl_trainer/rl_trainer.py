import abc

from dataclasses import dataclass, field
import logging
import numpy as np
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Hashable,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.core.rl_module.rl_module import (
    RLModule,
    ModuleID,
    SingleAgentRLModuleSpec,
)

from ray.rllib.core.rl_module.marl_module import (
    MultiAgentRLModule,
    MultiAgentRLModuleSpec,
)
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import TensorType
from ray.rllib.core.rl_trainer.scaling_config import TrainerScalingConfig


torch, _ = try_import_torch()
tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)

Optimizer = Union["torch.optim.Optimizer", "tf.keras.optimizers.Optimizer"]
ParamType = Union["torch.Tensor", "tf.Variable"]
ParamOptimizerPairs = List[Tuple[Sequence[ParamType], Optimizer]]
ParamRef = Hashable
ParamDictType = Dict[ParamRef, ParamType]


@dataclass
class FrameworkHPs:
    """The framework specific hyper-parameters.

    Args:
        eager_tracing: Whether to trace the model in eager mode. This enables tf
            tracing mode by wrapping the loss function computation in a tf.function.
            This is useful for speeding up the training loop. However, it is not
            compatible with all tf operations. For example, tf.print is not supported
            in tf.function.
    """

    eager_tracing: bool = False


@dataclass
class RLTrainerHPs:
    """The hyper-parameters for RLTrainer.

    When creating a new RLTrainer, the new hyper-parameters have to be defined by
    subclassing this class and adding the new hyper-parameters as fields.
    """

    pass


class RLTrainer:
    """Base class for RLlib algorithm trainers.

    Args:
        module_class: The (MA)RLModule class to use.
        module_kwargs: The kwargs for the (MA)RLModule.
        optimizer_config: The config for the optimizer.
        in_test: Whether to enable additional logging behavior for testing purposes.
        distributed: Whether this trainer is distributed or not.

    Abstract Methods:
        compute_gradients: Compute gradients for the module being optimized.
        apply_gradients: Apply gradients to the module being optimized with respect to
            a loss that is computed by the optimizer. Both compute_gradients and
            apply_gradients are meant for framework-specific specializations.
        compute_loss: Compute the loss for the module being optimized. Override this
            method to customize the loss function of the multi-agent RLModule that is
            being optimized.
        configure_optimizers: Configure the optimizers for the module being optimized.
            Override this to cutomize the optimizers and the parameters that they are
            optimizing.


    Example:
        .. code-block:: python

        trainer = MyRLTrainer(
            module_class,
            module_kwargs,
            optimizer_config
        )
        trainer.build()
        batch = ...
        results = trainer.update(batch)

        # add a new module, perhaps for league based training or lru caching
        trainer.add_module(
            module_id="new_player",
            module_cls=NewPlayerCls,
            module_kwargs=new_player_kwargs,
        )

        batch = ...
        results = trainer.update(batch)  # will train previous and new modules.

        # remove a module
        trainer.remove_module("new_player")

        batch = ...
        results = trainer.update(batch)  # will train previous modules only.

        # get the state of the trainer
        state = trainer.get_state()

        # set the state of the trainer
        trainer.set_state(state)

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
        optimizer_config: Mapping[str, Any] = None,
        trainer_scaling_config: TrainerScalingConfig = TrainerScalingConfig(),
        trainer_hyperparameters: Optional[RLTrainerHPs] = RLTrainerHPs(),
        framework_hyperparameters: Optional[FrameworkHPs] = FrameworkHPs(),
    ):
        # TODO (Kourosh): Having the entire algorithm_config inside trainer may not be
        # the best idea in the world, but it's easy to implement and user will
        # understand it. If we can find a better way to make subset of the config
        # available to the trainer, that would be great.
        # TODO (Kourosh): convert optimizer configs to dataclasses
        if module_spec is not None and module is not None:
            raise ValueError(
                "Only one of module spec or module can be provided to RLTrainer."
            )

        if module_spec is None and module is None:
            raise ValueError(
                "Either module_spec or module should be provided to RLTrainer."
            )

        self.module_spec = module_spec
        self.module_obj = module
        self.optimizer_config = optimizer_config
        self.config = trainer_hyperparameters

        # pick the configs that we need for the trainer from scaling config
        self._distributed = trainer_scaling_config.num_workers > 1

        # These are the attributes that are set during build
        self._module: MultiAgentRLModule = None
        # These are set for properly applying optimizers and adding or removing modules.
        self._optim_to_param: Dict[Optimizer, List[ParamRef]] = {}
        self._param_to_optim: Dict[ParamRef, Optimizer] = {}
        self._params: ParamDictType = {}

    @property
    def distributed(self) -> bool:
        return self._distributed

    @property
    def module(self) -> MultiAgentRLModule:
        return self._module

    @abc.abstractmethod
    def configure_optimizers(self) -> ParamOptimizerPairs:
        """Configures the optimizers for the trainer.

        This method is responsible for setting up the optimizers that will be used to
        train the model. The optimizers are responsible for updating the model's
        parameters during training, based on the computed gradients. The method should
        return a list of tuples, where each tuple consists of a list of model
        parameters and a deep learning optimizer that should be used to optimize those
        parameters. To support both tf and torch, we must explicitly return the
        parameters as the first element of the tuple regardless of whether those
        exist in the optimizer objects or not. This method is called once at
        initialization and everytime a new sub-module is added to the module.

        Returns:
            A list of tuples (parameters, optimizer), where parameters is a list of
            model parameters and optimizer is a deep learning optimizer.

        """

    def compute_loss(
        self,
        *,
        fwd_out: MultiAgentBatch,
        batch: MultiAgentBatch,
    ) -> Union[TensorType, Mapping[str, Any]]:
        """Computes the loss for the module being optimized.

        This method must be overridden by each algorithm's trainer to specify the
        specific loss computation logic. The input "fwd_out" is the output of a call to
        the "forward_train" method on the instance's "_module" attribute during
        training. The input "batch" is the data that was used to compute "fwd_out". The
        returned dictionary must contain a key called "total_loss", which will be used
        to compute gradients. It is recommended to not compute any forward passes
        within this method, and to use the "forward_train" outputs to compute the
        required tensors for loss calculation.

        Args:
            fwd_out: Output from a call to `forward_train` on self._module during
                training.
            batch: The data that was used to compute fwd_out.

        Returns:
            A dictionary of losses. NOTE that the dictionary
            must contain one protected key "total_loss" which will be used for
            computing gradients through.
        """
        # TODO (Kourosh): This method is built for multi-agent. While it is still
        # possible to write single-agent losses, it may become confusing to users. We
        # should find a way to allow them to specify single-agent losses as well,
        # without having to think about one extra layer of hierarchy for module ids.

        loss_total = None
        results_all_modules = {}
        for module_id in fwd_out:
            module_batch = batch[module_id]
            module_fwd_out = fwd_out[module_id]

            module_results = self._compute_loss_per_module(
                module_id, module_batch, module_fwd_out
            )
            results_all_modules[module_id] = module_results
            loss = module_results[self.TOTAL_LOSS_KEY]

            if loss_total is None:
                loss_total = loss
            else:
                loss_total += loss

        results_all_modules[self.TOTAL_LOSS_KEY] = loss_total

        return results_all_modules

    def _compute_loss_per_module(
        self, module_id: str, batch: SampleBatch, fwd_out: Mapping[str, TensorType]
    ) -> Mapping[str, Any]:
        """Computes the loss for a single module.

        Think of this as computing loss for a
        single agent. For multi-agent use-cases that require more complicated
        computation for loss, consider overriding the `compute_loss` method instead.

        Args:
            module_id: The id of the module.
            batch: The sample batch for this particular module.
            fwd_out: The output of the forward pass for this particular module.

        Returns:
            A dictionary of losses. NOTE that the dictionary
            must contain one protected key "total_loss" which will be used for
            computing gradients through.
        """
        raise NotImplementedError

    def postprocess_gradients(
        self, gradients_dict: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        """Called after gradients have been computed.

        Args:
            gradients_dict (Mapping[str, Any]): A dictionary of gradients.

        Note the relative order of operations looks like this:
            fwd_out = forward_train(batch)
            loss = compute_loss(batch, fwd_out)
            gradients = compute_gradients(loss)
            ---> postprocessed_gradients = postprocess_gradients(gradients)
            apply_gradients(postprocessed_gradients)

        Returns:
            Mapping[str, Any]: A dictionary of gradients.
        """
        return gradients_dict

    def compile_results(
        self,
        batch: NestedDict,
        fwd_out: Mapping[str, Any],
        postprocessed_loss: Mapping[str, Any],
        postprocessed_gradients: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """Compile results from the update.

        Args:
            batch: The batch that was used for the update.
            fwd_out: The output of the forward train pass.
            postprocessed_loss: The loss after postprocessing.
            postprocessed_gradients: The gradients after postprocessing.

        Returns:
            A dictionary of results.
        """
        # TODO (Kourosh): This method assumes that all the modules with in the
        # marl_module are accessible via looping through it rl_modules. This may not be
        # true for centralized critic for example. Therefore we need a better
        # generalization of this base-class implementation.
        loss_numpy = convert_to_numpy(postprocessed_loss)
        mean_grads = [
            np.mean(grad) for grad in convert_to_numpy(postprocessed_gradients.values())
        ]
        ret = {
            "loss": loss_numpy,
            "mean_gradient": np.mean(mean_grads),
        }

        return ret

    def update(self, batch: MultiAgentBatch) -> Mapping[str, Any]:
        """Perform an update on this Trainer.

        Args:
            batch: A batch of data.

        Returns:
            A dictionary of results.
        """
        self.__check_if_build_called()
        if not self.distributed:
            return self._update(batch)
        else:
            return self.do_distributed_update(batch)

    def _update(self, batch: MultiAgentBatch) -> Mapping[str, Any]:
        # TODO (Kourosh): remove the MultiAgentBatch from the type, it should be
        # NestedDict from the base class.
        batch = self._convert_batch_type(batch)
        fwd_out = self._module.forward_train(batch)
        loss = self.compute_loss(fwd_out=fwd_out, batch=batch)
        gradients = self.compute_gradients(loss)
        postprocessed_gradients = self.postprocess_gradients(gradients)
        self.apply_gradients(postprocessed_gradients)
        return self.compile_results(batch, fwd_out, loss, postprocessed_gradients)

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

    def additional_update(self, *args, **kwargs) -> Mapping[str, Any]:
        """Apply additional non-gradient based updates to this Trainer.

        For example, this could be used to do a polyak averaging update
        of a target network in off policy algorithms like SAC or DQN.

        This can be called on its own, or via a call to a `TrainerRunner`
        that is managing multiple RLTrainer instances via a call to
        `TrainerRunner.additional_update`.

        Args:
            *args: Arguments to use for the update.
            **kwargs: Keyword arguments to use for the additional update.

        Returns:
            A dictionary of results from the update
        """
        results_all_modules = {}
        for module_id in self._module.keys():
            module_results = self._additional_update_per_module(
                module_id, *args, **kwargs
            )
            results_all_modules[module_id] = module_results

        return results_all_modules

    def _additional_update_per_module(
        self, module_id: str, *args, **kwargs
    ) -> Mapping[str, Any]:
        """Apply additional non-gradient based updates for a single module.

        Args:
            module_id: The id of the module to update.
            *args: Arguments to use for the update.
            **kwargs: Keyword arguments to use for the additional update.

        Returns:
            A dictionary of results from the update
        """

        raise NotImplementedError

    @abc.abstractmethod
    def compute_gradients(
        self, loss: Union[TensorType, Mapping[str, Any]]
    ) -> ParamDictType:
        """Perform an update on self._module.

        For example compute and apply gradients to self._module if
        necessary.

        Args:
            loss: variable(s) used for optimizing self._module.

        Returns:
            A dictionary of extra information and statistics.
        """

    @abc.abstractmethod
    def apply_gradients(self, gradients: Dict[ParamRef, TensorType]) -> None:
        """Perform an update on self._module

        Args:
            gradients: A dictionary of gradients.
        """

    def set_state(self, state: Mapping[str, Any]) -> None:
        """Set the state of the trainer.

        Args:
            state: The state of the optimizer and module. Can be obtained
                from `get_state`.

        """
        self.__check_if_build_called()
        # TODO: once we figure out the optimizer format, we can set/get the state
        self._module.set_state(state.get("module_state", {}))

    def get_state(self) -> Mapping[str, Any]:
        """Get the state of the trainer.

        Returns:
            The state of the optimizer and module.

        """
        self.__check_if_build_called()
        # TODO: once we figure out the optimizer format, we can set/get the state
        return {"module_state": self._module.get_state()}

    def add_module(
        self,
        *,
        module_id: ModuleID,
        module_spec: SingleAgentRLModuleSpec,
        set_optimizer_fn: Optional[Callable[[RLModule], ParamOptimizerPairs]] = None,
        optimizer_cls: Optional[Type[Optimizer]] = None,
    ) -> None:
        """Add a module to the underlying MultiAgentRLModule and the trainer.

        Args:
            module_id: The id of the module to add.
            module_cls: The module class to add.
            module_kwargs: The config for the module.
            set_optimizer_fn: A function that takes in the module and returns a list of
                (param, optimizer) pairs. Each element in the tuple describes a
                parameter group that share the same optimizer object, if None, the
                default optimizer_cls will be used with all the parameters from the
                module.
            optimizer_cls: The optimizer class to use. If None, the set_optimizer_fn
                should be provided.
        """
        self.__check_if_build_called()
        module = module_spec.build()

        # construct a default set_optimizer_fn if not provided
        if set_optimizer_fn is None:
            if optimizer_cls is None:
                raise ValueError(
                    "Either set_optimizer_fn or optimizer_cls must be provided."
                )

            def set_optimizer_fn(module):
                optimizer = self.get_optimizer_obj(module, optimizer_cls)
                parameters = self.get_parameters(module)
                return [(parameters, optimizer)]

        for param_seq, optimizer in set_optimizer_fn(module):
            self._optim_to_param[optimizer] = []
            for param in param_seq:
                param_ref = self.get_param_ref(param)
                self._optim_to_param[optimizer].append(param_ref)
                self._params[param_ref] = param
                self._param_to_optim[param_ref] = optimizer

        self._module.add_module(module_id, module)

    def remove_module(self, module_id: ModuleID) -> None:
        """Remove a module from the trainer.

        Args:
            module_id: The id of the module to remove.

        """
        self.__check_if_build_called()
        module = self._module[module_id]

        parameters = self.get_parameters(module)
        for param in parameters:
            param_ref = self.get_param_ref(param)
            if param_ref in self._params:
                del self._params[param_ref]
            if param_ref in self._param_to_optim:
                optimizer = self._param_to_optim[param_ref]
                if optimizer in self._optim_to_param:
                    del self._optim_to_param[optimizer]
                del self._param_to_optim[param_ref]

        self._module.remove_module(module_id)

    def _make_module(self) -> MultiAgentRLModule:
        """Construct the multi-agent RL module for the trainer.

        This method uses `self.module_class` and `self.module_kwargs` to construct the
        module. If the module_class is a single agent RL module it will be wrapped to a
        multi-agent RL module.

        Returns:
            The constructed module.
        """
        if self.module_obj is not None:
            module = self.module_obj
        else:
            module = self.module_spec.build()
        module = module.as_multi_agent()
        return module

    def build(self) -> None:
        """Initialize the model."""
        if self.distributed:
            self._module = self._make_distributed_module()
        else:
            self._module = self._make_module()

        for param_seq, optimizer in self.configure_optimizers():
            self._optim_to_param[optimizer] = []
            for param in param_seq:
                param_ref = self.get_param_ref(param)
                self._optim_to_param[optimizer].append(param_ref)
                self._params[param_ref] = param
                self._param_to_optim[param_ref] = optimizer

    def do_distributed_update(self, batch: MultiAgentBatch) -> Mapping[str, Any]:
        """Perform a distributed update on this Trainer.

        Args:
            batch: A batch of data.

        Returns:
            A dictionary of results.
        """
        raise NotImplementedError

    def _make_distributed_module(self) -> MultiAgentRLModule:
        """Initialize this trainer in a distributed training setting.

        This method should be overriden in the framework specific trainer. It is
        expected the the module creation is wrapped in some context manager that will
        handle the distributed training. This is a common patterns used in torch and
        tf.

        Returns:
            The distributed module.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_param_ref(self, param: ParamType) -> Hashable:
        """Returns a reference to a parameter.

        This should be overriden in framework specific trainer. For example in torch it
        will return the parameter itself, while in tf it returns the .ref() of the
        variable. The purpose is to retrieve a unique reference to the parameters.

        Args:
            param: The parameter to get the reference to.

        Returns:
            A reference to the parameter.
        """

    @abc.abstractmethod
    def get_parameters(self, module: RLModule) -> Sequence[ParamType]:
        """Returns the parameters of a module.

        This should be overriden in framework specific trainer. For example in torch it
        will return .parameters(), while in tf it returns .trainable_variables.

        Args:
            module: The module to get the parameters from.

        Returns:
            The parameters of the module.
        """
        # TODO (Kourosh): Make this method a classmethod. This function's purpose is to
        # get the parameters of a module based on what the underlying framework is.

    @abc.abstractmethod
    def get_optimizer_obj(
        self, module: RLModule, optimizer_cls: Type[Optimizer]
    ) -> Optimizer:
        """Returns the optimizer instance of type optimizer_cls from the module

        In torch this is the optimizer object initialize with module parameters. In tf
        this is initialized without module parameters.

        Args:
            module: The module of type RLModule to get the optimizer from.
            optimizer_cls: The optimizer class to use.

        Returns:
            The optimizer object.
        """

    def __check_if_build_called(self):
        if self._module is None:
            raise ValueError(
                "RLTrainer.build() must be called after constructing a "
                "RLTrainer and before calling any methods on it."
            )


@dataclass
class RLTrainerSpec:
    """The spec for construcitng RLTrainer actors.

    Args:
        rl_trainer_class: The RLTrainer class to use.
        module_spec: The underlying (MA)RLModule spec to completely define the module.
        module: Alternatively the RLModule instance can be passed in directly. This
            only works if the RLTrainer is not an actor.
        backend_config: The backend config for properly distributing the RLModule.
        optimizer_config: The optimizer setting to apply during training.
        trainer_hyperparameters: The extra config for the loss/additional update. This
            should be a subclass of RLTrainerHPs. This is useful for passing in
            algorithm configs that contains the hyper-parameters for loss computation,
            change of training behaviors, etc. e.g lr, entropy_coeff.
    """

    rl_trainer_class: Type["RLTrainer"]
    module_spec: Union["SingleAgentRLModuleSpec", "MultiAgentRLModuleSpec"] = None
    module: Optional["RLModule"] = None
    trainer_scaling_config: TrainerScalingConfig = field(
        default_factory=TrainerScalingConfig
    )
    optimizer_config: Dict[str, Any] = field(default_factory=dict)
    trainer_hyperparameters: RLTrainerHPs = field(default_factory=RLTrainerHPs)
    framework_hyperparameters: FrameworkHPs = field(default_factory=FrameworkHPs)

    def get_params_dict(self) -> Dict[str, Any]:
        """Returns the parameters than be passed to the RLTrainer constructor."""
        return {
            "module": self.module,
            "module_spec": self.module_spec,
            "trainer_scaling_config": self.trainer_scaling_config,
            "optimizer_config": self.optimizer_config,
            "trainer_hyperparameters": self.trainer_hyperparameters,
            "framework_hyperparameters": self.framework_hyperparameters,
        }

    def build(self) -> "RLTrainer":
        """Builds the RLTrainer instance."""
        return self.rl_trainer_class(**self.get_params_dict())

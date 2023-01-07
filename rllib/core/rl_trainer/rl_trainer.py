import abc

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
from ray.rllib.core.rl_module.rl_module import RLModule, ModuleID
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import TensorType

torch, _ = try_import_torch()
tf1, tf, tfv = try_import_tf()


Optimizer = Union["torch.optim.Optimizer", "tf.keras.optimizers.Optimizer"]
ParamType = Union["torch.Tensor", "tf.Variable"]
ParamOptimizerPairs = List[Tuple[Sequence[ParamType], Optimizer]]
ParamRef = Hashable


class RLTrainer:
    """Base class for rllib algorithm trainers.

    Args:
        module_class: The (MA)RLModule class to use.
        module_kwargs: The kwargs for the (MA)RLModule.
        optimizer_class: The optimizer class to use.
        optimizer_kwargs: The kwargs for the optimizer.
        scaling_config: A mapping that holds the world size and rank of this
            trainer. Note this is only used for distributed training.
        distributed: Whether this trainer is distributed or not.

    Abstract Methods:
        compute_gradients: Compute gradients for the module being optimized.
        apply_gradients: Apply gradients to the module being optimized with respect to
            a loss that is computed by the optimizer.

    Example:
        .. code-block:: python

        trainer = MyRLTrainer(module_class, module_kwargs, optimizer_class,
                optimizer_kwargs, scaling_config)
        trainer.init_trainer()
        batch = ...
        results = trainer.update(batch)

        # add a new module, perhaps for league based training or lru caching
        trainer.add_module("new_player", NewPlayerCls, new_player_kwargs,
            NewPlayerOptimCls, new_player_optim_kwargs)

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

    TOTAL_LOSS_KEY = "total_loss"

    def __init__(
        self,
        module_class: Union[Type[RLModule], Type[MultiAgentRLModule]],
        module_kwargs: Mapping[str, Any],
        scaling_config: Mapping[str, Any],
        optimizer_config: Mapping[str, Any],
        distributed: bool = False,
        in_test: bool = False,
    ):
        # TODO: convert scaling and optimizer configs to dataclasses
        self.module_class = module_class
        self.module_kwargs = module_kwargs
        self.scaling_config = scaling_config
        self.optimizer_config = optimizer_config
        self.distributed = distributed
        self.in_test = in_test

        # These are the attributes that are set during build
        self._params: Dict[ParamRef, ParamType] = {}
        self._optimizers: Dict[ParamRef, Optimizer] = {}

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

    @abc.abstractmethod
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
        # TODO: This method is built for multi-agent. While it is still possible to
        # write single-agent losses, it may become confusing to users. We should find a
        # way to allow them to specify single-agent losses as well, without having to
        # think about one extra layer of hierarchy for module ids.

    def on_after_compute_gradients(
        self, gradients_dict: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        """Called after gradients have been computed.

        Args:
            gradients_dict (Mapping[str, Any]): A dictionary of gradients.

        Note the relative order of operations looks like this:
            fwd_out = forward_train(batch)
            loss = compute_loss(batch, fwd_out)
            gradients = compute_gradients(loss)
            ---> post_processed_gradients = on_after_compute_gradients(gradients)
            apply_gradients(post_processed_gradients)

        Returns:
            Mapping[str, Any]: A dictionary of gradients.
        """
        return gradients_dict

    def compile_results(
        self,
        batch: NestedDict,
        fwd_out: Mapping[str, Any],
        postprocessed_loss: Mapping[str, Any],
        post_processed_gradients: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """Compile results from the update.

        Args:
            batch: The batch that was used for the update.
            fwd_out: The output of the forward train pass.
            postprocessed_loss: The loss after postprocessing.
            post_processed_gradients: The gradients after postprocessing.

        Returns:
            A dictionary of results.
        """
        loss_numpy = convert_to_numpy(postprocessed_loss)
        rewards = batch["rewards"]
        rewards = convert_to_numpy(rewards)
        return {
            "avg_reward": rewards.mean(),
            **loss_numpy,
        }

    def update(self, batch: MultiAgentBatch) -> Mapping[str, Any]:
        """Perform an update on this Trainer.

        Args:
            batch: A batch of data.

        Returns:
            A dictionary of results.
        """
        if not self.distributed:
            fwd_out = self._module.forward_train(batch)
            loss = self.compute_loss(fwd_out=fwd_out, batch=batch)
            gradients = self.compute_gradients(loss)
            post_processed_gradients = self.on_after_compute_gradients(gradients)
            self.apply_gradients(post_processed_gradients)
        else:
            self.do_distributed_update(batch)
        return self.compile_results(batch, fwd_out, loss, post_processed_gradients)

    @abc.abstractmethod
    def compute_gradients(
        self, loss: Union[TensorType, Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        """Perform an update on self._module.

        For example compute and apply gradients to self._module if
        necessary.

        Args:
            loss: variable(s) used for optimizing self._module.

        Returns:
            A dictionary of extra information and statistics.
        """

    @abc.abstractmethod
    def apply_gradients(self, gradients: Mapping[str, Any]) -> None:
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
        # TODO: once we figure out the optimizer format, we can set/get the state
        self._module.set_state(state.get("module_state", {}))

    def get_state(self) -> Mapping[str, Any]:
        """Get the state of the trainer.

        Returns:
            The state of the optimizer and module.

        """
        # TODO: once we figure out the optimizer format, we can set/get the state
        return {"module_state": self._module.get_state()}

    def add_module(
        self,
        *,
        module_id: ModuleID,
        module_cls: Type[RLModule],
        module_kwargs: Mapping[str, Any],
        set_optimizer_fn: Optional[Callable[[RLModule], ParamOptimizerPairs]] = None,
    ) -> None:
        """Add a module to the trainer.

        Args:
            module_id: The id of the module to add.
            module_cls: The module class to add.
            module_kwargs: The config for the module.
            set_optimizer_fn: A function that takes in the module and returns a list of
                (param, optimizer) pairs. Each element in the tuple describes a
                parameter group that share the same optimizer object, if None, the
                default optimizer (obtained from the exiting optimizer dictionary) will
                be used.
        """
        module = module_cls.from_model_config(**module_kwargs)

        # construct a default set_optimizer_fn if not provided
        if set_optimizer_fn is None:
            # default is to use the first optimizer class and default parameters
            optimizer_obj = next(iter(self._optimizers.values()))
            optimizer_cls = optimizer_obj.__class__
            lr = self.optimizer_config.get("lr", 1e-3)
            if torch and isinstance(module, torch.nn.Module):
                optimizer = optimizer_cls(module.parameters(), lr=lr)
            elif tf and isinstance(module, tf.keras.Model):
                optimizer = optimizer_cls(learning_rate=lr)
            else:
                raise ValueError(
                    "Unknown module type. Only torch.nn.Module and "
                    "tf.keras.Model are supported."
                )

            def set_optimizer_fn(module):
                return [(module.parameters(), optimizer)]

        for param_seq, optimizer in set_optimizer_fn(module):
            for param in param_seq:
                param_ref = self.__get_param_ref(param)
                self._params[param_ref] = param
                self._optimizers[param_ref] = optimizer

        self._module.add_module(module_id, module)

    def remove_module(self, module_id: ModuleID) -> None:
        """Remove a module from the trainer.

        Args:
            module_id: The id of the module to remove.

        """
        module = self._module[module_id]
        parameters = self.__get_parameters(module)
        for param in parameters:
            param_ref = self.__get_param_ref(param)
            del self._params[param_ref]
            del self._optimizers[param_ref]

        self._module.remove_module(module_id)

    def _make_module(self) -> MultiAgentRLModule:
        """Construct the multi-agent RL module for the trainer.

        This method uses `self.module_class` and `self.module_kwargs` to construct the
        module. If the module_class is a single agent RL module it will be wrapped to a
        multi-agent RL module.

        Returns:
            The constructed module.
        """

        if issubclass(self.module_class, MultiAgentRLModule):
            module = self.module_class.from_multi_agent_config(**self.module_kwargs)
        elif issubclass(self.module_class, RLModule):
            module = self.module_class.from_model_config(
                **self.module_kwargs
            ).as_multi_agent()
        else:
            raise ValueError(
                f"Module class {self.module_class} is not a subclass of "
                f"RLModule or MultiAgentRLModule."
            )

        return module

    def build(self) -> None:
        """Initialize the model."""
        if self.distributed:
            self._module = self._make_distributed()
        else:
            self._module = self._make_module()

        for param_seq, optimizer in self.configure_optimizers():
            for param in param_seq:
                param_ref = self.__get_param_ref(param)
                self._params[param_ref] = param
                self._optimizers[param_ref] = optimizer

    def do_distributed_update(self, batch: MultiAgentBatch) -> Mapping[str, Any]:
        """Perform a distributed update on this Trainer.

        Args:
            batch: A batch of data.

        Returns:
            A dictionary of results.
        """
        raise NotImplementedError

    def _make_distributed(self) -> MultiAgentRLModule:
        """Initialize this trainer in a distributed training setting.

        This method should be overriden in the framework specific trainer. It is
        expected the the module creation is wrapped in some context manager that will
        handle the distributed training. This is a common patterns used in torch and
        tf.

        Returns:
            The distributed module.
        """
        raise NotImplementedError

    def __get_param_ref(self, param: ParamType) -> Hashable:
        """Returns a reference to a parameter.

        In torch this is simply the parameter itself. In tf this is the .ref() of the
        variable. The purpose is to retrieve a unique reference to the parameters.
        """
        if torch and isinstance(param, torch.nn.Parameter):
            return param
        elif tf and isinstance(param, tf.Variable):
            return param.ref()
        else:
            raise ValueError(
                "The parameter must be a torch.nn.Parameter or a tf.Variable."
            )

    def __get_parameters(self, module: RLModule) -> Sequence[ParamType]:
        """Returns the parameters of a module."""
        if torch and isinstance(module, torch.nn.Module):
            return module.parameters()
        elif tf and isinstance(module, tf.keras.Model):
            return module.trainable_variables
        else:
            raise ValueError(
                "The module must be a torch.nn.Module or a tf.keras.Model."
            )

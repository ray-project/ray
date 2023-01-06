import abc
from typing import Any, Mapping, NewType, Tuple, Union, Type

from ray.rllib.core.rl_module.rl_module import RLModule, ModuleID
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.core.optim.rl_optimizer import RLOptimizer
from ray.rllib.core.optim.marl_optimizer import MultiAgentRLOptimizer
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import TensorType

Optimizer = NewType("Optimizer", object)


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

    def __init__(
        self,
        module_class: Union[Type[RLModule], Type[MultiAgentRLModule]],
        module_kwargs: Mapping[str, Any],
        optimizer_class: Union[Type[RLOptimizer], Type[MultiAgentRLOptimizer]],
        optimizer_kwargs: Mapping[str, Any],
        scaling_config: Mapping[str, Any],
        distributed: bool = False,
    ):
        self.module_class = module_class
        self.module_kwargs = module_kwargs
        self.optimizer_class = optimizer_class
        self.optimizer_kwargs = optimizer_kwargs
        self.scaling_config = scaling_config
        self.distributed = distributed
        self._rl_optimizers: Mapping[ModuleID, Any] = {}

    @abc.abstractmethod
    def compute_loss(
        self, batch: Mapping[str, Any], fwd_out: Mapping[str, Any]
    ) -> Union[TensorType, Mapping[str, Any]]:
        """Computes variables for optimizing self._module based on fwd_out.

        Args:
            fwd_out: Output from a call to `forward_train` on self._module during
                training.
            batch: The data that was used to compute fwd_out.

        Returns:
            Either a single loss tensor which can be used for computing
            gradients through, or a dictionary of losses. NOTE the dictionary
            must contain one protected key "total_loss" which will be used for
            computing gradients through.
        """

    def update(self, batch: MultiAgentBatch) -> Mapping[str, Any]:
        """Perform an update on this Trainer.

        Args:
            batch: A batch of data.

        Returns:
            A dictionary of results.
        """
        if not self.distributed:
            fwd_out = self._module.forward_train(batch)
            loss = self.compute_loss(batch, fwd_out)
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

    def set_state(self, state: Mapping[str, Any]) -> None:
        """Set the state of the trainer.

        Args:
            state: The state of the optimizer and module. Can be obtained
                from `get_state`.

        """
        self._rl_optimizer.set_state(state.get("optimizer_state", {}))
        self._module.set_state(state.get("module_state", {}))

    def get_state(self) -> Mapping[str, Any]:
        """Get the state of the trainer.

        Returns:
            The state of the optimizer and module.

        """
        return {
            "module_state": self._module.get_state(),
            "optimizer_state": self._rl_optimizer.get_state(),
        }

    def add_module(
        self,
        module_id: ModuleID,
        module_cls: Type[RLModule],
        module_kwargs: Mapping[str, Any],
        optimizer_cls: Type[RLOptimizer],
        optimizer_kwargs: Mapping[str, Any],
    ) -> None:
        """Add a module to the trainer.

        Args:
            module_id: The id of the module to add.
            module_cls: The module class to add.
            module_kwargs: The config for the module.
            optimizer_cls: The optimizer class to use.
            optimizer_kwargs: The config for the optimizer.

        """
        raise NotImplementedError

    def remove_module(self, module_id: ModuleID) -> None:
        """Remove a module from the trainer.

        Args:
            module_id: The id of the module to remove.

        """
        self._module.remove_module(module_id)
        del self._rl_optimizers[module_id]

    @abc.abstractmethod
    def init_module(self, config: Mapping[str, Any]) -> RLModule:
        """Initialize the RLModule or MARLModule that is going to be trained.

        Args:
            config: The config to use for the initializing the module.

        Returns:
            The initialized module.

        Note: if an RLModule is returned it will be wrapped in as a
            MultiAgentRLModule.
        """

    @abc.abstractmethod
    def init_optimizers(self, module: MultiAgentRLModule) -> NestedDict[Optimizer]:
        """Configure the optimizer(s) to use.

        Returns:
            The optimizer(s) to use for updating the module being.
        """

    def init_trainer(self) -> None:
        """Initialize the model."""
        if self.distributed:
            self._module, self._rl_optimizer = self._make_distributed()
        else:
            module = self.init_module(self.module_kwargs)

            if not isinstance(module, MultiAgentRLModule) and isinstance(
                module, RLModule
            ):
                module = module.as_multi_agent()

            if not (
                isinstance(module, MultiAgentRLModule) or isinstance(module, RLModule)
            ):
                raise ValueError(
                    f"Module class {self.module_class} is not a subclass of "
                    f"RLModule or MultiAgentRLModule."
                )
            self._rl_optimizers = self.configure_optimizers()

    def do_distributed_update(self, batch: MultiAgentBatch) -> Mapping[str, Any]:
        """Perform a distributed update on this Trainer.

        Args:
            batch: A batch of data.

        Returns:
            A dictionary of results.
        """
        raise NotImplementedError

    def _make_distributed(self) -> Tuple[RLModule, RLOptimizer]:
        """Initialize this trainer in a distributed training setting.

        Returns:
            The distributed module and optimizer.
        """
        raise NotImplementedError

import abc
from typing import Any, Mapping, Tuple, Union, Type

from ray.rllib.core.rl_module.rl_module import RLModule, ModuleID
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.core.optim.rl_optimizer import RLOptimizer
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import TensorType


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
        distributed: bool = False,
    ):
        self.module_class = module_class
        self.module_kwargs = module_kwargs
        self.scaling_config = scaling_config
        self.distributed = distributed

    @abc.abstractmethod
    def _configure_optimizers(self):
        pass

    @abc.abstractmethod
    def compute_loss(
        self,
        fwd_out: MultiAgentBatch,
        batch: MultiAgentBatch,
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
        # self.set_autograd_optimizer_state(state.get("optimizer_state"))
        self._module.set_state(state.get("module_state", {}))

    def get_state(self) -> Mapping[str, Any]:
        """Get the state of the trainer.

        Returns:
            The state of the optimizer and module.

        """
        return {
            "module_state": self._module.get_state(),
            # "optimizer_state": self.get_autograd_optimizer_state(),
        }

    def add_module(
        self,
        module_id: ModuleID,
        module_cls: Type[RLModule],
        module_kwargs: Mapping[str, Any],
        optimizer_cls: Type[RLOptimizer] = None,
        optimizer_kwargs: Mapping[str, Any] = None,
    ) -> None:
        """Add a module to the trainer.

        Args:
            module_id: The id of the module to add.
            module_cls: The module class to add.
            module_kwargs: The config for the module.
            optimizer_cls: The optimizer class to use.
            optimizer_kwargs: The config for the optimizer.

        """
        module = module_cls.from_model_config(**module_kwargs)
        self._module.add_module(module_id, module)

        # TODO: Figure the optimizer part out.

    def remove_module(self, module_id: ModuleID) -> None:
        """Remove a module from the trainer.

        Args:
            module_id: The id of the module to remove.

        """
        self._module.remove_module(module_id)

    def init_trainer(self) -> None:
        """Initialize the model."""
        if self.distributed:
            self._module, self._rl_optimizer = self._make_distributed()
        else:
            if issubclass(self.module_class, MultiAgentRLModule):
                self._module = self.module_class.from_multi_agent_config(
                    **self.module_kwargs
                )
            elif issubclass(self.module_class, RLModule):
                self._module = self.module_class.from_model_config(
                    **self.module_kwargs
                ).as_multi_agent()
            else:
                raise ValueError(
                    f"Module class {self.module_class} is not a subclass of "
                    f"RLModule or MultiAgentRLModule."
                )

        self.params = []
        self.optimizers = []
        for param, optimizer in self._configure_optimizers():
            self.params.append(param)
            self.optimizers.append(optimizer)

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

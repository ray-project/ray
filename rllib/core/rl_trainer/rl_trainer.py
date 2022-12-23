import abc
from typing import Any, Mapping, Union, Type

from ray.rllib.core.rl_module.rl_module import RLModule, ModuleID
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.core.optim.rl_optimizer import RLOptimizer
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import TensorType


class RLTrainer:
    """Base class for rllib algorithm trainers.

    Args:
        module_class: The (MA)RLModule class to use.
        module_config: The config for the (MA)RLModule.
        optimizer_class: The optimizer class to use.
        optimizer_config: The config for the optimizer.

    """

    def __init__(
        self,
        module_class: Union[Type[RLModule], Type[MultiAgentRLModule]],
        module_config: Mapping[str, Any],
        optimizer_class: Type[RLOptimizer],
        optimizer_config: Mapping[str, Any],
        scaling_config: Mapping[str, Any],
        distributed: bool = False,
    ):
        self.module_class = module_class
        self.module_config = module_config
        self.optimizer_class = optimizer_class
        self.optimizer_config = optimizer_config
        self.distributed = distributed

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
        batch = convert_to_numpy(batch)
        return {
            "avg_reward": batch["rewards"].mean(),
            **loss_numpy,
        }

    def update(self, batch: SampleBatch) -> Mapping[str, Any]:
        """Perform an update on this Trainer.

        Args:
            batch: A batch of data.

        Returns:
            A dictionary of results.
        """
        if not self.distributed:
            fwd_out = self._module.forward_train(batch)
            loss = self._rl_optimizer.compute_loss(batch, fwd_out)
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
        module_config: Mapping[str, Any],
        optimizer_cls: Type[RLOptimizer],
        optimizer_config: Mapping[str, Any],
    ) -> None:
        """Add a module to the trainer.

        Args:
            module_id: The id of the module to add.
            module_cls: The module class to add.
            module_config: The config for the module.
            optimizer_cls: The optimizer class to use.
            optimizer_config: The config for the optimizer.

        """
        module = module_cls.from_model_config(**module_config)
        self._module.add_module(module_id, module)
        optimizer = optimizer_cls(module, optimizer_config)
        self._rl_optimizer.add_optimizer(module_id, optimizer)

    def remove_module(self, module_id: ModuleID) -> None:
        """Remove a module from the trainer.

        Args:
            module_id: The id of the module to remove.

        """
        self._module.remove_module(module_id)
        self._rl_optimizer.remove_optimizer(module_id)

    def init_trainer(self) -> None:
        """Initialize the model."""
        if self.distributed:
            self._module, self._rl_optimizer = self.make_distributed()
        else:
            self._module = self.module_class.from_model_config(
                **self.module_config
            ).as_multi_agent()
            self._rl_optimizer = self.optimizer_class(
                self._module, self.optimizer_config
            ).as_multi_agent()

    def do_distributed_update(self, batch: MultiAgentBatch) -> Mapping[str, Any]:
        """Perform a distributed update on this Trainer.

        Args:
            batch: A batch of data.

        Returns:
            A dictionary of results.
        """
        raise NotImplementedError

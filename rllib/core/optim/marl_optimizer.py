from collections.abc import Mapping as collections_mapping
from typing import Any, Dict, Mapping, Union, Type
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.core.optim.rl_optimizer import RLOptimizer
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TensorType


class MultiAgentRLOptimizer(RLOptimizer):
    def __init__(self, rl_optimizers: Mapping[ModuleID, RLOptimizer]):
        """Base class for defining optimizers for MultiAgentRLModules.

        Args:
            rl_optimizers: The named mapping of RLOptimizer instances to use

        Example:
        .. code-block:: python

            module = MyMultiAgentRLModule(...)
            rl_optimizer_classes = {"module_1": MyRLOptimizer,
                "module_2": MyRLOptimizer, ...}

            optim_kwargs = {"module_1": {"lr": 0.001, ...},
                "module_2": {"lr": 0.001, ...}, ...}

            marl_optim = MultiAgentRLOptimizer.from_marl_module(module,
                rl_optimizer_classes, optim_kwargs)

            ma_sample_batch = ...
            fwd_out = module.forward_train(ma_sample_batch)

            loss_dict = marl_optim.compute_loss(fwd_out, ma_sample_batch)

            # compute gradients of loss w.r.t. trainable variables
            ...
            for module_id, rl_optim in marl_optim.get_optimizers().items():
                for net_id, net_optim in rl_optim.get_optimizers().items():
                    net_optim.step()
                    net_optim.zero_grad()

        """
        self._optimizers = rl_optimizers

    @classmethod
    def from_marl_module(
        cls,
        module: MultiAgentRLModule,
        rl_optimizer_classes: Dict[ModuleID, Type[RLOptimizer]],
        optim_kwargs: Mapping[ModuleID, Mapping[str, Any]],
    ) -> "MultiAgentRLOptimizer":
        """Create a MultiAgentRLOptimizer from a MultiAgentRLModule.

        Args:
            module: The MultiAgentRLModule to optimize.
            rl_optimizer_classes: The named mapping of RLOptimizer classes to
                use for each sub-module of the MultiAgentRLModule.
            optim_kwargs: The named mapping of optimizer configs to use for

        Returns:
            A MultiAgentRLOptimizer.

        """
        assert len(rl_optimizer_classes) == len(optim_kwargs)
        assert set(rl_optimizer_classes.keys()) == set(optim_kwargs.keys())
        optimizers = {}
        for module_id in module.keys():
            submodule = module[module_id]
            config = optim_kwargs[module_id]
            optimizers[module_id] = rl_optimizer_classes[module_id].from_module(
                submodule, config
            )
        return cls(optimizers)

    @override(RLOptimizer)
    def compute_loss(
        self,
        fwd_out: Mapping[ModuleID, Mapping[str, Any]],
        batch: Mapping[ModuleID, Mapping[str, Any]],
    ) -> Union[TensorType, Mapping[str, Any]]:
        total_loss = None
        ret = {}
        for module_id in batch.keys():
            optimizer = self._optimizers[module_id]
            loss = optimizer.compute_loss(fwd_out[module_id], batch[module_id])
            if not isinstance(loss, collections_mapping):
                loss = {"total_loss": loss}
            if total_loss is None:
                total_loss = loss["total_loss"]
            else:
                total_loss += loss["total_loss"]
            ret[module_id] = loss
        ret["total_loss"] = total_loss
        return ret

    @override(RLOptimizer)
    def get_state(self) -> Mapping[ModuleID, Mapping[str, Any]]:
        """Get the optimizer state.

        Returns:
            The optimizer state.

        """
        return {
            module_id: optimizer.get_state()
            for module_id, optimizer in self._optimizers.items()
        }

    @override(RLOptimizer)
    def set_state(self, state: Mapping[ModuleID, Mapping[str, Any]]) -> None:
        """Set the optimizer state.

        Args:
            state: The optimizer state to set.

        """
        for module_id, sub_optimizer_state in state.items():
            optimizer = self._optimizers[module_id]
            optimizer.set_state(sub_optimizer_state)

    @override(RLOptimizer)
    def as_multi_agent(self) -> "MultiAgentRLOptimizer":
        return self

    def add_optimizer(self, module_id: ModuleID, optimizer: RLOptimizer) -> None:
        """Add a new optimizer to the multi-agent optimizer.

        Args:
            module_id: The module id of the optimizer.
            optimizer: The optimizer to add.

        """
        self._optimizers[module_id] = optimizer

    def remove_optimizer(self, module_id: ModuleID) -> None:
        """Remove an optimizer from the multi-agent optimizer.

        Args:
            module_id: The module id of the optimizer to remove.

        """
        del self._optimizers[module_id]

    def _configure_optimizers(self) -> None:
        # Do not override this.
        return self._optimizers

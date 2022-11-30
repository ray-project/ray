import abc
from typing import Mapping, Any

from ray.rllib.core.rl_module import RLModule
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
    PublicAPI,
)


@PublicAPI(stability="beta")
class RLOptimizer(abc.ABC):
    """Base class for defining a loss function and optimizer for a RLModule.

    Args:
        rl_module: The RLModule that will be optimized.
        config: The configuration for the optimizer.

    Abstract Methods:
        compute_loss: computing a loss to optimize rl_module over.
        construct_optimizers: constructing the optimizers for rl_module.

    Example:
    .. code-block:: python

        module = RLModule(...)
        rl_optim = RLOptimizer(module, config)
        optimizers = rl_optim.construct_optimizers()
        sample_batch = ...
        fwd_out = module.forward_train(sample_batch)
        loss_dict = rl_optim.compute_loss(fwd_out)

        # compute gradients of loss w.r.t. trainable variables
        ...

        for optim in optimizers:
            optim.step()
            optim.zero_grad()

    """

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def __init__(self, module: RLModule, config: Mapping[str, Any]):
        self.module = module
        self._config = config

    @abc.abstractmethod
    def compute_loss(self, fwd_out: Mapping[str, Any]) -> Mapping[str, Any]:
        """Computes variables for optimizing self._module based on fwd_out.

        Args:
            fwd_out: Output from a forward pass on self._module during
                training.

        Returns:
            A dictionary of tensors used for optimizing self._module.
        """

    @abc.abstractmethod
    def construct_optimizers(self):
        """Constructs the optimizers for the module's parameters.

        Returns:
            A list of optimizers for the module's parameters.
        """

    @OverrideToImplementCustomLogic
    @staticmethod
    def on_after_compute_loss(loss_dict: Mapping[str, Any]):
        """Called after `compute_loss` is called."""
        return loss_dict

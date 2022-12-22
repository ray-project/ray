import abc
from typing import Any, Mapping, Union

from ray.rllib.core.rl_module import RLModule
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.typing import TensorType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="beta")
class RLOptimizer(abc.ABC):
    """Base class for defining a loss function and optimizer for a RLModule.

    Args:
        rl_module: The RLModule that will be optimized.
        config: The configuration for the optimizer.

    Abstract Methods:
        compute_loss: computing a loss to optimize rl_module over.
        _configure_optimizers: constructing the optimizers for rl_module.
        get_state: getting the state of the optimizer.
        set_state: setting the state of the optimizer.

    Example:
    .. code-block:: python

        module = RLModule(...)
        rl_optim = RLOptimizer(module, config)
        sample_batch = ...
        fwd_out = module.forward_train(sample_batch)
        loss_dict = rl_optim.compute_loss(fwd_out, sample_batch)

        # compute gradients of loss w.r.t. trainable variables
        ...

        for optim in rl_optim.get_optimizers():
            optim.step()
            optim.zero_grad()

    """

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def __init__(self, module: RLModule, config: Mapping[str, Any]):
        self.module = module
        self._config = config
        self._optimizers = self._configure_optimizers()

    @abc.abstractmethod
    def compute_loss(
        self, fwd_out: Mapping[str, Any], batch: Mapping[str, Any]
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

    @abc.abstractmethod
    def _configure_optimizers(self) -> Mapping[str, Any]:
        """Configures the optimizers for self._module.

        Returns:
            A map of optimizers to be used for optimizing self._module.
        """

    def get_optimizers(self) -> Mapping[str, Any]:
        """Returns the map of optimizers for this RLOptimizer."""
        return self._optimizers

    @abc.abstractmethod
    def get_state(self) -> Mapping[str, Any]:
        """Returns the optimizer state.

        Returns:
            The optimizer state.
        """

    @abc.abstractmethod
    def set_state(self, state: Mapping[str, Any]):
        """Sets the optimizer state.

        Args:
            state: The optimizer state.
        """

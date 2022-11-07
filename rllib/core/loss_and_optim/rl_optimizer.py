import abc
from typing import Mapping, Any, List

from ray.rllib import SampleBatch
from ray.rllib.core.rl_module import RLModule
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)

class RLOptimizer(abc.ABC):
    """Base class for defining a loss function and optimizer for a RLModule.
    
    Args:
        rl_module: The RLModule that will be optimized.
        config: The configuration for the optimizer.

    Abstract Methods:
        compute_loss: computing a loss to optimize rl_module over.
        compute_gradients_and_apply: computing the gradients of the loss,
            outputted by `compute_loss` and applying them to the rl_module.

    Example:
    .. code-block:: python

        module = RLModule(...)
        rl_optim = RLOptimizer(module, ...)
        sample_batch = ...
        fwd_out = module.forward_train(sample_batch)
        loss_dict = rl_optim.compute_loss(fwd_out)
        infos_and_stats = rl_optim.compute_gradients_and_apply(loss_dict)

        # for checkpointing the RLOptimizer instance
        rl_optim_state = rl_optim.get_state()
        rl_optim.set_state(rl_optim_state)

    """
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def __init__(self, module: RLModule, config: Mapping[str, Any]):
        self._module = module
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

    @OverrideToImplementCustomLogic
    def on_compute_loss_start(self, fwd_out: Mapping[str, Any], fwd_in: SampleBatch):
        """Called before `compute_loss` is called."""
        pass

    @OverrideToImplementCustomLogic
    def on_compute_loss_end(self, loss_dict: Mapping[str, Any], fwd_out: Mapping[str, Any], fwd_in: SampleBatch):
        """Called after `compute_loss` is called."""
        pass

    @OverrideToImplementCustomLogic
    def on_compute_gradients_start(self, loss_dict: Mapping[str, Any]):
        pass

    @OverrideToImplementCustomLogic
    def on_compute_gradients_end(self, loss_dict: Mapping[str, Any], gradients_dict: Mapping[str, Any]):
        pass

    @OverrideToImplementCustomLogic
    def on_apply_gradients_start():
        pass

    # @abc.abstractmethod
    # def compute_gradients(self, optimization_vars: Mapping[str, Any], **kwargs) -> Mapping[str, Any]:
    #     """Perform an update on self._module
        
    #         For example compute and apply gradients to self._module if
    #             necessary.
        
    #     Args:
    #         optimization_vars: named variables used for optimizing self._module 
            
    #     Returns:
    #         A dictionary of extra information and statistics.
    #     """

    @abc.abstractmethod
    def get_state(self) -> Mapping[str, Any]:
        """Returns the state dict of this RLOptimizer instance."""

    @abc.abstractmethod
    def set_state(self, state_dict: Mapping[str, Any]) -> None:
        """Set the state of this RLOptimizer instance. """

    

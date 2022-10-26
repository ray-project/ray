import abc
from typing import Mapping, Any, List

from ray.rllib.core.rl_module import RLModule
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule

class RLLossAndOptim(abc.ABC):
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
        rl_l_and_a = RLLossAndOptim(module, ...)
        sample_batch = ...
        fwd_out = module.forward_train(sample_batch)
        loss_dict = rl_l_and_a.compute_loss(fwd_out)
        infos_and_stats = rl_l_and_a.compute_gradients_and_apply(loss_dict)

        # for checkpointing the RLLossAndOptim instance
        l_and_a_state = rl_l_and_a.get_state()
        rl_l_and_a.set_state(l_and_a_state)

    """

    def __init__(self, module: RLModule, config: Mapping[str, Any]):
        self._module = module
        self._optim_config = config

    @abc.abstractmethod
    def compute_loss(self, fwd_out: Mapping[str, Any]) -> Mapping[str, Any]:
        """Computes a loss based on fwd_out.

        Args:
            fwd_out: A dictionary of inputs for computing loss, typically
                generated from computed from self._module.forward_train.
        
        Returns:
            A dictionary of loss tensors used for computing the gradients of
            lossess with respect to fwd_out, and applying those gradients to
            self._module if necessary.
        """

    @abc.abstractmethod
    def compute_gradients_and_apply(self, loss_out: Mapping[str, Any], **kwargs) -> Mapping[str, Any]:
        """Compute and apply gradients to self._module if necessary.

        Computes the gradients for updating self._module based on loss_out and
        applies them to self._module if necessary. 

        """

    @abc.abstractmethod
    def get_state(self) -> Mapping[str, Any]:
        """Returns the state dict of this RLLossAndOptim instance."""

    @abc.abstractmethod
    def set_state(self, state_dict: Mapping[str, Any]) -> None:
        """Set the state of this RLLossAndOptim instance. """

    # Not sure how to make this work because I don't want to
    # create copies of the RLModule
    # @abc.abstractmethod
    # @classmethod
    # def get_multi_agent_class(cls):
    #     """Return a multi-agent-wrapper around this RLLossAndOptim"""

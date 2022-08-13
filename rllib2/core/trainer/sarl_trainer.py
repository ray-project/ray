
import abc
from typing import Any, Dict, Type, Optional
from torch.optim import Optimizer

from ray.rllib.policy.sample_batch import SampleBatch
from rllib2.core import RLTrainer, RLModule


class SARLTrainer(RLTrainer):
    """Single Agent Trainer."""

    def __init__(self) -> None:
        super().__init__()
        self._model: RLModule
    

    @abc.abstractmethod
    def compute_loss(self, batch: SampleBatch, fwd_out) -> Dict["LossID", "TensorType"]:
        """
        Computes the loss for each sub-module of the algorithm and returns the loss
        tensor computed for each loss_id that needs to get back-propagated and updated
        according to the corresponding optimizer.

        This method should use self.model.forward_train() to compute the forward-pass
        tensors required for training.

        Args:
            train_batch: SampleBatch to train with.

        Returns:
            Dict of optimizer names map their loss tensors.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def compute_grads_and_apply_if_needed(
        self,
        batch: BatchType, 
        fwd_out, 
        loss_out,
        apply_grad: bool=True,
        **kwargs
    ) -> Any:
        raise NotImplementedError
    

    def update(
        self, 
        batch: SampleBatch, 
        fwd_kwargs: Optional[Dict[str, Any]]=None, 
        loss_kwargs: Optional[Dict[str, Any]]=None, 
        grad_kwargs: Optional[Dict[str, Any]]=None, 
        **kwargs
    ) -> Any:
        fwd_kwargs = fwd_kwargs or {}
        loss_kwargs = loss_kwargs or {}
        grad_kwargs = grad_kwargs or {}

        self._model.train()
        fwd_out = self._model.forward_train(batch, **fwd_kwargs)
        loss_out = self.compute_loss(batch, fwd_out, **loss_kwargs)

        update_out = self.compute_grads_and_apply_if_needed(
            batch, fwd_out, loss_out, **grad_kwargs
        )

        return update_out



import abc
from typing import Any, Dict, Type
from rllib2.core.rl_trainer import RLTrainer
from ray.rllib.policy.sample_batch import SampleBatch
from torch.optim import Optimizer

class SARLTorchTrainer(RLTrainer):
    """Single Agent Torch Trainer.
    
    """

    def __init__(
        self,
        trainer_config: "SARLTrainerConfig",
        rl_module_class: Type["RLTorchModule"],
        rl_module_config: "RLTorchModuleConfig",
    ) -> None:
        super().__init__()

        self._config = trainer_config.build() # immutable
        self._module = rl_module_class(rl_module_config) 

        # register optimizers
        self._optimizers: Dict[str, Optimizer] = self.make_optimizer()

    @property
    def config(self) -> "SARLTrainerConfig":
        return self._config

    @property
    def model(self) -> "RLTorchModule":
        return self._model

    @property
    def optimizers(self) -> Dict[str, Optimizer]:
        return self._optimizers

    @abc.abstractmethod
    def make_optimizer(self) -> Dict[str, Optimizer]:
        raise NotImplementedError

    @abc.abstractmethod
    def loss(self, train_batch: SampleBatch, fwd_train_dict: "RLModuleOutput") -> Dict[LossID, torch.Tensor]:
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
    

    def update(self, train_batch: SampleBatch, **kwargs) -> Any:
        self.model.train()
        fwd_train_dict = self._module.forward_train(train_batch)
        loss_dict = self.loss(train_batch, fwd_train_dict)

        for loss_key, loss_value in loss_dict.items():
            self._optimizers[loss_key].zero_grad()
            loss_value.backward()
            self._optimizers[loss_key].step()

        # TODO: decide what needs to be returned
        return {'losses': loss_dict}

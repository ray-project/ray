"""Multi Agent Torch Trainer."""
from typing import Any

from rllib2.core.rl_trainer import RLTrainer
from ray.rllib.policy.sample_batch import MultiAgentBatch

class MARLTorchTrainer(RLTrainer):
    """An RL Trainer class made for multi Agent Torch Trainer

    This abstract class has access to all modules for all agents so that it can form any update rule it wants for all different cases of multi agent scenarios. This module does not implement any update rule and hence does not have a loss method. The loss method should be created (if needed) in the subclasses.
    
    """

    def __init__(self) -> None:
        super().__init__()

        self._config = trainer_config.to_immutable() # immutable
        self._module = self.make_module()

        # register optimizers
        self._optimizers: Dict[str, Optimizer] = self.make_optimizer()


    @abc.abstractmethod
    def make_optimizers(self):
        raise NotImplementedError

    @abc.abstractmethod
    def update(self, samples: MultiAgentBatch, **kwargs) -> Any:
        raise NotImplementedError

    def make_module(self) -> RLTorchModuleDict:
        module = RLTorchModuleDict(
            shared_modules = self._config.shared_modules,
            modules = self._config.modules,
        )

        return module


class IndependentMARLTorchTrainer(MARLTorchTrainer):
    """A MARLTrainer that trains each module through its own trainer's loss function.
    
    This trainer keeps track of a separate module and exactly one trainer for each 
    moodule. To update the modules, it runs their corresponding trainer.update() method 
    indepenedently, and performs an update step on all the losses at the same time.
    
    """

    def __init__(self) -> None:
        super().__init__()

        # create a dict to keep track of the trainers for each module
        self.module_trainers = self.make_module_trainers()

    def make_module_trainers(self):
        trainers = {}
        for pid, config in self.configs.items():
            rl_module = self.modules[pid]
            trainers[pid] = self._config.base_trainer(module=rl_module, config=config)
        return trainers

    def loss(self, 
        train_batch: MultiAgentBatch, 
        all_fwd_dicts, 
        losses
    ) -> Dict[str, torch.Tensor]:

        marl_loss_dict = {}
        for module_id, loss_dict in losses.items():
            for loss_key, loss_value in loss_dict.items():
                marl_loss_dict[f'{module_id}_{loss_key}'] = loss_value

        return marl_loss_dict


    def make_optimizers(self):
        optimizers = {}
        for module_id, trainer in self.module_trainers.items():
            for optim_key, optimizer in trainer.optimizers.items():
                optimizers[f'{module_id}_{optim_key}'] = optimizer
        return optimizers
    
    def update(self, ma_batch: MultiAgentBatch, **kwargs) -> Any:
        losses = {}
        all_fwd_dicts = {}
        for module_id, s_batch in ma_batch.items():
            module = self.module[module_id]
            trainer = self.module_trainers[module_id]

            # run forward train of each module on the corresponding sample batch
            fwd_dict = module.forward_train(s_batch)

            # compute the loss for each module 
            loss_dict = trainer.loss(s_batch, fwd_dict)

            # add them to the dicts
            all_fwd_dicts[module_id] = fwd_dict
            losses[module_id] = loss_dict

        # compute some total dict based on the forward passes and individual losses
        total_loss_dict = self.loss(ma_batch, all_fwd_dicts, losses)

        self.compute_grads_and_apply_if_needed(
            ma_batch,
            all_fwd_dicts, 
            total_loss_dict, 
            **grad_kwargs
            )


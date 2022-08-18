from .mixins import TorchTrainer
from rllib2.core.trainer.marl_trainer import MARLTrainer
from rllib2.core.trainer.sarl_trainer import SARLTrainer


class SARLTorchTrainer(SARLTrainer, TorchTrainer):
    """Single Agent Torch Trainer."""

    pass


class MARLTorchTrainer(MARLTrainer, TorchTrainer):
    """Single Agent Torch Trainer."""

    def make_optimizer(self) -> Dict[str, Optimizer]:
        marl_optimizers = {}
        for mid, trainer in self._module_trainers:
            optimizers = trainer.make_optimizer()
            for name, optimizer in optimizers.items():
                marl_optimizers[f'{mid}_{name}'] = optimizer
        return marl_optimizers
    

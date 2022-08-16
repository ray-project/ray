from rllib2.core.torch.mixins import TorchTrainer
from rllib2.core.trainer.marl_trainer import MARLTrainer
from rllib2.core.trainer.sarl_trainer import SARLTrainer


class SARLTorchTrainer(SARLTrainer, TorchTrainer):
    """Single Agent Torch Trainer."""

    pass


class MARLTorchTrainer(MARLTrainer, TorchTrainer):
    """Single Agent Torch Trainer."""

    pass

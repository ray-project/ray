
from rllib2.core.trainer.sarl_trainer import SARLTrainer
from rllib2.core.trainer.marl_trainer import MARLTrainer
from rllib2.core.torch.torch_trainer import TorchTrainer


class SARLTorchTrainer(SARLTrainer, TorchTrainer):
    """Single Agent Torch Trainer."""
    pass


class MARLTorchTrainer(MARLTrainer, TorchTrainer):
    """Single Agent Torch Trainer."""
    pass

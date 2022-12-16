from typing import Tuple
import tensorflow as tf

from ray.rllib.core.rl_trainer.rl_trainer import RLTrainer
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.optim.rl_optimizer import RLOptimizer

class TfRLTrainer(RLTrainer):
    """Base class for RLlib TF algorithm trainers."""
    def make_distributed(self) -> Tuple[RLModule, RLOptimizer]:
        self.strategy = tf.distribute.MultiWorkerMirroredStrategy()
        with self.strategy.scope():
            module = self.module_class.from_model_config(**self.module_config)
            optimizer = self.optimizer_class(self.optimizer_config)
        return module, optimizer

    def remove_module(self):
        self.module = None

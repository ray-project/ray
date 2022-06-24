from ray.rllib.algorithms.algorithm import Algorithm as Trainer, with_common_config
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig as TrainerConfig

__all__ = [
    "Trainer",
    "TrainerConfig",
    "with_common_config",
]

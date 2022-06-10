from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.utils.deprecation import deprecation_warning

TrainerConfig = AlgorithmConfig

deprecation_warning(
    old="ray.rllib.agents.trainer_config::TrainerConfig",
    new="ray.rllib.algorithms.algorithm_config::AlgorithmConfig",
    error=False,
)

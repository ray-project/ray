from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.agents.trainer_config::TrainerConfig",
    new="ray.rllib.algorithms.algorithm_config::AlgorithmConfig",
    error=False,
)

# Alias.
TrainerConfig = AlgorithmConfig

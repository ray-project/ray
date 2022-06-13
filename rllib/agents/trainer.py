from ray.rllib.algorithms.algorithm import (  # noqa
    COMMON_CONFIG,
    Algorithm,
    with_common_config,
)
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(old="rllib.agents::Trainer", new="rllib.algorithms::Algorithm")

# Alias.
Trainer = Algorithm

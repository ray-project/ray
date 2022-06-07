from ray.rllib.algorithms.algorithm import (
    Algorithm,
    COMMON_CONFIG,
    with_common_config,
)
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(old="rllib.agents::Trainer", new="rllib.algorithms::Algorithm")

# Alias.
Trainer = Algorithm
COMMON_CONFIG = COMMON_CONFIG  # noqa
with_common_config = with_common_config  # noqa

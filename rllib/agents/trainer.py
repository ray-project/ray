from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(old="rllib.agents::Trainer", new="rllib.algorithms::Algorithm")

# Alias.
Trainer = Algorithm

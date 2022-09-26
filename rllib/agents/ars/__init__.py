from ray.rllib.algorithms.ars.ars import ARS as ARSTrainer, DEFAULT_CONFIG
from ray.rllib.algorithms.ars.ars_tf_policy import ARSTFPolicy
from ray.rllib.algorithms.ars.ars_torch_policy import ARSTorchPolicy

__all__ = [
    "ARSTFPolicy",
    "ARSTorchPolicy",
    "ARSTrainer",
    "DEFAULT_CONFIG",
]

from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning("ray.rllib.agents.ars", "ray.rllib.algorithms.ars", error=True)

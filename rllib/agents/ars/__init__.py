from ray.rllib.algorithms.ars.ars import ARS as ARSTrainer
from ray.rllib.algorithms.ars.ars import DEFAULT_CONFIG
from ray.rllib.algorithms.ars.ars_tf_policy import ARSTFPolicy
from ray.rllib.algorithms.ars.ars_torch_policy import ARSTorchPolicy
from ray.rllib.utils.deprecation import deprecation_warning

__all__ = [
    "ARSTFPolicy",
    "ARSTorchPolicy",
    "ARSTrainer",
    "DEFAULT_CONFIG",
]


deprecation_warning("ray.rllib.agents.ars", "ray.rllib.algorithms.ars", error=False)

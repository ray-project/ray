from ray.rllib.algorithms.marwil.bc import BCTrainer, BC_DEFAULT_CONFIG
from ray.rllib.algorithms.marwil.marwil import MARWILTrainer, DEFAULT_CONFIG
from ray.rllib.algorithms.marwil.marwil_tf_policy import MARWILTFPolicy
from ray.rllib.algorithms.marwil.marwil_torch_policy import MARWILTorchPolicy

__all__ = [
    "BCTrainer",
    "BC_DEFAULT_CONFIG",
    "DEFAULT_CONFIG",
    "MARWILTFPolicy",
    "MARWILTorchPolicy",
    "MARWILTrainer",
]


from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    "ray.rllib.agents.marwil", "ray.rllib.algorithms.marwil", error=False
)

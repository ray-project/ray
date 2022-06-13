from ray.rllib.algorithms.bc.bc import BC as BCTrainer
from ray.rllib.algorithms.bc.bc import BC_DEFAULT_CONFIG, BCConfig
from ray.rllib.algorithms.marwil.marwil import DEFAULT_CONFIG
from ray.rllib.algorithms.marwil.marwil import MARWIL as MARWILTrainer
from ray.rllib.algorithms.marwil.marwil import MARWILConfig
from ray.rllib.algorithms.marwil.marwil_tf_policy import (
    MARWILTF1Policy,
    MARWILTF2Policy,
)
from ray.rllib.algorithms.marwil.marwil_torch_policy import MARWILTorchPolicy
from ray.rllib.utils.deprecation import deprecation_warning

__all__ = [
    "BCConfig",
    "BCTrainer",
    "MARWILConfig",
    "MARWILTF1Policy",
    "MARWILTF2Policy",
    "MARWILTorchPolicy",
    "MARWILTrainer",
    # Deprecated.
    "BC_DEFAULT_CONFIG",
    "DEFAULT_CONFIG",
]


deprecation_warning(
    "ray.rllib.agents.marwil", "ray.rllib.algorithms.[marwil|bc]", error=False
)

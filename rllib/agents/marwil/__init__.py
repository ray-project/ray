from ray.rllib.algorithms.bc.bc import BCConfig, BC as BCTrainer, BC_DEFAULT_CONFIG
from ray.rllib.algorithms.marwil.marwil import (
    DEFAULT_CONFIG,
    MARWILConfig,
    MARWIL as MARWILTrainer,
)
from ray.rllib.algorithms.marwil.marwil_tf_policy import (
    MARWILTF1Policy,
    MARWILTF2Policy,
)
from ray.rllib.algorithms.marwil.marwil_torch_policy import MARWILTorchPolicy

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


from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    "ray.rllib.agents.marwil", "ray.rllib.algorithms.[marwil|bc]", error=True
)

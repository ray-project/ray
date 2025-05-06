from ray.rllib.algorithms.marwil.marwil import (
    MARWIL,
    MARWILConfig,
)
from ray.rllib.algorithms.marwil.marwil_tf_policy import (
    MARWILTF1Policy,
    MARWILTF2Policy,
)
from ray.rllib.algorithms.marwil.marwil_torch_policy import MARWILTorchPolicy

__all__ = [
    "MARWIL",
    "MARWILConfig",
    # @OldAPIStack
    "MARWILTF1Policy",
    "MARWILTF2Policy",
    "MARWILTorchPolicy",
]

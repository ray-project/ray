from ray.rllib.algorithms.impala.impala import (
    IMPALA,
    IMPALAConfig,
    Impala,
    ImpalaConfig,
)
from ray.rllib.algorithms.impala.impala_tf_policy import (
    ImpalaTF1Policy,
    ImpalaTF2Policy,
)
from ray.rllib.algorithms.impala.impala_torch_policy import ImpalaTorchPolicy

__all__ = [
    "IMPALA",
    "IMPALAConfig",
    # @OldAPIStack
    "ImpalaTF1Policy",
    "ImpalaTF2Policy",
    "ImpalaTorchPolicy",
    # Deprecated names (lowercase)
    "ImpalaConfig",
    "Impala",
]

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
from ray.rllib.algorithms.impala.impala_learner import IMPALALearner
from ray.rllib.algorithms.impala.torch.impala_torch_learner import IMPALATorchLearner

__all__ = [
    "IMPALA",
    "IMPALAConfig",
    "IMPALALearner",
    "IMPALATorchLearner",
    # @OldAPIStack
    "ImpalaTF1Policy",
    "ImpalaTF2Policy",
    "ImpalaTorchPolicy",
    # Deprecated names (lowercase)
    "ImpalaConfig",
    "Impala",
]

from ray.rllib.algorithms.sac.sac import SAC as SACTrainer, DEFAULT_CONFIG
from ray.rllib.algorithms.sac.sac_tf_policy import SACTFPolicy
from ray.rllib.algorithms.sac.sac_torch_policy import SACTorchPolicy

from ray.rllib.algorithms.sac.rnnsac import (
    RNNSAC as RNNSACTrainer,
    DEFAULT_CONFIG as RNNSAC_DEFAULT_CONFIG,
)
from ray.rllib.algorithms.sac.rnnsac import RNNSACTorchPolicy

__all__ = [
    "DEFAULT_CONFIG",
    "SACTFPolicy",
    "SACTorchPolicy",
    "SACTrainer",
    "RNNSAC_DEFAULT_CONFIG",
    "RNNSACTorchPolicy",
    "RNNSACTrainer",
]

from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning("ray.rllib.agents.sac", "ray.rllib.algorithms.sac", error=True)

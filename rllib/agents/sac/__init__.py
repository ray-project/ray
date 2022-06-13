from ray.rllib.algorithms.sac.rnnsac import DEFAULT_CONFIG as RNNSAC_DEFAULT_CONFIG
from ray.rllib.algorithms.sac.rnnsac import RNNSAC as RNNSACTrainer
from ray.rllib.algorithms.sac.rnnsac import RNNSACTorchPolicy
from ray.rllib.algorithms.sac.sac import DEFAULT_CONFIG
from ray.rllib.algorithms.sac.sac import SAC as SACTrainer
from ray.rllib.algorithms.sac.sac_tf_policy import SACTFPolicy
from ray.rllib.algorithms.sac.sac_torch_policy import SACTorchPolicy
from ray.rllib.utils.deprecation import deprecation_warning

__all__ = [
    "DEFAULT_CONFIG",
    "SACTFPolicy",
    "SACTorchPolicy",
    "SACTrainer",
    "RNNSAC_DEFAULT_CONFIG",
    "RNNSACTorchPolicy",
    "RNNSACTrainer",
]


deprecation_warning("ray.rllib.agents.sac", "ray.rllib.algorithms.sac", error=False)

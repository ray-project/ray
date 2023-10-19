from ray.rllib.algorithms.r2d2.r2d2 import R2D2, R2D2Config
from ray.rllib.algorithms.r2d2.r2d2_tf_policy import R2D2TFPolicy
from ray.rllib.algorithms.r2d2.r2d2_torch_policy import R2D2TorchPolicy
from ray.rllib.algorithms import rllib_contrib_warning


rllib_contrib_warning("R2D2")

__all__ = [
    "R2D2",
    "R2D2Config",
    "R2D2TFPolicy",
    "R2D2TorchPolicy",
]

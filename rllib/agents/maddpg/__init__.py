from ray.rllib.algorithms.maddpg.maddpg import DEFAULT_CONFIG
from ray.rllib.algorithms.maddpg.maddpg import MADDPG as MADDPGTrainer
from ray.rllib.algorithms.maddpg.maddpg import MADDPGTFPolicy
from ray.rllib.utils.deprecation import deprecation_warning

__all__ = [
    "MADDPGTrainer",
    "MADDPGTFPolicy",
    "DEFAULT_CONFIG",
]


deprecation_warning(
    "ray.rllib.agents.maddpg",
    "ray.rllib.algorithms.maddpg",
    error=False,
)

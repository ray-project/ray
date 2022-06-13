from ray.rllib.algorithms.bandit.bandit import BanditLinTS as BanditLinTSTrainer
from ray.rllib.algorithms.bandit.bandit import BanditLinTSConfig
from ray.rllib.algorithms.bandit.bandit import BanditLinUCB as BanditLinUCBTrainer
from ray.rllib.algorithms.bandit.bandit import BanditLinUCBConfig
from ray.rllib.utils.deprecation import deprecation_warning

__all__ = [
    "BanditLinTSTrainer",
    "BanditLinUCBTrainer",
    "BanditLinTSConfig",
    "BanditLinUCBConfig",
]


deprecation_warning(
    "ray.rllib.agents.bandits", "ray.rllib.algorithms.bandits", error=False
)

from ray.rllib.algorithms.ppo import PPO, PPOConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/ddppo/",
    new="rllib_contrib/ddppo/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class DDPPOConfig(PPOConfig):
    pass


@Deprecated(
    old="rllib/algorithms/ddppo/",
    new="rllib_contrib/ddppo/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class DDPPO(PPO):
    pass

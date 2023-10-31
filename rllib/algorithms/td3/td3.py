from ray.rllib.algorithms.ddpg import DDPG, DDPGConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/td3/",
    new="rllib_contrib/td3/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class TD3Config(DDPGConfig):
    pass


@Deprecated(
    old="rllib/algorithms/td3/",
    new="rllib_contrib/td3/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class TD3(DDPG):
    pass

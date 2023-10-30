from ray.rllib.algorithms.ddpg import DDPG, DDPGConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/apex_ddpg/",
    new="rllib_contrib/apex_ddpg/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class ApexDDPGConfig(DDPGConfig):
    pass


@Deprecated(
    old="rllib/algorithms/apex_ddpg/",
    new="rllib_contrib/apex_ddpg/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class ApexDDPG(DDPG):
    pass

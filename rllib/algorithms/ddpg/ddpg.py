from ray.rllib.algorithms.simple_q import SimpleQ, SimpleQConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/ddpg/",
    new="rllib_contrib/ddpg/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class DDPGConfig(SimpleQConfig):
    pass


@Deprecated(
    old="rllib/algorithms/ddpg/",
    new="rllib_contrib/ddpg/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class DDPG(SimpleQ):
    pass

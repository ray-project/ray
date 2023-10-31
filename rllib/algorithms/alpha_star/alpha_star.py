from ray.rllib.algorithms.appo import APPO, APPOConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/alpha_star/",
    new="rllib_contrib/alpha_star/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class AlphaStarConfig(APPOConfig):
    pass


@Deprecated(
    old="rllib/algorithms/alpha_star/",
    new="rllib_contrib/alpha_star/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class AlphaStar(APPO):
    pass

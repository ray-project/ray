from ray.rllib.algorithms.algorithm import Algorithm, AlgorithmConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/mbmpo/",
    new="rllib_contrib/mbmpo/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class MBMPOConfig(AlgorithmConfig):
    pass


@Deprecated(
    old="rllib/algorithms/mbmpo/",
    new="rllib_contrib/mbmpo/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class MBMPO(Algorithm):
    pass

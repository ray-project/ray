from ray.rllib.algorithms.algorithm import Algorithm, AlgorithmConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/es/",
    new="rllib_contrib/es/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class ESConfig(AlgorithmConfig):
    pass


@Deprecated(
    old="rllib/algorithms/es/",
    new="rllib_contrib/es/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class ES(Algorithm):
    pass

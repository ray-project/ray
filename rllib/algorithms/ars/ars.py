from ray.rllib.algorithms.algorithm import Algorithm, AlgorithmConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/ars/",
    new="rllib_contrib/ars/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class ARSConfig(AlgorithmConfig):
    pass


@Deprecated(
    old="rllib/algorithms/ars/",
    new="rllib_contrib/ars/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class ARS(Algorithm):
    pass

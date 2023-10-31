from ray.rllib.algorithms.algorithm import Algorithm, AlgorithmConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/a3c/",
    new="rllib_contrib/a3c/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class A3CConfig(AlgorithmConfig):
    pass


@Deprecated(
    old="rllib/algorithms/a3c/",
    new="rllib_contrib/a3c/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class A3C(Algorithm):
    pass

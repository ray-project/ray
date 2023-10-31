from ray.rllib.algorithms.algorithm import Algorithm, AlgorithmConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/dt/",
    new="rllib_contrib/dt/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class DTConfig(AlgorithmConfig):
    pass


@Deprecated(
    old="rllib/algorithms/dt/",
    new="rllib_contrib/dt/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class DT(Algorithm):
    pass

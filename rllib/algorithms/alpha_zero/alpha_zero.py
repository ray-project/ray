from ray.rllib.algorithms.algorithm import Algorithm, AlgorithmConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/alpha_zero/",
    new="rllib_contrib/alpha_zero/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class AlphaZeroConfig(AlgorithmConfig):
    pass


@Deprecated(
    old="rllib/algorithms/alpha_zero/",
    new="rllib_contrib/alpha_zero/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class AlphaZero(Algorithm):
    pass

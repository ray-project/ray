from ray.rllib.algorithms.algorithm import Algorithm, AlgorithmConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/crr/",
    new="rllib_contrib/crr/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class CRRConfig(AlgorithmConfig):
    pass


@Deprecated(
    old="rllib/algorithms/crr/",
    new="rllib_contrib/crr/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class CRR(Algorithm):
    pass

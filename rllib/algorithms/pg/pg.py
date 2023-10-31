from ray.rllib.algorithms.algorithm import Algorithm, AlgorithmConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/pg/",
    new="rllib_contrib/pg/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class PGConfig(AlgorithmConfig):
    pass


@Deprecated(
    old="rllib/algorithms/pg/",
    new="rllib_contrib/pg/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class PG(Algorithm):
    pass

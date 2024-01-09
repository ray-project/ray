from ray.rllib.algorithms.algorithm import Algorithm, AlgorithmConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/dreamer/",
    new="rllib/algorithms/dreamerv3/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class DreamerConfig(AlgorithmConfig):
    pass


@Deprecated(
    old="rllib/algorithms/dreamer/",
    new="rllib/algorithms/dreamerv3/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class Dreamer(Algorithm):
    pass

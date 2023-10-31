from ray.rllib.algorithms.algorithm import Algorithm, AlgorithmConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/maml/",
    new="rllib_contrib/maml/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class MAMLConfig(AlgorithmConfig):
    pass


@Deprecated(
    old="rllib/algorithms/maml/",
    new="rllib_contrib/maml/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class MAML(Algorithm):
    pass

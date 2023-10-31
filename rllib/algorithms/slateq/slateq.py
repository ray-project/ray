from ray.rllib.algorithms.algorithm import Algorithm, AlgorithmConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/slate_q/",
    new="rllib_contrib/slate_q/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class SlateQConfig(AlgorithmConfig):
    pass


@Deprecated(
    old="rllib/algorithms/slate_q/",
    new="rllib_contrib/slate_q/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class SlateQ(Algorithm):
    pass

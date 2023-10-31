from ray.rllib.algorithms.algorithm import Algorithm, AlgorithmConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/maddpg/",
    new="rllib_contrib/maddpg/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class MADDPGConfig(AlgorithmConfig):
    pass


@Deprecated(
    old="rllib/algorithms/maddpg/",
    new="rllib_contrib/maddpg/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class MADDPG(Algorithm):
    pass

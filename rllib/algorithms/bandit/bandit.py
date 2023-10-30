from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/bandit/",
    new="rllib_contrib/bandit/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class BanditConfig(AlgorithmConfig):
    pass


@Deprecated(
    old="rllib/algorithms/bandit/",
    new="rllib_contrib/bandit/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class BanditLinTSConfig(BanditConfig):
    pass


@Deprecated(
    old="rllib/algorithms/bandit/",
    new="rllib_contrib/bandit/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class BanditLinUCBConfig(BanditConfig):
    pass


@Deprecated(
    old="rllib/algorithms/bandit/",
    new="rllib_contrib/bandit/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class BanditLinTS(Algorithm):
    pass


@Deprecated(
    old="rllib/algorithms/bandit/",
    new="rllib_contrib/bandit/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class BanditLinUCB(Algorithm):
    pass

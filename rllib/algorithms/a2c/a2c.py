from ray.rllib.algorithms.a3c import A3C, A3CConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/a2c/",
    new="rllib_contrib/a2c/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class A2CConfig(A3CConfig):
    pass


@Deprecated(
    old="rllib/algorithms/a2c/",
    new="rllib_contrib/a2c/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class A2C(A3C):
    pass

from ray.rllib.algorithms.dqn import DQN, DQNConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/r2d2/",
    new="rllib_contrib/r2d2/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class R2D2Config(DQNConfig):
    pass


@Deprecated(
    old="rllib/algorithms/r2d2/",
    new="rllib_contrib/r2d2/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class R2D2(DQN):
    pass

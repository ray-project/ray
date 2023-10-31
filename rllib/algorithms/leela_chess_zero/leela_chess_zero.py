from ray.rllib.algorithms.algorithm import Algorithm, AlgorithmConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/leela_chess_zero/",
    new="rllib_contrib/leela_chess_zero/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class LeelaChessZeroConfig(AlgorithmConfig):
    pass


@Deprecated(
    old="rllib/algorithms/leela_chess_zero/",
    new="rllib_contrib/leela_chess_zero/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class LeelaChessZero(Algorithm):
    pass

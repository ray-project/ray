from rllib_leela_chess_zero.leela_chess_zero.leela_chess_zero import (
    LeelaChessZero,
    LeelaChessZeroConfig,
)
from rllib_leela_chess_zero.leela_chess_zero.leela_chess_zero_model import (
    LeelaChessZeroModel,
)
from rllib_leela_chess_zero.leela_chess_zero.leela_chess_zero_policy import (
    LeelaChessZeroPolicy,
)

from ray.tune.registry import register_trainable

__all__ = [
    "LeelaChessZero",
    "LeelaChessZeroConfig",
    "LeelaChessZeroModel",
    "LeelaChessZeroPolicy",
]

register_trainable("rllib-contrib-leela-chess-zero", LeelaChessZero)

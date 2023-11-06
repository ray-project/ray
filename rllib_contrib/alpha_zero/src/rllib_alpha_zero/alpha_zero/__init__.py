from rllib_alpha_zero.alpha_zero.alpha_zero import (
    AlphaZero,
    AlphaZeroConfig,
    AlphaZeroDefaultCallbacks,
)
from rllib_alpha_zero.alpha_zero.alpha_zero_policy import AlphaZeroPolicy

from ray.tune.registry import register_trainable

__all__ = [
    "AlphaZeroConfig",
    "AlphaZero",
    "AlphaZeroDefaultCallbacks",
    "AlphaZeroPolicy",
]

register_trainable("rllib-contrib-alpha-zero", AlphaZero)

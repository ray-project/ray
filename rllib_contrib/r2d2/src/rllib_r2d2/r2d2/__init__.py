# Copyright 2023-onwards Anyscale, Inc. The use of this library is subject to the
# included LICENSE file.
from rllib_r2d2.r2d2.r2d2 import R2D2, R2D2Config

from ray.tune.registry import register_trainable

__all__ = [
    "R2D2",
    "R2D2Config",
]

register_trainable("rllib-contrib-r2d2", R2D2)

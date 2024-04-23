# Copyright 2023-onwards Anyscale, Inc. The use of this library is subject to the
# included LICENSE file.
from rllib_maml.maml.maml import MAML, MAMLConfig

from ray.tune.registry import register_trainable

__all__ = [
    "MAML",
    "MAMLConfig",
]

register_trainable("rllib-contrib-maml", MAML)

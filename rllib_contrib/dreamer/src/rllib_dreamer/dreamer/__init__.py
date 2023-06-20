from rllib_dreamer.dreamer.dreamer import Dreamer, DreamerConfig

from ray.tune.registry import register_trainable

__all__ = ["DreamerConfig", "Dreamer"]

register_trainable("rllib-contrib-dreamer", Dreamer)

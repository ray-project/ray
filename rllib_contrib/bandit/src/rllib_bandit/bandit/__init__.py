from rllib_bandit.bandit.bandit import (
    BanditLinTS,
    BanditLinTSConfig,
    BanditLinUCB,
    BanditLinUCBConfig,
)

from ray.tune.registry import register_trainable

__all__ = ["BanditLinTS", "BanditLinUCB", "BanditLinTSConfig", "BanditLinUCBConfig"]

register_trainable("rllib-contrib-bandit-lin-ts", BanditLinTS)
register_trainable("rllib-contrib-bandit-lin-ucb", BanditLinUCB)

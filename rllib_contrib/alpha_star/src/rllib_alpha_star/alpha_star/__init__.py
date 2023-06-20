from rllib_alpha_star.alpha_star.alpha_star import AlphaStar, AlphaStarConfig
from rllib_alpha_star.alpha_star.distributed_learners import DistributedLearners
from rllib_alpha_star.alpha_star.league_builder import (
    AlphaStarLeagueBuilder,
    LeagueBuilder,
    NoLeagueBuilder,
)

from ray.tune.registry import register_trainable

__all__ = [
    "AlphaStarConfig",
    "AlphaStar",
    "DistributedLearners",
    "LeagueBuilder",
    "AlphaStarLeagueBuilder",
    "NoLeagueBuilder",
]

register_trainable("rllib-contrib-alpha-star", AlphaStar)

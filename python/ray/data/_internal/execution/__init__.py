from .ranker import DefaultRanker, Ranker


def create_ranker() -> Ranker:
    """Create a ranker instance based on environment and configuration."""
    from ray._private.ray_constants import env_bool

    return DefaultRanker()

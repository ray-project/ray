from .ranker import DefaultRanker, Ranker


def create_ranker() -> Ranker:
    """Create a ranker instance based on environment and configuration."""
    from ray._private.ray_constants import env_bool

    # Check if RayTurbo ranker should be used
    if env_bool("RAY_DATA_USE_TURBO_RANKER", True):
        from ray.anyscale.data._internal.execution.ranker import LocationAwareRanker

        return LocationAwareRanker()
    else:
        return DefaultRanker()

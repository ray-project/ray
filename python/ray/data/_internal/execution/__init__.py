from .ranker import DefaultRanker, Ranker


def create_ranker() -> Ranker:
    """Create a ranker instance based on environment and configuration."""
    return DefaultRanker()

"""Runtime flags and env-backed constants for the Ray Data execution engine.

Defined in a dedicated module to avoid circular imports between
``streaming_executor`` and operator packages (e.g. ``actor_pool_map_operator``).
"""

from ray._common.utils import env_bool

# Grouped streaming-generator yields (block + metadata) for map actors when
# core actor backpressure is enabled. Must match ``yield_block_with_stats``.
CORE_ACTOR_BACKPRESSURE_NUM_OBJECTS_PER_YIELD: int = 2


def actor_only_backend_enabled() -> bool:
    """Return whether the actor-only Ray Data backend is enabled.

    Reads ``RAY_DATA_ACTOR_ONLY_BACKEND`` on every call (not cached) so tests can
    toggle the backend through the environment without clearing a cache.
    """
    return env_bool("RAY_DATA_ACTOR_ONLY_BACKEND", False)

"""Runtime flags and env-backed constants for the Ray Data execution engine.

Defined in a dedicated module to avoid circular imports between
``streaming_executor`` and operator packages (e.g. ``actor_pool_map_operator``).
"""

from typing import TYPE_CHECKING, Optional

from ray._common.utils import env_bool, env_float, env_integer
from ray.data._internal.util import GiB, MiB

if TYPE_CHECKING:
    from ray.data.context import DataContext

# Grouped streaming-generator yields (block + metadata) for map actors when
# core actor backpressure is enabled. Must match ``yield_block_with_stats``.
CORE_ACTOR_BACKPRESSURE_NUM_OBJECTS_PER_YIELD: int = 2

# The number of output bytes an actor can produce before its generator pulls
# are backpressured. When unset, a default is derived per node from
# ``OBJECT_STORE_OUTPUT_FRACTION`` and clamped to
# [PER_ACTOR_OBJECT_STORE_MIN_BYTES, PER_ACTOR_OBJECT_STORE_MAX_BYTES].
PER_ACTOR_OUTPUT_BYTES_BACKPRESSURE_LIMIT: Optional[int] = env_integer(
    "RAY_DATA_PER_ACTOR_OUTPUT_BYTES_BACKPRESSURE_LIMIT", None
)

# Fraction of each node's object-store memory that may be used for actor
# output buffering. Used to derive a default per-actor output-bytes
# backpressure limit when RAY_DATA_PER_ACTOR_OUTPUT_BYTES_BACKPRESSURE_LIMIT
# is unset.
OBJECT_STORE_OUTPUT_FRACTION: float = env_float(
    "RAY_DATA_OBJECT_STORE_OUTPUT_FRACTION", 0.5
)

# Fraction of a per-actor output-bytes budget used as the task's target max
# block size (0-1). Default 0.25 keeps ~4 sized blocks under the budget
# (e.g. 512MiB limit -> 128MiB blocks).
BLOCK_SIZE_TO_OUTPUT_BYTES_RATIO: float = env_float(
    "RAY_DATA_BLOCK_SIZE_TO_OUTPUT_BYTES_RATIO", 0.25
)

# Hard [min, max] bounds on the per-actor object-store output-bytes budget.
PER_ACTOR_OBJECT_STORE_MIN_BYTES: int = env_integer(
    "RAY_DATA_PER_ACTOR_OBJECT_STORE_MIN_BYTES", 16 * MiB
)
PER_ACTOR_OBJECT_STORE_MAX_BYTES: int = env_integer(
    "RAY_DATA_PER_ACTOR_OBJECT_STORE_MAX_BYTES", 2 * GiB
)

# The number of output blocks an actor can produce. If both the output bytes
# and blocks limits are set, whichever is hit first wins.
# NOTE: A default is needed to bound the number of object refs on the head
# node: ~100K refs can OOM it in practice. 20K is a reasonable default since
# pipelines usually have no more than ~5 operators.
PER_ACTOR_OUTPUT_BACKPRESSURE_LIMIT: Optional[int] = env_integer(
    "RAY_DATA_PER_ACTOR_OUTPUT_BACKPRESSURE_LIMIT", 20000
)

# The number of input tasks in flight per actor (per concurrency slot). Each
# task may accept multiple blocks, but the # of tasks cannot change. This is
# to allow for good pipelining.
PER_ACTOR_INPUT_BACKPRESSURE_LIMIT: int = env_integer(
    "RAY_DATA_PER_ACTOR_INPUT_BACKPRESSURE_LIMIT",
    2,  # roughly 2 * 128MiB = 256 MiB
)

# If True, refresh each running actor's GCS-reported state (ALIVE/RESTARTING/
# DEAD) and location on every pool refresh. This is an expensive operation and
# should only be enabled where actors may move or die out from under the pool
# (spot preemption, node/actor OOMs).
REFRESH_ACTOR_STATE: bool = env_bool("RAY_DATA_REFRESH_ACTOR_STATE", True)

# Seconds a PENDING actor may stay not-ready before the pool releases it,
# kills the handle, and frees its claim for re-placement. Covers pendings that
# can never resolve: the readiness probe is a task retried forever, so an
# actor stuck dying and restarting on a bad node is never ready AND never
# surfaces a RayError -- without this timeout the pending gate holds the
# operator's sizing indefinitely. Also covers stuck runtime-env installs.
# Generous by design: ordinary starts (model download included) finish well
# inside it.
PENDING_ACTOR_EXPIRY_S: int = env_integer("RAY_DATA_PENDING_ACTOR_EXPIRY_S", 300)

# Seconds a downscaled actor may stay in the draining state (no new tasks, but
# in-flight tasks still finishing or output blocks still unconsumed) before the
# pool emits a (debounced, aggregated) WARNING that it is holding node capacity.
DRAIN_STALL_WARN_S: int = env_integer("RAY_DATA_DRAIN_STALL_WARN_S", 120)


def actor_only_backend_enabled(
    data_context: Optional["DataContext"] = None,
) -> bool:
    """Return whether the actor-only Ray Data backend is enabled.

    ``data_context._actor_only_backend_override`` wins when set. Otherwise reads
    ``RAY_DATA_ACTOR_ONLY_BACKEND`` on every call (not cached) so tests can
    toggle the backend through the environment without clearing a cache.
    """
    if (
        data_context is not None
        and data_context._actor_only_backend_override is not None
    ):
        return data_context._actor_only_backend_override
    return env_bool("RAY_DATA_ACTOR_ONLY_BACKEND", False)

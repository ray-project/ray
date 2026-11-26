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

# Whether an actor-only map task shrinks its target block size to fit the
# actor's output budget (``budget * BLOCK_SIZE_TO_OUTPUT_BYTES_RATIO``). Off by
# default: a lineage re-run may land on an actor with a different budget, cut
# rows into different blocks, and break the positional "already delivered"
# check, which duplicates or drops rows.
BUDGET_BASED_BLOCK_SIZING: bool = env_bool(
    "RAY_DATA_ACTOR_ONLY_BUDGET_BASED_BLOCK_SIZING", False
)

# How long the withheld-deadlock escape hatch waits for a deadlocked actor's
# node peers to free output budget by draining, before it grants the actor
# bytes nothing backs (which can spill). Seconds.
WITHHELD_DEADLOCK_WAIT_MAX_S: float = env_float(
    "RAY_DATA_WITHHELD_DEADLOCK_WAIT_MAX_S", 60.0
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

# Seconds a PENDING actor may stay not-ready before the pool releases it,
# kills the handle, and frees its claim for re-placement. Covers pendings that
# can never resolve: an actor pinned (hard node affinity) to a node that went
# away is never placed AND never surfaces a RayError -- without this timeout
# the pending gate holds the operator's sizing indefinitely. Also covers stuck
# runtime-env installs.
# Generous by design: ordinary starts (model download included) finish well
# inside it.
PENDING_ACTOR_EXPIRY_S: int = env_integer("RAY_DATA_PENDING_ACTOR_EXPIRY_S", 300)

# Seconds a downscaled actor may stay in the draining state (no new tasks, but
# in-flight tasks still finishing or output blocks still unconsumed) before the
# pool emits a (debounced, aggregated) WARNING that it is holding node capacity.
DRAIN_STALL_WARN_S: int = env_integer("RAY_DATA_DRAIN_STALL_WARN_S", 120)

# --- OperatorSizer -----------------------------------------------------------

# How often (seconds) the sizer evaluates sizing. The executor calls in every
# scheduling-loop step (~100ms), but queue signals are noise below ~1s and
# actor startup dominates anything decided faster.
SIZER_CADENCE_S: float = env_float("RAY_DATA_SIZER_CADENCE_S", 2.0)

# Warmup equal-share: fraction of the allocation shared out across autoscaled
# operators; the rest is headroom for the sizing policy to upscale into.
SIZER_RESERVATION_RATIO: float = env_float("RAY_DATA_SIZER_RESERVATION_RATIO", 0.8)

# Backlog-target utilization divisor: once every upstream op has completed,
# an op upscales to at most its remaining backlog's actor count divided by
# this. Matches the standard actor autoscaler's default upscaling threshold.
SIZER_UTIL_THRESHOLD: float = env_float("RAY_DATA_SIZER_UTIL_THRESHOLD", 1.75)

# Step sizes after warmup. An op's first upscale multiplies its size by
# 1 + SIZER_UPSCALE_STEP and its first downscale by 1 - SIZER_DOWNSCALE_STEP.
# Each op then adapts its own steps: a step in the same direction as the last
# doubles, a step in the other direction halves (see policy/steps.py).
SIZER_UPSCALE_STEP: float = env_float("RAY_DATA_SIZER_UPSCALE_STEP", 0.5)
SIZER_DOWNSCALE_STEP: float = env_float("RAY_DATA_SIZER_DOWNSCALE_STEP", 0.2)

# A flag for the bottleneck model, which drives every sizing decision after
# warmup. Seconds of history behind each operator's busy / OBP / starved
# fractions. An op resized after warmup also waits this long, once its resize
# lands, before its next resize, so its window describes its new size.
SIZER_BOTTLENECK_WINDOW_S: float = env_float("RAY_DATA_SIZER_BOTTLENECK_WINDOW_S", 10.0)
# How far output-backpressured(producer) and starved(consumer) must differ
# before an edge's arrow picks a side; closer than this, the edge is a tie.
SIZER_BOTTLENECK_ARROW_MARGIN: float = env_float(
    "RAY_DATA_SIZER_BOTTLENECK_ARROW_MARGIN", 0.1
)
# Min severity for an op to be named a bottleneck: below it, the arrows'
# noise (e.g. a near-tie edge during ramp-up) rather than neighbours waiting.
SIZER_BOTTLENECK_MIN_SEVERITY: float = env_float(
    "RAY_DATA_SIZER_BOTTLENECK_MIN_SEVERITY", 0.1
)
# Seconds the sizer stays in warmup at most. Warmup normally ends once every
# critical operator has produced output; if one never does (e.g. it cannot get
# an actor), the sizer leaves warmup anyway, and the sizing policy can
# unstick it.
SIZER_WARMUP_TIMEOUT_S: float = env_float("RAY_DATA_SIZER_WARMUP_TIMEOUT_S", 300.0)


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

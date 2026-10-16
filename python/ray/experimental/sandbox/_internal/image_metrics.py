"""Metrics of a node's image cache and image builds, exported through Ray.

Each sandbox actor records what its own pulls and builds did; Ray's metrics
agent sums them per node. Outside a Ray worker -- a standalone
``SandboxRuntime`` -- every function here does nothing. The metrics are
created on first use, since ``ray.util.metrics`` registers each with the
process's metrics agent.
"""

import contextlib
import threading
import time
from typing import Dict, Iterator, Optional

_lock = threading.Lock()
_metrics: Optional[Dict[str, object]] = None

# From a cached COPY layer's few milliseconds to an hour-long step.
_SECONDS_BOUNDARIES = [0.01, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600]


def _get() -> Optional[Dict[str, object]]:
    global _metrics
    import ray

    if not ray.is_initialized():
        return None
    with _lock:
        if _metrics is None:
            from ray.util.metrics import Counter, Gauge, Histogram

            _metrics = {
                "requests": Counter(
                    "sandbox_image_requests",
                    description=(
                        "Images asked of a node's builder, by how each was "
                        "served: hit (cached), waited (another build on the "
                        "node made it), or built."
                    ),
                    tag_keys=("result",),
                ),
                "build_seconds": Histogram(
                    "sandbox_image_build_seconds",
                    description=(
                        "How long a node took to build an image, once it had "
                        "a build slot."
                    ),
                    boundaries=_SECONDS_BOUNDARIES,
                ),
                "phase_seconds": Histogram(
                    "sandbox_image_phase_seconds",
                    description=(
                        "Time spent in each phase of building an image's "
                        "layers: sandbox_create, commands, export, normalize, "
                        "copy, apply, verify."
                    ),
                    boundaries=_SECONDS_BOUNDARIES,
                    tag_keys=("phase",),
                ),
                "slot_wait_seconds": Histogram(
                    "sandbox_image_slot_wait_seconds",
                    description=(
                        "How long a build waited for one of its node's build "
                        "slots (RAY_SANDBOX_IMAGE_BUILD_CONCURRENCY)."
                    ),
                    boundaries=_SECONDS_BOUNDARIES,
                ),
                "evictions": Counter(
                    "sandbox_image_evictions",
                    description=(
                        "Images evicted from a node's cache: whole, or demoted "
                        "to their cached layer."
                    ),
                    tag_keys=("kind",),
                ),
                "cache_bytes": Gauge(
                    "sandbox_image_cache_bytes",
                    description=(
                        "Bytes in a node's image cache, as of its last "
                        "eviction scan."
                    ),
                ),
            }
        return _metrics


def count_request(result: str) -> None:
    """Count an image asked of the builder: ``hit``, ``waited`` or ``built``."""
    metrics = _get()
    if metrics is not None:
        metrics["requests"].inc(tags={"result": result})


def observe_build(seconds: float) -> None:
    metrics = _get()
    if metrics is not None:
        metrics["build_seconds"].observe(seconds)


def observe_slot_wait(seconds: float) -> None:
    metrics = _get()
    if metrics is not None:
        metrics["slot_wait_seconds"].observe(seconds)


def count_eviction(kind: str) -> None:
    """Count an eviction: ``whole``, or ``demote`` (only the flattened image)."""
    metrics = _get()
    if metrics is not None:
        metrics["evictions"].inc(tags={"kind": kind})


def set_cache_bytes(total: int) -> None:
    metrics = _get()
    if metrics is not None:
        metrics["cache_bytes"].set(total)


@contextlib.contextmanager
def phase(name: str) -> Iterator[None]:
    """Time a phase of a build, failed ones included."""
    started = time.monotonic()
    try:
        yield
    finally:
        metrics = _get()
        if metrics is not None:
            metrics["phase_seconds"].observe(
                time.monotonic() - started, tags={"phase": name}
            )

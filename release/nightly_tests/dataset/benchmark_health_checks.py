"""Cluster health checks for the release benchmark harness.

Ray-free so it can be unit tested without a Ray build; ``benchmark.py`` injects
all Ray and Prometheus access.
"""
import math
import time
from typing import Any, Callable, Collection, Dict, Iterable, List, Optional

IDLE_WORKER_EVICTION_METRIC_TYPE = "MemoryManager.IdleWorkerEviction.Total"

WORKER_OOM_METRIC = "ray_memory_manager_worker_eviction_total"
UNEXPECTED_WORKER_FAILURE_METRIC = "ray_node_manager_unexpected_worker_failure_total"

# Ray's default ``metrics_report_interval_ms``.
METRICS_REPORT_INTERVAL_S = 10

InstantSeries = List[Dict[str, Any]]


def summarize_instant_metric_series(series: InstantSeries) -> str:
    """Summarize ``sum(...) by (Type, Name)`` results as one line per series."""
    summaries = []
    for entry in series:
        labels = entry.get("metric", {})
        value = entry.get("value", [None, "unknown"])[-1]
        summaries.append(
            f"  - {labels.get('Name', 'unknown')} "
            f"({labels.get('Type', 'unknown')}): {value}"
        )
    return "\n".join(summaries)


def filter_idle_worker_kills(series: InstantSeries) -> InstantSeries:
    return [
        entry
        for entry in series
        if entry.get("metric", {}).get("Type") != IDLE_WORKER_EVICTION_METRIC_TYPE
    ]


def check_worker_failures(
    *,
    fail_on_worker_oom: bool,
    fail_on_unexpected_worker_failure: bool,
    query_positive_counters: Callable[[str], InstantSeries],
) -> List[str]:
    """Return a failure description for each enabled worker-failure check that fired.

    ``query_positive_counters(metric_name)`` returns the positive
    ``sum(...) by (Type, Name)`` series and raises if metrics are unavailable.
    """
    failures = []
    if fail_on_worker_oom:
        worker_oom_kills = filter_idle_worker_kills(
            query_positive_counters(WORKER_OOM_METRIC)
        )
        if worker_oom_kills:
            failures.append(
                "OOM worker kills detected. Latest cumulative counter values by "
                "metric:\n"
                f"{summarize_instant_metric_series(worker_oom_kills)}"
            )

    if fail_on_unexpected_worker_failure:
        unexpected_worker_failures = query_positive_counters(
            UNEXPECTED_WORKER_FAILURE_METRIC
        )
        if unexpected_worker_failures:
            failures.append(
                "Unexpected worker failures detected (potential kernel OOM kills or "
                "SIGKILLs not captured by Ray's memory monitor). Latest cumulative "
                "counter values by metric:\n"
                f"{summarize_instant_metric_series(unexpected_worker_failures)}"
            )

    return failures


def find_unexpectedly_dead_nodes(
    nodes: Iterable[Dict[str, Any]], unexpected_death_reasons: Collection[Any]
) -> List[str]:
    """Return IDs of dead ``ray.nodes()`` entries with an unexpected death reason."""
    return [
        node["NodeID"]
        for node in nodes
        if not node["Alive"] and node.get("DeathReason") in unexpected_death_reasons
    ]


def peak_metric_value(range_series: List[Dict[str, Any]]) -> Optional[float]:
    """Return the largest finite sample across a ``query_range`` result."""
    values = [
        float(value)
        for entry in range_series
        for _, value in entry.get("values", [])
        if math.isfinite(float(value))
    ]
    return max(values) if values else None


def check_object_store_utilization(
    peak_utilization: Optional[float], max_utilization: Optional[float]
) -> List[str]:
    """Return a failure if the peak exceeded the limit; a ``None`` peak passes."""
    if max_utilization is None or peak_utilization is None:
        return []
    if peak_utilization > max_utilization:
        return [
            f"Peak object-store utilization ({peak_utilization:.1%}) exceeded the "
            f"configured limit ({max_utilization:.1%})."
        ]
    return []


def wait_for_metrics_to_catch_up(
    *,
    workload_end_unix_time: float,
    oldest_node_sample_unix_time: Callable[[], Optional[float]],
    timeout_s: float,
    poll_interval_s: float,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.time,
) -> bool:
    """Block until every node's latest sample is past ``workload_end`` plus the
    report interval. Returns False on timeout (a node that died mid-run keeps a
    stale series, so callers should only warn).
    """
    needed = workload_end_unix_time + METRICS_REPORT_INTERVAL_S
    deadline = now() + timeout_s
    while True:
        oldest = oldest_node_sample_unix_time()
        if oldest is not None and oldest >= needed:
            return True
        if now() >= deadline:
            return False
        sleep(poll_interval_s)

"""Constants and metric allowlist for the Prometheus telemetry collector.

This module is intentionally small: it is the audited surface for what we
read from Prometheus and post off-cluster. Reviewers should treat
additions to ``METRIC_ALLOWLIST`` with the same care as additions to
``src/ray/protobuf/usage.proto``'s ``TagKey`` enum.

The collector emits a **cluster-level summary**, not a per-series time
series. Each cycle reduces every allowlisted metric to one cluster scalar
per aggregation (see ``_build_promql``), packs the ~few-dozen
``<metric>_<agg> -> value`` pairs into a ``UsageStats``-shaped envelope's
``extra_usage_tags``, writes it to the session directory, and (unless the
endpoint is empty) POSTs it. The summary carries no node IPs, operator, or
dataset names, so it is anonymous. Time is captured by the per-event
``collect_timestamp_ms``; the downstream reconstructs a series by stacking
the hourly events.
"""

from dataclasses import dataclass, field
from typing import List, Tuple

# --- Env vars --------------------------------------------------------------

DATABRICKS_TELEMETRY_ENABLED_ENV_VAR = "RAY_DATABRICKS_TELEMETRY_ENABLED"
DATABRICKS_TELEMETRY_INTERVAL_S_ENV_VAR = "RAY_DATABRICKS_TELEMETRY_INTERVAL_S"

# POST destination. Set to empty to disable the POST and leave the
# collector in write-only mode (the local audit file is still produced).
DATABRICKS_TELEMETRY_ENDPOINT_ENV_VAR = "RAY_DATABRICKS_TELEMETRY_ENDPOINT"

# Reused from the existing Grafana/Prometheus integration.
PROMETHEUS_HOST_ENV_VAR = "RAY_PROMETHEUS_HOST"

# --- Defaults --------------------------------------------------------------

DEFAULT_INTERVAL_S = 3600
DEFAULT_PROMETHEUS_HOST = "http://localhost:9090"

# Default POST endpoint. HTTPS is required: the usage-stats CloudFront
# distribution is HTTPS-only and returns 403 for plain ``http://``. This is
# the same sink Ray's usage stats already post to, unauthenticated.
DEFAULT_ENDPOINT = "https://usage-stats.ray.io/"

# POST timeout. Matches ``UsageStatsClient.report_usage_data``.
DEFAULT_POST_TIMEOUT_S = 10

# --- Payload envelope ------------------------------------------------------

# We reuse the existing ``UsageStats`` schema (``ray-project/telemetry``
# ``telemetry/schemas/v_0_1.py``, registered ``"0.1"``) and put our metrics
# in its free-form ``extra_usage_tags``. No new receiver schema, no
# ``samples[]`` array, no chunking.
SCHEMA_VERSION = "0.1"

# Value of the ``source`` field. Identifies the producer to the receiver
# (mirrors ``UsageStats.source``).
TELEMETRY_SOURCE = "databricks_telemetry"

# --- Local output files ----------------------------------------------------

# Operational status (success/failure counts, last error, allowlist).
STATUS_FILE = "databricks_telemetry_status.json"

# The exact summary posted on the most recent cycle. Lets operators audit
# what the collector shipped.
BATCH_FILE = "databricks_telemetry_latest_batch.json"

# --- Allowlist -------------------------------------------------------------

# Cluster-level aggregations supported by ``_build_promql``:
#   avg/max/min/p95 -> cross-node statistical reducers (intensive metrics,
#       e.g. utilization percentages);
#   sum             -> cross-series total (extensive metrics: counts, bytes,
#       cores; and counters, via ``increase`` over the window).
SUPPORTED_AGGS = frozenset({"avg", "max", "min", "p95", "sum"})

# Per-node series whose *count* gives the cluster node count. Every node's
# ReporterAgent emits it, so ``count(...)`` of this series counts nodes (the
# values — CPU counts — are summed separately as ``ray_node_cpu_count_sum``).
NODE_COUNT_METRIC = "ray_node_cpu_count"


@dataclass(frozen=True)
class MetricSpec:
    """A single allowlist entry.

    Attributes:
        name: Prometheus metric name, including the ``ray_`` prefix.
        type: ``"gauge"`` or ``"counter"``. Selects the inner per-series
            function for the ``sum`` aggregation (``last_over_time`` for a
            gauge's current value, ``increase`` for a counter's windowed
            delta).
        aggs: Cluster aggregations to emit (subset of ``SUPPORTED_AGGS``).
    """

    name: str
    type: str
    aggs: Tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        unsupported = set(self.aggs) - SUPPORTED_AGGS
        if unsupported:
            raise ValueError(
                f"MetricSpec({self.name!r}) has unsupported aggs: {unsupported}"
            )


# Cluster-level CPU + memory + GPU + OOM signals, plus aggregate Ray Data
# resource usage. Additions require code review (see module docstring).
#
# Intensive metrics (percentages) reduce across nodes with avg/max/min/p95.
# Extensive metrics (counts/bytes/cores) and counters reduce with sum.
# Memory / GPU-memory utilization % is derived downstream from the summed
# used / available bytes — Ray exports no direct utilization gauge.
METRIC_ALLOWLIST: List[MetricSpec] = [
    # --- Node CPU ----------------------------------------------------
    MetricSpec("ray_node_cpu_utilization", "gauge", ("avg", "max", "min", "p95")),
    MetricSpec("ray_node_cpu_count", "gauge", ("sum",)),  # total cluster CPUs
    # --- Node memory (bytes; util % derived downstream) --------------
    MetricSpec("ray_node_mem_used", "gauge", ("sum",)),
    MetricSpec("ray_node_mem_available", "gauge", ("sum",)),
    MetricSpec("ray_node_mem_total", "gauge", ("sum",)),
    # --- Node GPU ----------------------------------------------------
    MetricSpec("ray_node_gpus_utilization", "gauge", ("avg", "max", "min", "p95")),
    MetricSpec("ray_node_gram_used", "gauge", ("sum",)),
    MetricSpec("ray_node_gram_available", "gauge", ("sum",)),
    # --- OOM / worker-failure counters (events over the window) ------
    # Memory monitor evictions (src/ray/raylet/node_manager.cc) and the
    # broader SYSTEM_ERROR worker disconnect signal (src/ray/raylet/
    # metrics.h) which includes kernel OOM SIGKILLs.
    MetricSpec("ray_memory_manager_worker_eviction", "counter", ("sum",)),
    MetricSpec("ray_node_manager_unexpected_worker_failure", "counter", ("sum",)),
    # --- Ray Data aggregate resource usage ---------------------------
    # Per-operator series (python/ray/data/_internal/stats.py) summed
    # across operators into a cluster-wide total.
    MetricSpec("ray_data_cpu_usage_cores", "gauge", ("sum",)),
    MetricSpec("ray_data_gpu_usage_cores", "gauge", ("sum",)),
    MetricSpec("ray_data_current_bytes", "gauge", ("sum",)),
]

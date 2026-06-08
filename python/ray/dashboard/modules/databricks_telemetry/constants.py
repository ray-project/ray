"""Constants and metric allowlist for the Prometheus telemetry collector.

This module is intentionally small: it is the audited surface for what we
read from Prometheus and ship off-cluster. Each allowlisted ``(metric, agg)``
maps to a key that must also exist in the ``TagKey`` enum
(``src/ray/protobuf/usage.proto``) — that enum is the governance gate for
data that leaves the cluster.

The collector emits a **cluster-level summary**, not a per-series time
series. Each cycle reduces every allowlisted metric to one cluster scalar
per aggregation (see ``_build_promql``) and **records each as an extra usage
tag** (``record_extra_usage_tag``). The existing ``UsageStatsHead`` then
ships them in its hourly usage-stats report — so this module does **no** HTTP
POST of its own. The summary carries no node IPs, operator, or dataset
names, so it is anonymous. Time is captured by the usage report's
``collect_timestamp_ms``; the downstream reconstructs a series by stacking
the hourly reports.
"""

from dataclasses import dataclass, field
from typing import List, Tuple

# --- Env vars --------------------------------------------------------------

DATABRICKS_TELEMETRY_ENABLED_ENV_VAR = "RAY_DATABRICKS_TELEMETRY_ENABLED"
DATABRICKS_TELEMETRY_INTERVAL_S_ENV_VAR = "RAY_DATABRICKS_TELEMETRY_INTERVAL_S"

# Reused from the existing Grafana/Prometheus integration.
PROMETHEUS_HOST_ENV_VAR = "RAY_PROMETHEUS_HOST"

# --- Defaults --------------------------------------------------------------

DEFAULT_INTERVAL_S = 3600
DEFAULT_PROMETHEUS_HOST = "http://localhost:9090"

# --- Local output files ----------------------------------------------------

# Operational status (success/failure counts, last error, allowlist).
STATUS_FILE = "databricks_telemetry_status.json"

# The exact tag set recorded on the most recent cycle. Lets operators audit
# what the collector shipped into the usage report.
BATCH_FILE = "databricks_telemetry_latest_batch.json"

# --- Allowlist -------------------------------------------------------------

# Cluster-level aggregations supported by ``_build_promql``:
#   avg/max/min/p95 -> cross-node statistical reducers (intensive metrics,
#       e.g. utilization percentages);
#   sum             -> cross-series total (extensive metrics: bytes, cores;
#       and counters, via ``increase`` over the window).
SUPPORTED_AGGS = frozenset({"avg", "max", "min", "p95", "sum"})

# Per-node series whose *count* gives the cluster node count. Every node's
# ReporterAgent emits it, so ``count(...)`` of this series counts nodes.
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
            Each emitted key ``<name>_<agg>`` must have a matching ``TagKey``
            enum entry (uppercased) in ``src/ray/protobuf/usage.proto``.
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


# Cluster-level runtime/utilization signals. Additions require code review
# AND a matching TagKey enum entry (see module docstring).
#
# Intensive metrics (percentages) reduce across nodes with avg/max/min/p95.
# Extensive metrics (bytes/cores) and counters reduce with sum. Memory /
# GPU-memory utilization % is derived downstream from the summed
# used / available bytes — Ray exports no direct utilization gauge.
#
# Deliberately NOT collected — already in the usage-stats report
# (UsageStatsToReport, python/ray/_common/usage/usage_lib.py): cluster CPU
# count (total_num_cpus), total memory (total_memory_gb), GPU count
# (total_num_gpus), and node count (total_num_nodes).
METRIC_ALLOWLIST: List[MetricSpec] = [
    # --- Node CPU ----------------------------------------------------
    MetricSpec("ray_node_cpu_utilization", "gauge", ("avg", "max", "min", "p95")),
    # --- Node memory (bytes; util % = used/(used+available) downstream) ----
    MetricSpec("ray_node_mem_used", "gauge", ("sum",)),
    MetricSpec("ray_node_mem_available", "gauge", ("sum",)),
    # --- Node GPU ----------------------------------------------------
    MetricSpec("ray_node_gpus_utilization", "gauge", ("avg", "max", "min", "p95")),
    MetricSpec("ray_node_gram_used", "gauge", ("sum",)),
    MetricSpec("ray_node_gram_available", "gauge", ("sum",)),
    # --- OOM / worker-failure counters (events over the window) ------
    MetricSpec("ray_memory_manager_worker_eviction", "counter", ("sum",)),
    MetricSpec("ray_node_manager_unexpected_worker_failure", "counter", ("sum",)),
    # --- Ray Data aggregate resource usage ---------------------------
    MetricSpec("ray_data_cpu_usage_cores", "gauge", ("sum",)),
    MetricSpec("ray_data_gpu_usage_cores", "gauge", ("sum",)),
    MetricSpec("ray_data_current_bytes", "gauge", ("sum",)),
]

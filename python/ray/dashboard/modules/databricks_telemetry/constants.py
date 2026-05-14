"""Constants and metric allowlist for the Databricks telemetry forwarder.

This module is intentionally small: it is the audited surface for what leaves
the cluster. Reviewers should treat additions to ``METRIC_ALLOWLIST`` with the
same care as additions to ``src/ray/protobuf/usage.proto``'s ``TagKey`` enum.
"""

from dataclasses import dataclass, field
from typing import List, Tuple

# --- Env vars --------------------------------------------------------------

DATABRICKS_TELEMETRY_ENABLED_ENV_VAR = "RAY_DATABRICKS_TELEMETRY_ENABLED"
DATABRICKS_TELEMETRY_ENDPOINT_ENV_VAR = "RAY_DATABRICKS_TELEMETRY_ENDPOINT"
DATABRICKS_TELEMETRY_TOKEN_FILE_ENV_VAR = "RAY_DATABRICKS_TELEMETRY_TOKEN_FILE"
DATABRICKS_TELEMETRY_INTERVAL_S_ENV_VAR = "RAY_DATABRICKS_TELEMETRY_INTERVAL_S"

# Reused from the existing Grafana/Prometheus integration.
PROMETHEUS_HOST_ENV_VAR = "RAY_PROMETHEUS_HOST"

# --- Defaults --------------------------------------------------------------

DEFAULT_INTERVAL_S = 3600
DEFAULT_PROMETHEUS_HOST = "http://localhost:9090"

# Pre-baked production endpoint, sibling path under the existing usage-stats
# domain. Operators can override via ``RAY_DATABRICKS_TELEMETRY_ENDPOINT``.
DEFAULT_ENDPOINT = "https://usage-stats.ray.io/v1/metrics"

# --- Local mirror file -----------------------------------------------------

# Written into ``session_dir`` after every forward attempt so operators can
# audit exactly what's leaving and whether it succeeded.
STATUS_FILE = "databricks_telemetry_status.json"

# Wire format version. Bumped if the payload schema changes incompatibly.
SCHEMA_VERSION = "0.1"

# Identifies the producer for the server-side router. Distinct from
# usage-stats' ``"OSS"`` source.
SOURCE = "ray-prometheus-forwarder"

# --- Allowlist -------------------------------------------------------------

# Aggregation function names supported by ``_build_promql`` below. Each maps
# to a PromQL ``*_over_time`` form (or ``rate``/``increase`` for counters).
SUPPORTED_AGGS = frozenset(
    {"avg", "max", "min", "last", "p50", "p95", "p99", "rate", "increase"}
)


@dataclass(frozen=True)
class MetricSpec:
    """A single allowlist entry.

    Attributes:
        name: Prometheus metric name, including the ``ray_`` prefix.
        type: ``"gauge"`` or ``"counter"``. Used only to pick sensible
            default aggregations and to document intent; the server is told
            via the ``agg`` field on each sample.
        aggs: Aggregation functions to compute over the report window.
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


# CPU + memory only for v1. Additions require code review (see module
# docstring).
METRIC_ALLOWLIST: List[MetricSpec] = [
    # CPU
    MetricSpec("ray_node_cpu_utilization", "gauge", ("avg", "max", "p95")),
    MetricSpec("ray_node_cpu_count", "gauge", ("last",)),
    # Memory
    MetricSpec("ray_node_mem_used", "gauge", ("avg", "max")),
    MetricSpec("ray_node_mem_available", "gauge", ("avg", "min")),
    MetricSpec("ray_node_mem_total", "gauge", ("last",)),
]

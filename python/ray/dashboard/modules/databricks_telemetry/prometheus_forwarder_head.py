"""Dashboard head module that periodically queries Prometheus for an
allowlisted set of **cluster-level** metric aggregates, writes the summary
to a file in the session directory, and POSTs it to a telemetry endpoint.

Lifecycle is modeled on ``UsageStatsHead``: a periodic loop driven by
``async_loop_forever`` runs a blocking ``_collect_sync`` in a thread pool.
On each cycle the module:

  1. issues one instant PromQL query per ``(metric, agg)`` in the
     allowlist, each an outer cross-series reducer wrapped around a
     ``*_over_time`` window and filtered by ``{SessionName='<session>'}``,
     so every query returns a single cluster scalar;
  2. also counts cluster nodes from the per-node series;
  3. packs the ``<metric>_<agg> -> value`` pairs into a ``UsageStats``-shaped
     envelope's ``extra_usage_tags`` (schema ``0.1`` — no new receiver
     schema, no ``samples[]``, no chunking);
  4. writes the summary to ``<session_dir>/databricks_telemetry_latest_batch.json``;
  5. POSTs it to ``RAY_DATABRICKS_TELEMETRY_ENDPOINT`` (default the
     HTTPS usage-stats sink; set the env var empty for write-only mode);
  6. writes a status mirror.

The local file is always produced regardless of POST outcome — it is the
operator's audit trail and is intentionally decoupled from network
reachability. POST failures are tracked separately
(``total_post_failed`` / ``last_post_error``) so a transient endpoint
outage does not look like a collection failure.

Failures on individual PromQL queries are logged at DEBUG and skipped so a
single missing metric does not abort the whole summary. The next cycle
re-queries Prometheus over a fresh window.
"""

import asyncio
import json
import logging
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

import ray
import ray.dashboard.utils as dashboard_utils
from ray._common.utils import get_or_create_event_loop
from ray.dashboard.modules.databricks_telemetry.constants import (
    BATCH_FILE,
    DATABRICKS_TELEMETRY_ENABLED_ENV_VAR,
    DATABRICKS_TELEMETRY_ENDPOINT_ENV_VAR,
    DATABRICKS_TELEMETRY_INTERVAL_S_ENV_VAR,
    DEFAULT_ENDPOINT,
    DEFAULT_INTERVAL_S,
    DEFAULT_POST_TIMEOUT_S,
    DEFAULT_PROMETHEUS_HOST,
    METRIC_ALLOWLIST,
    NODE_COUNT_METRIC,
    PROMETHEUS_HOST_ENV_VAR,
    SCHEMA_VERSION,
    STATUS_FILE,
    TELEMETRY_SOURCE,
)
from ray.dashboard.utils import async_loop_forever

logger = logging.getLogger(__name__)


# --- Env-var readers (evaluated per call so changes take effect) ---------


def _collector_enabled() -> bool:
    return os.getenv(DATABRICKS_TELEMETRY_ENABLED_ENV_VAR, "1") == "1"


def _collector_interval_s() -> int:
    try:
        return int(
            os.getenv(DATABRICKS_TELEMETRY_INTERVAL_S_ENV_VAR, DEFAULT_INTERVAL_S)
        )
    except ValueError:
        return DEFAULT_INTERVAL_S


def _prometheus_host() -> str:
    return os.getenv(PROMETHEUS_HOST_ENV_VAR, DEFAULT_PROMETHEUS_HOST)


def _telemetry_endpoint() -> str:
    """Return the POST endpoint.

    Defaults to ``DEFAULT_ENDPOINT`` (the HTTPS usage-stats sink). Set the
    env var to an empty string for write-only mode (the local audit file is
    still written).
    """
    return os.getenv(DATABRICKS_TELEMETRY_ENDPOINT_ENV_VAR, DEFAULT_ENDPOINT).strip()


# --- PromQL construction --------------------------------------------------


def _build_promql(
    metric: str, metric_type: str, agg: str, window_s: int, session_name: str
) -> str:
    """Return the PromQL that yields ONE cluster scalar for ``agg``.

    An outer cross-series reducer is wrapped around the per-series
    ``*_over_time`` window so the result collapses every node/operator
    series to a single value:

      avg -> ``avg(avg_over_time(...))``   typical cluster value
      max -> ``max(max_over_time(...))``   absolute worst moment
      min -> ``min(min_over_time(...))``   least-loaded node's quietest point
      p95 -> ``avg(quantile_over_time(0.95, ...))``  mean per-node 1h-p95
      sum -> ``sum(last_over_time(...))``  total now (gauge), or
             ``sum(increase(...))``        events in the window (counter)

    ``session_name`` filters to this Ray session via the ``SessionName``
    label that ``ReporterAgent`` stamps on every metric. The filter is
    essential: a shared monitoring ingress carries series from many
    clusters, so without it we would aggregate across unrelated tenants.
    """
    selector = f"{metric}{{SessionName='{session_name}'}}"
    window = f"[{window_s}s]"
    if agg == "avg":
        return f"avg(avg_over_time({selector}{window}))"
    if agg == "max":
        return f"max(max_over_time({selector}{window}))"
    if agg == "min":
        return f"min(min_over_time({selector}{window}))"
    if agg == "p95":
        return f"avg(quantile_over_time(0.95, {selector}{window}))"
    if agg == "sum":
        if metric_type == "counter":
            return f"sum(increase({selector}{window}))"
        return f"sum(last_over_time({selector}{window}))"
    raise ValueError(f"Unsupported aggregation: {agg!r}")


def _node_count_queries(window_s: int, session_name: str) -> Dict[str, str]:
    """PromQL for cluster node counts, derived by ``count``-ing node series.

    ``count(...)`` counts the number of matching series (one per node) — it
    ignores the metric values, so it yields a node count, not a CPU count.
    """
    sel = f"{NODE_COUNT_METRIC}{{SessionName='{session_name}'}}"
    worker = f"{NODE_COUNT_METRIC}{{SessionName='{session_name}',IsHeadNode='false'}}"
    head = f"{NODE_COUNT_METRIC}{{SessionName='{session_name}',IsHeadNode='true'}}"
    return {
        "ray_cluster_num_nodes": f"count({sel})",
        "ray_cluster_num_workers": f"count({worker})",
        "ray_cluster_num_head": f"count({head})",
        "ray_cluster_num_nodes_peak": f"max_over_time(count({sel})[{window_s}s:60s])",
    }


# --- Value formatting -----------------------------------------------------


def _format_value(value: float) -> str:
    """Render a scalar for ``extra_usage_tags`` (a ``Dict[str, str]``)."""
    if value == int(value):
        return str(int(value))
    return f"{value:.6g}"


# --- Atomic file write ----------------------------------------------------


def _atomic_write_json(dir_path: str, file_name: str, data: Dict[str, Any]) -> None:
    """Write ``data`` to ``<dir_path>/<file_name>`` atomically."""
    dir_obj = Path(dir_path)
    destination = dir_obj / file_name
    temp = dir_obj / f"{file_name}.tmp"
    with temp.open("w") as f:
        f.write(json.dumps(data))
    if sys.platform == "win32":
        destination.unlink(missing_ok=True)
    temp.rename(destination)


class PrometheusForwarderHead(dashboard_utils.DashboardHeadModule):
    """Periodic cluster-level Prometheus-aggregate collector.

    Runs inside the dashboard process on the head node; no subprocess, no
    Ray actor. Output lands in the session directory; the same summary is
    POSTed to the configured endpoint.
    """

    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        super().__init__(config)
        self.enabled = _collector_enabled()
        self.total_success = 0
        self.total_failed = 0
        self.seq_no = 0
        self.last_error: Optional[str] = None
        self.last_success_ts_ms: Optional[int] = None
        self.last_metric_count: Optional[int] = None
        # POST-side counters, kept separate from collection so a transient
        # endpoint outage does not look like a collection failure.
        self.total_post_success = 0
        self.total_post_failed = 0
        self.last_post_ts_ms: Optional[int] = None
        self.last_post_status_code: Optional[int] = None
        self.last_post_error: Optional[str] = None

    # --- POST --------------------------------------------------------

    def _post_summary(self, body: Dict[str, Any]) -> None:
        """POST ``body`` to ``RAY_DATABRICKS_TELEMETRY_ENDPOINT``.

        No-op when the env var is empty (write-only mode). Mirrors
        ``UsageStatsClient.report_usage_data``: ``Content-Type:
        application/json``, 10s timeout, ``raise_for_status``. The summary
        is ~2 KB, so it is a single unchunked request. The caller does not
        raise on failure — the audit file has already been written.
        """
        endpoint = _telemetry_endpoint()
        if not endpoint:
            return
        try:
            resp = requests.post(
                endpoint,
                headers={"Content-Type": "application/json"},
                json=body,
                timeout=DEFAULT_POST_TIMEOUT_S,
            )
            resp.raise_for_status()
        except Exception as e:
            response = getattr(e, "response", None)
            self.last_post_status_code = getattr(response, "status_code", None)
            self.last_post_error = str(e)
            self.total_post_failed += 1
            logger.info(
                "Databricks telemetry POST failed (seq=%d, endpoint=%s): %s",
                self.seq_no,
                endpoint,
                e,
            )
            return
        self.last_post_status_code = resp.status_code
        self.last_post_error = None
        self.last_post_ts_ms = int(time.time() * 1000)
        self.total_post_success += 1

    # --- Prometheus query --------------------------------------------

    def _query_prometheus(self, query: str, eval_ts_s: int) -> List[Dict[str, Any]]:
        """Issue an instant query and return the raw ``data.result`` array.

        Raises on transport or non-success Prometheus response so the
        caller can account the failure per-query and continue.
        """
        url = f"{_prometheus_host()}/api/v1/query"
        params = {"query": query, "time": eval_ts_s}
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != "success":
            raise RuntimeError(
                f"Prometheus query failed: status={payload.get('status')!r}, "
                f"error={payload.get('error')!r}"
            )
        return payload.get("data", {}).get("result", []) or []

    @staticmethod
    def _extract_scalar(result: List[Dict[str, Any]]) -> Optional[float]:
        """Pull the single scalar from an aggregated instant-vector result.

        Returns ``None`` for an empty result (no matching series) or a
        NaN/Inf value.
        """
        for entry in result:
            value_pair = entry.get("value")
            if not value_pair or len(value_pair) != 2:
                continue
            try:
                value = float(value_pair[1])
            except (TypeError, ValueError):
                continue
            if value != value or value in (float("inf"), float("-inf")):
                continue
            return value
        return None

    # --- Summary assembly --------------------------------------------

    def _query_scalar(self, query: str, eval_ts_s: int, label: str) -> Optional[float]:
        try:
            result = self._query_prometheus(query, eval_ts_s)
        except Exception as e:
            logger.debug("Prometheus query failed for %s: %s", label, e)
            return None
        return self._extract_scalar(result)

    def _build_summary(self) -> Dict[str, Any]:
        eval_ts_s = int(time.time())
        eval_ts_ms = eval_ts_s * 1000
        window_s = _collector_interval_s()

        tags: Dict[str, str] = {}

        # One cluster scalar per (metric, agg).
        for spec in METRIC_ALLOWLIST:
            for agg in spec.aggs:
                query = _build_promql(
                    spec.name, spec.type, agg, window_s, self.session_name
                )
                value = self._query_scalar(query, eval_ts_s, f"{spec.name}/{agg}")
                if value is not None:
                    tags[f"{spec.name}_{agg}"] = _format_value(value)

        # Cluster node counts (count of node series, not values).
        for key, query in _node_count_queries(window_s, self.session_name).items():
            value = self._query_scalar(query, eval_ts_s, key)
            if value is not None:
                tags[key] = _format_value(value)

        # Cluster identity goes in extra_usage_tags: the UsageStats
        # top-level forbids unknown fields (Extra.forbid), so cluster_id /
        # session_id / ray_version cannot be added there.
        tags["cluster_id"] = self.gcs_client.cluster_id.hex()
        tags["session_id"] = self.session_name
        tags["ray_version"] = ray.__version__

        # UsageStats (schema 0.1) envelope; metrics live in extra_usage_tags.
        return {
            "schema_version": SCHEMA_VERSION,
            "source": TELEMETRY_SOURCE,
            "collect_timestamp_ms": eval_ts_ms,
            "seq_number": self.seq_no,
            "extra_usage_tags": tags,
        }

    # --- Cycle entry points -------------------------------------------

    def _status(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "interval_s": _collector_interval_s(),
            "prometheus_host": _prometheus_host(),
            "endpoint": _telemetry_endpoint() or None,
            "seq_no": self.seq_no,
            "total_success": self.total_success,
            "total_failed": self.total_failed,
            "last_error": self.last_error,
            "last_success_ts_ms": self.last_success_ts_ms,
            "last_metric_count": self.last_metric_count,
            "total_post_success": self.total_post_success,
            "total_post_failed": self.total_post_failed,
            "last_post_status_code": self.last_post_status_code,
            "last_post_error": self.last_post_error,
            "last_post_ts_ms": self.last_post_ts_ms,
            "allowlist": [spec.name for spec in METRIC_ALLOWLIST],
        }

    def _collect_sync(self) -> None:
        if not self.enabled:
            return

        try:
            summary = self._build_summary()
            metric_count = len(summary["extra_usage_tags"])
            try:
                _atomic_write_json(self.session_dir, BATCH_FILE, summary)
            except Exception as e:
                raise RuntimeError(f"failed to write {BATCH_FILE}: {e}") from e
            self.last_error = None
            self.last_success_ts_ms = summary["collect_timestamp_ms"]
            self.last_metric_count = metric_count
            self.total_success += 1
            logger.info(
                "Databricks telemetry collected %d cluster metrics (seq=%d) → %s",
                metric_count,
                self.seq_no,
                BATCH_FILE,
            )
            # POST after the file write succeeds. Tracked independently so a
            # transient endpoint outage does not flip total_success.
            self._post_summary(summary)
        except Exception as e:
            self.last_error = str(e)
            self.total_failed += 1
            logger.info(
                "Databricks telemetry collection failed (seq=%d): %s",
                self.seq_no,
                e,
            )
        finally:
            self.seq_no += 1
            try:
                _atomic_write_json(self.session_dir, STATUS_FILE, self._status())
            except Exception as e:
                logger.debug("Failed to write status mirror: %s", e)

    async def _collect_async(self) -> None:
        if not self.enabled:
            return
        loop = get_or_create_event_loop()
        with ThreadPoolExecutor(max_workers=1) as executor:
            await loop.run_in_executor(executor, self._collect_sync)

    @async_loop_forever(_collector_interval_s())
    async def periodically_collect(self) -> None:
        await self._collect_async()

    async def run(self) -> None:
        if not self.enabled:
            logger.info("Databricks telemetry collector is disabled.")
            return

        interval_s = _collector_interval_s()
        endpoint = _telemetry_endpoint()
        logger.info(
            "Databricks telemetry collector enabled. prometheus=%s "
            "interval=%ds endpoint=%s",
            _prometheus_host(),
            interval_s,
            endpoint or "(write-only)",
        )

        # Same warmup shape as UsageStatsHead: brief sleep so the cluster
        # has steady-state metrics, an initial collect, a random offset to
        # de-correlate many clusters running at the same wall-clock minute,
        # then the periodic loop.
        await asyncio.sleep(min(60, interval_s))
        await self._collect_async()
        await asyncio.sleep(random.randint(0, interval_s))
        await asyncio.gather(self.periodically_collect())

    @staticmethod
    def is_minimal_module() -> bool:
        # Pure-Python module with only ``requests`` as a runtime dep, which
        # is already in Ray's minimal install.
        return True

"""Dashboard head module that periodically queries Prometheus for an
allowlisted set of **cluster-level** metric aggregates and records each as an
**extra usage tag**, so the existing ``UsageStatsHead`` ships them in its
hourly usage-stats report. This module does **no** HTTP POST of its own.

Lifecycle is modeled on ``UsageStatsHead``: a periodic loop driven by
``async_loop_forever`` runs a blocking ``_collect_sync`` in a thread pool.
On each cycle the module:

  1. issues one instant PromQL query per ``(metric, agg)`` in the allowlist,
     each an outer cross-series reducer wrapped around a ``*_over_time``
     window and filtered by ``{SessionName='<session>'}``, so every query
     returns a single cluster scalar;
  2. also counts cluster nodes (head/worker split + hourly peak);
  3. records each ``<metric>_<agg> -> value`` via ``record_extra_usage_tag``
     (the key must exist in the ``TagKey`` enum), which writes to the GCS
     internal KV store. ``UsageStatsHead`` reads those on its next report and
     POSTs them as part of ``extra_usage_tags`` — no separate POST, endpoint,
     auth, or chunking here;
  4. writes an audit mirror of the recorded tags to
     ``<session_dir>/databricks_telemetry_latest_batch.json`` and a status
     file.

Failures on individual PromQL queries are logged at DEBUG and skipped so a
single missing metric does not abort the whole cycle. The next cycle
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

import ray.dashboard.utils as dashboard_utils
from ray._common.usage.usage_lib import TagKey, record_extra_usage_tag
from ray._common.utils import get_or_create_event_loop
from ray.dashboard.modules.databricks_telemetry.constants import (
    BATCH_FILE,
    DATABRICKS_TELEMETRY_ENABLED_ENV_VAR,
    DATABRICKS_TELEMETRY_INTERVAL_S_ENV_VAR,
    DEFAULT_INTERVAL_S,
    DEFAULT_PROMETHEUS_HOST,
    METRIC_ALLOWLIST,
    NODE_COUNT_METRIC,
    PROMETHEUS_HOST_ENV_VAR,
    STATUS_FILE,
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

    The plain alive-node count is omitted: usage stats already reports it as
    ``total_num_nodes`` (UsageStatsToReport). We only emit what it lacks —
    the head/worker split and the hourly peak (autoscaling churn).
    """
    sel = f"{NODE_COUNT_METRIC}{{SessionName='{session_name}'}}"
    worker = f"{NODE_COUNT_METRIC}{{SessionName='{session_name}',IsHeadNode='false'}}"
    head = f"{NODE_COUNT_METRIC}{{SessionName='{session_name}',IsHeadNode='true'}}"
    return {
        "ray_cluster_num_workers": f"count({worker})",
        "ray_cluster_num_head": f"count({head})",
        "ray_cluster_num_nodes_peak": f"max_over_time(count({sel})[{window_s}s:60s])",
    }


# --- Value formatting -----------------------------------------------------


def _format_value(value: float) -> str:
    """Render a scalar as a usage-tag string value."""
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
    Ray actor, no HTTP POST. Output is recorded as extra usage tags (shipped
    by ``UsageStatsHead``) and mirrored to the session directory for audit.
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
        self.last_recorded_count: Optional[int] = None

    # --- Prometheus query --------------------------------------------

    def _query_prometheus(self, query: str, eval_ts_s: int) -> List[Dict[str, Any]]:
        """Issue an instant query and return the raw ``data.result`` array."""
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
        """Pull the single scalar from an aggregated instant-vector result."""
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

    def _query_scalar(self, query: str, eval_ts_s: int, label: str) -> Optional[float]:
        try:
            result = self._query_prometheus(query, eval_ts_s)
        except Exception as e:
            logger.debug("Prometheus query failed for %s: %s", label, e)
            return None
        return self._extract_scalar(result)

    # --- Tag assembly + recording ------------------------------------

    def _build_tags(self) -> Dict[str, str]:
        """Query Prometheus and return the ``<metric>_<agg> -> value`` tags."""
        eval_ts_s = int(time.time())
        window_s = _collector_interval_s()
        tags: Dict[str, str] = {}

        for spec in METRIC_ALLOWLIST:
            for agg in spec.aggs:
                query = _build_promql(
                    spec.name, spec.type, agg, window_s, self.session_name
                )
                value = self._query_scalar(query, eval_ts_s, f"{spec.name}/{agg}")
                if value is not None:
                    tags[f"{spec.name}_{agg}"] = _format_value(value)

        for key, query in _node_count_queries(window_s, self.session_name).items():
            value = self._query_scalar(query, eval_ts_s, key)
            if value is not None:
                tags[key] = _format_value(value)

        return tags

    def _record_tags(self, tags: Dict[str, str]) -> int:
        """Record each tag via ``record_extra_usage_tag``; return the count.

        The key must map to a ``TagKey`` enum entry (uppercased). A missing
        entry is a programming error (allowlist/proto drift) — logged loudly
        and skipped so one bad key can't abort the rest.
        """
        recorded = 0
        for key, value in tags.items():
            try:
                tag_key = TagKey.Value(key.upper())
            except ValueError:
                logger.warning(
                    "No TagKey enum entry for %r; add it to usage.proto. Skipping.",
                    key,
                )
                continue
            try:
                record_extra_usage_tag(tag_key, value, self.gcs_client)
                recorded += 1
            except Exception as e:
                logger.debug("record_extra_usage_tag failed for %s: %s", key, e)
        return recorded

    # --- Cycle entry points -------------------------------------------

    def _status(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "interval_s": _collector_interval_s(),
            "prometheus_host": _prometheus_host(),
            "seq_no": self.seq_no,
            "total_success": self.total_success,
            "total_failed": self.total_failed,
            "last_error": self.last_error,
            "last_success_ts_ms": self.last_success_ts_ms,
            "last_metric_count": self.last_metric_count,
            "last_recorded_count": self.last_recorded_count,
            "allowlist": [spec.name for spec in METRIC_ALLOWLIST],
        }

    def _collect_sync(self) -> None:
        if not self.enabled:
            return

        try:
            tags = self._build_tags()
            try:
                _atomic_write_json(self.session_dir, BATCH_FILE, tags)
            except Exception as e:
                raise RuntimeError(f"failed to write {BATCH_FILE}: {e}") from e
            recorded = self._record_tags(tags)
            self.last_error = None
            self.last_success_ts_ms = int(time.time() * 1000)
            self.last_metric_count = len(tags)
            self.last_recorded_count = recorded
            self.total_success += 1
            logger.info(
                "Databricks telemetry recorded %d/%d cluster metrics as usage "
                "tags (seq=%d)",
                recorded,
                len(tags),
                self.seq_no,
            )
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
        logger.info(
            "Databricks telemetry collector enabled. prometheus=%s interval=%ds "
            "(records extra usage tags; shipped by the usage-stats report)",
            _prometheus_host(),
            interval_s,
        )

        # Same warmup shape as UsageStatsHead: brief sleep so the cluster has
        # steady-state metrics, an initial collect, a random offset to
        # de-correlate many clusters, then the periodic loop.
        await asyncio.sleep(min(60, interval_s))
        await self._collect_async()
        await asyncio.sleep(random.randint(0, interval_s))
        await asyncio.gather(self.periodically_collect())

    @staticmethod
    def is_minimal_module() -> bool:
        # Pure-Python module with only ``requests`` as a runtime dep, which
        # is already in Ray's minimal install.
        return True

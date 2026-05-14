"""Dashboard head module that forwards Ray Prometheus metrics to Databricks.

Lifecycle is modeled on ``UsageStatsHead``: a periodic loop driven by
``async_loop_forever`` runs a blocking ``_forward_sync`` in a thread pool. On
each cycle the module:

  1. issues PromQL aggregate queries against the local Prometheus (one per
     ``(metric, agg)`` pair in the allowlist),
  2. flattens the matrix-of-series response into long-table sample rows,
  3. POSTs the batch JSON to the ingest endpoint with the bearer token,
  4. writes a status mirror file into ``session_dir`` regardless of outcome.

Failures are dropped (no retry, no disk queue). The next cycle re-queries
Prometheus over a fresh window; in practice Prometheus's local retention
(default 15d) means we lose nothing unless the ingest is down for that long.
"""

import asyncio
import logging
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import requests

import ray
import ray.dashboard.utils as dashboard_utils
from ray._common.utils import get_or_create_event_loop
from ray.dashboard.modules.databricks_telemetry.constants import (
    DATABRICKS_TELEMETRY_ENABLED_ENV_VAR,
    DATABRICKS_TELEMETRY_ENDPOINT_ENV_VAR,
    DATABRICKS_TELEMETRY_INTERVAL_S_ENV_VAR,
    DATABRICKS_TELEMETRY_TOKEN_FILE_ENV_VAR,
    DEFAULT_ENDPOINT,
    DEFAULT_INTERVAL_S,
    DEFAULT_PROMETHEUS_HOST,
    METRIC_ALLOWLIST,
    PROMETHEUS_HOST_ENV_VAR,
    SCHEMA_VERSION,
    SOURCE,
    MetricSpec,
)
from ray.dashboard.modules.databricks_telemetry.forwarder_client import (
    ForwarderClient,
)
from ray.dashboard.utils import async_loop_forever

logger = logging.getLogger(__name__)


# --- Env-var readers (instantiated per call so changes take effect) -------


def _forwarder_enabled() -> bool:
    return os.getenv(DATABRICKS_TELEMETRY_ENABLED_ENV_VAR, "1") == "1"


def _forwarder_endpoint() -> str:
    return os.getenv(DATABRICKS_TELEMETRY_ENDPOINT_ENV_VAR, DEFAULT_ENDPOINT)


def _forwarder_interval_s() -> int:
    try:
        return int(
            os.getenv(DATABRICKS_TELEMETRY_INTERVAL_S_ENV_VAR, DEFAULT_INTERVAL_S)
        )
    except ValueError:
        return DEFAULT_INTERVAL_S


def _forwarder_token() -> Optional[str]:
    """Read the bearer token from the configured file path, or return None.

    Reread on every call so operators can rotate the token by replacing the
    file contents without restarting the dashboard.
    """
    path = os.getenv(DATABRICKS_TELEMETRY_TOKEN_FILE_ENV_VAR)
    if not path:
        return None
    try:
        with open(path) as f:
            return f.read().strip() or None
    except OSError as e:
        logger.warning("Failed to read Databricks telemetry token at %s: %s", path, e)
        return None


def _prometheus_host() -> str:
    return os.getenv(PROMETHEUS_HOST_ENV_VAR, DEFAULT_PROMETHEUS_HOST)


# --- PromQL construction --------------------------------------------------


def _build_promql(metric: str, agg: str, window_s: int) -> str:
    """Return the PromQL string that yields one value per series for ``agg``.

    ``window_s`` is rendered as ``[<N>s]`` and applied as a range selector.
    """
    window = f"[{window_s}s]"
    if agg == "avg":
        return f"avg_over_time({metric}{window})"
    if agg == "max":
        return f"max_over_time({metric}{window})"
    if agg == "min":
        return f"min_over_time({metric}{window})"
    if agg == "last":
        return f"last_over_time({metric}{window})"
    if agg == "p50":
        return f"quantile_over_time(0.5, {metric}{window})"
    if agg == "p95":
        return f"quantile_over_time(0.95, {metric}{window})"
    if agg == "p99":
        return f"quantile_over_time(0.99, {metric}{window})"
    if agg == "rate":
        return f"rate({metric}{window})"
    if agg == "increase":
        return f"increase({metric}{window})"
    raise ValueError(f"Unsupported aggregation: {agg!r}")


class PrometheusForwarderHead(dashboard_utils.DashboardHeadModule):
    """Periodic Prometheus → Databricks-ingest forwarder.

    Lives inside the dashboard process; no separate subprocess or Ray actor.
    """

    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        super().__init__(config)
        self.enabled = _forwarder_enabled()
        self.client = ForwarderClient()
        self.total_success = 0
        self.total_failed = 0
        self.seq_no = 0
        self.last_error: Optional[str] = None
        self.last_success_ts_ms: Optional[int] = None
        self.last_sample_count: Optional[int] = None

    # --- Prometheus query ---------------------------------------------

    def _query_prometheus(
        self,
        query: str,
        eval_ts_s: int,
    ) -> List[Dict[str, Any]]:
        """Issue an instant query against the local Prometheus.

        Returns the raw ``data.result`` array. Raises on transport or
        non-success Prometheus response so the caller can account the
        failure once for the whole batch rather than per-query.
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

    # --- Batch assembly -----------------------------------------------

    def _flatten_result(
        self,
        spec: MetricSpec,
        agg: str,
        result: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Convert one Prometheus instant-vector result into long-table rows."""
        samples: List[Dict[str, Any]] = []
        for entry in result:
            labels = dict(entry.get("metric") or {})
            # __name__ is redundant with spec.name (and may be missing for
            # derived expressions like rate()/quantile_over_time()).
            labels.pop("__name__", None)

            value_pair = entry.get("value")
            if not value_pair or len(value_pair) != 2:
                continue
            ts_s, raw = value_pair
            try:
                value = float(raw)
            except (TypeError, ValueError):
                # Prometheus returns "NaN" / "+Inf" / "-Inf" as strings; skip.
                continue
            # Drop NaN/Inf so the Delta side doesn't have to filter.
            if value != value or value in (float("inf"), float("-inf")):
                continue

            samples.append(
                {
                    "ts_ms": int(float(ts_s) * 1000),
                    "metric": spec.name,
                    "agg": agg,
                    "attrs": labels,
                    "value": value,
                }
            )
        return samples

    def _build_batch(self) -> Dict[str, Any]:
        eval_ts_s = int(time.time())
        eval_ts_ms = eval_ts_s * 1000
        interval_s = _forwarder_interval_s()
        window_start_ms = eval_ts_ms - interval_s * 1000

        samples: List[Dict[str, Any]] = []
        for spec in METRIC_ALLOWLIST:
            for agg in spec.aggs:
                query = _build_promql(spec.name, agg, interval_s)
                try:
                    result = self._query_prometheus(query, eval_ts_s)
                except Exception as e:
                    # Skip this (metric, agg); continue with the rest so a
                    # single missing series doesn't abort the whole batch.
                    logger.debug(
                        "Prometheus query failed for %s/%s: %s", spec.name, agg, e
                    )
                    continue
                samples.extend(self._flatten_result(spec, agg, result))

        return {
            "schema_version": SCHEMA_VERSION,
            "source": SOURCE,
            "cluster_id": self.gcs_client.cluster_id.hex(),
            "ray_version": ray.__version__,
            "session_name": self.session_name,
            "batch_window_start_ms": window_start_ms,
            "batch_window_end_ms": eval_ts_ms,
            "seq_no": self.seq_no,
            "samples": samples,
        }

    # --- Cycle entry points -------------------------------------------

    def _status(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "endpoint": _forwarder_endpoint(),
            "interval_s": _forwarder_interval_s(),
            "prometheus_host": _prometheus_host(),
            "seq_no": self.seq_no,
            "total_success": self.total_success,
            "total_failed": self.total_failed,
            "last_error": self.last_error,
            "last_success_ts_ms": self.last_success_ts_ms,
            "last_sample_count": self.last_sample_count,
            "allowlist": [spec.name for spec in METRIC_ALLOWLIST],
        }

    def _forward_sync(self) -> None:
        if not self.enabled:
            return

        try:
            batch = self._build_batch()
            sample_count = len(batch["samples"])
            try:
                self.client.post(_forwarder_endpoint(), batch, _forwarder_token())
            except Exception as e:
                self.last_error = str(e)
                self.total_failed += 1
                logger.info(
                    "Databricks telemetry forward failed (seq=%d, samples=%d): %s",
                    self.seq_no,
                    sample_count,
                    e,
                )
            else:
                self.last_error = None
                self.last_success_ts_ms = batch["batch_window_end_ms"]
                self.last_sample_count = sample_count
                self.total_success += 1
                logger.info(
                    "Databricks telemetry forwarded %d samples (seq=%d) to %s",
                    sample_count,
                    self.seq_no,
                    _forwarder_endpoint(),
                )
        finally:
            self.seq_no += 1
            try:
                self.client.write_status_mirror(self.session_dir, self._status())
            except Exception as e:
                logger.debug("Failed to write status mirror: %s", e)

    async def _forward_async(self) -> None:
        if not self.enabled:
            return
        loop = get_or_create_event_loop()
        with ThreadPoolExecutor(max_workers=1) as executor:
            await loop.run_in_executor(executor, self._forward_sync)

    @async_loop_forever(_forwarder_interval_s())
    async def periodically_forward(self) -> None:
        await self._forward_async()

    async def run(self) -> None:
        if not self.enabled:
            logger.info("Databricks telemetry forwarder is disabled.")
            return

        interval_s = _forwarder_interval_s()
        logger.info(
            "Databricks telemetry forwarder enabled. "
            "endpoint=%s prometheus=%s interval=%ds",
            _forwarder_endpoint(),
            _prometheus_host(),
            interval_s,
        )

        # Same warmup shape as UsageStatsHead: wait briefly so the cluster
        # has steady-state metrics, do an initial forward, then add a
        # random offset before settling into the periodic loop. The offset
        # de-correlates many clusters posting at the same wall-clock minute.
        await asyncio.sleep(min(60, interval_s))
        await self._forward_async()
        await asyncio.sleep(random.randint(0, interval_s))
        await asyncio.gather(self.periodically_forward())

    @staticmethod
    def is_minimal_module() -> bool:
        # Pure-Python module with only ``requests`` as a runtime dep, which
        # is already in Ray's minimal install.
        return True

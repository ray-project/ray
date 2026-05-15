"""Dashboard head module that periodically queries Prometheus for an
allowlisted set of hourly aggregates and dumps the result to a file in the
session directory.

Lifecycle is modeled on ``UsageStatsHead``: a periodic loop driven by
``async_loop_forever`` runs a blocking ``_collect_sync`` in a thread pool.
On each cycle the module:

  1. issues PromQL aggregate queries against the local Prometheus (one per
     ``(metric, agg)`` pair in the allowlist),
  2. flattens the matrix-of-series response into long-table sample rows,
  3. writes the batch JSON to ``<session_dir>/databricks_telemetry_latest_batch.json``,
  4. writes a status mirror to ``<session_dir>/databricks_telemetry_status.json``.

**Scope note.** v1 only collects locally — no HTTPS POST, no bearer token,
no endpoint configuration. The forward-to-server piece will be added in a
follow-up commit once the matching server-side route lands in
``ray-project/telemetry``. The on-disk batch file is the operator's audit
trail and the foundation that the future POST will read.

Failures on individual PromQL queries are logged at DEBUG and skipped so a
single missing series does not abort the whole batch. A failed batch
assembly (network error against Prometheus, JSON parse error) is counted
in ``total_failed`` and surfaced via ``last_error`` in the status file.
The next cycle re-queries Prometheus over a fresh window.
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
    DATABRICKS_TELEMETRY_INTERVAL_S_ENV_VAR,
    DEFAULT_INTERVAL_S,
    DEFAULT_PROMETHEUS_HOST,
    METRIC_ALLOWLIST,
    PROMETHEUS_HOST_ENV_VAR,
    SCHEMA_VERSION,
    STATUS_FILE,
    MetricSpec,
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


def _build_promql(metric: str, agg: str, window_s: int, session_name: str) -> str:
    """Return the PromQL string that yields one value per series for ``agg``.

    ``window_s`` is rendered as ``[<N>s]`` and applied as a range selector.

    ``session_name`` filters to this Ray session via the ``SessionName`` label
    that ``ReporterAgent`` stamps on every metric (see
    ``dashboard/modules/reporter/reporter_agent.py``'s ``record_and_export``
    call with ``global_tags={"SessionName": ...}``).

    The filter is essential, not defensive. Managed environments expose a
    shared Prometheus-compatible ingress that carries series from many
    clusters in the same organization (e.g. Anyscale's customer monitoring
    endpoint on workspaces returns thousands of series per metric across
    every running cluster). Without ``{SessionName='<our session>'}`` we
    would collect metrics from unrelated tenants on every cycle.
    """
    selector = f"{metric}{{SessionName='{session_name}'}}"
    window = f"[{window_s}s]"
    if agg == "avg":
        return f"avg_over_time({selector}{window})"
    if agg == "max":
        return f"max_over_time({selector}{window})"
    if agg == "min":
        return f"min_over_time({selector}{window})"
    if agg == "last":
        return f"last_over_time({selector}{window})"
    if agg == "p50":
        return f"quantile_over_time(0.5, {selector}{window})"
    if agg == "p95":
        return f"quantile_over_time(0.95, {selector}{window})"
    if agg == "p99":
        return f"quantile_over_time(0.99, {selector}{window})"
    if agg == "rate":
        return f"rate({selector}{window})"
    if agg == "increase":
        return f"increase({selector}{window})"
    raise ValueError(f"Unsupported aggregation: {agg!r}")


# --- Atomic file write ----------------------------------------------------


def _atomic_write_json(dir_path: str, file_name: str, data: Dict[str, Any]) -> None:
    """Write ``data`` to ``<dir_path>/<file_name>`` atomically.

    Uses a temp-then-rename pattern so concurrent readers never see a
    partially written file.
    """
    dir_obj = Path(dir_path)
    destination = dir_obj / file_name
    temp = dir_obj / f"{file_name}.tmp"
    with temp.open("w") as f:
        f.write(json.dumps(data))
    if sys.platform == "win32":
        destination.unlink(missing_ok=True)
    temp.rename(destination)


class PrometheusForwarderHead(dashboard_utils.DashboardHeadModule):
    """Periodic Prometheus-aggregate collector.

    Runs inside the dashboard process on the head node; no subprocess, no
    Ray actor. Output lands in the session directory; nothing leaves the
    cluster in this v1.
    """

    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        super().__init__(config)
        self.enabled = _collector_enabled()
        self.total_success = 0
        self.total_failed = 0
        self.seq_no = 0
        self.last_error: Optional[str] = None
        self.last_success_ts_ms: Optional[int] = None
        self.last_sample_count: Optional[int] = None

    # --- Prometheus query --------------------------------------------

    def _query_prometheus(
        self,
        query: str,
        eval_ts_s: int,
    ) -> List[Dict[str, Any]]:
        """Issue an instant query against the local Prometheus.

        Returns the raw ``data.result`` array. Raises on transport or
        non-success Prometheus response so the caller can account the
        failure per-query and continue with the rest of the batch.
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

    # --- Batch assembly ----------------------------------------------

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
            # Drop NaN/Inf so the consumer side does not have to filter.
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
        interval_s = _collector_interval_s()
        window_start_ms = eval_ts_ms - interval_s * 1000

        samples: List[Dict[str, Any]] = []
        for spec in METRIC_ALLOWLIST:
            for agg in spec.aggs:
                query = _build_promql(spec.name, agg, interval_s, self.session_name)
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
            "interval_s": _collector_interval_s(),
            "prometheus_host": _prometheus_host(),
            "seq_no": self.seq_no,
            "total_success": self.total_success,
            "total_failed": self.total_failed,
            "last_error": self.last_error,
            "last_success_ts_ms": self.last_success_ts_ms,
            "last_sample_count": self.last_sample_count,
            "allowlist": [spec.name for spec in METRIC_ALLOWLIST],
        }

    def _collect_sync(self) -> None:
        if not self.enabled:
            return

        try:
            batch = self._build_batch()
            sample_count = len(batch["samples"])
            try:
                _atomic_write_json(self.session_dir, BATCH_FILE, batch)
            except Exception as e:
                # Treat a failure to persist the batch as a cycle failure —
                # we have no other side effect to call this cycle a success.
                raise RuntimeError(f"failed to write {BATCH_FILE}: {e}") from e
            self.last_error = None
            self.last_success_ts_ms = batch["batch_window_end_ms"]
            self.last_sample_count = sample_count
            self.total_success += 1
            logger.info(
                "Databricks telemetry collected %d samples (seq=%d) → %s",
                sample_count,
                self.seq_no,
                BATCH_FILE,
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
            "Databricks telemetry collector enabled. prometheus=%s interval=%ds",
            _prometheus_host(),
            interval_s,
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

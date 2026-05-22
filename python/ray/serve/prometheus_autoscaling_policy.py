import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import aiohttp

from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.config import AutoscalingContext

logger = logging.getLogger(SERVE_LOGGER_NAME)

DEFAULT_FETCH_INTERVAL_S = 5.0
DEFAULT_CACHE_TTL_S = 15.0
DEFAULT_QUERY_TIMEOUT_S = 5.0


def _extract_scalar(payload: Dict[str, Any]) -> Optional[float]:
    """Extract a single float from a Prometheus query response.

    Returns None for any shape we don't recognize or that contains no value,
    so callers can treat a missing query the same way regardless of cause.
    """
    if payload.get("status") != "success":
        return None
    data = payload.get("data") or {}
    result_type = data.get("resultType")
    result = data.get("result")
    if result is None:
        return None
    try:
        if result_type == "vector":
            if not result:
                return None
            return float(result[0]["value"][1])
        if result_type == "scalar":
            return float(result[1])
        if result_type == "matrix":
            if not result or not result[0].get("values"):
                return None
            return float(result[0]["values"][-1][1])
    except (KeyError, IndexError, TypeError, ValueError):
        return None
    return None


class PrometheusAutoscalingPolicy:
    """Base class for autoscaling policies driven by Prometheus queries.

    Subclasses implement decide(ctx, metrics) and are invoked with a
    dictionary mapping each configured PromQL expression to its latest
    scalar value. The base class handles fetching, caching, and staleness.

    Fetching is one-shot: __call__ kicks off single fetch task whenever
    fetch_interval_s has elapsed since the last one was scheduled, and
    there is no in-flight task.

    Args:
        prometheus_query_url: Full URL of the Prometheus query endpoint.
        queries: PromQL expressions to evaluate every fetch interval.
            Each is expected to return a scalar; vector results use the
            first sample's value.
        fetch_interval_s: Minimum seconds between successive fetches.
        cache_ttl_s: How long the most recent successful fetch is treated
            as fresh.
        query_timeout_s: Per-request HTTP timeout.
    """

    def __init__(
        self,
        prometheus_query_url: str,
        queries: List[str],
        fetch_interval_s: float = DEFAULT_FETCH_INTERVAL_S,
        cache_ttl_s: float = DEFAULT_CACHE_TTL_S,
        query_timeout_s: float = DEFAULT_QUERY_TIMEOUT_S,
    ):
        if not queries:
            raise ValueError("PrometheusAutoscalingPolicy requires at least one query.")

        self._query_url = prometheus_query_url
        self._queries = list(queries)
        self._fetch_interval_s = fetch_interval_s
        self._cache_ttl_s = cache_ttl_s
        self._query_timeout_s = query_timeout_s

        # Per-query freshness: query string -> (value, timestamp)
        self._latest: Dict[str, Tuple[float, float]] = {}
        self._task: Optional[asyncio.Task] = None
        self._last_fetch_start: float = 0.0

    async def _fetch_one(
        self, session: aiohttp.ClientSession, query: str
    ) -> Tuple[str, Optional[float]]:
        try:
            async with session.get(self._query_url, params={"query": query}) as resp:
                resp.raise_for_status()
                payload = await resp.json()
        except Exception as e:
            logger.warning(f"Prometheus query failed [{query}]: {e}")
            return query, None
        return query, _extract_scalar(payload)

    async def _fetch_all_queries(self) -> None:
        timeout = aiohttp.ClientTimeout(total=self._query_timeout_s)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                outcomes = await asyncio.gather(
                    *(self._fetch_one(session, q) for q in self._queries)
                )
        except Exception as e:
            logger.warning(f"Prometheus fetch failed: {e}")
            return

        now = time.monotonic()
        for q, v in outcomes:
            if v is not None:
                self._latest[q] = (v, now)

    def _maybe_schedule_fetch(self) -> None:
        if self._task is not None and self._task.done():
            self._task = None

        if self._task is not None:
            return

        now = time.monotonic()
        if now - self._last_fetch_start < self._fetch_interval_s:
            return

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Skip scheduling; staleness will kick in if no fetch ever runs.
            return

        self._last_fetch_start = now
        self._task = loop.create_task(self._fetch_all_queries())

    def _fresh_metrics(self) -> Dict[str, float]:
        now = time.monotonic()
        return {
            q: v for q, (v, ts) in self._latest.items() if now - ts <= self._cache_ttl_s
        }

    def __call__(
        self, ctx: AutoscalingContext
    ) -> Tuple[Union[int, float], Dict[str, Any]]:
        self._maybe_schedule_fetch()

        fresh = self._fresh_metrics()
        if not fresh:
            return float(ctx.current_num_replicas), {"signal": "no_metrics"}

        return self.decide(ctx, fresh)

    def decide(
        self, ctx: AutoscalingContext, metrics: Dict[str, float]
    ) -> Tuple[Union[int, float], Dict[str, Any]]:
        """Subclasses override. Called only with at least one fresh metric."""
        raise NotImplementedError

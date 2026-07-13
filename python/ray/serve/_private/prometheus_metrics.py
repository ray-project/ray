"""Helpers for evaluating Prometheus queries as autoscaling signals.

The Serve controller evaluates user-configured PromQL expressions in a
background task and caches the results for custom autoscaling policies to
read via ``AutoscalingContext.prometheus_metrics``. A per-query timeout
lets a slow or unreachable Prometheus fail without stalling the control
loop.
"""

import asyncio
import logging
import math
from typing import Dict, List, Optional

import aiohttp

from ray.serve._private.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)

# Per-query HTTP timeout. One slow query must not stall the whole batch.
PROMETHEUS_QUERY_TIMEOUT_S = 5.0


def normalize_query_url(address: str) -> str:
    """Return the ``/api/v1/query`` URL for a Prometheus address.

    Accepts either ``host:port`` or ``http(s)://host:port``.
    """
    address = address.rstrip("/")
    if not address.startswith(("http://", "https://")):
        address = f"http://{address}"
    if address.endswith("/api/v1/query"):
        return address
    return f"{address}/api/v1/query"


async def _evaluate_query(
    session: aiohttp.ClientSession, query_url: str, query: str
) -> Optional[float]:
    """Evaluate one PromQL expression, returning its first scalar or None."""
    try:
        async with session.get(
            query_url,
            params={"query": query},
            timeout=aiohttp.ClientTimeout(total=PROMETHEUS_QUERY_TIMEOUT_S),
        ) as resp:
            resp.raise_for_status()
            body = await resp.json()
        result = body.get("data", {}).get("result", [])
        if result:
            value = float(result[0]["value"][1])
            # Prometheus returns NaN/Inf for empty ranges (e.g. an idle
            # histogram_quantile). Treat those as no data.
            return value if math.isfinite(value) else None
        return None
    except Exception as e:
        logger.warning(f"Failed to evaluate Prometheus query '{query}': {e}")
        logger.debug(f"Failed to evaluate Prometheus query '{query}'.", exc_info=True)
        return None


async def fetch_metrics(
    session: aiohttp.ClientSession, address: str, queries: List[str]
) -> Dict[str, float]:
    """Evaluate ``queries`` against ``address`` concurrently.

    Returns a mapping of query to scalar value, omitting queries that
    returned no data or failed.
    """
    query_url = normalize_query_url(address)
    values = await asyncio.gather(
        *(_evaluate_query(session, query_url, q) for q in queries)
    )
    return {q: v for q, v in zip(queries, values) if v is not None}

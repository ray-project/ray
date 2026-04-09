"""Actor that fetches Prometheus metrics for autoscaling.

Runs in a dedicated process, isolated from the controller's event loop.
Periodically evaluates PromQL expressions and pushes results to the
controller via fire-and-forget ``remote()`` calls.
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Tuple

import aiohttp

import ray
from ray._common.constants import HEAD_NODE_RESOURCE_NAME
from ray.actor import ActorHandle
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import (
    RAY_SERVE_PROMETHEUS_FETCH_INTERVAL_S,
    RAY_SERVE_PROMETHEUS_SERVER_ADDRESS,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.metrics_utils import MetricsPusher

logger = logging.getLogger(SERVE_LOGGER_NAME)

PROMETHEUS_FETCHER_ACTOR_NAME = "SERVE_PROMETHEUS_FETCHER"
PUSH_TASK_NAME = "push_prometheus_metrics"


@ray.remote(num_cpus=0)
class PrometheusMetricsFetcherActor:
    """Fetches PromQL metrics and pushes results to the Serve controller."""

    async def __init__(
        self,
        controller_handle: ActorHandle,
        prometheus_address: str,
    ):
        self._controller_handle = controller_handle
        self._prometheus_address = prometheus_address
        self._base_url = f"http://{prometheus_address}/api/v1/query"
        self._session: Optional[aiohttp.ClientSession] = None

        # DeploymentID -> list of PromQL expressions
        self._queries: Dict[DeploymentID, List[str]] = {}

        self._metrics_pusher = MetricsPusher()

        logger.info(
            f"PrometheusMetricsFetcher initialized "
            f"(server={prometheus_address}, "
            f"interval={RAY_SERVE_PROMETHEUS_FETCH_INTERVAL_S}s)."
        )

    def update_queries(self, queries: Dict[DeploymentID, List[str]]) -> None:
        """Update the set of PromQL queries to evaluate.

        Called by the controller when deployment configs change.

        Args:
            queries: mapping of DeploymentID to list of PromQL expressions.
        """
        self._queries = queries
        # Start pusher on first non-empty update; idempotent thereafter.
        if queries and PUSH_TASK_NAME not in self._metrics_pusher._tasks:
            self._metrics_pusher.register_or_update_task(
                PUSH_TASK_NAME,
                self._fetch_and_push,
                RAY_SERVE_PROMETHEUS_FETCH_INTERVAL_S,
            )
            self._metrics_pusher.start()
            logger.info(
                f"Started Prometheus fetch loop for {len(queries)} deployment(s)."
            )

    async def _fetch_and_push(self) -> None:
        """Fetch all configured PromQL queries and push results to controller."""
        if not self._queries:
            return

        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()

        # Budget: don't let a single iteration run forever.
        budget_s = RAY_SERVE_PROMETHEUS_FETCH_INTERVAL_S / 2

        all_results: Dict[DeploymentID, Dict[str, float]] = {}
        try:
            coros = []
            deployment_ids = []
            for dep_id, queries in self._queries.items():
                deployment_ids.append(dep_id)
                coros.append(self._fetch_for_deployment(queries))

            results = await asyncio.wait_for(
                asyncio.gather(*coros, return_exceptions=True),
                timeout=budget_s,
            )

            for dep_id, result in zip(deployment_ids, results):
                if isinstance(result, Exception):
                    logger.warning(
                        f"Failed to fetch Prometheus metrics for {dep_id}: {result}"
                    )
                elif result:
                    all_results[dep_id] = result

        except asyncio.TimeoutError:
            logger.warning(
                f"Prometheus fetch exceeded budget of {budget_s:.1f}s. "
                f"Consider reducing prometheus_queries or increasing "
                f"RAY_SERVE_PROMETHEUS_FETCH_INTERVAL_S."
            )

        if all_results:
            timestamp = time.time()
            self._controller_handle.record_prometheus_metrics.remote(
                all_results, timestamp
            )

    async def _fetch_for_deployment(
        self, queries: List[str]
    ) -> Optional[Dict[str, float]]:
        """Fetch all PromQL queries for a single deployment concurrently.

        Args:
            queries: list of PromQL expressions.

        Returns:
            Dict mapping query to scalar value, or None if all failed.
        """

        async def _fetch_one(query: str) -> Tuple[str, Optional[float]]:
            try:
                async with self._session.get(
                    self._base_url,
                    params={"query": query},
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    resp.raise_for_status()
                    body = await resp.json()
                    data = body.get("data", {}).get("result", [])
                    if data:
                        return query, float(data[0]["value"][1])
                    return query, None
            except Exception:
                logger.warning(
                    f"Failed to fetch Prometheus query '{query}'.",
                    exc_info=True,
                )
                return query, None

        results = await asyncio.gather(*[_fetch_one(q) for q in queries])
        metrics = {q: v for q, v in results if v is not None}
        return metrics if metrics else None

    def __ray_shutdown__(self):
        if self._metrics_pusher is not None:
            self._metrics_pusher.stop_tasks()
            self._metrics_pusher = None


def create_prometheus_fetcher_actor(
    controller_handle: ActorHandle,
    namespace: str = "serve",
) -> Optional[ActorHandle]:
    """Create the singleton PrometheusMetricsFetcher actor.

    Returns None if the Prometheus server address is not configured.

    Args:
        controller_handle: handle to the Serve controller.
        namespace: Ray namespace.

    Returns:
        ActorHandle or None.
    """
    if not RAY_SERVE_PROMETHEUS_SERVER_ADDRESS:
        logger.error(
            "RAY_SERVE_ENABLE_PROMETHEUS_AUTOSCALING is set but "
            "RAY_SERVE_PROMETHEUS_SERVER_ADDRESS is not configured."
        )
        return None

    try:
        existing = ray.get_actor(PROMETHEUS_FETCHER_ACTOR_NAME, namespace=namespace)
        logger.info("Reusing existing PrometheusMetricsFetcher actor.")
        return existing
    except ValueError:
        pass

    actor = PrometheusMetricsFetcherActor.options(
        name=PROMETHEUS_FETCHER_ACTOR_NAME,
        namespace=namespace,
        max_restarts=-1,
        max_task_retries=-1,
        resources={HEAD_NODE_RESOURCE_NAME: 0.001},
    ).remote(
        controller_handle=controller_handle,
        prometheus_address=RAY_SERVE_PROMETHEUS_SERVER_ADDRESS,
    )
    logger.info(
        f"Created PrometheusMetricsFetcher actor "
        f"(server={RAY_SERVE_PROMETHEUS_SERVER_ADDRESS})."
    )
    return actor

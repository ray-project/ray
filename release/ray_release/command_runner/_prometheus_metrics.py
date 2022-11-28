import asyncio
import aiohttp
import os
import time
import traceback
from urllib.parse import quote
from typing import Optional
import logging
import json
import argparse

logger = logging.getLogger(__name__)

DEFAULT_PROMETHEUS_HOST = "http://localhost:9090"
PROMETHEUS_HOST_ENV_VAR = "RAY_PROMETHEUS_HOST"
RETRIES = 3


class PrometheusQueryError(Exception):
    def __init__(self, status, message):
        self.message = (
            "Error fetching data from prometheus. "
            f"status: {status}, message: {message}"
        )
        super().__init__(self.message)


class PrometheusClient:
    def __init__(self) -> None:
        self.http_session = aiohttp.ClientSession()
        self.prometheus_host = os.environ.get(
            PROMETHEUS_HOST_ENV_VAR, DEFAULT_PROMETHEUS_HOST
        )

    async def query_prometheus(self, query_type, **kwargs):
        url = f"{self.prometheus_host}/api/v1/{query_type}?" + "&".join(
            [f"{k}={quote(str(v), safe='')}" for k, v in kwargs.items()]
        )
        logger.debug(f"Running Prometheus query {url}")
        async with self.http_session.get(url) as resp:
            for _ in range(RETRIES):
                if resp.status == 200:
                    prom_data = await resp.json()
                    return prom_data["data"]["result"]
                time.sleep(1)
            return None

    async def close(self):
        await self.http_session.close()


# Metrics here mirror what we have in Grafana.
async def _get_prometheus_metrics(start_time: float, end_time: float) -> dict:
    client = PrometheusClient()
    kwargs = {
        "query_type": "query_range",
        "start": int(start_time),
        "end": int(end_time),
        "step": 15,
    }
    metrics = {
        "cpu_utilization": client.query_prometheus(
            query="ray_node_cpu_utilization * ray_node_cpu_count / 100", **kwargs
        ),
        "cpu_count": client.query_prometheus(query="ray_node_cpu_count", **kwargs),
        "gpu_utilization": client.query_prometheus(
            query="ray_node_gpus_utilization / 100", **kwargs
        ),
        "gpu_count": client.query_prometheus(query="ray_node_gpus_available", **kwargs),
        "disk_usage": client.query_prometheus(query="ray_node_disk_usage", **kwargs),
        "disk_space": client.query_prometheus(
            query="sum(ray_node_disk_free) + sum(ray_node_disk_usage)", **kwargs
        ),
        "memory_usage": client.query_prometheus(query="ray_node_mem_used", **kwargs),
        "total_memory": client.query_prometheus(query="ray_node_mem_total", **kwargs),
        "gpu_memory_usage": client.query_prometheus(
            query="ray_node_gram_used * 1024 * 1024", **kwargs
        ),
        "gpu_total_memory": client.query_prometheus(
            query=(
                "(sum(ray_node_gram_available) + sum(ray_node_gram_used)) * 1024 * 1024"
            ),
            **kwargs,
        ),
        "network_receive_speed": client.query_prometheus(
            query="ray_node_network_receive_speed", **kwargs
        ),
        "network_send_speed": client.query_prometheus(
            query="ray_node_network_send_speed", **kwargs
        ),
        "cluster_active_nodes": client.query_prometheus(
            query="ray_cluster_active_nodes", **kwargs
        ),
        "cluster_failed_nodes": client.query_prometheus(
            query="ray_cluster_failed_nodes", **kwargs
        ),
        "cluster_pending_nodes": client.query_prometheus(
            query="ray_cluster_pending_nodes", **kwargs
        ),
    }
    metrics = {k: await v for k, v in metrics.items()}
    await client.close()
    return metrics


def get_prometheus_metrics(start_time: float, end_time: float) -> dict:
    try:
        return asyncio.run(_get_prometheus_metrics(start_time, end_time))
    except Exception:
        logger.error(
            "Couldn't obtain Prometheus metrics. "
            f"Exception below:\n{traceback.format_exc()}"
        )
        return {}


def save_prometheus_metrics(
    start_time: float,
    end_time: Optional[float] = None,
    path: Optional[str] = None,
    use_ray: bool = False,
) -> bool:
    path = path or os.environ.get("METRICS_OUTPUT_JSON", None)
    if path:
        if not end_time:
            end_time = time.time()
        if use_ray:
            import ray
            from ray.tune.utils.node import _force_on_current_node

            addr = os.environ.get("RAY_ADDRESS", None)
            ray.init(addr)

            @ray.remote(num_cpus=0)
            def get_metrics():
                end_time = time.time()
                return get_prometheus_metrics(start_time, end_time)

            remote_run = _force_on_current_node(get_metrics)
            ref = remote_run.remote()
            metrics = ray.get(ref, timeout=900)
        else:
            metrics = get_prometheus_metrics(start_time, end_time)
        with open(path, "w") as metrics_output_file:
            json.dump(metrics, metrics_output_file)
        return path
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("start_time", type=float, help="Start time")
    parser.add_argument(
        "--path", default="", type=str, help="Where to save the metrics json"
    )
    parser.add_argument(
        "--use_ray",
        default=False,
        action="store_true",
        help="Whether to run this script in a ray.remote call (for Ray Client)",
    )

    args = parser.parse_args()
    save_prometheus_metrics(args.start_time, path=args.path, use_ray=args.use_ray)

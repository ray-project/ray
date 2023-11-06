import json
import os
from enum import Enum
import aiohttp
from aiohttp.web import Request, Response
import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.dashboard.modules.metrics.metrics_head import (
    PROMETHEUS_HOST_ENV_VAR,
    DEFAULT_PROMETHEUS_HOST,
    PrometheusQueryError,
)
from urllib.parse import quote
import ray


MAX_TIME_WINDOW = "1h"
SAMPLE_RATE = "1s"


class PrometheusQuery(Enum):
    VALUE = ("value", "sum({}) by (dataset)")
    MAX = (
        "max",
        "max_over_time(sum({}) by (dataset)[" + f"{MAX_TIME_WINDOW}:{SAMPLE_RATE}])",
    )


DATASET_METRICS = {
    "ray_data_output_bytes": (PrometheusQuery.MAX,),
    "ray_data_spilled_bytes": (PrometheusQuery.MAX,),
    "ray_data_current_bytes": (PrometheusQuery.VALUE, PrometheusQuery.MAX),
}


class DataHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self.http_session = aiohttp.ClientSession()
        self.prometheus_host = os.environ.get(
            PROMETHEUS_HOST_ENV_VAR, DEFAULT_PROMETHEUS_HOST
        )

    @optional_utils.DashboardHeadRouteTable.get("/api/data/datasets")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_datasets(self, req: Request) -> Response:
        try:
            from ray.data._internal.stats import _get_or_create_stats_actor

            _stats_actor = _get_or_create_stats_actor()
            datasets = ray.get(_stats_actor.get_datasets.remote())
            # Initializes dataset metric values
            for dataset in datasets:
                for metric, queries in DATASET_METRICS.items():
                    datasets[dataset][metric] = {query.value[0]: 0 for query in queries}
            # Query dataset metric values from prometheus
            try:
                # TODO (Zandew): store results of completed datasets in stats actor.
                for metric, queries in DATASET_METRICS.items():
                    for query in queries:
                        result = await self._query_prometheus(
                            query.value[1].format(metric)
                        )
                        for res in result["data"]["result"]:
                            dataset, value = res["metric"]["dataset"], res["value"][1]
                            if dataset in datasets:
                                datasets[dataset][metric][query.value[0]] = value
            except Exception:
                # Prometheus server may not be running,
                # leave these values blank and return other data
                pass
            # Flatten response
            datasets = list(
                map(lambda item: {"dataset": item[0], **item[1]}, datasets.items())
            )
            # Sort by descending start time
            datasets = sorted(datasets, key=lambda x: x["start_time"], reverse=True)
            return Response(
                text=json.dumps({"datasets": datasets}),
                content_type="application/json",
            )
        except Exception as e:
            return Response(
                status=503,
                text=str(e),
            )

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False

    async def _query_prometheus(self, query):
        async with self.http_session.get(
            f"{self.prometheus_host}/api/v1/query?query={quote(query)}"
        ) as resp:
            if resp.status == 200:
                prom_data = await resp.json()
                return prom_data

            message = await resp.text()
            raise PrometheusQueryError(resp.status, message)

import json
import logging
import os
from enum import Enum
from urllib.parse import quote

import aiohttp
from aiohttp.web import Request, Response

import ray.dashboard.optional_utils as optional_utils
from ray.dashboard.modules.metrics.metrics_head import (
    DEFAULT_PROMETHEUS_HEADERS,
    DEFAULT_PROMETHEUS_HOST,
    PROMETHEUS_HEADERS_ENV_VAR,
    PROMETHEUS_HOST_ENV_VAR,
    PrometheusQueryError,
    parse_prom_headers,
)
from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.routes import SubprocessRouteTable as routes

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Window and sampling rate used for certain Prometheus queries.
# Datapoints up until `MAX_TIME_WINDOW` ago are queried at `SAMPLE_RATE` intervals.
MAX_TIME_WINDOW = "1h"
SAMPLE_RATE = "1s"


class PrometheusQuery(Enum):
    """Enum to store types of Prometheus queries for a given metric and grouping."""

    VALUE = ("value", "sum({}{{SessionName='{}'}}) by ({})")
    MAX = (
        "max",
        "max_over_time(sum({}{{SessionName='{}'}}) by ({})["
        + f"{MAX_TIME_WINDOW}:{SAMPLE_RATE}])",
    )


DATASET_METRICS = {
    "ray_data_output_rows": (PrometheusQuery.MAX,),
    "ray_data_spilled_bytes": (PrometheusQuery.MAX,),
    "ray_data_current_bytes": (PrometheusQuery.VALUE, PrometheusQuery.MAX),
    "ray_data_cpu_usage_cores": (PrometheusQuery.VALUE, PrometheusQuery.MAX),
    "ray_data_gpu_usage_cores": (PrometheusQuery.VALUE, PrometheusQuery.MAX),
}


class DataHead(SubprocessModule):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.prometheus_host = os.environ.get(
            PROMETHEUS_HOST_ENV_VAR, DEFAULT_PROMETHEUS_HOST
        )
        self.prometheus_headers = parse_prom_headers(
            os.environ.get(
                PROMETHEUS_HEADERS_ENV_VAR,
                DEFAULT_PROMETHEUS_HEADERS,
            )
        )

    @routes.get("/api/data/datasets/{job_id}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_datasets(self, req: Request) -> Response:
        job_id = req.match_info["job_id"]

        try:
            from ray.data._internal.stats import get_or_create_stats_actor

            _stats_actor = get_or_create_stats_actor()
            datasets = await _stats_actor.get_datasets.remote(job_id)
            # Initializes dataset metric values
            for dataset in datasets:
                for metric, queries in DATASET_METRICS.items():
                    datasets[dataset][metric] = {query.value[0]: 0 for query in queries}
                    for operator in datasets[dataset]["operators"]:
                        datasets[dataset]["operators"][operator][metric] = {
                            query.value[0]: 0 for query in queries
                        }
            # Query dataset metric values from prometheus
            try:
                # TODO (Zandew): store results of completed datasets in stats actor.
                for metric, queries in DATASET_METRICS.items():
                    for query in queries:
                        query_name, prom_query = query.value
                        # Dataset level
                        dataset_result = await self._query_prometheus(
                            prom_query.format(metric, self.session_name, "dataset")
                        )
                        for res in dataset_result["data"]["result"]:
                            dataset, value = res["metric"]["dataset"], res["value"][1]
                            if dataset in datasets:
                                datasets[dataset][metric][query_name] = value

                        # Operator level
                        operator_result = await self._query_prometheus(
                            prom_query.format(
                                metric, self.session_name, "dataset, operator"
                            )
                        )
                        for res in operator_result["data"]["result"]:
                            dataset, operator, value = (
                                res["metric"]["dataset"],
                                res["metric"]["operator"],
                                res["value"][1],
                            )
                            # Check if dataset/operator is in current _StatsActor scope.
                            # Prometheus server may contain metrics from previous
                            # cluster if not reset.
                            if (
                                dataset in datasets
                                and operator in datasets[dataset]["operators"]
                            ):
                                datasets[dataset]["operators"][operator][metric][
                                    query_name
                                ] = value
            except aiohttp.client_exceptions.ClientConnectorError:
                # Prometheus server may not be running,
                # leave these values blank and return other data
                logging.exception(
                    "Exception occurred while querying Prometheus. "
                    "The Prometheus server may not be running."
                )
            # Flatten response
            for dataset in datasets:
                datasets[dataset]["operators"] = list(
                    map(
                        lambda item: {"operator": item[0], **item[1]},
                        datasets[dataset]["operators"].items(),
                    )
                )
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
            logging.exception("Exception occurred while getting datasets.")
            return Response(
                status=503,
                text=str(e),
            )

    async def _query_prometheus(self, query):
        async with self.http_session.get(
            f"{self.prometheus_host}/api/v1/query?query={quote(query)}",
            headers=self.prometheus_headers,
        ) as resp:
            if resp.status == 200:
                prom_data = await resp.json()
                return prom_data

            message = await resp.text()
            raise PrometheusQueryError(resp.status, message)

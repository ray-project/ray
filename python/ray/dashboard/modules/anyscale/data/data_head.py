import json
import logging
import os
from dataclasses import asdict
from typing import Tuple, Dict
from urllib.parse import quote

import aiohttp
from aiohttp.web import Request, Response

import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.dashboard.modules.metrics.metrics_head import (
    DEFAULT_PROMETHEUS_HEADERS,
    DEFAULT_PROMETHEUS_HOST,
    PROMETHEUS_HEADERS_ENV_VAR,
    PROMETHEUS_HOST_ENV_VAR,
    PrometheusQueryError,
    parse_prom_headers,
)
from ray.dashboard.modules.anyscale.data.data_schema import (
    Metric,
    OperatorMetrics,
    DatasetMetrics,
    DatasetResponse,
)
from ray.dashboard.modules.job.common import JobInfoStorageClient
from ray.dashboard.modules.job.utils import find_jobs_by_job_ids


PROMETHEUS_MAX_TIME_WINDOW = "1h"
PROMETHEUS_SAMPLE_RATE = "1s"
PROMETHEUS_CURRENT_VALUE_QUERY = "sum({}{{SessionName='{}'}}) by (dataset, operator)"
PROMETHEUS_MAX_OVER_TIME_QUERY = (
    "max_over_time(sum({}{{SessionName='{}'}}) by (dataset, operator)["
    + f"{PROMETHEUS_MAX_TIME_WINDOW}:{PROMETHEUS_SAMPLE_RATE}])"
)
PROMETHEUS_METRICS = [
    "ray_data_average_bytes_inputs_per_task",
    "ray_data_average_bytes_outputs_per_task",
    "ray_data_average_bytes_per_output",
    "ray_data_average_num_outputs_per_task",
    "ray_data_block_generation_time",
    "ray_data_bytes_inputs_of_submitted_tasks",
    "ray_data_bytes_inputs_received",
    "ray_data_bytes_outputs_of_finished_tasks",
    "ray_data_bytes_outputs_taken",
    "ray_data_bytes_task_inputs_processed",
    "ray_data_bytes_task_outputs_generated",
    "ray_data_cpu_usage_cores",
    "ray_data_current_bytes",
    "ray_data_freed_bytes",
    "ray_data_gpu_usage_cores",
    "ray_data_iter_initialize_seconds",
    "ray_data_iter_total_blocked_seconds",
    "ray_data_iter_user_seconds",
    "ray_data_num_inputs_received",
    "ray_data_num_outputs_of_finished_tasks",
    "ray_data_num_outputs_taken",
    "ray_data_num_task_inputs_processed",
    "ray_data_num_task_outputs_generated",
    "ray_data_num_tasks_failed",
    "ray_data_num_tasks_finished",
    "ray_data_num_tasks_have_outputs",
    "ray_data_num_tasks_running",
    "ray_data_num_tasks_submitted",
    "ray_data_obj_store_mem_freed",
    "ray_data_obj_store_mem_internal_inqueue",
    "ray_data_obj_store_mem_internal_inqueue_blocks",
    "ray_data_obj_store_mem_internal_outqueue",
    "ray_data_obj_store_mem_internal_outqueue_blocks",
    "ray_data_obj_store_mem_pending_task_inputs",
    "ray_data_obj_store_mem_spilled",
    "ray_data_obj_store_mem_used",
    "ray_data_output_bytes",
    "ray_data_output_rows",
    "ray_data_rows_task_outputs_generated",
    "ray_data_spilled_bytes",
    "ray_data_task_submission_backpressure_time",
]


class AnyscaleDataHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        super().__init__(config)
        self._job_info_client = None
        self.prometheus_host = os.environ.get(
            PROMETHEUS_HOST_ENV_VAR, DEFAULT_PROMETHEUS_HOST
        )
        self.prometheus_headers = parse_prom_headers(
            os.environ.get(
                PROMETHEUS_HEADERS_ENV_VAR,
                DEFAULT_PROMETHEUS_HEADERS,
            )
        )
        self.stats_actor = None

    def _get_stats_actor(self):
        """
        Returns the singleton stats actor.
        """
        if self.stats_actor is None:
            from ray.data._internal.stats import _get_or_create_stats_actor

            self.stats_actors = _get_or_create_stats_actor()

        return self.stats_actors

    def _get_operator_id(self, dataset_id, operator_id):
        """
        Returns a unique identifier for an operator in a dataset.
        """
        return f"{operator_id}@{dataset_id}"

    async def _async_get_dataset_metrics(
        self,
    ) -> Tuple[Dict[str, DatasetMetrics], Dict[str, OperatorMetrics]]:
        """
        Computes the dataset and operator state for a given job_id. It
        returns a tuple of two dictionaries:
        - The first dictionary maps dataset names to DatasetMetrics objects.
        - The second dictionary maps operator names to OperatorMetrics objects.
        """
        dataset_metrics, operator_metrics = {}, {}
        stats_actor = self._get_stats_actor()
        datasets = await stats_actor.get_datasets.remote()
        for dataset_id, dataset_metadata in datasets.items():
            dataset_metric = DatasetMetrics(
                **{
                    **{
                        key: dataset_metadata.get(key)
                        for key in DatasetMetrics.__dataclass_fields__
                    },
                    **{
                        "id": dataset_id,
                        "name": dataset_id,
                        "session_name": self.session_name,
                        "operator_metrics": [],
                    },
                }
            )
            for operator_id, operator_metadata in dataset_metadata["operators"].items():
                operator_metric = OperatorMetrics(
                    **{
                        **{
                            key: operator_metadata.get(key)
                            for key in OperatorMetrics.__dataclass_fields__
                        },
                        **{
                            "id": self._get_operator_id(dataset_id, operator_id),
                            "metrics": {},
                        },
                    }
                )
                dataset_metric.operator_metrics.append(operator_metric)
                operator_metrics[operator_metric.id] = operator_metric
            dataset_metrics[dataset_metric.id] = dataset_metric

        return dataset_metrics, operator_metrics

    async def _async_get_operator_metrics(self, prom_query: str) -> Dict[str, float]:
        """
        Queries Prometheus for the given prom_query and returns a dictionary that
        maps dataset or operator names to the corresponding metric values.
        """
        metrics = {}
        results = await self._query_prometheus(prom_query)
        for res in results["data"]["result"]:
            dataset_id, operator_id, value = (
                res["metric"]["dataset"],
                res["metric"]["operator"],
                # value is return as a list of two values, the first value is the
                # timestamp, the second value is the actual value
                res["value"][1],
            )
            metrics[self._get_operator_id(dataset_id, operator_id)] = value

        return metrics

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

    def _get_metric_column_name(self, metric: str):
        """
        Returns the dashboard column name for the given metric.
        """
        return metric.replace("ray_data_", "").replace("_", " ").title()

    @optional_utils.DashboardHeadRouteTable.get("/api/anyscale/data/datasets")
    @optional_utils.init_ray_and_catch_exceptions()
    async def async_get_datasets(self, req: Request) -> Response:
        """
        This function is a route handler that returns the dataset metrics.
        It queries Prometheus for the metrics and returns the dataset metrics.
        """
        try:
            dataset_metrics, operator_metrics = await self._async_get_dataset_metrics()

            try:
                for metric in PROMETHEUS_METRICS:
                    current_value_metrics = await self._async_get_operator_metrics(
                        PROMETHEUS_CURRENT_VALUE_QUERY.format(metric, self.session_name)
                    )
                    max_over_time_metrics = await self._async_get_operator_metrics(
                        PROMETHEUS_MAX_OVER_TIME_QUERY.format(metric, self.session_name)
                    )
                    for operator_id, operator_metric in operator_metrics.items():
                        operator_metric.metrics[metric] = Metric(
                            name=self._get_metric_column_name(metric),
                            current_value=float(
                                current_value_metrics.get(operator_id, "0.0")
                            ),
                            max_over_time=float(
                                max_over_time_metrics.get(operator_id, "0.0")
                            ),
                        )
            except aiohttp.client_exceptions.ClientConnectorError:
                # Prometheus server may not be running,
                # leave these values blank and return other data
                logging.exception(
                    "Exception occurred while querying Prometheus. "
                    "The Prometheus server may not be running."
                )

            return Response(
                text=json.dumps(
                    asdict(DatasetResponse(datasets=list(dataset_metrics.values())))
                ),
                content_type="application/json",
            )
        except Exception as e:
            logging.exception("Exception occured while getting datasets.")
            return Response(
                status=503,
                text=str(e),
            )

    @optional_utils.DashboardHeadRouteTable.get("/api/anyscale/data/jobs")
    @optional_utils.init_ray_and_catch_exceptions()
    async def async_get_jobs(self, req: Request) -> Response:
        _stats_actor = self._get_stats_actor()
        datasets = await _stats_actor.get_datasets.remote(None)

        jobs = await find_jobs_by_job_ids(
            self.gcs_aio_client,
            self._job_info_client,
            [dataset["job_id"] for dataset in datasets.values()],
        )

        return Response(
            text=json.dumps([job.dict() for job in jobs.values()]),
            content_type="application/json",
        )

    async def run(self, server):
        if not self._job_info_client:
            self._job_info_client = JobInfoStorageClient(self.gcs_aio_client)

    @staticmethod
    def is_minimal_module():
        return False

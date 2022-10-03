from typing import Any, Dict, Optional
import aiohttp
import logging
import os
from pydantic import BaseModel
import shutil
from urllib.parse import quote

from ray.dashboard.modules.metrics.grafana_datasource_template import (
    GRAFANA_DATASOURCE_TEMPLATE,
)
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PROMETHEUS_HOST_ENV_VAR = "RAY_PROMETHEUS_HOST"
DEFAULT_PROMETHEUS_HOST = "http://localhost:9090"

routes = dashboard_optional_utils.ClassMethodRouteTable


METRICS_OUTPUT_ROOT_ENV_VAR = "RAY_METRICS_OUTPUT_ROOT"
METRICS_INPUT_ROOT = os.path.join(os.path.dirname(__file__), "export")

GRAFANA_CONFIG_INPUT_PATH = os.path.join(METRICS_INPUT_ROOT, "grafana")

PROMETHEUS_CONFIG_INPUT_PATH = os.path.join(
    METRICS_INPUT_ROOT, "prometheus", "prometheus.yml"
)


class TaskProgress(BaseModel):
    num_finished: int = 0
    num_pending_args_avail: int = 0
    num_submitted_to_worker: int = 0
    num_running: int = 0
    num_pending_node_assignment: int = 0
    num_unknown: int = 0


prometheus_metric_map = {
    "FINISHED": "num_finished",
    "PENDING_ARGS_AVAIL": "num_pending_args_avail",
    "SUBMITTED_TO_WORKER": "num_submitted_to_worker",
    "RUNNING": "num_running",
    "RUNNING_IN_RAY_GET": "num_running",
    "RUNNING_IN_RAY_WAIT": "num_running",
    "PENDING_NODE_ASSIGNMENT": "num_pending_node_assignment",
    "PENDING_ARGS_FETCH": "num_pending_node_assignment",
    "PENDING_OBJ_STORE_MEM_AVAIL": "num_pending_node_assignment",
}


class MetricsHead(dashboard_utils.DashboardHeadModule):
    def __init__(
        self, dashboard_head, http_session: Optional[aiohttp.ClientSession] = None
    ):
        super().__init__(dashboard_head)
        self.http_session = http_session or aiohttp.ClientSession()
        self.prometheus_host = os.environ.get(
            PROMETHEUS_HOST_ENV_VAR, DEFAULT_PROMETHEUS_HOST
        )
        default_metrics_root = os.path.join(self._dashboard_head.session_dir, "metrics")
        self.metrics_root = os.environ.get(
            METRICS_OUTPUT_ROOT_ENV_VAR, default_metrics_root
        )

    @routes.get("/api/progress")
    async def get_progress(self, req):
        """
        Fetches the progress of tasks by job id. If job_id is not provided,
        then we will fetch the progress across all jobs.
        """
        job_id = req.query.get("job_id")
        job_id_query = f'{{JobId="{job_id}"}}' if job_id else ""
        query = f"sum(ray_tasks{job_id_query}) by (State)"
        async with self.http_session.get(
            f"{self.prometheus_host}/api/v1/query?query={quote(query)}"
        ) as resp:
            if resp.status == 200:
                prom_data = await resp.json()
                progress = _format_prometheus_output(prom_data)
                if progress:
                    return dashboard_optional_utils.rest_response(
                        success=True, message="success", detail=progress.dict()
                    )

            message = await resp.text()
            return dashboard_optional_utils.rest_response(
                success=False,
                message="Error fetching data from prometheus. "
                f"status: {resp.status}, message: {message}",
            )

    @staticmethod
    def is_minimal_module():
        return False

    def _create_default_grafana_configs(self):
        """
        Creates the grafana configurations that are by default provided by Ray.
        """
        grafana_config_output_path = os.path.join(self.metrics_root, "grafana")

        # Copy default grafana configurations
        if os.path.exists(grafana_config_output_path):
            shutil.rmtree(grafana_config_output_path)
        os.makedirs(os.path.dirname(grafana_config_output_path), exist_ok=True)
        shutil.copytree(GRAFANA_CONFIG_INPUT_PATH, grafana_config_output_path)
        # Overwrite grafana's prometheus datasource based on env var
        prometheus_host = os.environ.get(
            PROMETHEUS_HOST_ENV_VAR, DEFAULT_PROMETHEUS_HOST
        )
        data_sources_path = os.path.join(
            grafana_config_output_path, "provisioning", "datasources"
        )
        os.makedirs(
            data_sources_path,
            exist_ok=True,
        )
        with open(
            os.path.join(
                data_sources_path,
                "default.yml",
            ),
            "w",
        ) as f:
            f.write(GRAFANA_DATASOURCE_TEMPLATE.format(prometheus_host=prometheus_host))

    def _create_default_prometheus_configs(self):
        """
        Creates the prometheus configurations that are by default provided by Ray.
        """
        prometheus_config_output_path = os.path.join(
            self.metrics_root, "prometheus", "prometheus.yml"
        )

        # Copy default prometheus configurations
        if os.path.exists(prometheus_config_output_path):
            os.remove(prometheus_config_output_path)
        os.makedirs(os.path.dirname(prometheus_config_output_path), exist_ok=True)
        shutil.copy(PROMETHEUS_CONFIG_INPUT_PATH, prometheus_config_output_path)

    async def run(self, server):
        self._create_default_grafana_configs()
        self._create_default_prometheus_configs()

        logger.info(
            f"Generated prometheus and grafana configurations in: {self.metrics_root}"
        )


def _format_prometheus_output(prom_data: Dict[str, Any]) -> Optional[TaskProgress]:
    if prom_data["status"] == "success" and prom_data["data"]["resultType"] == "vector":
        metrics = prom_data["data"]["result"]
        kwargs = {}
        for metric in metrics:
            metric_name = metric["metric"]["State"]
            kwarg_name = (
                prometheus_metric_map[metric_name]
                if metric_name in prometheus_metric_map
                else "num_unknown"
            )
            # metric["value"] is a tuple where first item is a timestamp
            # and second item is the value.
            metric_value = int(metric["value"][1])
            kwargs[kwarg_name] = kwargs.get(kwarg_name, 0) + metric_value

        return TaskProgress(**kwargs)

    return None

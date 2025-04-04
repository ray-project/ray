from urllib.parse import quote
import shutil
import aiohttp
import enum
import json
import logging
import os
from datetime import datetime
from typing import Optional

import aiohttp.web

from ray import ActorID
from ray._private.pydantic_compat import BaseModel, Extra, Field, validator
from ray._private.ray_constants import SESSION_LATEST, PROMETHEUS_SERVICE_DISCOVERY_FILE
from ray._private.utils import load_class
from ray.dashboard import (
    optional_utils as dashboard_optional_utils,
    utils as dashboard_utils,
)
from ray.dashboard.modules.api.metrics.consts import (
    GRAFANA_HOST_ENV_VAR,
    DEFAULT_GRAFANA_HOST,
    PROMETHEUS_HOST_ENV_VAR,
    DEFAULT_PROMETHEUS_HOST,
    PROMETHEUS_HEADERS_ENV_VAR,
    DEFAULT_PROMETHEUS_HEADERS,
    METRICS_OUTPUT_ROOT_ENV_VAR,
    GRAFANA_DASHBOARD_OUTPUT_DIR_ENV_VAR,
    PROMETHEUS_NAME_ENV_VAR,
    DEFAULT_PROMETHEUS_NAME,
    GRAFANA_HOST_DISABLED_VALUE,
    GRAFANA_IFRAME_HOST_ENV_VAR,
    GRAFANA_HEALTHCHECK_PATH,
    PROMETHEUS_HEALTHCHECK_PATH,
)
from ray.dashboard.modules.api.metrics.grafana_dashboard_factory import (
    generate_default_grafana_dashboard,
    generate_serve_grafana_dashboard,
    generate_serve_deployment_grafana_dashboard,
    generate_data_grafana_dashboard,
)
from ray.dashboard.modules.api.metrics.templates import (
    GRAFANA_INI_TEMPLATE,
    DASHBOARD_PROVISIONING_TEMPLATE,
    GRAFANA_DATASOURCE_TEMPLATE,
    PROMETHEUS_YML_TEMPLATE,
)
from ray.dashboard.modules.api.metrics.utils import (
    parse_prom_headers,
    PrometheusQueryError,
)
from ray.dashboard.modules.api.utils import HealthChecker
from ray.dashboard.consts import RAY_CLUSTER_ACTIVITY_HOOK
from ray.dashboard.subprocesses.routes import SubprocessRouteTable as routes
from ray.dashboard.subprocesses.module import SubprocessModule

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SNAPSHOT_API_TIMEOUT_SECONDS = 30


class RayActivityStatus(str, enum.Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    ERROR = "ERROR"


class RayActivityResponse(BaseModel, extra=Extra.allow):
    """
    Pydantic model used to inform if a particular Ray component can be considered
    active, and metadata about observation.
    """

    is_active: RayActivityStatus = Field(
        ...,
        description=(
            "Whether the corresponding Ray component is considered active or inactive, "
            "or if there was an error while collecting this observation."
        ),
    )
    reason: Optional[str] = Field(
        None, description="Reason if Ray component is considered active or errored."
    )
    timestamp: float = Field(
        ...,
        description=(
            "Timestamp of when this observation about the Ray component was made. "
            "This is in the format of seconds since unix epoch."
        ),
    )
    last_activity_at: Optional[float] = Field(
        None,
        description=(
            "Timestamp when last actvity of this Ray component finished in format of "
            "seconds since unix epoch. This field does not need to be populated "
            "for Ray components where it is not meaningful."
        ),
    )

    @validator("reason", always=True)
    def reason_required(cls, v, values, **kwargs):
        if "is_active" in values and values["is_active"] != RayActivityStatus.INACTIVE:
            if v is None:
                raise ValueError(
                    'Reason is required if is_active is "active" or "error"'
                )
        return v


class APIHead(SubprocessModule):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._health_checker = HealthChecker(self.gcs_aio_client)

        self.grafana_host = os.environ.get(GRAFANA_HOST_ENV_VAR, DEFAULT_GRAFANA_HOST)
        self.prometheus_host = os.environ.get(
            PROMETHEUS_HOST_ENV_VAR, DEFAULT_PROMETHEUS_HOST
        )
        default_metrics_root = os.path.join(self.session_dir, "metrics")
        self.prometheus_headers = parse_prom_headers(
            os.environ.get(
                PROMETHEUS_HEADERS_ENV_VAR,
                DEFAULT_PROMETHEUS_HEADERS,
            )
        )
        session_latest_metrics_root = os.path.join(
            self.temp_dir, SESSION_LATEST, "metrics"
        )
        self._metrics_root = os.environ.get(
            METRICS_OUTPUT_ROOT_ENV_VAR, default_metrics_root
        )
        self._metrics_root_session_latest = os.environ.get(
            METRICS_OUTPUT_ROOT_ENV_VAR, session_latest_metrics_root
        )
        self._grafana_config_output_path = os.path.join(self._metrics_root, "grafana")
        self._grafana_session_latest_config_output_path = os.path.join(
            self._metrics_root_session_latest, "grafana"
        )
        self._grafana_dashboard_output_dir = os.environ.get(
            GRAFANA_DASHBOARD_OUTPUT_DIR_ENV_VAR,
            os.path.join(self._grafana_config_output_path, "dashboards"),
        )

        self._prometheus_name = os.environ.get(
            PROMETHEUS_NAME_ENV_VAR, DEFAULT_PROMETHEUS_NAME
        )

        # To be set later when dashboards gets generated
        self._dashboard_uids = {}

    async def run(self):
        await super().run()
        self._create_default_grafana_configs()
        self._create_default_prometheus_configs()

        logger.info(
            f"Generated prometheus and grafana configurations in: {self._metrics_root}"
        )

    @routes.get("/api/gcs_healthz")
    async def health_check(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        try:
            alive = await self._health_checker.check_gcs_liveness()
            if alive is True:
                return aiohttp.web.Response(
                    text="success",
                    content_type="application/text",
                )
        except Exception as e:
            return aiohttp.web.HTTPServiceUnavailable(
                reason=f"Health check failed: {e}"
            )

        return aiohttp.web.HTTPServiceUnavailable(reason="Health check failed")

    @routes.get("/api/actors/kill")
    async def kill_actor_gcs(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        actor_id = req.query.get("actor_id")
        force_kill = req.query.get("force_kill", False) in ("true", "True")
        no_restart = req.query.get("no_restart", False) in ("true", "True")
        if not actor_id:
            return dashboard_optional_utils.rest_response(
                status_code=dashboard_utils.HTTPStatusCode.INTERNAL_ERROR,
                message="actor_id is required.",
            )

        status_code = await self.gcs_aio_client.kill_actor(
            ActorID.from_hex(actor_id),
            force_kill,
            no_restart,
            timeout=SNAPSHOT_API_TIMEOUT_SECONDS,
        )

        if status_code == dashboard_utils.HTTPStatusCode.NOT_FOUND:
            message = f"Actor with id {actor_id} not found."
        elif status_code == dashboard_utils.HTTPStatusCode.INTERNAL_ERROR:
            message = f"Failed to kill actor with id {actor_id}."
        elif status_code == dashboard_utils.HTTPStatusCode.OK:
            message = (
                f"Force killed actor with id {actor_id}"
                if force_kill
                else f"Requested actor with id {actor_id} to terminate. "
                + "It will exit once running tasks complete"
            )
        else:
            message = f"Unknown status code: {status_code}. Please open a bug report in the Ray repository."

        return dashboard_optional_utils.rest_response(
            status_code=status_code, message=message
        )

    @routes.get("/api/component_activities")
    async def get_component_activities(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        timeout = req.query.get("timeout", None)
        if timeout and timeout.isdigit():
            timeout = int(timeout)
        else:
            timeout = SNAPSHOT_API_TIMEOUT_SECONDS

        # Get activity information for driver
        driver_activity_info = await self._get_job_activity_info(timeout=timeout)
        resp = {"driver": dict(driver_activity_info)}

        if RAY_CLUSTER_ACTIVITY_HOOK in os.environ:
            try:
                cluster_activity_callable = load_class(
                    os.environ[RAY_CLUSTER_ACTIVITY_HOOK]
                )
                external_activity_output = cluster_activity_callable()
                assert isinstance(external_activity_output, dict), (
                    f"Output of hook {os.environ[RAY_CLUSTER_ACTIVITY_HOOK]} "
                    "should be Dict[str, RayActivityResponse]. Got "
                    f"output: {external_activity_output}"
                )
                for component_type in external_activity_output:
                    try:
                        component_activity_output = external_activity_output[
                            component_type
                        ]
                        # Parse and validate output to type RayActivityResponse
                        component_activity_output = RayActivityResponse(
                            **dict(component_activity_output)
                        )
                        resp[component_type] = dict(component_activity_output)
                    except Exception as e:
                        logger.exception(
                            f"Failed to get activity status of {component_type} "
                            f"from user hook {os.environ[RAY_CLUSTER_ACTIVITY_HOOK]}."
                        )
                        resp[component_type] = {
                            "is_active": RayActivityStatus.ERROR,
                            "reason": repr(e),
                            "timestamp": datetime.now().timestamp(),
                        }
            except Exception as e:
                logger.exception(
                    "Failed to get activity status from user "
                    f"hook {os.environ[RAY_CLUSTER_ACTIVITY_HOOK]}."
                )
                resp["external_component"] = {
                    "is_active": RayActivityStatus.ERROR,
                    "reason": repr(e),
                    "timestamp": datetime.now().timestamp(),
                }

        return aiohttp.web.Response(
            text=json.dumps(resp),
            content_type="application/json",
            status=aiohttp.web.HTTPOk.status_code,
        )

    async def _get_job_activity_info(self, timeout: int) -> RayActivityResponse:
        # Returns if there is Ray activity from drivers (job).
        # Drivers in namespaces that start with _ray_internal_ are not
        # considered activity.
        # This includes the _ray_internal_dashboard job that gets automatically
        # created with every cluster
        try:
            reply = await self.gcs_aio_client.get_all_job_info(
                skip_submission_job_info_field=True,
                skip_is_running_tasks_field=True,
                timeout=timeout,
            )

            num_active_drivers = 0
            latest_job_end_time = 0
            for job_table_entry in reply.values():
                is_dead = bool(job_table_entry.is_dead)
                in_internal_namespace = job_table_entry.config.ray_namespace.startswith(
                    "_ray_internal_"
                )
                latest_job_end_time = (
                    max(latest_job_end_time, job_table_entry.end_time)
                    if job_table_entry.end_time
                    else latest_job_end_time
                )
                if not is_dead and not in_internal_namespace:
                    num_active_drivers += 1

            current_timestamp = datetime.now().timestamp()
            # Latest job end time must be before or equal to the current timestamp.
            # Job end times may be provided in epoch milliseconds. Check if this
            # is true, and convert to seconds
            if latest_job_end_time > current_timestamp:
                latest_job_end_time = latest_job_end_time / 1000
                assert current_timestamp >= latest_job_end_time, (
                    f"Most recent job end time {latest_job_end_time} must be "
                    f"before or equal to the current timestamp {current_timestamp}"
                )

            is_active = (
                RayActivityStatus.ACTIVE
                if num_active_drivers > 0
                else RayActivityStatus.INACTIVE
            )
            return RayActivityResponse(
                is_active=is_active,
                reason=f"Number of active drivers: {num_active_drivers}"
                if num_active_drivers
                else None,
                timestamp=current_timestamp,
                # If latest_job_end_time == 0, no jobs have finished yet so don't
                # populate last_activity_at
                last_activity_at=latest_job_end_time if latest_job_end_time else None,
            )
        except Exception as e:
            logger.exception("Failed to get activity status of Ray drivers.")
            return RayActivityResponse(
                is_active=RayActivityStatus.ERROR,
                reason=repr(e),
                timestamp=datetime.now().timestamp(),
            )

    @routes.get("/api/grafana_health")
    async def grafana_health(self, req) -> aiohttp.web.Response:
        """
        Endpoint that checks if Grafana is running
        """
        # If disabled, we don't want to show the metrics tab at all.
        if self.grafana_host == GRAFANA_HOST_DISABLED_VALUE:
            return dashboard_optional_utils.rest_response(
                status_code=dashboard_utils.HTTPStatusCode.OK,
                message="Grafana disabled",
                grafana_host=GRAFANA_HOST_DISABLED_VALUE,
            )

        grafana_iframe_host = os.environ.get(
            GRAFANA_IFRAME_HOST_ENV_VAR, self.grafana_host
        )
        path = f"{self.grafana_host}/{GRAFANA_HEALTHCHECK_PATH}"
        try:
            async with self.http_session.get(path) as resp:
                if resp.status != 200:
                    return dashboard_optional_utils.rest_response(
                        status_code=dashboard_utils.HTTPStatusCode.INTERNAL_ERROR,
                        message="Grafana healtcheck failed",
                        status=resp.status,
                    )
                json = await resp.json()
                # Check if the required Grafana services are running.
                if json["database"] != "ok":
                    return dashboard_optional_utils.rest_response(
                        status_code=dashboard_utils.HTTPStatusCode.INTERNAL_ERROR,
                        message="Grafana healtcheck failed. Database not ok.",
                        status=resp.status,
                        json=json,
                    )

                return dashboard_optional_utils.rest_response(
                    status_code=dashboard_utils.HTTPStatusCode.OK,
                    message="Grafana running",
                    grafana_host=grafana_iframe_host,
                    session_name=self.session_name,
                    dashboard_uids=self._dashboard_uids,
                    dashboard_datasource=self._prometheus_name,
                )

        except Exception as e:
            logger.debug(
                "Error fetching grafana endpoint. Is grafana running?", exc_info=e
            )

            return dashboard_optional_utils.rest_response(
                status_code=dashboard_utils.HTTPStatusCode.INTERNAL_ERROR,
                message="Grafana healtcheck failed",
                exception=str(e),
            )

    @routes.get("/api/prometheus_health")
    async def prometheus_health(self, req):
        try:
            path = f"{self.prometheus_host}/{PROMETHEUS_HEALTHCHECK_PATH}"

            async with self.http_session.get(
                path, headers=self.prometheus_headers
            ) as resp:
                if resp.status != 200:
                    return dashboard_optional_utils.rest_response(
                        status_code=dashboard_utils.HTTPStatusCode.INTERNAL_ERROR,
                        message="prometheus healthcheck failed.",
                        status=resp.status,
                    )

                return dashboard_optional_utils.rest_response(
                    status_code=dashboard_utils.HTTPStatusCode.OK,
                    message="prometheus running",
                )
        except Exception as e:
            logger.debug(
                "Error fetching prometheus endpoint. Is prometheus running?", exc_info=e
            )
            return dashboard_optional_utils.rest_response(
                status_code=dashboard_utils.HTTPStatusCode.INTERNAL_ERROR,
                message="prometheus healthcheck failed.",
                reason=str(e),
            )

    def _create_default_grafana_configs(self):
        """
        Creates the Grafana configurations that are by default provided by Ray.
        """
        # Create Grafana configuration folder
        if os.path.exists(self._grafana_config_output_path):
            shutil.rmtree(self._grafana_config_output_path)
        os.makedirs(self._grafana_config_output_path, exist_ok=True)

        # Overwrite Grafana's configuration file
        grafana_provisioning_folder = os.path.join(
            self._grafana_config_output_path, "provisioning"
        )
        grafana_prov_folder_with_latest_session = os.path.join(
            self._grafana_session_latest_config_output_path, "provisioning"
        )
        with open(
            os.path.join(
                self._grafana_config_output_path,
                "grafana.ini",
            ),
            "w",
        ) as f:
            f.write(
                GRAFANA_INI_TEMPLATE.format(
                    grafana_provisioning_folder=grafana_prov_folder_with_latest_session
                )
            )

        # Overwrite Grafana's dashboard provisioning directory based on env var
        dashboard_provisioning_path = os.path.join(
            grafana_provisioning_folder, "dashboards"
        )
        os.makedirs(
            dashboard_provisioning_path,
            exist_ok=True,
        )
        with open(
            os.path.join(
                dashboard_provisioning_path,
                "default.yml",
            ),
            "w",
        ) as f:
            f.write(
                DASHBOARD_PROVISIONING_TEMPLATE.format(
                    dashboard_output_folder=self._grafana_dashboard_output_dir
                )
            )

        # Overwrite Grafana's Prometheus datasource based on env var
        prometheus_host = os.environ.get(
            PROMETHEUS_HOST_ENV_VAR, DEFAULT_PROMETHEUS_HOST
        )
        prometheus_headers = parse_prom_headers(
            os.environ.get(PROMETHEUS_HEADERS_ENV_VAR, DEFAULT_PROMETHEUS_HEADERS)
        )
        # parse_prom_headers will make sure the prometheus_headers is either format of:
        # 1. {"H1": "V1", "H2": "V2"} or
        # 2. [["H1", "V1"], ["H2", "V2"], ["H2", "V3"]]
        prometheus_header_pairs = []
        if isinstance(prometheus_headers, list):
            prometheus_header_pairs = prometheus_headers
        elif isinstance(prometheus_headers, dict):
            prometheus_header_pairs = list(prometheus_headers.items())

        data_sources_path = os.path.join(grafana_provisioning_folder, "datasources")
        os.makedirs(
            data_sources_path,
            exist_ok=True,
        )
        os.makedirs(
            self._grafana_dashboard_output_dir,
            exist_ok=True,
        )
        with open(
            os.path.join(
                data_sources_path,
                "default.yml",
            ),
            "w",
        ) as f:
            f.write(
                GRAFANA_DATASOURCE_TEMPLATE(
                    prometheus_host=prometheus_host,
                    prometheus_name=self._prometheus_name,
                    jsonData={
                        f"httpHeaderName{i+1}": header
                        for i, (header, _) in enumerate(prometheus_header_pairs)
                    },
                    secureJsonData={
                        f"httpHeaderValue{i+1}": value
                        for i, (_, value) in enumerate(prometheus_header_pairs)
                    },
                )
            )
        with open(
            os.path.join(
                self._grafana_dashboard_output_dir,
                "default_grafana_dashboard.json",
            ),
            "w",
        ) as f:
            (
                content,
                self._dashboard_uids["default"],
            ) = generate_default_grafana_dashboard()
            f.write(content)
        with open(
            os.path.join(
                self._grafana_dashboard_output_dir,
                "serve_grafana_dashboard.json",
            ),
            "w",
        ) as f:
            content, self._dashboard_uids["serve"] = generate_serve_grafana_dashboard()
            f.write(content)
        with open(
            os.path.join(
                self._grafana_dashboard_output_dir,
                "serve_deployment_grafana_dashboard.json",
            ),
            "w",
        ) as f:
            (
                content,
                self._dashboard_uids["serve_deployment"],
            ) = generate_serve_deployment_grafana_dashboard()
            f.write(content)
        with open(
            os.path.join(
                self._grafana_dashboard_output_dir,
                "data_grafana_dashboard.json",
            ),
            "w",
        ) as f:
            (
                content,
                self._dashboard_uids["data"],
            ) = generate_data_grafana_dashboard()
            f.write(content)

    def _create_default_prometheus_configs(self):
        """
        Creates the Prometheus configurations that are by default provided by Ray.
        """
        prometheus_config_output_path = os.path.join(
            self._metrics_root, "prometheus", "prometheus.yml"
        )

        # Generate the default Prometheus configurations
        if os.path.exists(prometheus_config_output_path):
            os.remove(prometheus_config_output_path)
        os.makedirs(os.path.dirname(prometheus_config_output_path), exist_ok=True)

        # This code generates the Prometheus config based on the custom temporary root
        # path set by the user at Ray cluster start up (via --temp-dir). In contrast,
        # start_prometheus in install_and_start_prometheus.py uses a hardcoded
        # Prometheus config at PROMETHEUS_CONFIG_INPUT_PATH that always uses "/tmp/ray".
        # Other than the root path, the config file generated here is identical to that
        # hardcoded config file.
        prom_discovery_file_path = os.path.join(
            self.temp_dir, PROMETHEUS_SERVICE_DISCOVERY_FILE
        )
        with open(prometheus_config_output_path, "w") as f:
            f.write(
                PROMETHEUS_YML_TEMPLATE.format(
                    prom_metrics_service_discovery_file_path=prom_discovery_file_path
                )
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

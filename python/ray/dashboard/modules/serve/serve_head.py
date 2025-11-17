import asyncio
import dataclasses
import glob
import json
import logging
import os
from functools import wraps
from typing import Any, Dict, Optional

import aiohttp
from aiohttp.web import Request, Response

import ray
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray._common.pydantic_compat import ValidationError
from ray.dashboard.modules.version import CURRENT_VERSION, VersionResponse
from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.routes import SubprocessRouteTable as routes
from ray.exceptions import RayTaskError
from ray.serve._private.logging_utils import get_serve_logs_dir
from ray.serve.schema import (
    DeploymentAutoscalingDetail,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def validate_endpoint():
    def decorator(func):
        @wraps(func)
        async def check(self, *args, **kwargs):
            try:
                from ray import serve  # noqa: F401
            except ImportError:
                return Response(
                    status=501,
                    text=(
                        "Serve dependencies are not installed. Please run `pip "
                        'install "ray[serve]"`.'
                    ),
                )
            return await func(self, *args, **kwargs)

        return check

    return decorator


# NOTE (shrekris-anyscale): This class uses delayed imports for all
# Ray Serve-related modules. That way, users can use the Ray dashboard agent for
# non-Serve purposes without downloading Serve dependencies.
class ServeHead(SubprocessModule):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._controller = None
        self._controller_lock = asyncio.Lock()

        # serve_start_async is not thread-safe call. This lock
        # will make sure there is only one call that starts the serve instance.
        # If the lock is already acquired by another async task, the async task
        # will asynchronously wait for the lock.
        self._controller_start_lock = asyncio.Lock()

        # To init gcs_client in internal_kv for record_extra_usage_tag.
        assert self.gcs_client is not None
        assert ray.experimental.internal_kv._internal_kv_initialized()

    # TODO: It's better to use `/api/version`.
    # It requires a refactor of ClassMethodRouteTable to differentiate the server.
    @routes.get("/api/ray/version")
    async def get_version(self, req: Request) -> Response:
        # NOTE(edoakes): CURRENT_VERSION should be bumped and checked on the
        # client when we have backwards-incompatible changes.
        resp = VersionResponse(
            version=CURRENT_VERSION,
            ray_version=ray.__version__,
            ray_commit=ray.__commit__,
            session_name=self.session_name,
        )
        return Response(
            text=json.dumps(dataclasses.asdict(resp)),
            content_type="application/json",
            status=aiohttp.web.HTTPOk.status_code,
        )

    @routes.get("/api/serve/applications/")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    @validate_endpoint()
    async def get_serve_instance_details(self, req: Request) -> Response:
        from ray.serve.schema import APIType, ServeInstanceDetails

        api_type: Optional[APIType] = None
        api_type_str = req.query.get("api_type")

        if api_type_str:
            api_type_lower = api_type_str.lower()
            valid_values = APIType.get_valid_user_values()

            if api_type_lower not in valid_values:
                # Explicitly check against valid user values (excludes 'unknown')
                return Response(
                    status=400,
                    text=(
                        f"Invalid 'api_type' value: '{api_type_str}'. "
                        f"Must be one of: {', '.join(valid_values)}"
                    ),
                    content_type="text/plain",
                )

            api_type = APIType(api_type_lower)

        controller = await self.get_serve_controller()

        if controller is None:
            # If no serve instance is running, return a dict that represents that.
            details = ServeInstanceDetails.get_empty_schema_dict()
        else:
            try:
                details = await controller.get_serve_instance_details.remote(
                    source=api_type
                )
            except ray.exceptions.RayTaskError as e:
                # Task failure sometimes are due to GCS
                # failure. When GCS failed, we expect a longer time
                # to recover.
                return Response(
                    status=503,
                    text=(
                        "Failed to get a response from the controller. "
                        f"The GCS may be down, please retry later: {e}"
                    ),
                )

        return Response(
            text=json.dumps(details),
            content_type="application/json",
        )

    @routes.delete("/api/serve/applications/")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    async def delete_serve_applications(self, req: Request) -> Response:
        from ray import serve

        if await self.get_serve_controller() is not None:
            serve.shutdown()

        return Response()

    @routes.put("/api/serve/applications/")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    @validate_endpoint()
    async def put_all_applications(self, req: Request) -> Response:
        from ray._common.usage.usage_lib import TagKey, record_extra_usage_tag
        from ray.serve._private.api import serve_start_async
        from ray.serve.config import ProxyLocation
        from ray.serve.schema import ServeDeploySchema

        try:
            config: ServeDeploySchema = ServeDeploySchema.parse_obj(await req.json())
        except ValidationError as e:
            return Response(
                status=400,
                text=repr(e),
            )

        config_http_options = config.http_options.dict()
        location = ProxyLocation._to_deployment_mode(config.proxy_location)
        full_http_options = dict({"location": location}, **config_http_options)
        grpc_options = config.grpc_options.dict()

        async with self._controller_start_lock:
            client = await serve_start_async(
                http_options=full_http_options,
                grpc_options=grpc_options,
                global_logging_config=config.logging_config,
            )

        # Serve ignores HTTP options if it was already running when
        # serve_start_async() is called. Therefore we validate that no
        # existing HTTP options are updated and print warning in case they are
        self.validate_http_options(client, full_http_options)

        try:
            if config.logging_config:
                client.update_global_logging_config(config.logging_config)
            client.deploy_apps(config)
            record_extra_usage_tag(TagKey.SERVE_REST_API_VERSION, "v2")
        except RayTaskError as e:
            return Response(
                status=400,
                text=str(e),
            )
        else:
            return Response()

    def _create_json_response(self, data, status: int) -> Response:
        """Create a JSON response with the given data and status."""
        return Response(
            status=status,
            text=json.dumps(data),
            content_type="application/json",
        )

    @routes.post(
        "/api/v1/applications/{application_name}/deployments/{deployment_name}/scale"
    )
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    @validate_endpoint()
    async def scale_deployment(self, req: Request) -> Response:
        from ray.serve._private.common import DeploymentID
        from ray.serve._private.exceptions import DeploymentIsBeingDeletedError
        from ray.serve.schema import ScaleDeploymentRequest

        # Extract path parameters
        application_name = req.match_info.get("application_name")
        deployment_name = req.match_info.get("deployment_name")

        if not application_name or not deployment_name:
            return self._create_json_response(
                {"error": "Missing application_name or deployment_name in path"}, 400
            )

        try:
            request_data = await req.json()
            scale_request = ScaleDeploymentRequest(**request_data)
        except Exception as e:
            return self._create_json_response(
                {"error": f"Invalid request body: {str(e)}"}, 400
            )

        controller = await self.get_serve_controller()

        if controller is None:
            return self._create_json_response(
                {"error": "Serve controller is not available"}, 503
            )

        try:
            deployment_id = DeploymentID(
                name=deployment_name, app_name=application_name
            )

            # Update the target number of replicas
            logger.info(
                f"Scaling deployment {deployment_name}, application {application_name} to {scale_request.target_num_replicas} replicas"
            )
            await controller.update_deployment_replicas.remote(
                deployment_id, scale_request.target_num_replicas
            )

            return self._create_json_response(
                {
                    "message": "Scaling request received. Deployment will get scaled asynchronously."
                },
                200,
            )
        except Exception as e:
            if isinstance(e.cause, DeploymentIsBeingDeletedError):
                return self._create_json_response(
                    # From customer's viewpoint, the deployment is deleted instead of being deleted
                    # as they must have already executed the delete command
                    {"error": "Deployment is deleted"},
                    412,
                )
            if isinstance(e, ValueError) and "not found" in str(e):
                return self._create_json_response(
                    {"error": "Application or Deployment not found"}, 400
                )
            else:
                logger.error(
                    f"Got an Internal Server Error while scaling deployment, error: {e}"
                )
                return self._create_json_response(
                    {"error": "Internal Server Error"}, 503
                )

    def validate_http_options(self, client, http_options):
        divergent_http_options = []

        for option, new_value in http_options.items():
            prev_value = getattr(client.http_config, option)
            if prev_value != new_value:
                divergent_http_options.append(option)

        if divergent_http_options:
            logger.warning(
                "Serve is already running on this Ray cluster and "
                "it's not possible to update its HTTP options without "
                "restarting it. Following options are attempted to be "
                f"updated: {divergent_http_options}."
            )

    async def get_serve_controller(self):
        """Gets the ServeController to the this cluster's Serve app.

        return: If Serve is running on this Ray cluster, returns a client to
            the Serve controller. If Serve is not running, returns None.
        """
        async with self._controller_lock:
            if self._controller is not None:
                try:
                    await self._controller.check_alive.remote()
                    return self._controller
                except ray.exceptions.RayActorError:
                    logger.info("Controller is dead")
                self._controller = None

            # Try to connect to serve even when we detect the actor is dead
            # because the user might have started a new
            # serve cluter.
            from ray.serve._private.constants import (
                SERVE_CONTROLLER_NAME,
                SERVE_NAMESPACE,
            )

            try:
                # get_actor is a sync call but it'll timeout after
                # ray.dashboard.consts.GCS_RPC_TIMEOUT_SECONDS
                self._controller = ray.get_actor(
                    SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE
                )
            except Exception as e:
                logger.debug(
                    "There is no "
                    "instance running on this Ray cluster. Please "
                    "call `serve.start(detached=True) to start "
                    f"one: {e}"
                )

            return self._controller

    # Route for autoscaling observability
    @routes.get("/api/serve/autoscaling/observability")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    async def get_autoscaling_observability(self, req: Request) -> Response:
        """
        Returns the latest autoscaling details for all applications and deployments.
        """
        # Read the latest snapshot files and store it as {app_name,{dep_name:raw_data}}
        latest_snapshots = await self.read_snapshot_files()
        if not latest_snapshots:
            # Return an empty dictionary if there are no snapshot files
            return Response(text=json.dumps({}), content_type="application/json")

        details: Dict[str, Dict[str, Any]] = {}
        # Build the autoscaling details
        for app_name, deployments in latest_snapshots.items():
            details[app_name] = {}
            for deployment_name, deployment_data in deployments.items():
                details[app_name][
                    deployment_name
                ] = DeploymentAutoscalingDetail.from_snapshot(deployment_data).dict()

        return Response(text=json.dumps(details), content_type="application/json")

    async def read_snapshot_files(self):
        loop = asyncio.get_event_loop()
        # Run the blocking file operations in a thread pool
        return await loop.run_in_executor(None, self._read_snapshot_files_sync)

    def _read_snapshot_files_sync(self) -> Dict[str, Dict[str, Any]]:
        """
        Finds the latest snapshot entry for each deployment by searching across all rotated log files,
        sorted from newest to oldest based on their modification time.
        Stores the latest entry that matches the application and deployment names not seen before.
        """
        # {application_name,{deployment_name:raw data}}
        latest_snapshots = dict()
        serve_logs_dir = get_serve_logs_dir()
        snapshot_pattern = os.path.join(serve_logs_dir, "autoscaling_snapshot_*.log*")

        all_snapshot_files = glob.glob(snapshot_pattern)
        # Return if there are no files
        if not all_snapshot_files:
            return {}

        # Sort files by their last modification time, newest first.
        sorted_files = sorted(all_snapshot_files, key=os.path.getmtime, reverse=True)
        # Iterate through each file from newest to oldest
        for log_file in sorted_files:
            try:
                with open(log_file, "r") as f:
                    all_lines = f.readlines()
                for line in reversed(all_lines):
                    line = line.strip()
                    if not line:
                        continue
                    raw = json.loads(line)
                    # Return the most recent entry for the deployment
                    app_name = raw.get("application_name")
                    deployment_name = raw.get("deployment_name")
                    if not app_name or not deployment_name:
                        continue
                    if app_name not in latest_snapshots:
                        latest_snapshots[app_name] = {}
                    # Only append the most recent entry for the deployment
                    if deployment_name not in latest_snapshots[app_name]:
                        latest_snapshots[app_name][deployment_name] = raw
            # Continue reading other files if there were errors while reading the current file
            except Exception as e:
                logger.error(f"Error reading snapshot file {log_file}: {e}")
                continue
        return latest_snapshots

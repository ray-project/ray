import logging
import random
import time
from functools import wraps
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import ray
from ray.actor import ActorHandle
from ray.serve._private.common import (
    ApplicationStatus,
    DeploymentID,
    DeploymentStatus,
    DeploymentStatusInfo,
    MultiplexedReplicaInfo,
    StatusOverview,
)
from ray.serve._private.config import DeploymentConfig, ReplicaConfig
from ray.serve._private.constants import (
    CLIENT_CHECK_CREATION_POLLING_INTERVAL_S,
    CLIENT_POLLING_INTERVAL_S,
    MAX_CACHED_HANDLES,
    RAY_SERVE_ENABLE_NEW_HANDLE_API,
    SERVE_DEFAULT_APP_NAME,
)
from ray.serve._private.controller import ServeController
from ray.serve._private.deploy_utils import get_deploy_args
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve.config import HTTPOptions
from ray.serve.exceptions import RayServeException
from ray.serve.generated.serve_pb2 import (
    DeploymentArgs,
    DeploymentRoute,
    DeploymentRouteList,
)
from ray.serve.generated.serve_pb2 import (
    DeploymentStatusInfo as DeploymentStatusInfoProto,
)
from ray.serve.generated.serve_pb2 import StatusOverview as StatusOverviewProto
from ray.serve.handle import DeploymentHandle, RayServeHandle, RayServeSyncHandle
from ray.serve.schema import LoggingConfig, ServeApplicationSchema, ServeDeploySchema

logger = logging.getLogger(__file__)


def _ensure_connected(f: Callable) -> Callable:
    @wraps(f)
    def check(self, *args, **kwargs):
        if self._shutdown:
            raise RayServeException("Client has already been shut down.")
        return f(self, *args, **kwargs)

    return check


class ServeControllerClient:
    def __init__(
        self,
        controller: ActorHandle,
        controller_name: str,
    ):
        self._controller: ServeController = controller
        self._controller_name = controller_name
        self._shutdown = False
        self._http_config: HTTPOptions = ray.get(controller.get_http_config.remote())
        self._root_url = ray.get(controller.get_root_url.remote())

        # Each handle has the overhead of long poll client, therefore cached.
        self.handle_cache = dict()
        self._evicted_handle_keys = set()

    @property
    def root_url(self):
        return self._root_url

    @property
    def http_config(self):
        return self._http_config

    def __reduce__(self):
        raise RayServeException(("Ray Serve client cannot be serialized."))

    def shutdown_cached_handles(self):
        """Shuts down all cached handles.

        Remove the reference to the cached handles so that they can be
        garbage collected.
        """
        for cache_key in list(self.handle_cache):
            self.handle_cache[cache_key].shutdown()
            del self.handle_cache[cache_key]

    def shutdown(self, timeout_s: float = 30.0) -> None:
        """Completely shut down the connected Serve instance.

        Shuts down all processes and deletes all state associated with the
        instance.
        """
        self.shutdown_cached_handles()

        if ray.is_initialized() and not self._shutdown:
            try:
                ray.get(self._controller.graceful_shutdown.remote(), timeout=timeout_s)
            except ray.exceptions.RayActorError:
                # Controller has been shut down.
                pass
            except TimeoutError:
                logger.warning(
                    f"Controller failed to shut down within {timeout_s}s. "
                    "Check controller logs for more details."
                )
            self._shutdown = True

    def _wait_for_deployment_healthy(self, name: str, timeout_s: int = -1):
        """Waits for the named deployment to enter "HEALTHY" status.

        Raises RuntimeError if the deployment enters the "UNHEALTHY" status
        instead.

        Raises TimeoutError if this doesn't happen before timeout_s.
        """
        start = time.time()
        while time.time() - start < timeout_s or timeout_s < 0:
            status_bytes = ray.get(self._controller.get_deployment_status.remote(name))

            if status_bytes is None:
                raise RuntimeError(
                    f"Waiting for deployment {name} to be HEALTHY, "
                    "but deployment doesn't exist."
                )

            status = DeploymentStatusInfo.from_proto(
                DeploymentStatusInfoProto.FromString(status_bytes)
            )

            if status.status == DeploymentStatus.HEALTHY:
                break
            elif status.status == DeploymentStatus.UNHEALTHY:
                raise RuntimeError(
                    f"Deployment {name} is UNHEALTHY: " f"{status.message}"
                )
            else:
                # Guard against new unhandled statuses being added.
                assert status.status == DeploymentStatus.UPDATING

            logger.debug(
                f"Waiting for {name} to be healthy, current status: "
                f"{status.status}."
            )
            time.sleep(CLIENT_POLLING_INTERVAL_S)
        else:
            raise TimeoutError(
                f"Deployment {name} did not become HEALTHY after {timeout_s}s."
            )

    def _wait_for_deployment_deleted(
        self, name: str, app_name: str, timeout_s: int = 60
    ):
        """Waits for the named deployment to be shut down and deleted.

        Raises TimeoutError if this doesn't happen before timeout_s.
        """
        start = time.time()
        while time.time() - start < timeout_s:
            curr_status_bytes = ray.get(
                self._controller.get_deployment_status.remote(name)
            )
            if curr_status_bytes is None:
                break
            curr_status = DeploymentStatusInfo.from_proto(
                DeploymentStatusInfoProto.FromString(curr_status_bytes)
            )
            logger.debug(
                f"Waiting for {name} to be deleted, current status: {curr_status}."
            )
            time.sleep(CLIENT_POLLING_INTERVAL_S)
        else:
            raise TimeoutError(f"Deployment {name} wasn't deleted after {timeout_s}s.")

    def _wait_for_deployment_created(
        self, deployment_name: str, app_name: str, timeout_s: int = -1
    ):
        """Waits for the named deployment to be created.

        A deployment being created simply means that its been registered
        with the deployment state manager. The deployment state manager
        will then continue to reconcile the deployment towards its
        target state.

        Raises TimeoutError if this doesn't happen before timeout_s.
        """

        start = time.time()
        while time.time() - start < timeout_s or timeout_s < 0:
            status_bytes = ray.get(
                self._controller.get_deployment_status.remote(deployment_name, app_name)
            )

            if status_bytes is not None:
                break

            logger.debug(
                f"Waiting for deployment '{deployment_name}' in application "
                f"'{app_name}' to be created."
            )
            time.sleep(CLIENT_CHECK_CREATION_POLLING_INTERVAL_S)
        else:
            raise TimeoutError(
                f"Deployment '{deployment_name}' in application '{app_name}' "
                f"did not become HEALTHY after {timeout_s}s."
            )

    def _wait_for_application_running(self, name: str, timeout_s: int = -1):
        """Waits for the named application to enter "RUNNING" status.

        Raises:
            RuntimeError: if the application enters the "DEPLOY_FAILED" status instead.
            TimeoutError: if this doesn't happen before timeout_s.
        """
        start = time.time()
        while time.time() - start < timeout_s or timeout_s < 0:
            status_bytes = ray.get(self._controller.get_serve_status.remote(name))
            if status_bytes is None:
                raise RuntimeError(
                    f"Waiting for application {name} to be RUNNING, "
                    "but application doesn't exist."
                )

            status = StatusOverview.from_proto(
                StatusOverviewProto.FromString(status_bytes)
            )

            if status.app_status.status == ApplicationStatus.RUNNING:
                break
            elif status.app_status.status == ApplicationStatus.DEPLOY_FAILED:
                raise RuntimeError(
                    f"Deploying application {name} failed: {status.app_status.message}"
                )

            logger.debug(
                f"Waiting for {name} to be RUNNING, current status: "
                f"{status.app_status.status}."
            )
            time.sleep(CLIENT_POLLING_INTERVAL_S)
        else:
            raise TimeoutError(
                f"Application {name} did not become RUNNING after {timeout_s}s."
            )

    @_ensure_connected
    def deploy(
        self,
        name: str,
        replica_config: ReplicaConfig,
        deployment_config: Union[None, DeploymentConfig, Dict[str, Any]] = None,
        version: Optional[str] = None,
        route_prefix: Optional[str] = None,
        url: Optional[str] = None,
        _blocking: Optional[bool] = True,
    ):
        controller_deploy_args = get_deploy_args(
            name=name,
            replica_config=replica_config,
            deployment_config=deployment_config,
            version=version,
            route_prefix=route_prefix,
        )
        controller_deploy_args.pop("ingress")
        controller_deploy_args["name"] = controller_deploy_args.pop("deployment_name")

        updating = ray.get(
            self._controller.deploy.remote(
                # TODO(edoakes): this is a hack because the deployment_language
                # doesn't seem to get set properly from Java.
                is_deployed_from_python=True,
                **controller_deploy_args,
            )
        )

        tag = self.log_deployment_update_status(name, version, updating)

        if _blocking:
            self._wait_for_deployment_healthy(name)
            self.log_deployment_ready(name, version, url, tag)

    @_ensure_connected
    def deploy_application(
        self,
        name,
        deployments: List[Dict],
        _blocking: bool = True,
    ):
        deployment_args_list = []
        for deployment in deployments:
            deployment_args = get_deploy_args(
                deployment["name"],
                replica_config=deployment["replica_config"],
                ingress=deployment["ingress"],
                deployment_config=deployment["deployment_config"],
                version=deployment["version"],
                route_prefix=deployment["route_prefix"],
                docs_path=deployment["docs_path"],
            )

            deployment_args_proto = DeploymentArgs()
            deployment_args_proto.deployment_name = deployment_args["deployment_name"]
            deployment_args_proto.deployment_config = deployment_args[
                "deployment_config_proto_bytes"
            ]
            deployment_args_proto.replica_config = deployment_args[
                "replica_config_proto_bytes"
            ]
            deployment_args_proto.deployer_job_id = deployment_args["deployer_job_id"]
            if deployment_args["route_prefix"]:
                deployment_args_proto.route_prefix = deployment_args["route_prefix"]
            deployment_args_proto.ingress = deployment_args["ingress"]
            if deployment_args["docs_path"]:
                deployment_args_proto.docs_path = deployment_args["docs_path"]

            deployment_args_list.append(deployment_args_proto.SerializeToString())

        ray.get(self._controller.deploy_application.remote(name, deployment_args_list))
        if _blocking:
            self._wait_for_application_running(name)
            for deployment in deployments:
                deployment_name = deployment["name"]
                tag = f"component=serve deployment={deployment_name}"
                url = deployment["url"]
                version = deployment["version"]

                self.log_deployment_ready(deployment_name, version, url, tag)

    @_ensure_connected
    def deploy_apps(
        self,
        config: Union[ServeApplicationSchema, ServeDeploySchema],
        _blocking: bool = False,
    ) -> None:
        """Starts a task on the controller that deploys application(s) from a config.

        Args:
            config: A single-application config (ServeApplicationSchema) or a
                multi-application config (ServeDeploySchema)
            _blocking: Whether to block until the application is running.

        Raises:
            RayTaskError: If the deploy task on the controller fails. This can be
                because a single-app config was deployed after deploying a multi-app
                config, or vice versa.
        """
        ray.get(self._controller.deploy_config.remote(config))

        if _blocking:
            timeout_s = 60

            start = time.time()
            while time.time() - start < timeout_s:
                curr_status = self.get_serve_status()
                if curr_status.app_status.status == ApplicationStatus.RUNNING:
                    break
                time.sleep(CLIENT_POLLING_INTERVAL_S)
            else:
                raise TimeoutError(
                    f"Serve application isn't running after {timeout_s}s."
                )

    @_ensure_connected
    def delete_apps(self, names: List[str], blocking: bool = True):
        if not names:
            return

        logger.info(f"Deleting app {names}")
        self._controller.delete_apps.remote(names)
        if blocking:
            start = time.time()
            while time.time() - start < 60:
                curr_statuses_bytes = ray.get(
                    self._controller.get_serve_statuses.remote(names)
                )
                all_deleted = True
                for cur_status_bytes in curr_statuses_bytes:
                    cur_status = StatusOverview.from_proto(
                        StatusOverviewProto.FromString(cur_status_bytes)
                    )
                    if cur_status.app_status.status != ApplicationStatus.NOT_STARTED:
                        all_deleted = False
                if all_deleted:
                    return
                time.sleep(CLIENT_POLLING_INTERVAL_S)
            else:
                raise TimeoutError(
                    f"Some of these applications weren't deleted after 60s: {names}"
                )

    @_ensure_connected
    def delete_all_apps(self, blocking: bool = True):
        """Delete all applications"""
        all_apps = []
        for status_bytes in ray.get(self._controller.list_serve_statuses.remote()):
            proto = StatusOverviewProto.FromString(status_bytes)
            status = StatusOverview.from_proto(proto)
            all_apps.append(status.name)
        self.delete_apps(all_apps, blocking)

    @_ensure_connected
    def delete_deployments(self, names: Iterable[str], blocking: bool = True) -> None:
        """Delete 1.x deployments."""

        ray.get(self._controller.delete_deployments.remote(names))
        if blocking:
            for name in names:
                self._wait_for_deployment_deleted(name, "")

    @_ensure_connected
    def get_deployment_info(
        self, name: str, app_name: str
    ) -> Tuple[DeploymentInfo, str]:
        deployment_route = DeploymentRoute.FromString(
            ray.get(self._controller.get_deployment_info.remote(name, app_name))
        )
        return (
            DeploymentInfo.from_proto(deployment_route.deployment_info),
            deployment_route.route if deployment_route.route != "" else None,
        )

    @_ensure_connected
    def list_deployments_v1(self) -> Dict[str, Tuple[DeploymentInfo, str]]:
        """Gets the current information about all 1.x deployments."""

        deployment_route_list = DeploymentRouteList.FromString(
            ray.get(self._controller.list_deployments_v1.remote())
        )
        return {
            deployment_route.deployment_info.name: (
                DeploymentInfo.from_proto(deployment_route.deployment_info),
                deployment_route.route if deployment_route.route != "" else None,
            )
            for deployment_route in deployment_route_list.deployment_routes
        }

    @_ensure_connected
    def list_deployments(self) -> Dict[DeploymentID, DeploymentInfo]:
        """Gets the current information about all deployments (1.x and 2.x)."""

        return ray.get(self._controller.list_deployments.remote())

    @_ensure_connected
    def get_app_config(self, name: str = SERVE_DEFAULT_APP_NAME) -> Dict:
        """Returns the most recently requested Serve config."""
        return ray.get(self._controller.get_app_config.remote(name))

    @_ensure_connected
    def get_serve_status(self, name: str = SERVE_DEFAULT_APP_NAME) -> StatusOverview:
        proto = StatusOverviewProto.FromString(
            ray.get(self._controller.get_serve_status.remote(name))
        )
        return StatusOverview.from_proto(proto)

    @_ensure_connected
    def get_all_deployment_statuses(self) -> List[DeploymentStatusInfo]:
        statuses_bytes = ray.get(self._controller.get_all_deployment_statuses.remote())
        return [
            DeploymentStatusInfo.from_proto(
                DeploymentStatusInfoProto.FromString(status_bytes)
            )
            for status_bytes in statuses_bytes
        ]

    @_ensure_connected
    def get_serve_details(self) -> Dict:
        return ray.get(self._controller.get_serve_instance_details.remote())

    @_ensure_connected
    def get_handle(
        self,
        deployment_name: str,
        app_name: Optional[str] = "default",
        missing_ok: Optional[bool] = False,
        sync: bool = True,
        use_new_handle_api: bool = RAY_SERVE_ENABLE_NEW_HANDLE_API,
    ) -> Union[DeploymentHandle, RayServeHandle, RayServeSyncHandle]:
        """Construct a handle for the specified deployment.

        Args:
            deployment_name: Deployment name.
            app_name: Application name.
            missing_ok: If true, then Serve won't check the deployment
                is registered. False by default.
            sync: If true, then Serve will return a ServeHandle that
                works everywhere. Otherwise, Serve will return a ServeHandle
                that's only usable in asyncio loop.

        Returns:
            RayServeHandle
        """
        cache_key = (deployment_name, app_name, missing_ok, sync)
        if cache_key in self.handle_cache:
            return self.handle_cache[cache_key]

        all_deployments = ray.get(self._controller.list_deployment_ids.remote())
        if (
            not missing_ok
            and DeploymentID(deployment_name, app_name) not in all_deployments
        ):
            raise KeyError(
                f"Deployment '{deployment_name}' in application '{app_name}' does not "
                "exist."
            )

        if use_new_handle_api:
            handle = DeploymentHandle(
                deployment_name,
                app_name,
                # Only used when users convert this back to deprecated handle types.
                sync=sync,
            )
        elif sync:
            handle = RayServeSyncHandle(
                deployment_name,
                app_name,
                # Only used when users convert this back to deprecated handle types.
                sync=sync,
            )
        else:
            handle = RayServeHandle(
                deployment_name,
                app_name,
                # Only used when users convert this back to deprecated handle types.
                sync=sync,
            )

        self.handle_cache[cache_key] = handle
        if cache_key in self._evicted_handle_keys:
            logger.warning(
                "You just got a ServeHandle that was evicted from internal "
                "cache. This means you are getting too many ServeHandles in "
                "the same process, this will bring down Serve's performance. "
                "Please post a github issue at "
                "https://github.com/ray-project/ray/issues to let the Serve "
                "team to find workaround for your use case."
            )

        if len(self.handle_cache) > MAX_CACHED_HANDLES:
            # Perform random eviction to keep the handle cache from growing
            # infinitely. We used use WeakValueDictionary but hit
            # https://github.com/ray-project/ray/issues/18980.
            evict_key = random.choice(list(self.handle_cache.keys()))
            self._evicted_handle_keys.add(evict_key)
            self.handle_cache.pop(evict_key)

        return handle

    @_ensure_connected
    def log_deployment_update_status(
        self, name: str, version: str, updating: bool
    ) -> str:
        tag = f"component=serve deployment={name}"

        if updating:
            msg = f"Updating deployment '{name}'"
            if version is not None:
                msg += f" to version '{version}'"
            logger.info(f"{msg}. {tag}")
        else:
            logger.info(
                f"Deployment '{name}' is already at version "
                f"'{version}', not updating. {tag}"
            )

        return tag

    @_ensure_connected
    def log_deployment_ready(self, name: str, version: str, url: str, tag: str) -> None:
        if url is not None:
            url_part = f" at `{url}`"
        else:
            url_part = ""
        logger.info(
            f"Deployment '{name}{':'+version if version else ''}' is ready"
            f"{url_part}. {tag}"
        )

    @_ensure_connected
    def record_multiplexed_replica_info(self, info: MultiplexedReplicaInfo):
        """Record multiplexed replica information for replica.

        Args:
            info: MultiplexedReplicaInfo including deployment name, replica tag and
                model ids.
        """
        self._controller.record_multiplexed_replica_info.remote(info)

    @_ensure_connected
    def update_system_logging_config(self, logging_config: LoggingConfig):
        """Reconfigure the logging config for the controller & proxies."""
        self._controller.reconfigure_system_logging_config.remote(logging_config)

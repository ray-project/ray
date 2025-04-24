import logging
import random
import time
from collections.abc import Sequence
from functools import wraps
from typing import Callable, Dict, List, Optional, Tuple, Union

import ray
from ray.actor import ActorHandle
from ray.serve._private.application_state import StatusOverview
from ray.serve._private.build_app import BuiltApplication
from ray.serve._private.common import (
    DeploymentID,
    DeploymentStatus,
    DeploymentStatusInfo,
    MultiplexedReplicaInfo,
)
from ray.serve._private.constants import (
    CLIENT_CHECK_CREATION_POLLING_INTERVAL_S,
    CLIENT_POLLING_INTERVAL_S,
    MAX_CACHED_HANDLES,
    SERVE_DEFAULT_APP_NAME,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.controller import ServeController
from ray.serve._private.deploy_utils import get_deploy_args
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve._private.utils import get_random_string
from ray.serve.config import HTTPOptions
from ray.serve.exceptions import RayServeException
from ray.serve.generated.serve_pb2 import DeploymentArgs, DeploymentRoute
from ray.serve.generated.serve_pb2 import (
    DeploymentStatusInfo as DeploymentStatusInfoProto,
)
from ray.serve.generated.serve_pb2 import StatusOverview as StatusOverviewProto
from ray.serve.handle import DeploymentHandle
from ray.serve.schema import (
    ApplicationStatus,
    LoggingConfig,
    ServeApplicationSchema,
    ServeDeploySchema,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


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
    ):
        self._controller: ServeController = controller
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
    def deploy_applications(
        self,
        built_apps: Sequence[BuiltApplication],
        *,
        wait_for_ingress_deployment_creation: bool = True,
        wait_for_applications_running: bool = True,
    ) -> List[DeploymentHandle]:
        name_to_deployment_args_list = {}
        for app in built_apps:
            deployment_args_list = []
            for deployment in app.deployments:
                if deployment.logging_config is None and app.logging_config:
                    deployment = deployment.options(logging_config=app.logging_config)

                is_ingress = deployment.name == app.ingress_deployment_name
                deployment_args = get_deploy_args(
                    deployment.name,
                    ingress=is_ingress,
                    replica_config=deployment._replica_config,
                    deployment_config=deployment._deployment_config,
                    version=deployment._version or get_random_string(),
                    route_prefix=app.route_prefix if is_ingress else None,
                    docs_path=deployment._docs_path,
                )

                deployment_args_proto = DeploymentArgs()
                deployment_args_proto.deployment_name = deployment_args[
                    "deployment_name"
                ]
                deployment_args_proto.deployment_config = deployment_args[
                    "deployment_config_proto_bytes"
                ]
                deployment_args_proto.replica_config = deployment_args[
                    "replica_config_proto_bytes"
                ]
                deployment_args_proto.deployer_job_id = deployment_args[
                    "deployer_job_id"
                ]
                if deployment_args["route_prefix"]:
                    deployment_args_proto.route_prefix = deployment_args["route_prefix"]
                deployment_args_proto.ingress = deployment_args["ingress"]
                if deployment_args["docs_path"]:
                    deployment_args_proto.docs_path = deployment_args["docs_path"]

                deployment_args_list.append(deployment_args_proto.SerializeToString())

            name_to_deployment_args_list[app.name] = deployment_args_list

        ray.get(
            self._controller.deploy_applications.remote(name_to_deployment_args_list)
        )

        handles = []
        for app in built_apps:
            # The deployment state is not guaranteed to be created after
            # deploy_application returns; the application state manager will
            # need another reconcile iteration to create it.
            if wait_for_ingress_deployment_creation:
                self._wait_for_deployment_created(app.ingress_deployment_name, app.name)

            if wait_for_applications_running:
                self._wait_for_application_running(app.name)
                if app.route_prefix is not None:
                    url_part = " at " + self._root_url + app.route_prefix
                else:
                    url_part = ""
                logger.info(f"Application '{app.name}' is ready{url_part}.")

            handles.append(
                self.get_handle(
                    app.ingress_deployment_name, app.name, check_exists=False
                )
            )

        return handles

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
        ray.get(self._controller.apply_config.remote(config))

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
        app_name: Optional[str] = SERVE_DEFAULT_APP_NAME,
        check_exists: bool = True,
    ) -> DeploymentHandle:
        """Construct a handle for the specified deployment.

        Args:
            deployment_name: Deployment name.
            app_name: Application name.
            check_exists: If False, then Serve won't check the deployment
                is registered. True by default.

        Returns:
            DeploymentHandle
        """
        deployment_id = DeploymentID(name=deployment_name, app_name=app_name)
        cache_key = (deployment_name, app_name, check_exists)
        if cache_key in self.handle_cache:
            return self.handle_cache[cache_key]

        if check_exists:
            all_deployments = ray.get(self._controller.list_deployment_ids.remote())
            if deployment_id not in all_deployments:
                raise KeyError(f"{deployment_id} does not exist.")

        handle = DeploymentHandle(deployment_name, app_name)
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
    def record_multiplexed_replica_info(self, info: MultiplexedReplicaInfo):
        """Record multiplexed replica information for replica.

        Args:
            info: MultiplexedReplicaInfo including deployment name, replica tag and
                model ids.
        """
        self._controller.record_multiplexed_replica_info.remote(info)

    @_ensure_connected
    def update_global_logging_config(self, logging_config: LoggingConfig):
        """Reconfigure the logging config for the controller & proxies."""
        self._controller.reconfigure_global_logging_config.remote(logging_config)

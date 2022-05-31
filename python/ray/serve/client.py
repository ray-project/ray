import asyncio
import atexit
import random
import logging
import time
from functools import wraps
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    Tuple,
    Type,
    Union,
    List,
    Iterable,
)

import ray
from ray.actor import ActorHandle
from ray.serve.common import (
    DeploymentInfo,
    DeploymentStatus,
    StatusOverview,
)
from ray.serve.config import (
    DeploymentConfig,
    HTTPOptions,
    ReplicaConfig,
)
from ray.serve.constants import (
    MAX_CACHED_HANDLES,
    CLIENT_POLLING_INTERVAL_S,
    ANONYMOUS_NAMESPACE_PATTERN,
)
from ray.serve.controller import ServeController
from ray.serve.exceptions import RayServeException
from ray.serve.generated.serve_pb2 import (
    DeploymentRoute,
    DeploymentRouteList,
    StatusOverview as StatusOverviewProto,
)
from ray.serve.handle import RayServeHandle, RayServeSyncHandle


logger = logging.getLogger(__file__)
# Whether to issue warnings about using sync handles in async context
# or using async handle in sync context.
_WARN_SYNC_ASYNC_HANDLE_CONTEXT: bool = True


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
        detached: bool = False,
        _override_controller_namespace: Optional[str] = None,
    ):
        self._controller: ServeController = controller
        self._controller_name = controller_name
        self._detached = detached
        self._override_controller_namespace = _override_controller_namespace
        self._shutdown = False
        self._http_config: HTTPOptions = ray.get(controller.get_http_config.remote())
        self._root_url = ray.get(controller.get_root_url.remote())
        self._checkpoint_path = ray.get(controller.get_checkpoint_path.remote())

        # Each handle has the overhead of long poll client, therefore cached.
        self.handle_cache = dict()
        self._evicted_handle_keys = set()

        # NOTE(edoakes): Need this because the shutdown order isn't guaranteed
        # when the interpreter is exiting so we can't rely on __del__ (it
        # throws a nasty stacktrace).
        if not self._detached:

            def shutdown_serve_client():
                self.shutdown()

            atexit.register(shutdown_serve_client)

    @property
    def root_url(self):
        return self._root_url

    @property
    def http_config(self):
        return self._http_config

    @property
    def checkpoint_path(self):
        return self._checkpoint_path

    def __del__(self):
        if not self._detached:
            logger.debug(
                "Shutting down Ray Serve because client went out of "
                "scope. To prevent this, either keep a reference to "
                "the client or use serve.start(detached=True)."
            )
            self.shutdown()

    def __reduce__(self):
        raise RayServeException(("Ray Serve client cannot be serialized."))

    def shutdown(self) -> None:
        """Completely shut down the connected Serve instance.

        Shuts down all processes and deletes all state associated with the
        instance.
        """
        if ray.is_initialized() and not self._shutdown:
            ray.get(self._controller.shutdown.remote())
            self._wait_for_deployments_shutdown()

            ray.kill(self._controller, no_restart=True)

            # Wait for the named actor entry gets removed as well.
            started = time.time()
            while True:
                try:
                    controller_namespace = get_controller_namespace(
                        self._detached,
                        self._override_controller_namespace,
                    )
                    ray.get_actor(self._controller_name, namespace=controller_namespace)
                    if time.time() - started > 5:
                        logger.warning(
                            "Waited 5s for Serve to shutdown gracefully but "
                            "the controller is still not cleaned up. "
                            "You can ignore this warning if you are shutting "
                            "down the Ray cluster."
                        )
                        break
                except ValueError:  # actor name is removed
                    break

            self._shutdown = True

    def _wait_for_deployments_shutdown(self, timeout_s: int = 60):
        """Waits for all deployments to be shut down and deleted.

        Raises TimeoutError if this doesn't happen before timeout_s.
        """
        start = time.time()
        while time.time() - start < timeout_s:
            deployment_statuses = self.get_serve_status().deployment_statuses
            if len(deployment_statuses) == 0:
                break
            else:
                logger.debug(
                    f"Waiting for shutdown, {len(deployment_statuses)} "
                    "deployments still alive."
                )
            time.sleep(CLIENT_POLLING_INTERVAL_S)
        else:
            live_names = [
                deployment_status.name for deployment_status in deployment_statuses
            ]
            raise TimeoutError(
                f"Shutdown didn't complete after {timeout_s}s. "
                f"Deployments still alive: {live_names}."
            )

    def _wait_for_deployment_healthy(self, name: str, timeout_s: int = -1):
        """Waits for the named deployment to enter "HEALTHY" status.

        Raises RuntimeError if the deployment enters the "UNHEALTHY" status
        instead.

        Raises TimeoutError if this doesn't happen before timeout_s.
        """
        start = time.time()
        while time.time() - start < timeout_s or timeout_s < 0:

            status = self.get_serve_status().get_deployment_status(name)

            if status is None:
                raise RuntimeError(
                    f"Waiting for deployment {name} to be HEALTHY, "
                    "but deployment doesn't exist."
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

    def _wait_for_deployment_deleted(self, name: str, timeout_s: int = 60):
        """Waits for the named deployment to be shut down and deleted.

        Raises TimeoutError if this doesn't happen before timeout_s.
        """
        start = time.time()
        while time.time() - start < timeout_s:
            curr_status = self.get_serve_status().get_deployment_status(name)
            if curr_status is None:
                break
            logger.debug(
                f"Waiting for {name} to be deleted, current status: {curr_status}."
            )
            time.sleep(CLIENT_POLLING_INTERVAL_S)
        else:
            raise TimeoutError(f"Deployment {name} wasn't deleted after {timeout_s}s.")

    @_ensure_connected
    def deploy(
        self,
        name: str,
        deployment_def: Union[Callable, Type[Callable], str],
        init_args: Tuple[Any],
        init_kwargs: Dict[Any, Any],
        ray_actor_options: Optional[Dict] = None,
        config: Optional[Union[DeploymentConfig, Dict[str, Any]]] = None,
        version: Optional[str] = None,
        prev_version: Optional[str] = None,
        route_prefix: Optional[str] = None,
        url: Optional[str] = None,
        _blocking: Optional[bool] = True,
    ):

        controller_deploy_args = self.get_deploy_args(
            name=name,
            deployment_def=deployment_def,
            init_args=init_args,
            init_kwargs=init_kwargs,
            ray_actor_options=ray_actor_options,
            config=config,
            version=version,
            prev_version=prev_version,
            route_prefix=route_prefix,
        )

        updating = ray.get(self._controller.deploy.remote(**controller_deploy_args))

        tag = self.log_deployment_update_status(name, version, updating)

        if _blocking:
            self._wait_for_deployment_healthy(name)
            self.log_deployment_ready(name, version, url, tag)

    @_ensure_connected
    def deploy_group(
        self,
        deployments: List[Dict],
        _blocking: bool = True,
        remove_past_deployments: bool = True,
    ):
        deployment_args_list = []
        for deployment in deployments:
            deployment_args_list.append(
                self.get_deploy_args(
                    deployment["name"],
                    deployment["func_or_class"],
                    deployment["init_args"],
                    deployment["init_kwargs"],
                    ray_actor_options=deployment["ray_actor_options"],
                    config=deployment["config"],
                    version=deployment["version"],
                    prev_version=deployment["prev_version"],
                    route_prefix=deployment["route_prefix"],
                )
            )

        updating_list = ray.get(
            self._controller.deploy_group.remote(deployment_args_list)
        )

        tags = []
        for i, updating in enumerate(updating_list):
            deployment = deployments[i]
            name, version = deployment["name"], deployment["version"]

            tags.append(self.log_deployment_update_status(name, version, updating))

        for i, deployment in enumerate(deployments):
            name = deployment["name"]
            url = deployment["url"]

            if _blocking:
                self._wait_for_deployment_healthy(name)
                self.log_deployment_ready(name, version, url, tags[i])

        if remove_past_deployments:
            # clean up the old deployments
            new_deployments_names = set()
            for deployment in deployments:
                new_deployments_names.add(deployment["name"])

            all_deployments_names = set(self.list_deployments().keys())
            deployment_names_to_delete = all_deployments_names.difference(
                new_deployments_names
            )
            self.delete_deployments(deployment_names_to_delete, blocking=_blocking)

    @_ensure_connected
    def delete_deployments(self, names: Iterable[str], blocking: bool = True) -> None:
        ray.get(self._controller.delete_deployments.remote(names))
        if blocking:
            for name in names:
                self._wait_for_deployment_deleted(name)

    @_ensure_connected
    def get_deployment_info(self, name: str) -> Tuple[DeploymentInfo, str]:
        deployment_route = DeploymentRoute.FromString(
            ray.get(self._controller.get_deployment_info.remote(name))
        )
        return (
            DeploymentInfo.from_proto(deployment_route.deployment_info),
            deployment_route.route if deployment_route.route != "" else None,
        )

    @_ensure_connected
    def list_deployments(self) -> Dict[str, Tuple[DeploymentInfo, str]]:
        deployment_route_list = DeploymentRouteList.FromString(
            ray.get(self._controller.list_deployments.remote())
        )
        return {
            deployment_route.deployment_info.name: (
                DeploymentInfo.from_proto(deployment_route.deployment_info),
                deployment_route.route if deployment_route.route != "" else None,
            )
            for deployment_route in deployment_route_list.deployment_routes
        }

    @_ensure_connected
    def get_serve_status(self) -> StatusOverview:
        proto = StatusOverviewProto.FromString(
            ray.get(self._controller.get_serve_status.remote())
        )
        return StatusOverview.from_proto(proto)

    @_ensure_connected
    def get_handle(
        self,
        deployment_name: str,
        missing_ok: Optional[bool] = False,
        sync: bool = True,
        _internal_pickled_http_request: bool = False,
    ) -> Union[RayServeHandle, RayServeSyncHandle]:
        """Retrieve RayServeHandle for service deployment to invoke it from Python.

        Args:
            deployment_name (str): A registered service deployment.
            missing_ok (bool): If true, then Serve won't check the deployment
                is registered. False by default.
            sync (bool): If true, then Serve will return a ServeHandle that
                works everywhere. Otherwise, Serve will return a ServeHandle
                that's only usable in asyncio loop.

        Returns:
            RayServeHandle
        """
        cache_key = (deployment_name, missing_ok, sync)
        if cache_key in self.handle_cache:
            cached_handle = self.handle_cache[cache_key]
            if cached_handle.is_polling and cached_handle.is_same_loop:
                return cached_handle

        all_endpoints = ray.get(self._controller.get_all_endpoints.remote())
        if not missing_ok and deployment_name not in all_endpoints:
            raise KeyError(f"Deployment '{deployment_name}' does not exist.")

        try:
            asyncio_loop_running = asyncio.get_event_loop().is_running()
        except RuntimeError as ex:
            if "There is no current event loop in thread" in str(ex):
                asyncio_loop_running = False
            else:
                raise ex

        if asyncio_loop_running and sync and _WARN_SYNC_ASYNC_HANDLE_CONTEXT:
            logger.warning(
                "You are retrieving a sync handle inside an asyncio loop. "
                "Try getting client.get_handle(.., sync=False) to get better "
                "performance. Learn more at https://docs.ray.io/en/master/"
                "serve/http-servehandle.html#sync-and-async-handles"
            )

        if not asyncio_loop_running and not sync and _WARN_SYNC_ASYNC_HANDLE_CONTEXT:
            logger.warning(
                "You are retrieving an async handle outside an asyncio loop. "
                "You should make sure client.get_handle is called inside a "
                "running event loop. Or call client.get_handle(.., sync=True) "
                "to create sync handle. Learn more at https://docs.ray.io/en/"
                "master/serve/http-servehandle.html#sync-and-async-handles"
            )

        if sync:
            handle = RayServeSyncHandle(
                self._controller,
                deployment_name,
                _internal_pickled_http_request=_internal_pickled_http_request,
            )
        else:
            handle = RayServeHandle(
                self._controller,
                deployment_name,
                _internal_pickled_http_request=_internal_pickled_http_request,
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
    def get_deploy_args(
        self,
        name: str,
        deployment_def: Union[Callable, Type[Callable], str],
        init_args: Tuple[Any],
        init_kwargs: Dict[Any, Any],
        ray_actor_options: Optional[Dict] = None,
        config: Optional[Union[DeploymentConfig, Dict[str, Any]]] = None,
        version: Optional[str] = None,
        prev_version: Optional[str] = None,
        route_prefix: Optional[str] = None,
    ) -> Dict:
        """
        Takes a deployment's configuration, and returns the arguments needed
        for the controller to deploy it.
        """

        if config is None:
            config = {}
        if ray_actor_options is None:
            ray_actor_options = {}

        curr_job_env = ray.get_runtime_context().runtime_env
        if "runtime_env" in ray_actor_options:
            # It is illegal to set field working_dir to None.
            if curr_job_env.get("working_dir") is not None:
                ray_actor_options["runtime_env"].setdefault(
                    "working_dir", curr_job_env.get("working_dir")
                )
        else:
            ray_actor_options["runtime_env"] = curr_job_env

        replica_config = ReplicaConfig(
            deployment_def,
            init_args=init_args,
            init_kwargs=init_kwargs,
            ray_actor_options=ray_actor_options,
        )

        if isinstance(config, dict):
            deployment_config = DeploymentConfig.parse_obj(config)
        elif isinstance(config, DeploymentConfig):
            deployment_config = config
        else:
            raise TypeError("config must be a DeploymentConfig or a dictionary.")

        deployment_config.version = version
        deployment_config.prev_version = prev_version

        if (
            deployment_config.autoscaling_config is not None
            and deployment_config.max_concurrent_queries
            < deployment_config.autoscaling_config.target_num_ongoing_requests_per_replica  # noqa: E501
        ):
            logger.warning(
                "Autoscaling will never happen, "
                "because 'max_concurrent_queries' is less than "
                "'target_num_ongoing_requests_per_replica' now."
            )

        controller_deploy_args = {
            "name": name,
            "deployment_config_proto_bytes": deployment_config.to_proto_bytes(),
            "replica_config_proto_bytes": replica_config.to_proto_bytes(),
            "route_prefix": route_prefix,
            "deployer_job_id": ray.get_runtime_context().job_id,
        }

        return controller_deploy_args

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


def get_controller_namespace(
    detached: bool, _override_controller_namespace: Optional[str] = None
):
    """Gets the controller's namespace.

    Args:
        detached (bool): Whether serve.start() was called with detached=True
        _override_controller_namespace (Optional[str]): When set, this is the
            controller's namespace
    """

    if _override_controller_namespace is not None:
        return _override_controller_namespace

    controller_namespace = ray.get_runtime_context().namespace

    if not detached:
        return controller_namespace

    # Start controller in "serve" namespace if detached and currently
    # in anonymous namespace.
    if ANONYMOUS_NAMESPACE_PATTERN.fullmatch(controller_namespace) is not None:
        controller_namespace = "serve"
    return controller_namespace

import asyncio
import atexit
import collections
import inspect
import logging
import random
import re
import time
from dataclasses import dataclass
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
    overload,
)

from fastapi import APIRouter, FastAPI
from ray.exceptions import RayActorError
from starlette.requests import Request
from uvicorn.config import Config
from uvicorn.lifespan.on import LifespanOn

from ray.actor import ActorHandle
from ray.serve.common import (
    DeploymentInfo,
    DeploymentStatus,
    DeploymentStatusInfo,
    ReplicaTag,
)
from ray.serve.config import (
    AutoscalingConfig,
    DeploymentConfig,
    HTTPOptions,
    ReplicaConfig,
)
from ray.serve.constants import (
    DEFAULT_CHECKPOINT_PATH,
    HTTP_PROXY_TIMEOUT,
    SERVE_CONTROLLER_NAME,
    MAX_CACHED_HANDLES,
    CONTROLLER_MAX_CONCURRENCY,
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
)
from ray.serve.controller import ServeController
from ray.serve.deployment import Deployment
from ray.serve.exceptions import RayServeException
from ray.serve.generated.serve_pb2 import (
    DeploymentRoute,
    DeploymentRouteList,
    DeploymentStatusInfoList,
)
from ray.experimental.dag import DAGNode
from ray.serve.handle import RayServeHandle, RayServeSyncHandle
from ray.serve.http_util import ASGIHTTPSender, make_fastapi_class_based_view
from ray.serve.utils import (
    LoggingContext,
    ensure_serialization_context,
    format_actor_name,
    get_current_node_resource_key,
    get_random_letters,
    get_deployment_import_path,
    in_interactive_shell,
    logger,
    DEFAULT,
)
from ray.util.annotations import PublicAPI
import ray
from ray import cloudpickle
from ray.serve.schema import (
    RayActorOptionsSchema,
    DeploymentSchema,
    DeploymentStatusSchema,
    ServeApplicationSchema,
    ServeApplicationStatusSchema,
)
from ray.serve.deployment_graph import DeploymentNode, DeploymentFunctionNode
from ray.serve.application import Application


_INTERNAL_REPLICA_CONTEXT = None
_global_client: "Client" = None

_UUID_RE = re.compile(
    "[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12}"
)

# The polling interval for serve client to wait to deployment state
_CLIENT_POLLING_INTERVAL_S: float = 1


def _get_controller_namespace(
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
    if _UUID_RE.fullmatch(controller_namespace) is not None:
        controller_namespace = "serve"
    return controller_namespace


def internal_get_global_client(
    _override_controller_namespace: Optional[str] = None,
    _health_check_controller: bool = False,
) -> "Client":
    """Gets the global client, which stores the controller's handle.

    Args:
        _override_controller_namespace (Optional[str]): If None and there's no
            cached client, searches for the controller in this namespace.
        _health_check_controller (bool): If True, run a health check on the
            cached controller if it exists. If the check fails, try reconnecting
            to the controller.

    Raises:
        RayServeException: if there is no Serve controller actor in the
            expected namespace.
    """

    try:
        if _global_client is not None:
            if _health_check_controller:
                ray.get(_global_client._controller.check_alive.remote())
            return _global_client
    except RayActorError:
        logger.info("The cached controller has died. Reconnecting.")
        _set_global_client(None)

    return _connect(_override_controller_namespace=_override_controller_namespace)


def _set_global_client(client):
    global _global_client
    _global_client = client


@dataclass
class ReplicaContext:
    """Stores data for Serve API calls from within deployments."""

    deployment: str
    replica_tag: ReplicaTag
    _internal_controller_name: str
    _internal_controller_namespace: str
    servable_object: Callable


def _set_internal_replica_context(
    deployment: str,
    replica_tag: ReplicaTag,
    controller_name: str,
    controller_namespace: str,
    servable_object: Callable,
):
    global _INTERNAL_REPLICA_CONTEXT
    _INTERNAL_REPLICA_CONTEXT = ReplicaContext(
        deployment, replica_tag, controller_name, controller_namespace, servable_object
    )


def _ensure_connected(f: Callable) -> Callable:
    @wraps(f)
    def check(self, *args, **kwargs):
        if self._shutdown:
            raise RayServeException("Client has already been shut down.")
        return f(self, *args, **kwargs)

    return check


class Client:
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
                    controller_namespace = _get_controller_namespace(
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
            statuses = self.get_deployment_statuses()
            if len(statuses) == 0:
                break
            else:
                logger.debug(
                    f"Waiting for shutdown, {len(statuses)} deployments still alive."
                )
            time.sleep(_CLIENT_POLLING_INTERVAL_S)
        else:
            live_names = list(statuses.keys())
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
            statuses = self.get_deployment_statuses()
            try:
                status = statuses[name]
            except KeyError:
                raise RuntimeError(
                    f"Waiting for deployment {name} to be HEALTHY, "
                    "but deployment doesn't exist."
                ) from None

            if status.status == DeploymentStatus.HEALTHY:
                break
            elif status.status == DeploymentStatus.UNHEALTHY:
                raise RuntimeError(f"Deployment {name} is UNHEALTHY: {status.message}")
            else:
                # Guard against new unhandled statuses being added.
                assert status.status == DeploymentStatus.UPDATING

            logger.debug(
                f"Waiting for {name} to be healthy, current status: {status.status}."
            )
            time.sleep(_CLIENT_POLLING_INTERVAL_S)
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
            statuses = self.get_deployment_statuses()
            if name not in statuses:
                break
            else:
                curr_status = statuses[name].status
                logger.debug(
                    f"Waiting for {name} to be deleted, current status: {curr_status}."
                )
            time.sleep(_CLIENT_POLLING_INTERVAL_S)
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
    def deploy_group(self, deployments: List[Dict], _blocking: bool = True):
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
    def get_deployment_statuses(self) -> Dict[str, DeploymentStatusInfo]:
        proto = DeploymentStatusInfoList.FromString(
            ray.get(self._controller.get_deployment_statuses.remote())
        )
        return {
            deployment_status_info.name: DeploymentStatusInfo.from_proto(
                deployment_status_info
            )
            for deployment_status_info in proto.deployment_status_infos
        }

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

        if asyncio_loop_running and sync:
            logger.warning(
                "You are retrieving a sync handle inside an asyncio loop. "
                "Try getting client.get_handle(.., sync=False) to get better "
                "performance. Learn more at https://docs.ray.io/en/master/"
                "serve/http-servehandle.html#sync-and-async-handles"
            )

        if not asyncio_loop_running and not sync:
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


def _check_http_and_checkpoint_options(
    client: Client,
    http_options: Union[dict, HTTPOptions],
    checkpoint_path: str,
) -> None:
    if checkpoint_path and checkpoint_path != client.checkpoint_path:
        logger.warning(
            f"The new client checkpoint path '{checkpoint_path}' "
            f"is different from the existing one '{client.checkpoint_path}'. "
            "The new checkpoint path is ignored."
        )

    if http_options:
        client_http_options = client.http_config
        new_http_options = (
            http_options
            if isinstance(http_options, HTTPOptions)
            else HTTPOptions.parse_obj(http_options)
        )
        different_fields = []
        all_http_option_fields = new_http_options.__dict__
        for field in all_http_option_fields:
            if getattr(new_http_options, field) != getattr(client_http_options, field):
                different_fields.append(field)

        if len(different_fields):
            logger.warning(
                "The new client HTTP config differs from the existing one "
                f"in the following fields: {different_fields}. "
                "The new HTTP config is ignored."
            )


@PublicAPI(stability="beta")
def start(
    detached: bool = False,
    http_options: Optional[Union[dict, HTTPOptions]] = None,
    dedicated_cpu: bool = False,
    _checkpoint_path: str = DEFAULT_CHECKPOINT_PATH,
    _override_controller_namespace: Optional[str] = None,
    **kwargs,
) -> Client:
    """Initialize a serve instance.

    By default, the instance will be scoped to the lifetime of the returned
    Client object (or when the script exits). If detached is set to True, the
    instance will instead persist until serve.shutdown() is called. This is
    only relevant if connecting to a long-running Ray cluster (e.g., with
    ray.init(address="auto") or ray.init("ray://<remote_addr>")).

    Args:
        detached (bool): Whether not the instance should be detached from this
          script. If set, the instance will live on the Ray cluster until it is
          explicitly stopped with serve.shutdown().
        http_options (Optional[Dict, serve.HTTPOptions]): Configuration options
          for HTTP proxy. You can pass in a dictionary or HTTPOptions object
          with fields:

            - host(str, None): Host for HTTP servers to listen on. Defaults to
              "127.0.0.1". To expose Serve publicly, you probably want to set
              this to "0.0.0.0".
            - port(int): Port for HTTP server. Defaults to 8000.
            - root_path(str): Root path to mount the serve application
              (for example, "/serve"). All deployment routes will be prefixed
              with this path. Defaults to "".
            - middlewares(list): A list of Starlette middlewares that will be
              applied to the HTTP servers in the cluster. Defaults to [].
            - location(str, serve.config.DeploymentMode): The deployment
              location of HTTP servers:

                - "HeadOnly": start one HTTP server on the head node. Serve
                  assumes the head node is the node you executed serve.start
                  on. This is the default.
                - "EveryNode": start one HTTP server per node.
                - "NoServer" or None: disable HTTP server.
            - num_cpus (int): The number of CPU cores to reserve for each
              internal Serve HTTP proxy actor.  Defaults to 0.
        dedicated_cpu (bool): Whether to reserve a CPU core for the internal
          Serve controller actor.  Defaults to False.
    """
    http_deprecated_args = ["http_host", "http_port", "http_middlewares"]
    for key in http_deprecated_args:
        if key in kwargs:
            raise ValueError(
                f"{key} is deprecated, please use serve.start(http_options="
                f'{{"{key}": {kwargs[key]}}}) instead.'
            )
    # Initialize ray if needed.
    ray.worker.global_worker.filter_logs_by_job = False
    if not ray.is_initialized():
        ray.init(namespace="serve")

    controller_namespace = _get_controller_namespace(
        detached, _override_controller_namespace=_override_controller_namespace
    )

    try:
        client = internal_get_global_client(
            _override_controller_namespace=_override_controller_namespace,
            _health_check_controller=True,
        )
        logger.info(
            "Connecting to existing Serve instance in namespace "
            f"'{controller_namespace}'."
        )

        _check_http_and_checkpoint_options(client, http_options, _checkpoint_path)
        return client
    except RayServeException:
        pass

    if detached:
        controller_name = SERVE_CONTROLLER_NAME
    else:
        controller_name = format_actor_name(get_random_letters(), SERVE_CONTROLLER_NAME)

    if isinstance(http_options, dict):
        http_options = HTTPOptions.parse_obj(http_options)
    if http_options is None:
        http_options = HTTPOptions()

    controller = ServeController.options(
        num_cpus=1 if dedicated_cpu else 0,
        name=controller_name,
        lifetime="detached" if detached else None,
        max_restarts=-1,
        max_task_retries=-1,
        # Pin Serve controller on the head node.
        resources={get_current_node_resource_key(): 0.01},
        namespace=controller_namespace,
        max_concurrency=CONTROLLER_MAX_CONCURRENCY,
    ).remote(
        controller_name,
        http_options,
        _checkpoint_path,
        detached=detached,
        _override_controller_namespace=_override_controller_namespace,
    )

    proxy_handles = ray.get(controller.get_http_proxies.remote())
    if len(proxy_handles) > 0:
        try:
            ray.get(
                [handle.ready.remote() for handle in proxy_handles.values()],
                timeout=HTTP_PROXY_TIMEOUT,
            )
        except ray.exceptions.GetTimeoutError:
            raise TimeoutError(
                "HTTP proxies not available after {HTTP_PROXY_TIMEOUT}s."
            )

    client = Client(
        controller,
        controller_name,
        detached=detached,
        _override_controller_namespace=_override_controller_namespace,
    )
    _set_global_client(client)
    logger.info(
        f"Started{' detached ' if detached else ' '}Serve instance in "
        f"namespace '{controller_namespace}'."
    )
    return client


def _connect(_override_controller_namespace: Optional[str] = None) -> Client:
    """Connect to an existing Serve instance on this Ray cluster.

    If calling from the driver program, the Serve instance on this Ray cluster
    must first have been initialized using `serve.start(detached=True)`.

    If called from within a replica, this will connect to the same Serve
    instance that the replica is running in.

    Args:
        _override_controller_namespace (Optional[str]): The namespace to use
            when looking for the controller. If None, Serve recalculates the
            controller's namespace using _get_controller_namespace().

    Raises:
        RayServeException: if there is no Serve controller actor in the
            expected namespace.
    """

    # Initialize ray if needed.
    ray.worker.global_worker.filter_logs_by_job = False
    if not ray.is_initialized():
        ray.init(namespace="serve")

    # When running inside of a replica, _INTERNAL_REPLICA_CONTEXT is set to
    # ensure that the correct instance is connected to.
    if _INTERNAL_REPLICA_CONTEXT is None:
        controller_name = SERVE_CONTROLLER_NAME
        controller_namespace = _get_controller_namespace(
            detached=True, _override_controller_namespace=_override_controller_namespace
        )
    else:
        controller_name = _INTERNAL_REPLICA_CONTEXT._internal_controller_name
        controller_namespace = _INTERNAL_REPLICA_CONTEXT._internal_controller_namespace

    # Try to get serve controller if it exists
    try:
        controller = ray.get_actor(controller_name, namespace=controller_namespace)
    except ValueError:
        raise RayServeException(
            "There is no "
            "instance running on this Ray cluster. Please "
            "call `serve.start(detached=True) to start "
            "one."
        )

    client = Client(
        controller,
        controller_name,
        detached=True,
        _override_controller_namespace=_override_controller_namespace,
    )
    _set_global_client(client)
    return client


@PublicAPI
def shutdown() -> None:
    """Completely shut down the connected Serve instance.

    Shuts down all processes and deletes all state associated with the
    instance.
    """

    try:
        client = internal_get_global_client()
    except RayServeException:
        logger.info(
            "Nothing to shut down. There's no Serve application "
            "running on this Ray cluster."
        )
        return

    client.shutdown()
    _set_global_client(None)


@PublicAPI
def get_replica_context() -> ReplicaContext:
    """If called from a deployment, returns the deployment and replica tag.

    A replica tag uniquely identifies a single replica for a Ray Serve
    deployment at runtime.  Replica tags are of the form
    `<deployment_name>#<random letters>`.

    Raises:
        RayServeException: if not called from within a Ray Serve deployment.

    Example:
        >>> from ray import serve
        >>> # deployment_name
        >>> serve.get_replica_context().deployment # doctest: +SKIP
        >>> # deployment_name#krcwoa
        >>> serve.get_replica_context().replica_tag # doctest: +SKIP
    """
    if _INTERNAL_REPLICA_CONTEXT is None:
        raise RayServeException(
            "`serve.get_replica_context()` "
            "may only be called from within a "
            "Ray Serve deployment."
        )
    return _INTERNAL_REPLICA_CONTEXT


@PublicAPI(stability="beta")
def ingress(app: Union["FastAPI", "APIRouter", Callable]):
    """Mark an ASGI application ingress for Serve.

    Args:
        app (FastAPI,APIRouter,Starlette,etc): the app or router object serve
            as ingress for this deployment. It can be any ASGI compatible
            object.

    Example:
        >>> from fastapi import FastAPI
        >>> from ray import serve
        >>> app = FastAPI() # doctest: +SKIP
        >>> app = FastAPI() # doctest: +SKIP
        >>> @serve.deployment # doctest: +SKIP
        ... @serve.ingress(app) # doctest: +SKIP
        ... class App: # doctest: +SKIP
        ...     pass # doctest: +SKIP
        >>> App.deploy() # doctest: +SKIP
    """

    def decorator(cls):
        if not inspect.isclass(cls):
            raise ValueError("@serve.ingress must be used with a class.")

        if issubclass(cls, collections.abc.Callable):
            raise ValueError(
                "Class passed to @serve.ingress may not have __call__ method."
            )

        # Sometimes there are decorators on the methods. We want to fix
        # the fast api routes here.
        if isinstance(app, (FastAPI, APIRouter)):
            make_fastapi_class_based_view(app, cls)

        # Free the state of the app so subsequent modification won't affect
        # this ingress deployment. We don't use copy.copy here to avoid
        # recursion issue.
        ensure_serialization_context()
        frozen_app = cloudpickle.loads(cloudpickle.dumps(app))

        class ASGIAppWrapper(cls):
            async def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)

                self._serve_app = frozen_app

                # Use uvicorn's lifespan handling code to properly deal with
                # startup and shutdown event.
                self._serve_asgi_lifespan = LifespanOn(
                    Config(self._serve_app, lifespan="on")
                )
                # Replace uvicorn logger with our own.
                self._serve_asgi_lifespan.logger = logger
                # LifespanOn's logger logs in INFO level thus becomes spammy
                # Within this block we temporarily uplevel for cleaner logging
                with LoggingContext(
                    self._serve_asgi_lifespan.logger, level=logging.WARNING
                ):
                    await self._serve_asgi_lifespan.startup()

            async def __call__(self, request: Request):
                sender = ASGIHTTPSender()
                await self._serve_app(
                    request.scope,
                    request.receive,
                    sender,
                )
                return sender.build_asgi_response()

            # NOTE: __del__ must be async so that we can run asgi shutdown
            # in the same event loop.
            async def __del__(self):
                # LifespanOn's logger logs in INFO level thus becomes spammy
                # Within this block we temporarily uplevel for cleaner logging
                with LoggingContext(
                    self._serve_asgi_lifespan.logger, level=logging.WARNING
                ):
                    await self._serve_asgi_lifespan.shutdown()

                # Make sure to call user's del method as well.
                super_cls = super()
                if hasattr(super_cls, "__del__"):
                    super_cls.__del__()

        ASGIAppWrapper.__name__ = cls.__name__
        return ASGIAppWrapper

    return decorator


@overload
def deployment(func_or_class: Callable) -> Deployment:
    pass


@overload
def deployment(
    name: Optional[str] = None,
    version: Optional[str] = None,
    prev_version: Optional[str] = None,
    num_replicas: Optional[int] = None,
    init_args: Optional[Tuple[Any]] = None,
    init_kwargs: Optional[Dict[Any, Any]] = None,
    route_prefix: Union[str, None, DEFAULT] = DEFAULT.VALUE,
    ray_actor_options: Optional[Dict] = None,
    user_config: Optional[Any] = None,
    max_concurrent_queries: Optional[int] = None,
    _autoscaling_config: Optional[Union[Dict, AutoscalingConfig]] = None,
    _graceful_shutdown_wait_loop_s: Optional[float] = None,
    _graceful_shutdown_timeout_s: Optional[float] = None,
    _health_check_period_s: Optional[float] = None,
    _health_check_timeout_s: Optional[float] = None,
) -> Callable[[Callable], Deployment]:
    pass


@PublicAPI
def deployment(
    _func_or_class: Optional[Callable] = None,
    name: Optional[str] = None,
    version: Optional[str] = None,
    prev_version: Optional[str] = None,
    num_replicas: Optional[int] = None,
    init_args: Optional[Tuple[Any]] = None,
    init_kwargs: Optional[Dict[Any, Any]] = None,
    route_prefix: Union[str, None, DEFAULT] = DEFAULT.VALUE,
    ray_actor_options: Optional[Dict] = None,
    user_config: Optional[Any] = None,
    max_concurrent_queries: Optional[int] = None,
    _autoscaling_config: Optional[Union[Dict, AutoscalingConfig]] = None,
    _graceful_shutdown_wait_loop_s: Optional[float] = None,
    _graceful_shutdown_timeout_s: Optional[float] = None,
    _health_check_period_s: Optional[float] = None,
    _health_check_timeout_s: Optional[float] = None,
) -> Callable[[Callable], Deployment]:
    """Define a Serve deployment.

    Args:
        name (Optional[str]): Globally-unique name identifying this deployment.
            If not provided, the name of the class or function will be used.
        version (Optional[str]): Version of the deployment. This is used to
            indicate a code change for the deployment; when it is re-deployed
            with a version change, a rolling update of the replicas will be
            performed. If not provided, every deployment will be treated as a
            new version.
        prev_version (Optional[str]): Version of the existing deployment which
            is used as a precondition for the next deployment. If prev_version
            does not match with the existing deployment's version, the
            deployment will fail. If not provided, deployment procedure will
            not check the existing deployment's version.
        num_replicas (Optional[int]): The number of processes to start up that
            will handle requests to this deployment. Defaults to 1.
        init_args (Optional[Tuple]): Positional args to be passed to the class
            constructor when starting up deployment replicas. These can also be
            passed when you call `.deploy()` on the returned Deployment.
        init_kwargs (Optional[Dict]): Keyword args to be passed to the class
            constructor when starting up deployment replicas. These can also be
            passed when you call `.deploy()` on the returned Deployment.
        route_prefix (Optional[str]): Requests to paths under this HTTP path
            prefix will be routed to this deployment. Defaults to '/{name}'.
            When set to 'None', no HTTP endpoint will be created.
            Routing is done based on longest-prefix match, so if you have
            deployment A with a prefix of '/a' and deployment B with a prefix
            of '/a/b', requests to '/a', '/a/', and '/a/c' go to A and requests
            to '/a/b', '/a/b/', and '/a/b/c' go to B. Routes must not end with
            a '/' unless they're the root (just '/'), which acts as a
            catch-all.
        ray_actor_options (dict): Options to be passed to the Ray actor
            constructor such as resource requirements.
        user_config (Optional[Any]): [experimental] Config to pass to the
            reconfigure method of the deployment. This can be updated
            dynamically without changing the version of the deployment and
            restarting its replicas. The user_config needs to be hashable to
            keep track of updates, so it must only contain hashable types, or
            hashable types nested in lists and dictionaries.
        max_concurrent_queries (Optional[int]): The maximum number of queries
            that will be sent to a replica of this deployment without receiving
            a response. Defaults to 100.

    Example:
    >>> from ray import serve
    >>> @serve.deployment(name="deployment1", version="v1") # doctest: +SKIP
    ... class MyDeployment: # doctest: +SKIP
    ...     pass # doctest: +SKIP

    >>> MyDeployment.deploy(*init_args) # doctest: +SKIP
    >>> MyDeployment.options( # doctest: +SKIP
    ...     num_replicas=2, init_args=init_args).deploy()

    Returns:
        Deployment
    """

    if num_replicas is not None and _autoscaling_config is not None:
        raise ValueError(
            "Manually setting num_replicas is not allowed when "
            "_autoscaling_config is provided."
        )

    config = DeploymentConfig()
    if num_replicas is not None:
        config.num_replicas = num_replicas

    if user_config is not None:
        config.user_config = user_config

    if max_concurrent_queries is not None:
        config.max_concurrent_queries = max_concurrent_queries

    if _autoscaling_config is not None:
        config.autoscaling_config = _autoscaling_config

    if _graceful_shutdown_wait_loop_s is not None:
        config.graceful_shutdown_wait_loop_s = _graceful_shutdown_wait_loop_s

    if _graceful_shutdown_timeout_s is not None:
        config.graceful_shutdown_timeout_s = _graceful_shutdown_timeout_s

    if _health_check_period_s is not None:
        config.health_check_period_s = _health_check_period_s

    if _health_check_timeout_s is not None:
        config.health_check_timeout_s = _health_check_timeout_s

    def decorator(_func_or_class):
        return Deployment(
            _func_or_class,
            name if name is not None else _func_or_class.__name__,
            config,
            version=version,
            prev_version=prev_version,
            init_args=init_args,
            init_kwargs=init_kwargs,
            route_prefix=route_prefix,
            ray_actor_options=ray_actor_options,
            _internal=True,
        )

    # This handles both parametrized and non-parametrized usage of the
    # decorator. See the @serve.batch code for more details.
    return decorator(_func_or_class) if callable(_func_or_class) else decorator


@PublicAPI
def get_deployment(name: str) -> Deployment:
    """Dynamically fetch a handle to a Deployment object.

    This can be used to update and redeploy a deployment without access to
    the original definition.

    Example:
    >>> from ray import serve
    >>> MyDeployment = serve.get_deployment("name")  # doctest: +SKIP
    >>> MyDeployment.options(num_replicas=10).deploy()  # doctest: +SKIP

    Args:
        name(str): name of the deployment. This must have already been
        deployed.

    Returns:
        Deployment
    """
    try:
        (
            deployment_info,
            route_prefix,
        ) = internal_get_global_client().get_deployment_info(name)
    except KeyError:
        raise KeyError(
            f"Deployment {name} was not found. Did you call Deployment.deploy()?"
        )
    return Deployment(
        cloudpickle.loads(deployment_info.replica_config.serialized_deployment_def),
        name,
        deployment_info.deployment_config,
        version=deployment_info.version,
        init_args=deployment_info.replica_config.init_args,
        init_kwargs=deployment_info.replica_config.init_kwargs,
        route_prefix=route_prefix,
        ray_actor_options=deployment_info.replica_config.ray_actor_options,
        _internal=True,
    )


@PublicAPI
def list_deployments() -> Dict[str, Deployment]:
    """Returns a dictionary of all active deployments.

    Dictionary maps deployment name to Deployment objects.
    """
    infos = internal_get_global_client().list_deployments()

    deployments = {}
    for name, (deployment_info, route_prefix) in infos.items():
        deployments[name] = Deployment(
            cloudpickle.loads(deployment_info.replica_config.serialized_deployment_def),
            name,
            deployment_info.deployment_config,
            version=deployment_info.version,
            init_args=deployment_info.replica_config.init_args,
            init_kwargs=deployment_info.replica_config.init_kwargs,
            route_prefix=route_prefix,
            ray_actor_options=deployment_info.replica_config.ray_actor_options,
            _internal=True,
        )

    return deployments


def get_deployment_statuses() -> Dict[str, DeploymentStatusInfo]:
    """Returns a dictionary of deployment statuses.

    A deployment's status is one of {UPDATING, UNHEALTHY, and HEALTHY}.

    Example:
    >>> from ray.serve.api import get_deployment_statuses
    >>> statuses = get_deployment_statuses() # doctest: +SKIP
    >>> status_info = statuses["deployment_name"] # doctest: +SKIP
    >>> status = status_info.status # doctest: +SKIP
    >>> message = status_info.message # doctest: +SKIP

    Returns:
            Dict[str, DeploymentStatus]: This dictionary maps the running
                deployment's name to a DeploymentStatus object containing its
                status and a message explaining the status.
    """

    return internal_get_global_client().get_deployment_statuses()


@PublicAPI(stability="alpha")
def run(
    target: Union[DeploymentNode, DeploymentFunctionNode],
    _blocking: bool = True,
    *,
    host: str = DEFAULT_HTTP_HOST,
    port: int = DEFAULT_HTTP_PORT,
) -> Optional[RayServeHandle]:
    """Run a Serve application and return a ServeHandle to the ingress.

    Either a DeploymentNode, DeploymentFunctionNode, or a pre-built application
    can be passed in. If a node is passed in, all of the deployments it depends
    on will be deployed. If there is an ingress, its handle will be returned.

    Args:
        target (Union[DeploymentNode, DeploymentFunctionNode, Application]):
            A user-built Serve Application or a DeploymentNode that acts as the
            root node of DAG. By default DeploymentNode is the Driver
            deployment unless user provides a customized one.
        host (str): The host passed into serve.start().
        port (int): The port passed into serve.start().

    Returns:
        RayServeHandle: A regular ray serve handle that can be called by user
            to execute the serve DAG.
    """
    # TODO (jiaodong): Resolve circular reference in pipeline codebase and serve
    from ray.serve.pipeline.api import build as pipeline_build
    from ray.serve.pipeline.api import get_and_validate_ingress_deployment

    client = start(detached=True, http_options={"host": host, "port": port})

    if isinstance(target, Application):
        deployments = list(target.deployments.values())
        ingress = target.ingress
    # Each DAG should always provide a valid Driver DeploymentNode
    elif isinstance(target, DeploymentNode):
        deployments = pipeline_build(target)
        ingress = get_and_validate_ingress_deployment(deployments)
    # Special case where user is doing single function serve.run(func.bind())
    elif isinstance(target, DeploymentFunctionNode):
        deployments = pipeline_build(target)
        ingress = get_and_validate_ingress_deployment(deployments)
        if len(deployments) != 1:
            raise ValueError(
                "We only support single function node in serve.run, ex: "
                "serve.run(func.bind()). For more than one nodes in your DAG, "
                "Please provide a driver class and bind it as entrypoint to "
                "your Serve DAG."
            )
    elif isinstance(target, DAGNode):
        raise ValueError(
            "Invalid DAGNode type as entry to serve.run(), "
            f"type: {type(target)}, accepted: DeploymentNode, "
            "DeploymentFunctionNode please provide a driver class and bind it "
            "as entrypoint to your Serve DAG."
        )
    else:
        raise TypeError(
            "Expected a DeploymentNode, DeploymentFunctionNode, or "
            "Application as target. Got unexpected type "
            f'"{type(target)}" instead.'
        )

    parameter_group = []

    for deployment in deployments:
        deployment_parameters = {
            "name": deployment._name,
            "func_or_class": deployment._func_or_class,
            "init_args": deployment.init_args,
            "init_kwargs": deployment.init_kwargs,
            "ray_actor_options": deployment._ray_actor_options,
            "config": deployment._config,
            "version": deployment._version,
            "prev_version": deployment._prev_version,
            "route_prefix": deployment.route_prefix,
            "url": deployment.url,
        }

        parameter_group.append(deployment_parameters)

    client.deploy_group(parameter_group, _blocking=_blocking)

    if ingress is not None:
        return ingress.get_handle()


def build(target: Union[DeploymentNode, DeploymentFunctionNode]) -> Application:
    """Builds a Serve application into a static application.

    Takes in a DeploymentNode or DeploymentFunctionNode and converts it to a
    Serve application consisting of one or more deployments. This is intended
    to be used for production scenarios and deployed via the Serve REST API or
    CLI, so there are some restrictions placed on the deployments:
        1) All of the deployments must be importable. That is, they cannot be
           defined in __main__ or inline defined. The deployments will be
           imported in production using the same import path they were here.
        2) All arguments bound to the deployment must be JSON-serializable.

    The returned Application object can be exported to a dictionary or YAML
    config.
    """
    # TODO (jiaodong): Resolve circular reference in pipeline codebase and serve
    from ray.serve.pipeline.api import build as pipeline_build

    if in_interactive_shell():
        raise RuntimeError(
            "build cannot be called from an interactive shell like "
            "IPython or Jupyter because it requires all deployments to be "
            "importable to run the app after building."
        )

    # TODO(edoakes): this should accept host and port, but we don't
    # currently support them in the REST API.
    return Application(pipeline_build(target))


def deployment_to_schema(d: Deployment) -> DeploymentSchema:
    """Converts a live deployment object to a corresponding structured schema.

    If the deployment has a class or function, it will be attemptetd to be
    converted to a valid corresponding import path.

    init_args and init_kwargs must also be JSON-serializable or this call will
    fail.
    """
    from ray.serve.pipeline.json_serde import convert_to_json_safe_obj

    if d.ray_actor_options is not None:
        ray_actor_options_schema = RayActorOptionsSchema.parse_obj(d.ray_actor_options)
    else:
        ray_actor_options_schema = None

    return DeploymentSchema(
        name=d.name,
        import_path=get_deployment_import_path(
            d, enforce_importable=True, replace_main=True
        ),
        init_args=convert_to_json_safe_obj(d.init_args, err_key="init_args"),
        init_kwargs=convert_to_json_safe_obj(d.init_kwargs, err_key="init_kwargs"),
        num_replicas=d.num_replicas,
        route_prefix=d.route_prefix,
        max_concurrent_queries=d.max_concurrent_queries,
        user_config=d.user_config,
        autoscaling_config=d._config.autoscaling_config,
        graceful_shutdown_wait_loop_s=d._config.graceful_shutdown_wait_loop_s,
        graceful_shutdown_timeout_s=d._config.graceful_shutdown_timeout_s,
        health_check_period_s=d._config.health_check_period_s,
        health_check_timeout_s=d._config.health_check_timeout_s,
        ray_actor_options=ray_actor_options_schema,
    )


def schema_to_deployment(s: DeploymentSchema) -> Deployment:
    from ray.serve.pipeline.json_serde import convert_from_json_safe_obj

    if s.ray_actor_options is None:
        ray_actor_options = None
    else:
        ray_actor_options = s.ray_actor_options.dict(exclude_unset=True)

    return deployment(
        name=s.name,
        init_args=convert_from_json_safe_obj(s.init_args, err_key="init_args"),
        init_kwargs=convert_from_json_safe_obj(s.init_kwargs, err_key="init_kwargs"),
        num_replicas=s.num_replicas,
        route_prefix=s.route_prefix,
        max_concurrent_queries=s.max_concurrent_queries,
        user_config=s.user_config,
        _autoscaling_config=s.autoscaling_config,
        _graceful_shutdown_wait_loop_s=s.graceful_shutdown_wait_loop_s,
        _graceful_shutdown_timeout_s=s.graceful_shutdown_timeout_s,
        _health_check_period_s=s.health_check_period_s,
        _health_check_timeout_s=s.health_check_timeout_s,
        ray_actor_options=ray_actor_options,
    )(s.import_path)


def serve_application_to_schema(
    deployments: List[Deployment],
) -> ServeApplicationSchema:
    return ServeApplicationSchema(
        deployments=[deployment_to_schema(d) for d in deployments]
    )


def schema_to_serve_application(schema: ServeApplicationSchema) -> List[Deployment]:
    return [schema_to_deployment(s) for s in schema.deployments]


def status_info_to_schema(
    deployment_name: str, status_info: Union[DeploymentStatusInfo, Dict]
) -> DeploymentStatusSchema:
    if isinstance(status_info, DeploymentStatusInfo):
        return DeploymentStatusSchema(
            name=deployment_name, status=status_info.status, message=status_info.message
        )
    elif isinstance(status_info, dict):
        return DeploymentStatusSchema(
            name=deployment_name,
            status=status_info["status"],
            message=status_info["message"],
        )
    else:
        raise TypeError(
            f"Got {type(status_info)} as status_info's "
            "type. Expected status_info to be either a "
            "DeploymentStatusInfo or a dictionary."
        )


def serve_application_status_to_schema(
    status_infos: Dict[str, Union[DeploymentStatusInfo, Dict]]
) -> ServeApplicationStatusSchema:
    schemas = [
        status_info_to_schema(deployment_name, status_info)
        for deployment_name, status_info in status_infos.items()
    ]
    return ServeApplicationStatusSchema(statuses=schemas)

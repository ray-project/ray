import asyncio
import atexit
import collections
from copy import copy
import inspect
import logging
import random
import re
import time
import yaml
import json
from dataclasses import dataclass
from functools import wraps
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    TextIO,
    Tuple,
    Type,
    Union,
    List,
    Iterable,
    overload,
)

from fastapi import APIRouter, FastAPI
from ray.exceptions import RayActorError
from ray.experimental.dag.class_node import ClassNode
from ray.experimental.dag.function_node import FunctionNode
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
from ray.serve.exceptions import RayServeException
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
            statuses = ray.get(self._controller.get_deployment_statuses.remote())
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
            statuses = ray.get(self._controller.get_deployment_statuses.remote())
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
            statuses = ray.get(self._controller.get_deployment_statuses.remote())
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
        return ray.get(self._controller.get_deployment_info.remote(name))

    @_ensure_connected
    def list_deployments(self) -> Dict[str, Tuple[DeploymentInfo, str]]:
        return ray.get(self._controller.list_deployments.remote())

    @_ensure_connected
    def get_deployment_statuses(self) -> Dict[str, DeploymentStatusInfo]:
        return ray.get(self._controller.get_deployment_statuses.remote())

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
            "replica_config": replica_config,
            "version": version,
            "prev_version": prev_version,
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
    if _global_client is None:
        return

    internal_get_global_client().shutdown()
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
        >>> serve.get_replica_context().deployment # deployment_name
        >>> serve.get_replica_context().replica_tag # deployment_name#krcwoa
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
    >>> app = FastAPI()
    >>> @serve.deployment
        @serve.ingress(app)
        class App:
            pass
    >>> App.deploy()
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


@PublicAPI(stability="alpha")
class RayServeDAGHandle:
    """Resolved from a DeploymentNode at runtime.

    This can be used to call the DAG from a driver deployment to efficiently
    orchestrate a multi-deployment pipeline.
    """

    def __init__(self, dag_node_json: str) -> None:

        self.dag_node_json = dag_node_json

        # NOTE(simon): Making this lazy to avoid deserialization in controller for now
        # This would otherwise hang because it's trying to get handles from within
        # the controller.
        self.dag_node = None

    @classmethod
    def _deserialize(cls, *args):
        """Required for this class's __reduce__ method to be picklable."""
        return cls(*args)

    def __reduce__(self):
        return RayServeDAGHandle._deserialize, (self.dag_node_json,)

    def remote(self, *args, **kwargs):
        from ray.serve.pipeline.json_serde import dagnode_from_json

        if self.dag_node is None:
            self.dag_node = json.loads(
                self.dag_node_json, object_hook=dagnode_from_json
            )
        return self.dag_node.execute(*args, **kwargs)


@PublicAPI(stability="alpha")
class DeploymentMethodNode(DAGNode):
    """Represents a method call on a bound deployment node.

    These method calls can be composed into an optimized call DAG and passed
    to a "driver" deployment that will orchestrate the calls at runtime.

    This class cannot be called directly. Instead, when it is bound to a
    deployment node, it will be resolved to a DeployedCallGraph at runtime.
    """

    # TODO (jiaodong): Later unify and refactor this with pipeline node class
    pass


@PublicAPI(stability="alpha")
class DeploymentNode(ClassNode):
    """Represents a deployment with its bound config options and arguments.

    The bound deployment can be run, deployed, or built to a production config
    using serve.run, serve.deploy, and serve.build, respectively.

    A bound deployment can be passed as an argument to other bound deployments
    to build a multi-deployment application. When the application is deployed, the
    bound deployments passed into a constructor will be converted to
    RayServeHandles that can be used to send requests.

    Calling deployment.method.bind() will return a DeploymentMethodNode
    that can be used to compose an optimized call graph.
    """

    # TODO (jiaodong): Later unify and refactor this with pipeline node class
    def bind(self, *args, **kwargs):
        """Bind the default __call__ method and return a DeploymentMethodNode"""
        return self.__call__.bind(*args, **kwargs)


@PublicAPI(stability="alpha")
class DeploymentFunctionNode(FunctionNode):
    """Represents a serve.deployment decorated function from user.

    It's the counterpart of DeploymentNode that represents function as body
    instead of class.
    """

    pass


@PublicAPI
class Deployment:
    def __init__(
        self,
        func_or_class: Union[Callable, str],
        name: str,
        config: DeploymentConfig,
        version: Optional[str] = None,
        prev_version: Optional[str] = None,
        init_args: Optional[Tuple[Any]] = None,
        init_kwargs: Optional[Tuple[Any]] = None,
        route_prefix: Union[str, None, DEFAULT] = DEFAULT.VALUE,
        ray_actor_options: Optional[Dict] = None,
        _internal=False,
    ) -> None:
        """Construct a Deployment. CONSTRUCTOR SHOULDN'T BE USED DIRECTLY.

        Deployments should be created, retrieved, and updated using
        `@serve.deployment`, `serve.get_deployment`, and `Deployment.options`,
        respectively.
        """

        if not _internal:
            raise RuntimeError(
                "The Deployment constructor should not be called "
                "directly. Use `@serve.deployment` instead."
            )
        if not callable(func_or_class) and not isinstance(func_or_class, str):
            raise TypeError("@serve.deployment must be called on a class or function.")
        if not isinstance(name, str):
            raise TypeError("name must be a string.")
        if not (version is None or isinstance(version, str)):
            raise TypeError("version must be a string.")
        if not (prev_version is None or isinstance(prev_version, str)):
            raise TypeError("prev_version must be a string.")
        if not (init_args is None or isinstance(init_args, (tuple, list))):
            raise TypeError("init_args must be a tuple.")
        if not (init_kwargs is None or isinstance(init_kwargs, dict)):
            raise TypeError("init_kwargs must be a dict.")
        if route_prefix is not DEFAULT.VALUE and route_prefix is not None:
            if not isinstance(route_prefix, str):
                raise TypeError("route_prefix must be a string.")
            if not route_prefix.startswith("/"):
                raise ValueError("route_prefix must start with '/'.")
            if route_prefix != "/" and route_prefix.endswith("/"):
                raise ValueError(
                    "route_prefix must not end with '/' unless it's the root."
                )
            if "{" in route_prefix or "}" in route_prefix:
                raise ValueError("route_prefix may not contain wildcards.")
        if not (ray_actor_options is None or isinstance(ray_actor_options, dict)):
            raise TypeError("ray_actor_options must be a dict.")

        if init_args is None:
            init_args = ()
        if init_kwargs is None:
            init_kwargs = {}

        # TODO(architkulkarni): Enforce that autoscaling_config and
        # user-provided num_replicas should be mutually exclusive.
        if version is None and config.autoscaling_config is not None:
            # TODO(architkulkarni): Remove this restriction.
            raise ValueError(
                "Currently autoscaling is only supported for "
                "versioned deployments. Try @serve.deployment(version=...)."
            )

        self._func_or_class = func_or_class
        self._name = name
        self._version = version
        self._prev_version = prev_version
        self._config = config
        self._init_args = init_args
        self._init_kwargs = init_kwargs
        self._route_prefix = route_prefix
        self._ray_actor_options = ray_actor_options

    @property
    def name(self) -> str:
        """Unique name of this deployment."""
        return self._name

    @property
    def version(self) -> Optional[str]:
        """Version of this deployment.

        If None, will be redeployed every time `.deploy()` is called.
        """
        return self._version

    @property
    def prev_version(self) -> Optional[str]:
        """Existing version of deployment to target.

        If prev_version does not match with existing deployment
        version, the deployment will fail to be deployed.
        """
        return self._prev_version

    @property
    def func_or_class(self) -> Union[Callable, str]:
        """Underlying class or function that this deployment wraps."""
        return self._func_or_class

    @property
    def num_replicas(self) -> int:
        """Current target number of replicas."""
        return self._config.num_replicas

    @property
    def user_config(self) -> Any:
        """Current dynamic user-provided config options."""
        return self._config.user_config

    @property
    def max_concurrent_queries(self) -> int:
        """Current max outstanding queries from each handle."""
        return self._config.max_concurrent_queries

    @property
    def route_prefix(self) -> Optional[str]:
        """HTTP route prefix that this deployment is exposed under."""
        if self._route_prefix is DEFAULT.VALUE:
            return f"/{self._name}"
        return self._route_prefix

    @property
    def ray_actor_options(self) -> Optional[Dict]:
        """Actor options such as resources required for each replica."""
        return self._ray_actor_options

    @property
    def init_args(self) -> Tuple[Any]:
        """Positional args passed to the underlying class's constructor."""
        return self._init_args

    @property
    def init_kwargs(self) -> Tuple[Any]:
        """Keyword args passed to the underlying class's constructor."""
        return self._init_kwargs

    @property
    def url(self) -> Optional[str]:
        """Full HTTP url for this deployment."""
        if self._route_prefix is None:
            # this deployment is not exposed over HTTP
            return None

        return internal_get_global_client().root_url + self.route_prefix

    def __call__(self):
        raise RuntimeError(
            "Deployments cannot be constructed directly. "
            "Use `deployment.deploy() instead.`"
        )

    @PublicAPI(stability="alpha")
    def bind(self, *args, **kwargs) -> Union[DeploymentNode, DeploymentFunctionNode]:
        """Bind the provided arguments and return a DeploymentNode.

        The returned bound deployment can be deployed or bound to other
        deployments to create a multi-deployment application.
        """
        copied_self = copy(self)
        copied_self._init_args = []
        copied_self._init_kwargs = {}
        copied_self._func_or_class = "dummpy.module"
        schema_shell = deployment_to_schema(copied_self)

        if inspect.isfunction(self._func_or_class):
            return DeploymentFunctionNode(
                self._func_or_class,
                args,  # Used to bind and resolve DAG only, can take user input
                kwargs,  # Used to bind and resolve DAG only, can take user input
                self._ray_actor_options or dict(),
                other_args_to_resolve={
                    "deployment_schema": schema_shell,
                    "is_from_serve_deployment": True,
                },
            )
        else:
            return DeploymentNode(
                self._func_or_class,
                args,
                kwargs,
                cls_options=self._ray_actor_options or dict(),
                other_args_to_resolve={
                    "deployment_schema": schema_shell,
                    "is_from_serve_deployment": True,
                },
            )

    @PublicAPI
    def deploy(self, *init_args, _blocking=True, **init_kwargs):
        """Deploy or update this deployment.

        Args:
            init_args (optional): args to pass to the class __init__
                method. Not valid if this deployment wraps a function.
            init_kwargs (optional): kwargs to pass to the class __init__
                method. Not valid if this deployment wraps a function.
        """
        if len(init_args) == 0 and self._init_args is not None:
            init_args = self._init_args
        if len(init_kwargs) == 0 and self._init_kwargs is not None:
            init_kwargs = self._init_kwargs

        return internal_get_global_client().deploy(
            self._name,
            self._func_or_class,
            init_args,
            init_kwargs,
            ray_actor_options=self._ray_actor_options,
            config=self._config,
            version=self._version,
            prev_version=self._prev_version,
            route_prefix=self.route_prefix,
            url=self.url,
            _blocking=_blocking,
        )

    @PublicAPI
    def delete(self):
        """Delete this deployment."""
        return internal_get_global_client().delete_deployments([self._name])

    @PublicAPI
    def get_handle(
        self, sync: Optional[bool] = True
    ) -> Union[RayServeHandle, RayServeSyncHandle]:
        """Get a ServeHandle to this deployment to invoke it from Python.

        Args:
            sync (bool): If true, then Serve will return a ServeHandle that
                works everywhere. Otherwise, Serve will return an
                asyncio-optimized ServeHandle that's only usable in an asyncio
                loop.

        Returns:
            ServeHandle
        """
        return internal_get_global_client().get_handle(
            self._name, missing_ok=True, sync=sync
        )

    @PublicAPI
    def options(
        self,
        func_or_class: Optional[Callable] = None,
        name: Optional[str] = None,
        version: Optional[str] = None,
        prev_version: Optional[str] = None,
        init_args: Optional[Tuple[Any]] = None,
        init_kwargs: Optional[Dict[Any, Any]] = None,
        route_prefix: Union[str, None, DEFAULT] = DEFAULT.VALUE,
        num_replicas: Optional[int] = None,
        ray_actor_options: Optional[Dict] = None,
        user_config: Optional[Any] = None,
        max_concurrent_queries: Optional[int] = None,
        _autoscaling_config: Optional[Union[Dict, AutoscalingConfig]] = None,
        _graceful_shutdown_wait_loop_s: Optional[float] = None,
        _graceful_shutdown_timeout_s: Optional[float] = None,
        _health_check_period_s: Optional[float] = None,
        _health_check_timeout_s: Optional[float] = None,
    ) -> "Deployment":
        """Return a copy of this deployment with updated options.

        Only those options passed in will be updated, all others will remain
        unchanged from the existing deployment.
        """
        new_config = self._config.copy()
        if num_replicas is not None:
            new_config.num_replicas = num_replicas
        if user_config is not None:
            new_config.user_config = user_config
        if max_concurrent_queries is not None:
            new_config.max_concurrent_queries = max_concurrent_queries

        if func_or_class is None:
            func_or_class = self._func_or_class

        if name is None:
            name = self._name

        if version is None:
            version = self._version

        if prev_version is None:
            prev_version = self._prev_version

        if init_args is None:
            init_args = self._init_args

        if init_kwargs is None:
            init_kwargs = self._init_kwargs

        if route_prefix is DEFAULT.VALUE:
            # Default is to keep the previous value
            route_prefix = self._route_prefix

        if ray_actor_options is None:
            ray_actor_options = self._ray_actor_options

        if _autoscaling_config is not None:
            new_config.autoscaling_config = _autoscaling_config

        if _graceful_shutdown_wait_loop_s is not None:
            new_config.graceful_shutdown_wait_loop_s = _graceful_shutdown_wait_loop_s

        if _graceful_shutdown_timeout_s is not None:
            new_config.graceful_shutdown_timeout_s = _graceful_shutdown_timeout_s

        if _health_check_period_s is not None:
            new_config.health_check_period_s = _health_check_period_s

        if _health_check_timeout_s is not None:
            new_config.health_check_timeout_s = _health_check_timeout_s

        return Deployment(
            func_or_class,
            name,
            new_config,
            version=version,
            prev_version=prev_version,
            init_args=init_args,
            init_kwargs=init_kwargs,
            route_prefix=route_prefix,
            ray_actor_options=ray_actor_options,
            _internal=True,
        )

    @PublicAPI(stability="alpha")
    def set_options(
        self,
        func_or_class: Optional[Callable] = None,
        name: Optional[str] = None,
        version: Optional[str] = None,
        prev_version: Optional[str] = None,
        init_args: Optional[Tuple[Any]] = None,
        init_kwargs: Optional[Dict[Any, Any]] = None,
        route_prefix: Union[str, None, DEFAULT] = DEFAULT.VALUE,
        num_replicas: Optional[int] = None,
        ray_actor_options: Optional[Dict] = None,
        user_config: Optional[Any] = None,
        max_concurrent_queries: Optional[int] = None,
        _autoscaling_config: Optional[Union[Dict, AutoscalingConfig]] = None,
        _graceful_shutdown_wait_loop_s: Optional[float] = None,
        _graceful_shutdown_timeout_s: Optional[float] = None,
        _health_check_period_s: Optional[float] = None,
        _health_check_timeout_s: Optional[float] = None,
    ) -> None:
        """Overwrite this deployment's options. Mutates the deployment.

        Only those options passed in will be updated, all others will remain
        unchanged.
        """

        validated = self.options(
            func_or_class=func_or_class,
            name=name,
            version=version,
            prev_version=prev_version,
            init_args=init_args,
            init_kwargs=init_kwargs,
            route_prefix=route_prefix,
            num_replicas=num_replicas,
            ray_actor_options=ray_actor_options,
            user_config=user_config,
            max_concurrent_queries=max_concurrent_queries,
            _autoscaling_config=_autoscaling_config,
            _graceful_shutdown_wait_loop_s=_graceful_shutdown_wait_loop_s,
            _graceful_shutdown_timeout_s=_graceful_shutdown_timeout_s,
            _health_check_period_s=_health_check_period_s,
            _health_check_timeout_s=_health_check_timeout_s,
        )

        self._func_or_class = validated._func_or_class
        self._name = validated._name
        self._version = validated._version
        self._prev_version = validated._prev_version
        self._init_args = validated._init_args
        self._init_kwargs = validated._init_kwargs
        self._route_prefix = validated._route_prefix
        self._ray_actor_options = validated._ray_actor_options
        self._config = validated._config

    def __eq__(self, other):
        return all(
            [
                self._name == other._name,
                self._version == other._version,
                self._config == other._config,
                self._init_args == other._init_args,
                self._init_kwargs == other._init_kwargs,
                # compare route prefix with default value resolved
                self.route_prefix == other.route_prefix,
                self._ray_actor_options == self._ray_actor_options,
            ]
        )

    def __str__(self):
        return (
            f"Deployment(name={self._name},"
            f"version={self._version},"
            f"route_prefix={self.route_prefix})"
        )

    def __repr__(self):
        return str(self)


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

    >>> @serve.deployment(name="deployment1", version="v1")
        class MyDeployment:
            pass

    >>> MyDeployment.deploy(*init_args)
    >>> MyDeployment.options(num_replicas=2, init_args=init_args).deploy()

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

    >>> MyDeployment = serve.get_deployment("name")
    >>> MyDeployment.options(num_replicas=10).deploy()

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

    >>> statuses = get_deployment_statuses()
    >>> status_info = statuses["deployment_name"]
    >>> status = status_info.status
    >>> message = status_info.message

    Returns:
            Dict[str, DeploymentStatus]: This dictionary maps the running
                deployment's name to a DeploymentStatus object containing its
                status and a message explaining the status.
    """

    return internal_get_global_client().get_deployment_statuses()


class ImmutableDeploymentDict(dict):
    def __init__(self, deployments: Dict[str, Deployment]):
        super().__init__()
        self.update(deployments)

    def __setitem__(self, *args):
        """Not allowed. Modify deployment options using set_options instead."""
        raise RuntimeError(
            "Setting deployments in a built app is not allowed. Modify the "
            'options using app.deployments["deployment"].set_options instead.'
        )


class Application:
    """A static, pre-built Serve application.

    An application consists of a number of Serve deployments that can send
    requests to each other. One of the deployments acts as the "ingress,"
    meaning that it receives external traffic and is the entrypoint to the
    application.

    The ingress deployment can be accessed via app.ingress and a dictionary of
    all deployments can be accessed via app.deployments.

    The config options of each deployment can be modified using set_options:
    app.deployments["name"].set_options(...).

    This application object can be written to a config file and later deployed
    to production using the Serve CLI or REST API.
    """

    def __init__(self, deployments: List[Deployment]):
        deployment_dict = {}
        for d in deployments:
            if not isinstance(d, Deployment):
                raise TypeError(f"Got {type(d)}. Expected deployment.")
            elif d.name in deployment_dict:
                raise ValueError(f"App got multiple deployments named '{d.name}'.")

            deployment_dict[d.name] = d

        self._deployments = ImmutableDeploymentDict(deployment_dict)

    @property
    def deployments(self) -> ImmutableDeploymentDict:
        return self._deployments

    @property
    def ingress(self) -> Optional[Deployment]:
        """Gets the app's ingress, if one exists.

        The ingress is the single deployment with a non-None route prefix. If more
        or less than one deployment has a route prefix, no single ingress exists,
        so returns None.
        """

        ingress = None

        for deployment in self._deployments.values():
            if deployment.route_prefix is not None:
                if ingress is None:
                    ingress = deployment
                else:
                    return None

        return ingress

    def to_dict(self) -> Dict:
        """Returns this Application's deployments as a dictionary.

        This dictionary adheres to the Serve REST API schema. It can be deployed
        via the Serve REST API.

        Returns:
            Dict: The Application's deployments formatted in a dictionary.
        """
        return ServeApplicationSchema(
            deployments=[deployment_to_schema(d) for d in self._deployments.values()]
        ).dict()

    @classmethod
    def from_dict(cls, d: Dict) -> "Application":
        """Converts a dictionary of deployment data to an application.

        Takes in a dictionary matching the Serve REST API schema and converts
        it to an application containing those deployments.

        Args:
            d (Dict): A dictionary containing the deployments' data that matches
                the Serve REST API schema.

        Returns:
            Application: a new application object containing the deployments.
        """

        schema = ServeApplicationSchema.parse_obj(d)
        return cls([schema_to_deployment(s) for s in schema.deployments])

    def to_yaml(self, f: Optional[TextIO] = None) -> Optional[str]:
        """Returns this application's deployments as a YAML string.

        Optionally writes the YAML string to a file as well. To write to a
        file, use this pattern:

        with open("file_name.txt", "w") as f:
            app.to_yaml(f=f)

        This file is formatted as a Serve YAML config file. It can be deployed
        via the Serve CLI.

        Args:
            f (Optional[TextIO]): A pointer to the file where the YAML should
                be written.

        Returns:
            Optional[String]: The deployments' YAML string. The output is from
                yaml.safe_dump(). Returned only if no file pointer is passed in.
        """
        return yaml.safe_dump(
            self.to_dict(), stream=f, default_flow_style=False, sort_keys=False
        )

    @classmethod
    def from_yaml(cls, str_or_file: Union[str, TextIO]) -> "Application":
        """Converts YAML data to deployments for an application.

        Takes in a string or a file pointer to a file containing deployment
        definitions in YAML. These definitions are converted to a new
        application object containing the deployments.

        To read from a file, use the following pattern:

        with open("file_name.txt", "w") as f:
            app = app.from_yaml(str_or_file)

        Args:
            str_or_file (Union[String, TextIO]): Either a string containing
                YAML deployment definitions or a pointer to a file containing
                YAML deployment definitions. The YAML format must adhere to the
                ServeApplicationSchema JSON Schema defined in
                ray.serve.schema. This function works with
                Serve YAML config files.

        Returns:
            Application: a new Application object containing the deployments.
        """
        return cls.from_dict(yaml.safe_load(str_or_file))


@PublicAPI(stability="alpha")
def run(
    target: Union[DeploymentNode, DeploymentFunctionNode, Application],
    _blocking: bool = True,
    *,
    host: str = DEFAULT_HTTP_HOST,
    port: int = DEFAULT_HTTP_PORT,
) -> RayServeHandle:
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


@PublicAPI(stability="alpha")
def build(target: DeploymentNode) -> Application:
    """Builds a Serve application into a static application.

    Takes in a DeploymentNode and converts it to a Serve application
    consisting of one or more deployments. This is intended to be used for
    production scenarios and deployed via the Serve REST API or CLI, so there
    are some restrictions placed on the deployments:
        1) All of the deployments must be importable. That is, they cannot be
           defined in __main__ or inline defined. The deployments will be
           imported in production using the same import path they were here.
        2) All arguments bound to the deployment must be JSON-serializable.

    The returned Application object can be exported to a dictionary or YAML
    config.
    """
    # TODO(edoakes): this should accept host and port, but we don't
    # currently support them in the REST API.
    raise NotImplementedError()


def deployment_to_schema(d: Deployment) -> DeploymentSchema:
    """Converts a live deployment object to a corresponding structured schema.

    If the deployment has a class or function, it will be attemptetd to be
    converted to a valid corresponding import path.

    init_args and init_kwargs must also be JSON-serializable or this call will
    fail.
    """

    if d.ray_actor_options is not None:
        ray_actor_options_schema = RayActorOptionsSchema.parse_obj(d.ray_actor_options)
    else:
        ray_actor_options_schema = None

    return DeploymentSchema(
        name=d.name,
        import_path=get_deployment_import_path(d),
        init_args=d.init_args,
        init_kwargs=d.init_kwargs,
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
        init_args=convert_from_json_safe_obj(s.init_args),
        init_kwargs=convert_from_json_safe_obj(s.init_kwargs),
        num_replicas=s.num_replicas,
        route_prefix=s.route_prefix,
        max_concurrent_queries=s.max_concurrent_queries,
        user_config=convert_from_json_safe_obj(s.user_config),
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
    schemas = [deployment_to_schema(d) for d in deployments]
    return ServeApplicationSchema(deployments=schemas)


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

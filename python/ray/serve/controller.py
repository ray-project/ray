import asyncio
import logging
import os
import pickle
import time
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import ray
from ray.util import metrics
from ray._private.utils import run_background_task
from ray.actor import ActorHandle
from ray._private.resource_spec import HEAD_NODE_RESOURCE_NAME
from ray._raylet import GcsClient
from ray.serve._private.common import (
    DeploymentID,
    DeploymentInfo,
    EndpointInfo,
    EndpointTag,
    NodeId,
    RunningReplicaInfo,
    StatusOverview,
    ServeDeployMode,
    MultiplexedReplicaInfo,
)
from ray.serve.config import gRPCOptions, HTTPOptions
from ray.serve._private.constants import (
    CONTROL_LOOP_PERIOD_S,
    SERVE_LOGGER_NAME,
    CONTROLLER_MAX_CONCURRENCY,
    SERVE_ROOT_URL_ENV_KEY,
    SERVE_NAMESPACE,
    RECOVERING_LONG_POLL_BROADCAST_TIMEOUT_S,
    SERVE_DEFAULT_APP_NAME,
    MULTI_APP_MIGRATION_MESSAGE,
    RAY_SERVE_CONTROLLER_CALLBACK_IMPORT_PATH,
)
from ray.serve._private.deploy_utils import deploy_args_to_deployment_info
from ray.serve._private.deployment_state import DeploymentStateManager
from ray.serve._private.endpoint_state import EndpointState
from ray.serve._private.http_state import HTTPProxyStateManager
from ray.serve._private.logging_utils import (
    configure_component_logger,
    configure_component_memory_profiler,
    configure_component_cpu_profiler,
    get_component_logger_file_path,
)
from ray.serve._private.long_poll import LongPollHost
from ray.serve.exceptions import RayServeException
from ray.serve.schema import (
    ServeApplicationSchema,
    ServeDeploySchema,
    ApplicationDetails,
    ServeInstanceDetails,
    gRPCOptionsSchema,
    HTTPOptionsSchema,
    ServeActorDetails,
)
from ray.serve._private.storage.kv_store import RayInternalKVStore
from ray.serve._private.usage import ServeUsageTag
from ray.serve._private.utils import (
    DEFAULT,
    call_function_from_import_path,
    get_all_live_placement_group_names,
    get_head_node_id,
)
from ray.serve._private.application_state import ApplicationStateManager
from ray.serve._private.default_impl import (
    create_cluster_node_info_cache,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)

# Used for testing purposes only. If this is set, the controller will crash
# after writing each checkpoint with the specified probability.
_CRASH_AFTER_CHECKPOINT_PROBABILITY = 0

CONFIG_CHECKPOINT_KEY = "serve-app-config-checkpoint"


@ray.remote(num_cpus=0)
class ServeController:
    """Responsible for managing the state of the serving system.

    The controller implements fault tolerance by persisting its state in
    a new checkpoint each time a state change is made. If the actor crashes,
    the latest checkpoint is loaded and the state is recovered. Checkpoints
    are written/read using a provided KV-store interface.

    All hard state in the system is maintained by this actor and persisted via
    these checkpoints. Soft state required by other components is fetched by
    those actors from this actor on startup and updates are pushed out from
    this actor.

    All other actors started by the controller are named, detached actors
    so they will not fate share with the controller if it crashes.

    The following guarantees are provided for state-changing calls to the
    controller:
        - If the call succeeds, the change was made and will be reflected in
          the system even if the controller or other actors die unexpectedly.
        - If the call fails, the change may have been made but isn't guaranteed
          to have been. The client should retry in this case. Note that this
          requires all implementations here to be idempotent.
    """

    async def __init__(
        self,
        controller_name: str,
        *,
        http_config: HTTPOptions,
        detached: bool = False,
        _disable_http_proxy: bool = False,
        grpc_options: Optional[gRPCOptions] = None,
    ):
        self._controller_node_id = ray.get_runtime_context().get_node_id()
        assert (
            self._controller_node_id == get_head_node_id()
        ), "Controller must be on the head node."

        configure_component_logger(
            component_name="controller", component_id=str(os.getpid())
        )
        configure_component_memory_profiler(
            component_name="controller", component_id=str(os.getpid())
        )
        self.cpu_profiler, self.cpu_profiler_log = configure_component_cpu_profiler(
            component_name="controller", component_id=str(os.getpid())
        )
        if RAY_SERVE_CONTROLLER_CALLBACK_IMPORT_PATH:
            logger.info(
                "Calling user-provided callback from import path "
                f"{RAY_SERVE_CONTROLLER_CALLBACK_IMPORT_PATH}."
            )
            call_function_from_import_path(RAY_SERVE_CONTROLLER_CALLBACK_IMPORT_PATH)

        # Used to read/write checkpoints.
        self.ray_worker_namespace = ray.get_runtime_context().namespace
        self.controller_name = controller_name
        self.gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
        kv_store_namespace = f"{self.controller_name}-{self.ray_worker_namespace}"
        self.kv_store = RayInternalKVStore(kv_store_namespace, self.gcs_client)
        self.cluster_node_info_cache = create_cluster_node_info_cache(self.gcs_client)
        self.cluster_node_info_cache.update()

        # Dictionary of deployment_name -> proxy_name -> queue length.
        self.deployment_stats = defaultdict(lambda: defaultdict(dict))

        self.long_poll_host = LongPollHost()
        self.done_recovering_event = asyncio.Event()

        if _disable_http_proxy:
            self.http_proxy_state_manager = None
        else:
            self.http_proxy_state_manager = HTTPProxyStateManager(
                controller_name,
                detached,
                http_config,
                self._controller_node_id,
                self.cluster_node_info_cache,
                grpc_options,
            )

        self.endpoint_state = EndpointState(self.kv_store, self.long_poll_host)

        # Fetch all running actors in current cluster as source of current
        # replica state for controller failure recovery
        all_current_actors = ray.util.list_named_actors(all_namespaces=True)
        all_serve_actor_names = [
            actor["name"]
            for actor in all_current_actors
            if actor["namespace"] == SERVE_NAMESPACE
        ]

        self.deployment_state_manager = DeploymentStateManager(
            controller_name,
            detached,
            self.kv_store,
            self.long_poll_host,
            all_serve_actor_names,
            get_all_live_placement_group_names(),
            self.cluster_node_info_cache,
        )

        # Manage all applications' state
        self.application_state_manager = ApplicationStateManager(
            self.deployment_state_manager, self.endpoint_state, self.kv_store
        )

        # Keep track of single-app vs multi-app
        self.deploy_mode = ServeDeployMode.UNSET
        # Controller actor details
        self._actor_details = ServeActorDetails(
            node_id=ray.get_runtime_context().get_node_id(),
            node_ip=ray.util.get_node_ip_address(),
            actor_id=ray.get_runtime_context().get_actor_id(),
            actor_name=self.controller_name,
            worker_id=ray.get_runtime_context().get_worker_id(),
            log_file_path=get_component_logger_file_path(),
        )
        self._shutting_down = False
        self._shutdown_event = asyncio.Event()
        self._shutdown_start_time = None

        self._create_control_loop_metrics()
        run_background_task(self.run_control_loop())

        self._recover_config_from_checkpoint()
        # Nodes where http proxy actors should run.
        self._http_proxy_nodes = set()
        self._update_http_proxy_nodes()

        # Track the number of times the controller has started
        metrics.Counter(
            "serve_controller_num_starts",
            description="The number of times that controller has started.",
        ).inc()

    def check_alive(self) -> None:
        """No-op to check if this controller is alive."""
        return

    def get_pid(self) -> int:
        return os.getpid()

    def record_autoscaling_metrics(self, data: Dict[str, float], send_timestamp: float):
        logger.debug(
            f"Received autoscaling metrics: {data} at timestamp {send_timestamp}"
        )
        self.deployment_state_manager.record_autoscaling_metrics(data, send_timestamp)

    def record_handle_metrics(self, data: Dict[str, float], send_timestamp: float):
        self.deployment_state_manager.record_handle_metrics(data, send_timestamp)

    def _dump_autoscaling_metrics_for_testing(self):
        return self.deployment_state_manager.get_autoscaling_metrics()

    def _dump_replica_states_for_testing(self, deployment_id: DeploymentID):
        return self.deployment_state_manager._deployment_states[deployment_id]._replicas

    def _stop_one_running_replica_for_testing(self, deployment_name):
        self.deployment_state_manager._deployment_states[
            deployment_name
        ]._stop_one_running_replica_for_testing()

    async def listen_for_change(self, keys_to_snapshot_ids: Dict[str, int]):
        """Proxy long pull client's listen request.

        Args:
            keys_to_snapshot_ids (Dict[str, int]): Snapshot IDs are used to
              determine whether or not the host should immediately return the
              data or wait for the value to be changed.
        """
        if not self.done_recovering_event.is_set():
            await self.done_recovering_event.wait()

        return await (self.long_poll_host.listen_for_change(keys_to_snapshot_ids))

    async def listen_for_change_java(self, keys_to_snapshot_ids_bytes: bytes):
        """Proxy long pull client's listen request.

        Args:
            keys_to_snapshot_ids_bytes (Dict[str, int]): the protobuf bytes of
              keys_to_snapshot_ids (Dict[str, int]).
        """
        if not self.done_recovering_event.is_set():
            await self.done_recovering_event.wait()

        return await (
            self.long_poll_host.listen_for_change_java(keys_to_snapshot_ids_bytes)
        )

    def get_all_endpoints(self) -> Dict[EndpointTag, Dict[str, Any]]:
        """Returns a dictionary of deployment name to config."""
        return self.endpoint_state.get_endpoints()

    def get_all_endpoints_java(self) -> bytes:
        """Returns a dictionary of deployment name to config."""
        from ray.serve.generated.serve_pb2 import (
            EndpointSet,
            EndpointInfo as EndpointInfoProto,
        )

        endpoints = self.get_all_endpoints()
        # NOTE(zcin): Java only supports 1.x deployments, so only return
        # a dictionary of deployment name -> endpoint info
        data = {
            endpoint_tag.name: EndpointInfoProto(route=endppint_dict["route"])
            for endpoint_tag, endppint_dict in endpoints.items()
        }
        return EndpointSet(endpoints=data).SerializeToString()

    def get_http_proxies(self) -> Dict[NodeId, ActorHandle]:
        """Returns a dictionary of node ID to http_proxy actor handles."""
        if self.http_proxy_state_manager is None:
            return {}
        return self.http_proxy_state_manager.get_http_proxy_handles()

    def get_http_proxy_names(self) -> bytes:
        """Returns the http_proxy actor name list serialized by protobuf."""
        if self.http_proxy_state_manager is None:
            return None

        from ray.serve.generated.serve_pb2 import ActorNameList

        actor_name_list = ActorNameList(
            names=self.http_proxy_state_manager.get_http_proxy_names().values()
        )
        return actor_name_list.SerializeToString()

    def _update_http_proxy_nodes(self):
        """Update the nodes set where http proxy actors should run.

        Controller decides where http proxy actors should run
        (head node and nodes with deployment replicas).
        """
        new_http_proxy_nodes = self.deployment_state_manager.get_active_node_ids()
        new_http_proxy_nodes = (
            new_http_proxy_nodes - self.cluster_node_info_cache.get_draining_node_ids()
        )
        new_http_proxy_nodes.add(self._controller_node_id)
        self._http_proxy_nodes = new_http_proxy_nodes

    async def run_control_loop(self) -> None:
        # NOTE(edoakes): we catch all exceptions here and simply log them,
        # because an unhandled exception would cause the main control loop to
        # halt, which should *never* happen.
        recovering_timeout = RECOVERING_LONG_POLL_BROADCAST_TIMEOUT_S
        num_loops = 0
        start_time = time.time()
        while True:
            loop_start_time = time.time()

            try:
                self.cluster_node_info_cache.update()
            except Exception:
                logger.exception("Exception updating cluster node info cache.")

            if self._shutting_down:
                try:
                    self.shutdown()
                except Exception:
                    logger.exception("Exception during shutdown.")

            if (
                not self.done_recovering_event.is_set()
                and time.time() - start_time > recovering_timeout
            ):
                logger.warning(
                    f"Replicas still recovering after {recovering_timeout}s, "
                    "setting done recovering event to broadcast long poll updates."
                )
                self.done_recovering_event.set()

            try:
                dsm_update_start_time = time.time()
                any_recovering = self.deployment_state_manager.update()
                self.dsm_update_duration_gauge_s.set(
                    time.time() - dsm_update_start_time
                )
                if not self.done_recovering_event.is_set() and not any_recovering:
                    self.done_recovering_event.set()
                    logger.info(
                        "Finished recovering deployments after "
                        f"{(time.time() - start_time):.2f}s.",
                        extra={"log_to_stderr": False},
                    )
            except Exception:
                logger.exception("Exception updating deployment state.")

            try:
                asm_update_start_time = time.time()
                self.application_state_manager.update()
                self.asm_update_duration_gauge_s.set(
                    time.time() - asm_update_start_time
                )
            except Exception:
                logger.exception("Exception updating application state.")

            # Update the http proxy nodes set before updating the HTTP proxy states,
            # so they are more consistent.
            node_update_start_time = time.time()
            self._update_http_proxy_nodes()
            self.node_update_duration_gauge_s.set(time.time() - node_update_start_time)

            # Don't update http_state until after the done recovering event is set,
            # otherwise we may start a new HTTP proxy but not broadcast it any
            # info about available deployments & their replicas.
            if self.http_proxy_state_manager and self.done_recovering_event.is_set():
                try:
                    proxy_update_start_time = time.time()
                    self.http_proxy_state_manager.update(
                        http_proxy_nodes=self._http_proxy_nodes
                    )
                    self.proxy_update_duration_gauge_s.set(
                        time.time() - proxy_update_start_time
                    )
                except Exception:
                    logger.exception("Exception updating HTTP state.")

            loop_duration = time.time() - loop_start_time
            if loop_duration > 10:
                logger.warning(
                    f"The last control loop was slow (took {loop_duration}s). "
                    "This is likely caused by running a large number of "
                    "replicas in a single Ray cluster. Consider using "
                    "multiple Ray clusters.",
                    extra={"log_to_stderr": False},
                )
            self.control_loop_duration_gauge_s.set(loop_duration)

            num_loops += 1
            self.num_control_loops_gauge.set(num_loops)

            sleep_start_time = time.time()
            await asyncio.sleep(CONTROL_LOOP_PERIOD_S)
            self.sleep_duration_gauge_s.set(time.time() - sleep_start_time)

    def _create_control_loop_metrics(self):
        self.node_update_duration_gauge_s = metrics.Gauge(
            "serve_controller_node_update_duration_s",
            description="The control loop time spent on collecting proxy node info.",
        )
        self.proxy_update_duration_gauge_s = metrics.Gauge(
            "serve_controller_proxy_state_update_duration_s",
            description="The control loop time spent on updating proxy state.",
        )
        self.dsm_update_duration_gauge_s = metrics.Gauge(
            "serve_controller_deployment_state_update_duration_s",
            description="The control loop time spent on updating deployment state.",
        )
        self.asm_update_duration_gauge_s = metrics.Gauge(
            "serve_controller_application_state_update_duration_s",
            description="The control loop time spent on updating application state.",
        )
        self.sleep_duration_gauge_s = metrics.Gauge(
            "serve_controller_sleep_duration_s",
            description="The duration of the last control loop's sleep.",
        )
        self.control_loop_duration_gauge_s = metrics.Gauge(
            "serve_controller_control_loop_duration_s",
            description="The duration of the last control loop.",
        )
        self.num_control_loops_gauge = metrics.Gauge(
            "serve_controller_num_control_loops",
            description=(
                "The number of control loops performed by the controller. "
                "Increases monotonically over the controller's lifetime."
            ),
            tag_keys=("actor_id",),
        )
        self.num_control_loops_gauge.set_default_tags(
            {"actor_id": ray.get_runtime_context().get_actor_id()}
        )

    def _recover_config_from_checkpoint(self):
        checkpoint = self.kv_store.get(CONFIG_CHECKPOINT_KEY)
        if checkpoint is not None:
            logger.info(
                "Recovering config from checkpoint.", extra={"log_to_stderr": False}
            )
            deployment_time, deploy_mode, config_checkpoints_dict = pickle.loads(
                checkpoint
            )
            applications = list(config_checkpoints_dict.values())
            if deploy_mode == ServeDeployMode.SINGLE_APP:
                self.deploy_config(
                    ServeApplicationSchema.parse_obj(applications[0]),
                    deployment_time,
                )
            else:
                self.deploy_config(
                    ServeDeploySchema.parse_obj({"applications": applications}),
                    deployment_time,
                )

    def _all_running_replicas(self) -> Dict[DeploymentID, List[RunningReplicaInfo]]:
        """Used for testing.

        Returned dictionary maps deployment names to replica infos.
        """

        return self.deployment_state_manager.get_running_replica_infos()

    def get_http_config(self) -> HTTPOptions:
        """Return the HTTP proxy configuration."""
        if self.http_proxy_state_manager is None:
            return HTTPOptions()
        return self.http_proxy_state_manager.get_config()

    def get_grpc_config(self) -> gRPCOptions:
        """Return the gRPC proxy configuration."""
        if self.http_proxy_state_manager is None:
            return gRPCOptions()
        return self.http_proxy_state_manager.get_grpc_config()

    def get_root_url(self):
        """Return the root url for the serve instance."""
        if self.http_proxy_state_manager is None:
            return None
        http_config = self.get_http_config()
        if http_config.root_url == "":
            if SERVE_ROOT_URL_ENV_KEY in os.environ:
                return os.environ[SERVE_ROOT_URL_ENV_KEY]
            else:
                return (
                    f"http://{http_config.host}:{http_config.port}"
                    f"{http_config.root_path}"
                )
        return http_config.root_url

    def config_checkpoint_deleted(self) -> bool:
        """Returns whether the config checkpoint has been deleted.

        Get the config checkpoint from the kv store. If it is None, then it has been
        deleted.
        """
        return self.kv_store.get(CONFIG_CHECKPOINT_KEY) is None

    def shutdown(self):
        """Shuts down the serve instance completely.

        This method will only be triggered when `self._shutting_down` is true. It
        deletes the kv store for config checkpoints, sets application state to deleting,
        delete all deployments, and shuts down all HTTP proxies. Once all these
        resources are released, it then kills the controller actor.
        """
        if not self._shutting_down:
            return

        if self._shutdown_start_time is None:
            self._shutdown_start_time = time.time()

        logger.info("Controller shutdown started!", extra={"log_to_stderr": False})
        self.kv_store.delete(CONFIG_CHECKPOINT_KEY)
        self.application_state_manager.shutdown()
        self.deployment_state_manager.shutdown()
        self.endpoint_state.shutdown()
        if self.http_proxy_state_manager:
            self.http_proxy_state_manager.shutdown()

        config_checkpoint_deleted = self.config_checkpoint_deleted()
        application_is_shutdown = self.application_state_manager.is_ready_for_shutdown()
        deployment_is_shutdown = self.deployment_state_manager.is_ready_for_shutdown()
        endpoint_is_shutdown = self.endpoint_state.is_ready_for_shutdown()
        http_state_is_shutdown = (
            self.http_proxy_state_manager is None
            or self.http_proxy_state_manager.is_ready_for_shutdown()
        )
        if (
            config_checkpoint_deleted
            and application_is_shutdown
            and deployment_is_shutdown
            and endpoint_is_shutdown
            and http_state_is_shutdown
        ):
            logger.warning(
                "All resources have shut down, shutting down controller!",
                extra={"log_to_stderr": False},
            )
            _controller_actor = ray.get_runtime_context().current_actor
            ray.kill(_controller_actor, no_restart=True)
        elif time.time() - self._shutdown_start_time > 10:
            if not config_checkpoint_deleted:
                logger.warning(
                    f"{CONFIG_CHECKPOINT_KEY} not yet deleted",
                    extra={"log_to_stderr": False},
                )
            if not application_is_shutdown:
                logger.warning(
                    "application not yet shutdown",
                    extra={"log_to_stderr": False},
                )
            if not deployment_is_shutdown:
                logger.warning(
                    "deployment not yet shutdown",
                    extra={"log_to_stderr": False},
                )
            if not endpoint_is_shutdown:
                logger.warning(
                    "endpoint not yet shutdown",
                    extra={"log_to_stderr": False},
                )
            if not http_state_is_shutdown:
                logger.warning(
                    "http_state not yet shutdown",
                    extra={"log_to_stderr": False},
                )

    def deploy(
        self,
        name: str,
        deployment_config_proto_bytes: bytes,
        replica_config_proto_bytes: bytes,
        route_prefix: Optional[str],
        deployer_job_id: Union[str, bytes],
        docs_path: Optional[str] = None,
        is_driver_deployment: Optional[bool] = False,
        # TODO(edoakes): this is a hack because the deployment_language doesn't seem
        # to get set properly from Java.
        is_deployed_from_python: bool = False,
    ) -> bool:
        """Deploys a deployment. This should only be used for 1.x deployments."""
        if route_prefix is not None:
            assert route_prefix.startswith("/")
        if docs_path is not None:
            assert docs_path.startswith("/")

        deployment_info = deploy_args_to_deployment_info(
            deployment_name=name,
            deployment_config_proto_bytes=deployment_config_proto_bytes,
            replica_config_proto_bytes=replica_config_proto_bytes,
            deployer_job_id=deployer_job_id,
            route_prefix=route_prefix,
            docs_path=docs_path,
            is_driver_deployment=is_driver_deployment,
            app_name="",
        )

        # TODO(architkulkarni): When a deployment is redeployed, even if
        # the only change was num_replicas, the start_time_ms is refreshed.
        # Is this the desired behaviour?
        updating = self.deployment_state_manager.deploy(
            DeploymentID(name, ""), deployment_info
        )

        if route_prefix is not None:
            endpoint_info = EndpointInfo(
                route=route_prefix,
                app_is_cross_language=not is_deployed_from_python,
            )
            self.endpoint_state.update_endpoint(EndpointTag(name, ""), endpoint_info)
        else:
            self.endpoint_state.delete_endpoint(EndpointTag(name, ""))

        return updating

    def deploy_application(self, name: str, deployment_args_list: List[Dict]) -> None:
        """
        Takes in a list of dictionaries that contain deployment arguments.
        If same app name deployed, old application will be overwrriten.

        Args:
            name: Application name.
            deployment_args_list: List of deployment infomation, each item in the list
                contains all the information for the single deployment.
        """

        self.application_state_manager.apply_deployment_args(name, deployment_args_list)

    def deploy_config(
        self,
        config: Union[ServeApplicationSchema, ServeDeploySchema],
        deployment_time: float = 0,
    ) -> None:
        """Kicks off a task that deploys a set of Serve applications.

        Cancels in-progress tasks that are deploying Serve applications with the same
        name as newly deployed applications.

        Args:
            config:
                [if ServeApplicationSchema]
                    name: Application name. If not provided, it is empty string.
                    import_path: Serve deployment graph's import path
                    runtime_env: runtime_env to run the deployment graph in
                    deployments: Dictionaries that contain argument-value options
                        that can be passed directly into a set_options() call. Overrides
                        deployment options set in the graph's code itself.
                [if ServeDeploySchema]
                    applications: Dictionaries of the format ServeApplicationSchema.

            deployment_time: set deployment_timestamp. If not provided, time.time() is
                used to indicate the deployment time.
        """
        ServeUsageTag.API_VERSION.record("v2")
        # TODO (zcin): We should still support single-app mode, i.e.
        # ServeApplicationSchema. Eventually, after migration is complete, we should
        # deprecate such usage.
        if isinstance(config, ServeApplicationSchema):
            if "name" in config.dict(exclude_unset=True):
                error_msg = (
                    "Specifying the name of an application is only allowed for apps "
                    "that are listed as part of a multi-app config file. "
                ) + MULTI_APP_MIGRATION_MESSAGE
                logger.warning(error_msg)
                raise RayServeException(error_msg)

            applications = [config]
            if self.deploy_mode == ServeDeployMode.MULTI_APP:
                raise RayServeException(
                    "You are trying to deploy a single-application config, however "
                    "a multi-application config has been deployed to the current "
                    "Serve instance already. Mixing single-app and multi-app is not "
                    "allowed. Please either redeploy using the multi-application "
                    "config format `ServeDeploySchema`, or shutdown and restart Serve "
                    "to submit a single-app config of format `ServeApplicationSchema`. "
                    "If you are using the REST API, you can submit a single-app config "
                    "to the single-app API endpoint `/api/serve/deployments/`."
                )
            self.deploy_mode = ServeDeployMode.SINGLE_APP
        else:
            applications = config.applications
            if self.deploy_mode == ServeDeployMode.SINGLE_APP:
                raise RayServeException(
                    "You are trying to deploy a multi-application config, however "
                    "a single-application config has been deployed to the current "
                    "Serve instance already. Mixing single-app and multi-app is not "
                    "allowed. Please either redeploy using the single-application "
                    "config format `ServeApplicationSchema`, or shutdown and restart "
                    "Serve to submit a multi-app config of format `ServeDeploySchema`. "
                    "If you are using the REST API, you can submit a multi-app config "
                    "to the the multi-app API endpoint `/api/serve/applications/`."
                )
            self.deploy_mode = ServeDeployMode.MULTI_APP

        if not deployment_time:
            deployment_time = time.time()

        new_config_checkpoint = {}

        for app_config in applications:
            for deployments in app_config.deployments:
                if deployments.route_prefix != DEFAULT.VALUE:
                    logger.warning(
                        "Specifying route prefix for a deployment is deprecated. "
                        "Please specify route prefix at an application level in the "
                        "Serve config instead."
                    )

            app_config_dict = app_config.dict(exclude_unset=True)
            new_config_checkpoint[app_config.name] = app_config_dict

            self.application_state_manager.deploy_config(
                app_config.name,
                app_config,
                deployment_time=deployment_time,
            )

        self.kv_store.put(
            CONFIG_CHECKPOINT_KEY,
            pickle.dumps((deployment_time, self.deploy_mode, new_config_checkpoint)),
        )

        # Delete live applications not listed in config
        existing_applications = set(
            self.application_state_manager._application_states.keys()
        )
        new_applications = {app_config.name for app_config in applications}
        self.delete_apps(existing_applications.difference(new_applications))

    def delete_deployment(self, name: str):
        """Should only be used for 1.x deployments."""

        id = DeploymentID(name, "")
        self.endpoint_state.delete_endpoint(id)
        return self.deployment_state_manager.delete_deployment(id)

    def delete_deployments(self, names: Iterable[str]) -> None:
        """Should only be used for 1.x deployments."""

        for name in names:
            self.delete_deployment(name)

    def get_deployment_info(self, name: str, app_name: str = "") -> bytes:
        """Get the current information about a deployment.

        Args:
            name: the name of the deployment.

        Returns:
            DeploymentRoute's protobuf serialized bytes

        Raises:
            KeyError if the deployment doesn't exist.
        """
        id = DeploymentID(name, app_name)
        deployment_info = self.deployment_state_manager.get_deployment(id)
        if deployment_info is None:
            app_msg = f" in application '{app_name}'" if app_name else ""
            raise KeyError(f"Deployment '{name}' does not exist{app_msg}.")

        route = self.endpoint_state.get_endpoint_route(id)

        from ray.serve.generated.serve_pb2 import DeploymentRoute

        deployment_route = DeploymentRoute(
            deployment_info=deployment_info.to_proto(), route=route
        )
        return deployment_route.SerializeToString()

    def list_deployments_internal(
        self, include_deleted: Optional[bool] = False
    ) -> Dict[DeploymentID, Tuple[DeploymentInfo, str]]:
        """Gets the current information about all deployments.

        Args:
            include_deleted: Whether to include information about
                deployments that have been deleted.

        Returns:
            Dict(deployment_id, (DeploymentInfo, route))
        """
        return {
            id: (info, self.endpoint_state.get_endpoint_route(id))
            for id, info in self.deployment_state_manager.get_deployment_infos(
                include_deleted=include_deleted
            ).items()
        }

    def list_deployments_v1(self, include_deleted: Optional[bool] = False) -> bytes:
        """Gets the current information about all 1.x deployments.

        Args:
            include_deleted: Whether to include information about
                deployments that have been deleted.

        Returns:
            DeploymentRouteList's protobuf serialized bytes
        """
        from ray.serve.generated.serve_pb2 import DeploymentRoute, DeploymentRouteList

        deployment_route_list = DeploymentRouteList()
        for deployment_id, (
            deployment_info,
            route_prefix,
        ) in self.list_deployments_internal(include_deleted=include_deleted).items():
            # Only list 1.x deployments, which should have app=""
            if deployment_id.app:
                continue

            deployment_info_proto = deployment_info.to_proto()
            deployment_info_proto.name = deployment_id.name
            deployment_route_list.deployment_routes.append(
                DeploymentRoute(
                    deployment_info=deployment_info_proto, route=route_prefix
                )
            )
        return deployment_route_list.SerializeToString()

    def list_deployments(self) -> Dict[DeploymentID, DeploymentInfo]:
        """Gets the current information about all deployments (1.x and 2.x)"""
        return {
            deployment_id: deployment_info
            for deployment_id, (
                deployment_info,
                _,
            ) in self.list_deployments_internal().items()
        }

    def list_deployment_ids(self) -> List[DeploymentID]:
        """Gets the current list of all deployments' identifiers."""
        return self.deployment_state_manager._deployment_states.keys()

    def get_serve_instance_details(self) -> Dict:
        """Gets details on all applications on the cluster and system-level info.

        The information includes application and deployment statuses, config options,
        error messages, etc.

        Returns:
            Dict that follows the format of the schema ServeInstanceDetails. Currently,
            there is a value set for every field at all schema levels, except for the
            route_prefix in the deployment_config for each deployment.
        """

        http_config = self.get_http_config()
        grpc_config = self.get_grpc_config()
        applications = {}

        for (
            app_name,
            app_status_info,
        ) in self.application_state_manager.list_app_statuses().items():
            applications[app_name] = ApplicationDetails(
                name=app_name,
                route_prefix=self.application_state_manager.get_route_prefix(app_name),
                docs_path=self.get_docs_path(app_name),
                status=app_status_info.status,
                message=app_status_info.message,
                last_deployed_time_s=app_status_info.deployment_timestamp,
                # This can be none if the app was deployed through
                # serve.run, or if the app is in deleting state
                deployed_app_config=self.get_app_config(app_name),
                deployments=self.application_state_manager.list_deployment_details(
                    app_name
                ),
            )

        # NOTE(zcin): We use exclude_unset here because we explicitly and intentionally
        # fill in all info that should be shown to users. Currently, every field is set
        # except for the route_prefix in the deployment_config of each deployment, since
        # route_prefix is set instead in each application.
        # Eventually we want to remove route_prefix from DeploymentSchema.
        http_options = HTTPOptionsSchema.parse_obj(http_config.dict(exclude_unset=True))
        grpc_options = gRPCOptionsSchema.parse_obj(grpc_config.dict(exclude_unset=True))
        return ServeInstanceDetails(
            controller_info=self._actor_details,
            proxy_location=http_config.location,
            http_options=http_options,
            grpc_options=grpc_options,
            proxies=self.http_proxy_state_manager.get_proxy_details()
            if self.http_proxy_state_manager
            else None,
            deploy_mode=self.deploy_mode,
            applications=applications,
        ).dict(exclude_unset=True)

    def get_serve_status(self, name: str = SERVE_DEFAULT_APP_NAME) -> bytes:
        """Return application status
        Args:
            name: application name. If application name doesn't exist, app_status
            is NOT_STARTED.
        """

        app_status = self.application_state_manager.get_app_status_info(name)
        deployment_statuses = self.application_state_manager.get_deployments_statuses(
            name
        )
        status_info = StatusOverview(
            name=name,
            app_status=app_status,
            deployment_statuses=deployment_statuses,
        )
        return status_info.to_proto().SerializeToString()

    def get_serve_statuses(self, names: List[str]) -> List[bytes]:
        statuses = []
        for name in names:
            statuses.append(self.get_serve_status(name))
        return statuses

    def list_serve_statuses(self) -> List[bytes]:
        statuses = []
        for name in self.application_state_manager.list_app_statuses():
            statuses.append(self.get_serve_status(name))
        return statuses

    def get_app_config(self, name: str = SERVE_DEFAULT_APP_NAME) -> Optional[Dict]:
        checkpoint = self.kv_store.get(CONFIG_CHECKPOINT_KEY)
        if checkpoint is not None:
            _, _, config_checkpoints_dict = pickle.loads(checkpoint)
            if name in config_checkpoints_dict:
                config = config_checkpoints_dict[name]
                return ServeApplicationSchema.parse_obj(config).dict(exclude_unset=True)

    def get_all_deployment_statuses(self) -> List[bytes]:
        """Gets deployment status bytes for all live deployments."""
        statuses = self.deployment_state_manager.get_deployment_statuses()
        return [status.to_proto().SerializeToString() for status in statuses]

    def get_deployment_status(
        self, name: str, app_name: str = ""
    ) -> Union[None, bytes]:
        """Get deployment status by deployment name.

        Args:
            name: Deployment name.
            app_name: Application name. Default is "" because 1.x
                deployments go through this API.
        """

        id = DeploymentID(name, app_name)
        status = self.deployment_state_manager.get_deployment_statuses([id])
        if not status:
            return None
        return status[0].to_proto().SerializeToString()

    def get_docs_path(self, name: str):
        """Docs path for application.

        Currently, this is the OpenAPI docs path for FastAPI-integrated applications."""
        return self.application_state_manager.get_docs_path(name)

    def get_ingress_deployment_name(self, app_name: str) -> Optional[str]:
        """Name of the ingress deployment in an application.

        Returns:
            Ingress deployment name (str): if the application exists.
            None: if the application does not exist.
        """
        return self.application_state_manager.get_ingress_deployment_name(app_name)

    def delete_apps(self, names: Iterable[str]):
        """Delete applications based on names

        During deletion, the application status is DELETING
        """
        for name in names:
            self.application_state_manager.delete_application(name)

    def record_multiplexed_replica_info(self, info: MultiplexedReplicaInfo):
        """Record multiplexed model ids for a replica of deployment
        Args:
            info: MultiplexedReplicaInfo including deployment name, replica tag and
                model ids.
        """
        self.deployment_state_manager.record_multiplexed_replica_info(info)

    async def graceful_shutdown(self, wait: bool = True):
        """Set the shutting down flag on controller to signal shutdown in
        run_control_loop().

        This is used to signal to the controller that it should proceed with shutdown
        process, so it can shut down gracefully. It also waits until the shutdown
        event is triggered if wait is true.

        Raises:
            RayActorError: if wait is True, the caller waits until the controller
                is killed, which raises a RayActorError.
        """
        self._shutting_down = True
        if not wait:
            return

        # This event never gets set. The caller waits indefinitely on this event
        # until the controller is killed, which raises a RayActorError.
        await self._shutdown_event.wait()

    def _save_cpu_profile_data(self) -> str:
        """Saves CPU profiling data, if CPU profiling is enabled.

        Logs a warning if CPU profiling is disabled.
        """

        if self.cpu_profiler is not None:
            import marshal

            self.cpu_profiler.snapshot_stats()
            with open(self.cpu_profiler_log, "wb") as f:
                marshal.dump(self.cpu_profiler.stats, f)
            logger.info(f'Saved CPU profile data to file "{self.cpu_profiler_log}"')
            return self.cpu_profiler_log
        else:
            logger.error(
                "Attempted to save CPU profile data, but failed because no "
                "CPU profiler was running! Enable CPU profiling by enabling "
                "the RAY_SERVE_ENABLE_CPU_PROFILING env var."
            )


@ray.remote(num_cpus=0)
class ServeControllerAvatar:
    """A hack that proxy the creation of async actors from Java.

    To be removed after https://github.com/ray-project/ray/pull/26037

    Java api can not support python async actor. If we use java api create
    python async actor. The async init method won't be executed. The async
    method will fail with pickle error. And the run_control_loop of controller
    actor can't be executed too. We use this proxy actor create python async
    actor to avoid the above problem.
    """

    def __init__(
        self,
        controller_name: str,
        detached: bool = False,
        dedicated_cpu: bool = False,
        http_proxy_port: int = 8000,
    ):
        try:
            self._controller = ray.get_actor(controller_name, namespace=SERVE_NAMESPACE)
        except ValueError:
            self._controller = None
        if self._controller is None:
            http_config = HTTPOptions()
            http_config.port = http_proxy_port
            self._controller = ServeController.options(
                num_cpus=1 if dedicated_cpu else 0,
                name=controller_name,
                lifetime="detached" if detached else None,
                max_restarts=-1,
                max_task_retries=-1,
                resources={HEAD_NODE_RESOURCE_NAME: 0.001},
                namespace=SERVE_NAMESPACE,
                max_concurrency=CONTROLLER_MAX_CONCURRENCY,
            ).remote(
                controller_name,
                http_config=http_config,
                detached=detached,
            )

    def check_alive(self) -> None:
        """No-op to check if this actor is alive."""
        return

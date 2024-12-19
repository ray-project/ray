import asyncio
import logging
import marshal
import os
import pickle
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import ray
from ray._private.resource_spec import HEAD_NODE_RESOURCE_NAME
from ray._private.utils import run_background_task
from ray._raylet import GcsClient
from ray.actor import ActorHandle
from ray.serve._private.application_state import ApplicationStateManager, StatusOverview
from ray.serve._private.autoscaling_state import AutoscalingStateManager
from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    MultiplexedReplicaInfo,
    NodeId,
    RunningReplicaInfo,
    TargetCapacityDirection,
)
from ray.serve._private.constants import (
    CONTROL_LOOP_INTERVAL_S,
    CONTROLLER_MAX_CONCURRENCY,
    RAY_SERVE_CONTROLLER_CALLBACK_IMPORT_PATH,
    RAY_SERVE_ENABLE_TASK_EVENTS,
    RECOVERING_LONG_POLL_BROADCAST_TIMEOUT_S,
    SERVE_CONTROLLER_NAME,
    SERVE_DEFAULT_APP_NAME,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
    SERVE_ROOT_URL_ENV_KEY,
)
from ray.serve._private.default_impl import create_cluster_node_info_cache
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve._private.deployment_state import DeploymentStateManager
from ray.serve._private.endpoint_state import EndpointState
from ray.serve._private.logging_utils import (
    configure_component_cpu_profiler,
    configure_component_logger,
    configure_component_memory_profiler,
    get_component_logger_file_path,
)
from ray.serve._private.long_poll import LongPollHost, LongPollNamespace
from ray.serve._private.proxy_state import ProxyStateManager
from ray.serve._private.storage.kv_store import RayInternalKVStore
from ray.serve._private.usage import ServeUsageTag
from ray.serve._private.utils import (
    call_function_from_import_path,
    get_all_live_placement_group_names,
    get_head_node_id,
)
from ray.serve.config import HTTPOptions, ProxyLocation, gRPCOptions
from ray.serve.generated.serve_pb2 import ActorNameList, DeploymentArgs, DeploymentRoute
from ray.serve.generated.serve_pb2 import EndpointInfo as EndpointInfoProto
from ray.serve.generated.serve_pb2 import EndpointSet
from ray.serve.schema import (
    ApplicationDetails,
    DeploymentDetails,
    HTTPOptionsSchema,
    LoggingConfig,
    ProxyDetails,
    ServeActorDetails,
    ServeApplicationSchema,
    ServeDeploySchema,
    ServeInstanceDetails,
    gRPCOptionsSchema,
)
from ray.util import metrics

logger = logging.getLogger(SERVE_LOGGER_NAME)

# Used for testing purposes only. If this is set, the controller will crash
# after writing each checkpoint with the specified probability.
_CRASH_AFTER_CHECKPOINT_PROBABILITY = 0

CONFIG_CHECKPOINT_KEY = "serve-app-config-checkpoint"
LOGGING_CONFIG_CHECKPOINT_KEY = "serve-logging-config-checkpoint"


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
        *,
        http_config: HTTPOptions,
        global_logging_config: LoggingConfig,
        grpc_options: Optional[gRPCOptions] = None,
    ):
        self._controller_node_id = ray.get_runtime_context().get_node_id()
        assert (
            self._controller_node_id == get_head_node_id()
        ), "Controller must be on the head node."

        self.ray_worker_namespace = ray.get_runtime_context().namespace
        self.gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
        kv_store_namespace = f"ray-serve-{self.ray_worker_namespace}"
        self.kv_store = RayInternalKVStore(kv_store_namespace, self.gcs_client)

        self.long_poll_host = LongPollHost()
        self.done_recovering_event = asyncio.Event()

        # Try to read config from checkpoint
        # logging config from checkpoint take precedence over the one passed in
        # the constructor.
        self.global_logging_config = None
        log_config_checkpoint = self.kv_store.get(LOGGING_CONFIG_CHECKPOINT_KEY)
        if log_config_checkpoint is not None:
            global_logging_config = pickle.loads(log_config_checkpoint)
        self.reconfigure_global_logging_config(global_logging_config)

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
        self.cluster_node_info_cache = create_cluster_node_info_cache(self.gcs_client)
        self.cluster_node_info_cache.update()

        self.proxy_state_manager = ProxyStateManager(
            config=http_config,
            head_node_id=self._controller_node_id,
            cluster_node_info_cache=self.cluster_node_info_cache,
            logging_config=self.global_logging_config,
            grpc_options=grpc_options,
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

        self.autoscaling_state_manager = AutoscalingStateManager()
        self.deployment_state_manager = DeploymentStateManager(
            self.kv_store,
            self.long_poll_host,
            all_serve_actor_names,
            get_all_live_placement_group_names(),
            self.cluster_node_info_cache,
            self.autoscaling_state_manager,
        )

        # Manage all applications' state
        self.application_state_manager = ApplicationStateManager(
            self.deployment_state_manager,
            self.endpoint_state,
            self.kv_store,
            self.global_logging_config,
        )

        # Controller actor details
        self._actor_details = ServeActorDetails(
            node_id=ray.get_runtime_context().get_node_id(),
            node_ip=ray.util.get_node_ip_address(),
            actor_id=ray.get_runtime_context().get_actor_id(),
            actor_name=SERVE_CONTROLLER_NAME,
            worker_id=ray.get_runtime_context().get_worker_id(),
            log_file_path=get_component_logger_file_path(),
        )
        self._shutting_down = False
        self._shutdown_event = asyncio.Event()
        self._shutdown_start_time = None

        self._create_control_loop_metrics()
        run_background_task(self.run_control_loop())

        # The target capacity percentage for all deployments across the cluster.
        self._target_capacity: Optional[float] = None
        self._target_capacity_direction: Optional[TargetCapacityDirection] = None
        self._recover_state_from_checkpoint()

        # Nodes where proxy actors should run.
        self._proxy_nodes = set()
        self._update_proxy_nodes()

    def reconfigure_global_logging_config(self, global_logging_config: LoggingConfig):
        if (
            self.global_logging_config
            and self.global_logging_config == global_logging_config
        ):
            return
        self.kv_store.put(
            LOGGING_CONFIG_CHECKPOINT_KEY, pickle.dumps(global_logging_config)
        )
        self.global_logging_config = global_logging_config

        self.long_poll_host.notify_changed(
            {LongPollNamespace.GLOBAL_LOGGING_CONFIG: global_logging_config}
        )
        configure_component_logger(
            component_name="controller",
            component_id=str(os.getpid()),
            logging_config=global_logging_config,
        )

        logger.info(
            f"Controller starting (version='{ray.__version__}').",
            extra={"log_to_stderr": False},
        )
        logger.debug(
            "Configure the serve controller logger "
            f"with logging config: {self.global_logging_config}"
        )

    def check_alive(self) -> None:
        """No-op to check if this controller is alive."""
        return

    def get_pid(self) -> int:
        return os.getpid()

    def record_autoscaling_metrics(
        self, replica_id: str, window_avg: Optional[float], send_timestamp: float
    ):
        logger.debug(
            f"Received metrics from replica {replica_id}: {window_avg} running requests"
        )
        self.autoscaling_state_manager.record_request_metrics_for_replica(
            replica_id, window_avg, send_timestamp
        )

    def record_handle_metrics(
        self,
        deployment_id: str,
        handle_id: str,
        actor_id: Optional[str],
        handle_source: DeploymentHandleSource,
        queued_requests: float,
        running_requests: Dict[str, float],
        send_timestamp: float,
    ):
        logger.debug(
            f"Received metrics from handle {handle_id} for deployment {deployment_id}: "
            f"{queued_requests} queued requests and {running_requests} running requests"
        )
        self.autoscaling_state_manager.record_request_metrics_for_handle(
            deployment_id=deployment_id,
            handle_id=handle_id,
            actor_id=actor_id,
            handle_source=handle_source,
            queued_requests=queued_requests,
            running_requests=running_requests,
            send_timestamp=send_timestamp,
        )

    def _dump_autoscaling_metrics_for_testing(self):
        return self.autoscaling_state_manager.get_metrics()

    def _dump_replica_states_for_testing(self, deployment_id: DeploymentID):
        return self.deployment_state_manager._deployment_states[deployment_id]._replicas

    def _stop_one_running_replica_for_testing(self, deployment_id):
        self.deployment_state_manager._deployment_states[
            deployment_id
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

        return await self.long_poll_host.listen_for_change(keys_to_snapshot_ids)

    async def listen_for_change_java(self, keys_to_snapshot_ids_bytes: bytes):
        """Proxy long pull client's listen request.

        Args:
            keys_to_snapshot_ids_bytes (Dict[str, int]): the protobuf bytes of
              keys_to_snapshot_ids (Dict[str, int]).
        """
        if not self.done_recovering_event.is_set():
            await self.done_recovering_event.wait()

        return await self.long_poll_host.listen_for_change_java(
            keys_to_snapshot_ids_bytes
        )

    def get_all_endpoints(self) -> Dict[DeploymentID, Dict[str, Any]]:
        """Returns a dictionary of deployment name to config."""
        return self.endpoint_state.get_endpoints()

    def get_all_endpoints_java(self) -> bytes:
        """Returns a dictionary of deployment name to config."""
        endpoints = self.get_all_endpoints()
        # NOTE(zcin): Java only supports 1.x deployments, so only return
        # a dictionary of deployment name -> endpoint info
        data = {
            endpoint_tag.name: EndpointInfoProto(route=endpoint_dict["route"])
            for endpoint_tag, endpoint_dict in endpoints.items()
        }
        return EndpointSet(endpoints=data).SerializeToString()

    def get_proxies(self) -> Dict[NodeId, ActorHandle]:
        """Returns a dictionary of node ID to proxy actor handles."""
        if self.proxy_state_manager is None:
            return {}
        return self.proxy_state_manager.get_proxy_handles()

    def get_proxy_names(self) -> bytes:
        """Returns the proxy actor name list serialized by protobuf."""
        if self.proxy_state_manager is None:
            return None

        actor_name_list = ActorNameList(
            names=self.proxy_state_manager.get_proxy_names().values()
        )
        return actor_name_list.SerializeToString()

    def _update_proxy_nodes(self):
        """Update the nodes set where proxy actors should run.

        Controller decides where proxy actors should run
        (head node and nodes with deployment replicas).
        """
        new_proxy_nodes = self.deployment_state_manager.get_active_node_ids()
        new_proxy_nodes = new_proxy_nodes - set(
            self.cluster_node_info_cache.get_draining_nodes()
        )
        new_proxy_nodes.add(self._controller_node_id)
        self._proxy_nodes = new_proxy_nodes

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
                    if num_loops > 0:
                        # Only log if we actually needed to recover anything.
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

            # Update the proxy nodes set before updating the proxy states,
            # so they are more consistent.
            node_update_start_time = time.time()
            self._update_proxy_nodes()
            self.node_update_duration_gauge_s.set(time.time() - node_update_start_time)

            # Don't update proxy_state until after the done recovering event is set,
            # otherwise we may start a new proxy but not broadcast it any
            # info about available deployments & their replicas.
            if self.proxy_state_manager and self.done_recovering_event.is_set():
                try:
                    proxy_update_start_time = time.time()
                    self.proxy_state_manager.update(proxy_nodes=self._proxy_nodes)
                    self.proxy_update_duration_gauge_s.set(
                        time.time() - proxy_update_start_time
                    )
                except Exception:
                    logger.exception("Exception updating proxy state.")

            # When the controller is done recovering, drop invalid handle metrics
            # that may be stale for autoscaling
            if not any_recovering:
                self.autoscaling_state_manager.drop_stale_handle_metrics(
                    self.deployment_state_manager.get_alive_replica_actor_ids()
                    | self.proxy_state_manager.get_alive_proxy_actor_ids()
                )

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
            await asyncio.sleep(CONTROL_LOOP_INTERVAL_S)
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

    def _recover_state_from_checkpoint(self):
        (
            deployment_time,
            serve_config,
            target_capacity_direction,
        ) = self._read_config_checkpoint()
        self._target_capacity_direction = target_capacity_direction
        if serve_config is not None:
            logger.info(
                "Recovered config from checkpoint.", extra={"log_to_stderr": False}
            )
            self.apply_config(serve_config, deployment_time=deployment_time)

    def _read_config_checkpoint(
        self,
    ) -> Tuple[float, Optional[ServeDeploySchema], Optional[TargetCapacityDirection]]:
        """Reads the current Serve config checkpoint.

        The Serve config checkpoint stores active application configs and
        other metadata.

        Returns:

        If the GCS contains a checkpoint, tuple of:
            1. A deployment timestamp.
            2. A Serve config. This Serve config is reconstructed from the
                active application states. It may not exactly match the
                submitted config (e.g. the top-level http options may be
                different).
            3. The target_capacity direction calculated after the Serve
               was submitted.

        If the GCS doesn't contain a checkpoint, returns (0, None, None).
        """

        checkpoint = self.kv_store.get(CONFIG_CHECKPOINT_KEY)
        if checkpoint is not None:
            (
                deployment_time,
                target_capacity,
                target_capacity_direction,
                config_checkpoints_dict,
            ) = pickle.loads(checkpoint)

            return (
                deployment_time,
                ServeDeploySchema(
                    applications=list(config_checkpoints_dict.values()),
                    target_capacity=target_capacity,
                ),
                target_capacity_direction,
            )
        else:
            return (0.0, None, None)

    def _all_running_replicas(self) -> Dict[DeploymentID, List[RunningReplicaInfo]]:
        """Used for testing.

        Returned dictionary maps deployment names to replica infos.
        """

        return self.deployment_state_manager.get_running_replica_infos()

    def get_actor_details(self) -> ServeActorDetails:
        """Returns the actor details for this controller.

        Currently used for test only.
        """
        return self._actor_details

    def get_proxy_details(self, node_id: str) -> Optional[ProxyDetails]:
        """Returns the proxy details for the proxy on the given node.

        Currently used for test only. Will return None if the proxy doesn't exist on
        the given node.
        """
        if self.proxy_state_manager is None:
            return None

        return self.proxy_state_manager.get_proxy_details().get(node_id)

    def get_deployment_timestamps(self, app_name: str) -> float:
        """Returns the deployment timestamp for the given app.

        Currently used for test only.
        """
        for (
            _app_name,
            app_status_info,
        ) in self.application_state_manager.list_app_statuses().items():
            if app_name == _app_name:
                return app_status_info.deployment_timestamp

    def get_deployment_details(
        self, app_name: str, deployment_name: str
    ) -> DeploymentDetails:
        """Returns the deployment details for the app and deployment.

        Currently used for test only.
        """
        return self.application_state_manager.list_deployment_details(app_name)[
            deployment_name
        ]

    def get_http_config(self) -> HTTPOptions:
        """Return the HTTP proxy configuration."""
        if self.proxy_state_manager is None:
            return HTTPOptions()
        return self.proxy_state_manager.get_config()

    def get_grpc_config(self) -> gRPCOptions:
        """Return the gRPC proxy configuration."""
        if self.proxy_state_manager is None:
            return gRPCOptions()
        return self.proxy_state_manager.get_grpc_config()

    def get_root_url(self):
        """Return the root url for the serve instance."""
        if self.proxy_state_manager is None:
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
        delete all deployments, and shuts down all proxies. Once all these
        resources are released, it then kills the controller actor.
        """
        if not self._shutting_down:
            return

        if self._shutdown_start_time is None:
            self._shutdown_start_time = time.time()
            logger.info("Controller shutdown started.", extra={"log_to_stderr": False})

        self.kv_store.delete(CONFIG_CHECKPOINT_KEY)
        self.kv_store.delete(LOGGING_CONFIG_CHECKPOINT_KEY)
        self.application_state_manager.shutdown()
        self.deployment_state_manager.shutdown()
        self.endpoint_state.shutdown()
        if self.proxy_state_manager:
            self.proxy_state_manager.shutdown()

        config_checkpoint_deleted = self.config_checkpoint_deleted()
        application_is_shutdown = self.application_state_manager.is_ready_for_shutdown()
        deployment_is_shutdown = self.deployment_state_manager.is_ready_for_shutdown()
        endpoint_is_shutdown = self.endpoint_state.is_ready_for_shutdown()
        proxy_state_is_shutdown = (
            self.proxy_state_manager is None
            or self.proxy_state_manager.is_ready_for_shutdown()
        )
        if (
            config_checkpoint_deleted
            and application_is_shutdown
            and deployment_is_shutdown
            and endpoint_is_shutdown
            and proxy_state_is_shutdown
        ):
            logger.warning(
                "All resources have shut down, controller exiting.",
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
            if not proxy_state_is_shutdown:
                logger.warning(
                    "proxy_state not yet shutdown",
                    extra={"log_to_stderr": False},
                )

    def deploy_application(self, name: str, deployment_args_list: List[bytes]) -> None:
        """
        Takes in a list of dictionaries that contain deployment arguments.
        If same app name deployed, old application will be overwrriten.

        Args:
            name: Application name.
            deployment_args_list: List of serialized deployment infomation,
                where each item in the list is bytes representing the serialized
                protobuf `DeploymentArgs` object. `DeploymentArgs` contains all the
                information for the single deployment.
        """
        deployment_args_deserialized = []
        for deployment_args_bytes in deployment_args_list:
            deployment_args = DeploymentArgs.FromString(deployment_args_bytes)
            deployment_args_deserialized.append(
                {
                    "deployment_name": deployment_args.deployment_name,
                    "deployment_config_proto_bytes": deployment_args.deployment_config,
                    "replica_config_proto_bytes": deployment_args.replica_config,
                    "deployer_job_id": deployment_args.deployer_job_id,
                    "ingress": deployment_args.ingress,
                    "route_prefix": (
                        deployment_args.route_prefix
                        if deployment_args.HasField("route_prefix")
                        else None
                    ),
                    "docs_path": (
                        deployment_args.docs_path
                        if deployment_args.HasField("docs_path")
                        else None
                    ),
                }
            )
        self.application_state_manager.deploy_app(name, deployment_args_deserialized)

    def apply_config(
        self,
        config: ServeDeploySchema,
        deployment_time: float = 0.0,
    ) -> None:
        """Apply the config described in `ServeDeploySchema`.

        This will upgrade the applications to the goal state specified in the
        config.

        If `deployment_time` is not provided, `time.time()` is used.
        """
        ServeUsageTag.API_VERSION.record("v2")
        if not deployment_time:
            deployment_time = time.time()

        new_config_checkpoint = {}

        _, curr_config, _ = self._read_config_checkpoint()

        self._target_capacity_direction = calculate_target_capacity_direction(
            curr_config=curr_config,
            new_config=config,
            curr_target_capacity_direction=self._target_capacity_direction,
        )
        log_target_capacity_change(
            self._target_capacity,
            config.target_capacity,
            self._target_capacity_direction,
        )
        self._target_capacity = config.target_capacity

        for app_config in config.applications:
            # If the application logging config is not set, use the global logging
            # config.
            if app_config.logging_config is None and config.logging_config:
                app_config.logging_config = config.logging_config

            app_config_dict = app_config.dict(exclude_unset=True)
            new_config_checkpoint[app_config.name] = app_config_dict

        self.kv_store.put(
            CONFIG_CHECKPOINT_KEY,
            pickle.dumps(
                (
                    deployment_time,
                    self._target_capacity,
                    self._target_capacity_direction,
                    new_config_checkpoint,
                )
            ),
        )

        # Declaratively apply the new set of applications.
        # This will delete any applications no longer in the config that were
        # previously deployed via the REST API.
        self.application_state_manager.apply_app_configs(
            config.applications,
            deployment_time=deployment_time,
            target_capacity=self._target_capacity,
            target_capacity_direction=self._target_capacity_direction,
        )

    def get_deployment_info(self, name: str, app_name: str = "") -> bytes:
        """Get the current information about a deployment.

        Args:
            name: the name of the deployment.

        Returns:
            DeploymentRoute's protobuf serialized bytes

        Raises:
            KeyError if the deployment doesn't exist.
        """
        id = DeploymentID(name=name, app_name=app_name)
        deployment_info = self.deployment_state_manager.get_deployment(id)
        if deployment_info is None:
            app_msg = f" in application '{app_name}'" if app_name else ""
            raise KeyError(f"Deployment '{name}' does not exist{app_msg}.")

        route = self.endpoint_state.get_endpoint_route(id)

        deployment_route = DeploymentRoute(
            deployment_info=deployment_info.to_proto(), route=route
        )
        return deployment_route.SerializeToString()

    def list_deployments_internal(
        self,
    ) -> Dict[DeploymentID, Tuple[DeploymentInfo, str]]:
        """Gets the current information about all deployments.

        Returns:
            Dict(deployment_id, (DeploymentInfo, route))
        """
        return {
            id: (info, self.endpoint_state.get_endpoint_route(id))
            for id, info in self.deployment_state_manager.get_deployment_infos().items()
        }

    def list_deployment_ids(self) -> List[DeploymentID]:
        """Gets the current list of all deployments' identifiers."""
        return self.deployment_state_manager._deployment_states.keys()

    def get_serve_instance_details(self) -> Dict:
        """Gets details on all applications on the cluster and system-level info.

        The information includes application and deployment statuses, config options,
        error messages, etc.

        Returns:
            Dict that follows the format of the schema ServeInstanceDetails.
        """

        http_config = self.get_http_config()
        grpc_config = self.get_grpc_config()
        applications = {}

        app_statuses = self.application_state_manager.list_app_statuses()

        # If there are no app statuses, there's no point getting the app configs.
        # Moreover, there might be no app statuses because the GCS is down,
        # in which case getting the app configs would fail anyway,
        # since they're stored in the checkpoint in the GCS.
        app_configs = self.get_app_configs() if app_statuses else {}

        for (
            app_name,
            app_status_info,
        ) in app_statuses.items():
            applications[app_name] = ApplicationDetails(
                name=app_name,
                route_prefix=self.application_state_manager.get_route_prefix(app_name),
                docs_path=self.get_docs_path(app_name),
                status=app_status_info.status,
                message=app_status_info.message,
                last_deployed_time_s=app_status_info.deployment_timestamp,
                # This can be none if the app was deployed through
                # serve.run, the app is in deleting state,
                # or a checkpoint hasn't been set yet
                deployed_app_config=app_configs.get(app_name),
                source=self.application_state_manager.get_app_source(app_name),
                deployments=self.application_state_manager.list_deployment_details(
                    app_name
                ),
            )

        # NOTE(zcin): We use exclude_unset here because we explicitly and intentionally
        # fill in all info that should be shown to users.
        http_options = HTTPOptionsSchema.parse_obj(http_config.dict(exclude_unset=True))
        grpc_options = gRPCOptionsSchema.parse_obj(grpc_config.dict(exclude_unset=True))
        return ServeInstanceDetails(
            target_capacity=self._target_capacity,
            controller_info=self._actor_details,
            proxy_location=ProxyLocation._from_deployment_mode(http_config.location),
            http_options=http_options,
            grpc_options=grpc_options,
            proxies=(
                self.proxy_state_manager.get_proxy_details()
                if self.proxy_state_manager
                else None
            ),
            applications=applications,
        )._get_user_facing_json_serializable_dict(exclude_unset=True)

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

    def get_app_configs(self) -> Dict[str, ServeApplicationSchema]:
        checkpoint = self.kv_store.get(CONFIG_CHECKPOINT_KEY)
        if checkpoint is None:
            return {}

        _, _, _, config_checkpoints_dict = pickle.loads(checkpoint)
        return {
            app: ServeApplicationSchema.parse_obj(config)
            for app, config in config_checkpoints_dict.items()
        }

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

        id = DeploymentID(name=name, app_name=app_name)
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
            self.application_state_manager.delete_app(name)

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

    def _get_logging_config(self) -> Tuple:
        """Get the logging configuration (for testing purposes)."""
        log_file_path = None
        for handler in logger.handlers:
            if isinstance(handler, logging.handlers.RotatingFileHandler):
                log_file_path = handler.baseFilename
        return self.global_logging_config, log_file_path

    def _get_target_capacity_direction(self) -> Optional[TargetCapacityDirection]:
        """Gets the controller's scale direction (for testing purposes)."""

        return self._target_capacity_direction


def calculate_target_capacity_direction(
    curr_config: Optional[ServeDeploySchema],
    new_config: ServeDeploySchema,
    curr_target_capacity_direction: Optional[float],
) -> Optional[TargetCapacityDirection]:
    """Compares two Serve configs to calculate the next scaling direction."""

    curr_target_capacity = None
    next_target_capacity_direction = None

    if curr_config is not None and applications_match(curr_config, new_config):
        curr_target_capacity = curr_config.target_capacity
        next_target_capacity = new_config.target_capacity

        if curr_target_capacity == next_target_capacity:
            next_target_capacity_direction = curr_target_capacity_direction
        elif curr_target_capacity is None and next_target_capacity is not None:
            # target_capacity is scaling down from None to a number.
            next_target_capacity_direction = TargetCapacityDirection.DOWN
        elif next_target_capacity is None:
            next_target_capacity_direction = None
        elif curr_target_capacity < next_target_capacity:
            next_target_capacity_direction = TargetCapacityDirection.UP
        else:
            next_target_capacity_direction = TargetCapacityDirection.DOWN
    elif new_config.target_capacity is not None:
        # A config with different apps has been applied, and it contains a
        # target_capacity. Serve must start scaling this config up.
        next_target_capacity_direction = TargetCapacityDirection.UP
    else:
        next_target_capacity_direction = None

    return next_target_capacity_direction


def applications_match(config1: ServeDeploySchema, config2: ServeDeploySchema) -> bool:
    """Checks whether the applications in config1 and config2 match.

    Two applications match if they have the same name.
    """

    config1_app_names = {app.name for app in config1.applications}
    config2_app_names = {app.name for app in config2.applications}

    return config1_app_names == config2_app_names


def log_target_capacity_change(
    curr_target_capacity: Optional[float],
    next_target_capacity: Optional[float],
    next_target_capacity_direction: Optional[TargetCapacityDirection],
):
    """Logs changes in the target_capacity."""

    if curr_target_capacity != next_target_capacity:
        if isinstance(next_target_capacity_direction, TargetCapacityDirection):
            logger.info(
                "Target capacity scaling "
                f"{next_target_capacity_direction.value.lower()} "
                f"from {curr_target_capacity} to {next_target_capacity}."
            )
        else:
            logger.info("Target capacity entering 100% at steady state.")


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
        http_proxy_port: int = 8000,
    ):
        try:
            self._controller = ray.get_actor(
                SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE
            )
        except ValueError:
            self._controller = None
        if self._controller is None:
            http_config = HTTPOptions()
            logging_config = LoggingConfig()
            http_config.port = http_proxy_port
            self._controller = ServeController.options(
                num_cpus=0,
                name=SERVE_CONTROLLER_NAME,
                lifetime="detached",
                max_restarts=-1,
                max_task_retries=-1,
                resources={HEAD_NODE_RESOURCE_NAME: 0.001},
                namespace=SERVE_NAMESPACE,
                max_concurrency=CONTROLLER_MAX_CONCURRENCY,
                enable_task_events=RAY_SERVE_ENABLE_TASK_EVENTS,
            ).remote(
                http_config=http_config,
                global_logging_config=logging_config,
            )

    def check_alive(self) -> None:
        """No-op to check if this actor is alive."""
        return

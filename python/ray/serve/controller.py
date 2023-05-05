import asyncio
import json
import logging
import os
import pickle
import time
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import ray
from ray._private.utils import (
    import_attr,
    run_background_task,
)
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.actor import ActorHandle
from ray._raylet import GcsClient
from ray.serve._private.common import (
    DeploymentInfo,
    EndpointInfo,
    EndpointTag,
    NodeId,
    RunningReplicaInfo,
    StatusOverview,
    ServeDeployMode,
)
from ray.serve.config import HTTPOptions
from ray.serve._private.constants import (
    CONTROL_LOOP_PERIOD_S,
    SERVE_LOGGER_NAME,
    CONTROLLER_MAX_CONCURRENCY,
    SERVE_ROOT_URL_ENV_KEY,
    SERVE_NAMESPACE,
    RAY_INTERNAL_SERVE_CONTROLLER_PIN_ON_NODE,
    RECOVERING_LONG_POLL_BROADCAST_TIMEOUT_S,
    SERVE_DEFAULT_APP_NAME,
    DEPLOYMENT_NAME_PREFIX_SEPARATOR,
    MULTI_APP_MIGRATION_MESSAGE,
)
from ray.serve._private.deploy_utils import (
    deploy_args_to_deployment_info,
    get_app_code_version,
)
from ray.serve._private.deployment_state import DeploymentStateManager, ReplicaState
from ray.serve._private.endpoint_state import EndpointState
from ray.serve._private.http_state import HTTPState
from ray.serve._private.logging_utils import configure_component_logger
from ray.serve._private.long_poll import LongPollHost
from ray.serve.exceptions import RayServeException
from ray.serve.schema import (
    ServeApplicationSchema,
    ServeDeploySchema,
    ApplicationDetails,
    ServeInstanceDetails,
    HTTPOptionsSchema,
)
from ray.serve._private.storage.kv_store import RayInternalKVStore
from ray.serve._private.utils import (
    DEFAULT,
    override_runtime_envs_except_env_vars,
)
from ray.serve._private.application_state import ApplicationStateManager

logger = logging.getLogger(SERVE_LOGGER_NAME)

# Used for testing purposes only. If this is set, the controller will crash
# after writing each checkpoint with the specified probability.
_CRASH_AFTER_CHECKPOINT_PROBABILITY = 0

SNAPSHOT_KEY = "serve-deployments-snapshot"
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
        head_node_id: str,
        detached: bool = False,
        _disable_http_proxy: bool = False,
    ):
        configure_component_logger(
            component_name="controller", component_id=str(os.getpid())
        )

        # Used to read/write checkpoints.
        self.ray_worker_namespace = ray.get_runtime_context().namespace
        self.controller_name = controller_name
        gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
        kv_store_namespace = f"{self.controller_name}-{self.ray_worker_namespace}"
        self.kv_store = RayInternalKVStore(kv_store_namespace, gcs_client)
        self.snapshot_store = RayInternalKVStore(kv_store_namespace, gcs_client)

        # Dictionary of deployment_name -> proxy_name -> queue length.
        self.deployment_stats = defaultdict(lambda: defaultdict(dict))

        self.long_poll_host = LongPollHost()
        self.done_recovering_event = asyncio.Event()

        if _disable_http_proxy:
            self.http_state = None
        else:
            self.http_state = HTTPState(
                controller_name,
                detached,
                http_config,
                head_node_id,
                gcs_client,
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
        )

        # Manage all applications' state
        self.application_state_manager = ApplicationStateManager(
            self.deployment_state_manager
        )

        # Keep track of single-app vs multi-app
        self.deploy_mode = ServeDeployMode.UNSET

        run_background_task(self.run_control_loop())

        self._recover_config_from_checkpoint()

    def check_alive(self) -> None:
        """No-op to check if this controller is alive."""
        return

    def get_pid(self) -> int:
        return os.getpid()

    def record_autoscaling_metrics(self, data: Dict[str, float], send_timestamp: float):
        self.deployment_state_manager.record_autoscaling_metrics(data, send_timestamp)

    def record_handle_metrics(self, data: Dict[str, float], send_timestamp: float):
        self.deployment_state_manager.record_handle_metrics(data, send_timestamp)

    def _dump_autoscaling_metrics_for_testing(self):
        return self.deployment_state_manager.get_autoscaling_metrics()

    def _dump_replica_states_for_testing(self, deployment_name):
        return self.deployment_state_manager._deployment_states[
            deployment_name
        ]._replicas

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
        data = {
            endpoint_tag: EndpointInfoProto(route=endppint_dict["route"])
            for endpoint_tag, endppint_dict in endpoints.items()
        }
        return EndpointSet(endpoints=data).SerializeToString()

    def get_http_proxies(self) -> Dict[NodeId, ActorHandle]:
        """Returns a dictionary of node ID to http_proxy actor handles."""
        if self.http_state is None:
            return {}
        return self.http_state.get_http_proxy_handles()

    def get_http_proxy_names(self) -> bytes:
        """Returns the http_proxy actor name list serialized by protobuf."""
        if self.http_state is None:
            return None

        from ray.serve.generated.serve_pb2 import ActorNameList

        actor_name_list = ActorNameList(
            names=self.http_state.get_http_proxy_names().values()
        )
        return actor_name_list.SerializeToString()

    async def run_control_loop(self) -> None:
        # NOTE(edoakes): we catch all exceptions here and simply log them,
        # because an unhandled exception would cause the main control loop to
        # halt, which should *never* happen.
        recovering_timeout = RECOVERING_LONG_POLL_BROADCAST_TIMEOUT_S
        start_time = time.time()
        while True:
            if (
                not self.done_recovering_event.is_set()
                and time.time() - start_time > recovering_timeout
            ):
                logger.warning(
                    f"Replicas still recovering after {recovering_timeout}s, "
                    "setting done recovering event to broadcast long poll updates."
                )
                self.done_recovering_event.set()

            # Don't update http_state until after the done recovering event is set,
            # otherwise we may start a new HTTP proxy but not broadcast it any
            # info about available deployments & their replicas.
            if self.http_state and self.done_recovering_event.is_set():
                try:
                    self.http_state.update()
                except Exception:
                    logger.exception("Exception updating HTTP state.")

            try:
                any_recovering = self.deployment_state_manager.update()
                if not self.done_recovering_event.is_set() and not any_recovering:
                    self.done_recovering_event.set()
            except Exception:
                logger.exception("Exception updating deployment state.")

            try:
                self.application_state_manager.update()
            except Exception:
                logger.exception("Exception updating application state.")

            try:
                self._put_serve_snapshot()
            except Exception:
                logger.exception("Exception putting serve snapshot.")
            await asyncio.sleep(CONTROL_LOOP_PERIOD_S)

    def _put_serve_snapshot(self) -> None:
        val = dict()
        for deployment_name, (
            deployment_info,
            route_prefix,
        ) in self.list_deployments_internal(include_deleted=True).items():
            entry = dict()
            entry["name"] = deployment_name
            entry["namespace"] = ray.get_runtime_context().namespace
            entry["ray_job_id"] = deployment_info.deployer_job_id
            entry["class_name"] = deployment_info.replica_config.deployment_def_name
            entry["version"] = deployment_info.version
            entry["http_route"] = route_prefix
            entry["start_time"] = deployment_info.start_time_ms
            entry["end_time"] = deployment_info.end_time_ms or 0
            entry["status"] = "DELETED" if deployment_info.end_time_ms else "RUNNING"
            entry["actors"] = dict()
            if entry["status"] == "RUNNING":
                replicas = self.deployment_state_manager._deployment_states[
                    deployment_name
                ]._replicas
                running_replicas = replicas.get([ReplicaState.RUNNING])
                for replica in running_replicas:
                    try:
                        actor_handle = replica.actor_handle
                    except ValueError:
                        # Actor died or hasn't yet been created.
                        continue
                    actor_id = actor_handle._ray_actor_id.hex()
                    replica_tag = replica.replica_tag
                    replica_version = (
                        None
                        if (replica.version is None or replica.version.unversioned)
                        else replica.version.code_version
                    )
                    entry["actors"][actor_id] = {
                        "replica_tag": replica_tag,
                        "version": replica_version,
                    }

            val[deployment_name] = entry
        self.snapshot_store.put(SNAPSHOT_KEY, json.dumps(val).encode("utf-8"))

    def _recover_config_from_checkpoint(self):
        checkpoint = self.kv_store.get(CONFIG_CHECKPOINT_KEY)
        if checkpoint is not None:
            deployment_time, deploy_mode, config_checkpoints_dict = pickle.loads(
                checkpoint
            )
            applications = list(config_checkpoints_dict.values())
            if deploy_mode == ServeDeployMode.SINGLE_APP:
                self.deploy_apps(
                    ServeApplicationSchema.parse_obj(applications[0]),
                    deployment_time,
                )
            else:
                self.deploy_apps(
                    ServeDeploySchema.parse_obj({"applications": applications}),
                    deployment_time,
                )

    def _all_running_replicas(self) -> Dict[str, List[RunningReplicaInfo]]:
        """Used for testing.

        Returned dictionary maps deployment names to replica infos.
        """

        return self.deployment_state_manager.get_running_replica_infos()

    def get_http_config(self):
        """Return the HTTP proxy configuration."""
        if self.http_state is None:
            return None
        return self.http_state.get_config()

    def get_root_url(self):
        """Return the root url for the serve instance."""
        if self.http_state is None:
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

    def shutdown(self):
        """Shuts down the serve instance completely."""
        self.kv_store.delete(CONFIG_CHECKPOINT_KEY)
        self.deployment_state_manager.shutdown()
        self.endpoint_state.shutdown()
        if self.http_state:
            self.http_state.shutdown()

    def deploy(
        self,
        name: str,
        deployment_config_proto_bytes: bytes,
        replica_config_proto_bytes: bytes,
        route_prefix: Optional[str],
        deployer_job_id: Union[str, bytes],
        docs_path: Optional[str] = None,
        is_driver_deployment: Optional[bool] = False,
        app_name: str = None,
    ) -> bool:
        """Deploys a deployment."""
        if route_prefix is not None:
            assert route_prefix.startswith("/")
        if docs_path is not None:
            assert docs_path.startswith("/")

        # app_name is None for V1 API, reset it to empty string to avoid
        # breaking metrics.
        if app_name is None:
            app_name = ""

        deployment_info = deploy_args_to_deployment_info(
            deployment_name=name,
            deployment_config_proto_bytes=deployment_config_proto_bytes,
            replica_config_proto_bytes=replica_config_proto_bytes,
            deployer_job_id=deployer_job_id,
            route_prefix=route_prefix,
            is_driver_deployment=is_driver_deployment,
            app_name=app_name,
        )

        # TODO(architkulkarni): When a deployment is redeployed, even if
        # the only change was num_replicas, the start_time_ms is refreshed.
        # Is this the desired behaviour?
        updating = self.deployment_state_manager.deploy(name, deployment_info)

        if route_prefix is not None:
            endpoint_info = EndpointInfo(route=route_prefix, app_name=app_name)
            self.endpoint_state.update_endpoint(name, endpoint_info)
        else:
            self.endpoint_state.delete_endpoint(name)

        return updating

    def deploy_application(
        self, name: str, deployment_args_list: List[Dict]
    ) -> List[bool]:
        """
        Takes in a list of dictionaries that contain keyword arguments for the
        controller's deploy() function. Calls deploy on all the argument
        dictionaries in the list. Effectively executes an atomic deploy on a
        group of deployments.
        If same app name deployed, old application will be overwrriten.

        Args:
            name: Application name.
            deployment_args_list: List of deployment infomation, each item in the list
                contains all the information for the single deployment.

        Returns: list of deployment status to indicate whether each deployment is
            deployed successfully or not.
        """

        deployments_to_delete = self.application_state_manager.deploy_application(
            name, deployment_args_list
        )
        self.delete_deployments(deployments_to_delete)
        return [self.deploy(**args) for args in deployment_args_list]

    def deploy_apps(
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
            code_version = get_app_code_version(app_config)

            app_config_dict = app_config.dict(exclude_unset=True)
            new_config_checkpoint[app_config.name] = app_config_dict

            logger.info(
                "Starting deploy_serve_application "
                f"task for application {app_config.name}."
            )
            deploy_obj_ref = deploy_serve_application.options(
                runtime_env=app_config.runtime_env
            ).remote(
                app_config.import_path,
                app_config.runtime_env,
                app_config_dict.get("deployments", []),
                code_version,
                app_config_dict.get("route_prefix", DEFAULT.VALUE),
                app_config.name,
                app_config.args,
            )

            self.application_state_manager.create_application_state(
                app_config.name,
                deploy_obj_ref=deploy_obj_ref,
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
        self.endpoint_state.delete_endpoint(name)
        return self.deployment_state_manager.delete_deployment(name)

    def delete_deployments(self, names: Iterable[str]) -> None:
        for name in names:
            self.delete_deployment(name)

    def get_deployment_info(self, name: str) -> bytes:
        """Get the current information about a deployment.

        Args:
            name: the name of the deployment.

        Returns:
            DeploymentRoute's protobuf serialized bytes

        Raises:
            KeyError if the deployment doesn't exist.
        """
        deployment_info = self.deployment_state_manager.get_deployment(name)
        if deployment_info is None:
            raise KeyError(f"Deployment {name} does not exist.")

        route = self.endpoint_state.get_endpoint_route(name)

        from ray.serve.generated.serve_pb2 import DeploymentRoute

        deployment_route = DeploymentRoute(
            deployment_info=deployment_info.to_proto(), route=route
        )
        return deployment_route.SerializeToString()

    def list_deployments_internal(
        self, include_deleted: Optional[bool] = False
    ) -> Dict[str, Tuple[DeploymentInfo, str]]:
        """Gets the current information about all deployments.

        Args:
            include_deleted: Whether to include information about
                deployments that have been deleted.

        Returns:
            Dict(deployment_name, (DeploymentInfo, route))

        Raises:
            KeyError if the deployment doesn't exist.
        """
        return {
            name: (
                self.deployment_state_manager.get_deployment(
                    name, include_deleted=include_deleted
                ),
                self.endpoint_state.get_endpoint_route(name),
            )
            for name in self.deployment_state_manager.get_deployment_configs(
                include_deleted=include_deleted
            )
        }

    def list_deployments(self, include_deleted: Optional[bool] = False) -> bytes:
        """Gets the current information about all deployments.

        Args:
            include_deleted: Whether to include information about
                deployments that have been deleted.

        Returns:
            DeploymentRouteList's protobuf serialized bytes
        """
        from ray.serve.generated.serve_pb2 import DeploymentRoute, DeploymentRouteList

        deployment_route_list = DeploymentRouteList()
        for deployment_name, (
            deployment_info,
            route_prefix,
        ) in self.list_deployments_internal(include_deleted=include_deleted).items():
            deployment_info_proto = deployment_info.to_proto()
            deployment_info_proto.name = deployment_name
            deployment_route_list.deployment_routes.append(
                DeploymentRoute(
                    deployment_info=deployment_info_proto, route=route_prefix
                )
            )
        return deployment_route_list.SerializeToString()

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
        return ServeInstanceDetails(
            proxy_location=http_config.location,
            http_options=HTTPOptionsSchema(
                host=http_config.host,
                port=http_config.port,
            ),
            deploy_mode=self.deploy_mode,
            applications=applications,
        ).dict(exclude_unset=True)

    def get_serve_status(self, name: str = SERVE_DEFAULT_APP_NAME) -> bytes:
        """Return application status
        Args:
            name: application name. If application name doesn't exist, app_status
            is NOT_STARTED.
        """

        app_status = self.application_state_manager.get_app_status(name)
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

    def get_deployment_status(self, name: str) -> Union[None, bytes]:
        """Get deployment status by deployment name"""
        status = self.deployment_state_manager.get_deployment_statuses([name])
        if not status:
            return None
        return status[0].to_proto().SerializeToString()

    def get_docs_path(self, name: str):
        """Docs path for application.

        Currently, this is the OpenAPI docs path for FastAPI-integrated applications."""
        return self.application_state_manager.get_docs_path(name)

    def delete_apps(self, names: Iterable[str]):
        """Delete applications based on names

        During deletion, the application status is DELETING
        """
        deployments_to_delete = []
        for name in names:
            deployments_to_delete.extend(
                self.application_state_manager.get_deployments(name)
            )
            self.application_state_manager.delete_application(name)
        self.delete_deployments(deployments_to_delete)


@ray.remote(num_cpus=0, max_calls=1)
def deploy_serve_application(
    import_path: str,
    runtime_env: Dict,
    deployment_override_options: List[Dict],
    code_version: str,
    route_prefix: str,
    name: str,
    args: Dict,
):
    """Deploy Serve application from a user-provided config.

    Args:
        import_path: import path to top-level bound deployment.
        runtime_env: runtime_env for the application.
        deployment_override_options: Dictionary of options that overrides
            deployment options set in the graph's code itself.
        deployment_versions: Versions of each deployment, each of which is
            the same as the last deployment if it is a config update or
            a new randomly generated version if it is a code update
        name: application name. If specified, application will be deployed
            without removing existing applications.
        route_prefix: route_prefix. Define the route path for the application.
    """
    try:
        from ray import serve
        from ray.serve.api import build
        from ray.serve._private.api import call_app_builder_with_args_if_necessary

        # Import and build the application.
        app = call_app_builder_with_args_if_necessary(import_attr(import_path), args)
        app = build(app, name)

        # Override options for each deployment listed in the config.
        for options in deployment_override_options:
            deployment_name = options["name"]
            unique_deployment_name = (
                (name + DEPLOYMENT_NAME_PREFIX_SEPARATOR) if len(name) else ""
            ) + deployment_name

            if unique_deployment_name not in app.deployments:
                raise KeyError(
                    f'There is no deployment named "{deployment_name}" in the '
                    f'application "{name}".'
                )

            # Merge app-level and deployment-level runtime_envs.
            if "ray_actor_options" in options:
                # If specified, get ray_actor_options from config
                ray_actor_options = options["ray_actor_options"] or {}
            else:
                # Otherwise, get options from application code (and default to {}
                # if the code sets options to None).
                ray_actor_options = (
                    app.deployments[unique_deployment_name].ray_actor_options or {}
                )
            deployment_env = ray_actor_options.get("runtime_env", {})
            merged_env = override_runtime_envs_except_env_vars(
                runtime_env, deployment_env
            )
            ray_actor_options.update({"runtime_env": merged_env})
            options["ray_actor_options"] = ray_actor_options
            options["name"] = unique_deployment_name
            # Update the deployment's options
            app.deployments[unique_deployment_name].set_options(
                **options, _internal=True
            )

        # Set code version for each deployment
        for deployment_name in app.deployments:
            app.deployments[deployment_name].set_options(
                version=code_version, _internal=True
            )

        # Run the application locally on the cluster.
        serve.run(app, name=name, route_prefix=route_prefix)
    except KeyboardInterrupt:
        # Error is raised when this task is canceled with ray.cancel(), which
        # happens when deploy_apps() is called.
        logger.debug("Existing config deployment request terminated.")


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
            self._controller = ray.get_actor(controller_name, namespace="serve")
        except ValueError:
            self._controller = None
        if self._controller is None:
            # Used for scheduling things to the head node explicitly.
            head_node_id = ray.get_runtime_context().get_node_id()
            http_config = HTTPOptions()
            http_config.port = http_proxy_port
            self._controller = ServeController.options(
                num_cpus=1 if dedicated_cpu else 0,
                name=controller_name,
                lifetime="detached" if detached else None,
                max_restarts=-1,
                max_task_retries=-1,
                # Schedule the controller on the head node with a soft constraint. This
                # prefers it to run on the head node in most cases, but allows it to be
                # restarted on other nodes in an HA cluster.
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    head_node_id, soft=True
                )
                if RAY_INTERNAL_SERVE_CONTROLLER_PIN_ON_NODE
                else None,
                namespace="serve",
                max_concurrency=CONTROLLER_MAX_CONCURRENCY,
            ).remote(
                controller_name,
                http_config=http_config,
                head_node_id=head_node_id,
                detached=detached,
            )

    def check_alive(self) -> None:
        """No-op to check if this actor is alive."""
        return

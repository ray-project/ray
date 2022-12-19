import asyncio
import json
import logging
import os
import pickle
import time
import traceback
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import ray
from ray._private.utils import (
    import_attr,
    run_background_task,
)
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.actor import ActorHandle
from ray.exceptions import RayTaskError
from ray._private.gcs_utils import GcsClient
from ray.serve._private.autoscaling_policy import BasicAutoscalingPolicy
from ray.serve._private.common import (
    ApplicationStatus,
    ApplicationStatusInfo,
    DeploymentInfo,
    EndpointInfo,
    EndpointTag,
    NodeId,
    RunningReplicaInfo,
    StatusOverview,
)
from ray.serve.config import DeploymentConfig, HTTPOptions, ReplicaConfig
from ray.serve._private.constants import (
    CONTROL_LOOP_PERIOD_S,
    SERVE_LOGGER_NAME,
    CONTROLLER_MAX_CONCURRENCY,
    SERVE_ROOT_URL_ENV_KEY,
    SERVE_NAMESPACE,
    RAY_INTERNAL_SERVE_CONTROLLER_PIN_ON_NODE,
)
from ray.serve._private.deployment_state import DeploymentStateManager, ReplicaState
from ray.serve._private.endpoint_state import EndpointState
from ray.serve._private.http_state import HTTPState
from ray.serve._private.logging_utils import configure_component_logger
from ray.serve._private.long_poll import LongPollHost
from ray.serve.schema import ServeApplicationSchema
from ray.serve._private.storage.kv_store import RayInternalKVStore
from ray.serve._private.utils import (
    override_runtime_envs_except_env_vars,
    get_random_letters,
)
from ray.types import ObjectRef

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

        # Used to ensure that only a single state-changing operation happens
        # at any given time.
        self.write_lock = asyncio.Lock()

        self.long_poll_host = LongPollHost()

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

        # Reference to Ray task executing most recent deployment request
        self.config_deployment_request_ref: ObjectRef = None

        # Unix timestamp of latest config deployment request. Defaults to 0.
        self.deployment_timestamp = 0

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
        return await (self.long_poll_host.listen_for_change(keys_to_snapshot_ids))

    async def listen_for_change_java(self, keys_to_snapshot_ids_bytes: bytes):
        """Proxy long pull client's listen request.

        Args:
            keys_to_snapshot_ids_bytes (Dict[str, int]): the protobuf bytes of
              keys_to_snapshot_ids (Dict[str, int]).
        """
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
        while True:

            async with self.write_lock:
                if self.http_state:
                    try:
                        self.http_state.update()
                    except Exception:
                        logger.exception("Exception updating HTTP state.")
                try:
                    self.deployment_state_manager.update()
                except Exception:
                    logger.exception("Exception updating deployment state.")

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
            entry["ray_job_id"] = deployment_info.deployer_job_id.hex()
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
            self.deployment_timestamp, config, _ = pickle.loads(checkpoint)
            self.deploy_app(ServeApplicationSchema.parse_obj(config), update_time=False)

    def _all_running_replicas(self) -> Dict[str, List[RunningReplicaInfo]]:
        """Used for testing."""
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

    async def shutdown(self):
        """Shuts down the serve instance completely."""
        async with self.write_lock:
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
        deployer_job_id: Union["ray._raylet.JobID", bytes],
        is_driver_deployment: Optional[bool] = False,
    ) -> bool:
        if route_prefix is not None:
            assert route_prefix.startswith("/")

        deployment_config = DeploymentConfig.from_proto_bytes(
            deployment_config_proto_bytes
        )
        version = deployment_config.version
        replica_config = ReplicaConfig.from_proto_bytes(
            replica_config_proto_bytes, deployment_config.needs_pickle()
        )

        autoscaling_config = deployment_config.autoscaling_config
        if autoscaling_config is not None:
            if autoscaling_config.initial_replicas is not None:
                deployment_config.num_replicas = autoscaling_config.initial_replicas
            else:
                previous_deployment = self.deployment_state_manager.get_deployment(name)
                if previous_deployment is None:
                    deployment_config.num_replicas = autoscaling_config.min_replicas
                else:
                    deployment_config.num_replicas = (
                        previous_deployment.deployment_config.num_replicas
                    )

            autoscaling_policy = BasicAutoscalingPolicy(autoscaling_config)
        else:
            autoscaling_policy = None
        if isinstance(deployer_job_id, bytes):
            deployer_job_id = ray.JobID.from_int(
                int.from_bytes(deployer_job_id, "little")
            )
        deployment_info = DeploymentInfo(
            actor_name=name,
            version=version,
            deployment_config=deployment_config,
            replica_config=replica_config,
            deployer_job_id=deployer_job_id,
            start_time_ms=int(time.time() * 1000),
            autoscaling_policy=autoscaling_policy,
            is_driver_deployment=is_driver_deployment,
        )
        # TODO(architkulkarni): When a deployment is redeployed, even if
        # the only change was num_replicas, the start_time_ms is refreshed.
        # Is this the desired behaviour?
        updating = self.deployment_state_manager.deploy(name, deployment_info)

        if route_prefix is not None:
            endpoint_info = EndpointInfo(route=route_prefix)
            self.endpoint_state.update_endpoint(name, endpoint_info)
        else:
            self.endpoint_state.delete_endpoint(name)

        return updating

    def deploy_group(self, deployment_args_list: List[Dict]) -> List[bool]:
        """
        Takes in a list of dictionaries that contain keyword arguments for the
        controller's deploy() function. Calls deploy on all the argument
        dictionaries in the list. Effectively executes an atomic deploy on a
        group of deployments.
        """

        return [self.deploy(**args) for args in deployment_args_list]

    def deploy_app(
        self, config: ServeApplicationSchema, update_time: bool = True
    ) -> None:
        """Kicks off a task that deploys a Serve application.

        Cancels any previous in-progress task that is deploying a Serve
        application.

        Args:
            config: Contains the following:
                import_path: Serve deployment graph's import path
                runtime_env: runtime_env to run the deployment graph in
                deployment_override_options: Dictionaries that
                    contain argument-value options that can be passed directly
                    into a set_options() call. Overrides deployment options set
                    in the graph's code itself.
            update_time: Whether to update the deployment_timestamp.
        """

        if update_time:
            self.deployment_timestamp = time.time()

        config_dict = config.dict(exclude_unset=True)

        # Compare new config options with old ones and set versions of new deployments
        config_checkpoint = self.kv_store.get(CONFIG_CHECKPOINT_KEY)

        if config_checkpoint is not None:
            _, last_config_dict, last_version_dict = pickle.loads(config_checkpoint)
            updated_version_dict = _generate_deployment_config_versions(
                config_dict, last_config_dict, last_version_dict
            )
        else:
            updated_version_dict = _generate_deployment_config_versions(config_dict)

        self.kv_store.put(
            CONFIG_CHECKPOINT_KEY,
            pickle.dumps(
                (self.deployment_timestamp, config_dict, updated_version_dict)
            ),
        )

        deployment_override_options = config_dict.get("deployments", [])

        if self.config_deployment_request_ref is not None:
            ray.cancel(self.config_deployment_request_ref)
            logger.info(
                "Received new config deployment request. Cancelling "
                "previous request."
            )

        self.config_deployment_request_ref = run_graph.options(
            runtime_env=config.runtime_env
        ).remote(
            config.import_path,
            config.runtime_env,
            deployment_override_options,
            updated_version_dict,
        )

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

    async def get_serve_status(self) -> bytes:

        serve_app_status = ApplicationStatus.RUNNING
        serve_app_message = ""
        deployment_timestamp = self.deployment_timestamp

        if self.config_deployment_request_ref:
            finished, pending = ray.wait(
                [self.config_deployment_request_ref], timeout=0
            )

            if pending:
                serve_app_status = ApplicationStatus.DEPLOYING
            else:
                try:
                    await finished[0]
                except RayTaskError:
                    serve_app_status = ApplicationStatus.DEPLOY_FAILED
                    serve_app_message = f"Deployment failed:\n{traceback.format_exc()}"

        app_status = ApplicationStatusInfo(
            serve_app_status, serve_app_message, deployment_timestamp
        )
        deployment_statuses = self.deployment_state_manager.get_deployment_statuses()

        status_info = StatusOverview(
            app_status=app_status,
            deployment_statuses=deployment_statuses,
        )

        return status_info.to_proto().SerializeToString()

    def get_app_config(self) -> Dict:
        checkpoint = self.kv_store.get(CONFIG_CHECKPOINT_KEY)
        if checkpoint is None:
            return ServeApplicationSchema.get_empty_schema_dict()
        else:
            _, config, _ = pickle.loads(checkpoint)
            return config


def _generate_deployment_config_versions(
    new_config: Dict,
    last_deployed_config: Dict = None,
    last_deployed_versions: Dict = None,
) -> Dict[str, str]:
    """
    This function determines whether each deployment's version should be changed based
    on the newly deployed config.

    When ``import_path`` or ``runtime_env`` is changed, the versions for all deployments
    should be changed, so old replicas are torn down. When the options for a deployment
    in ``deployments`` change, its version should generally change. The only deployment
    options that can be changed without tearing down replicas (i.e. changing the
    version) are:
    * num_replicas
    * user_config
    * autoscaling_config

    A deployment option is considered changed when:
    * it was not specified in last_deployed_config and is specified in new_config
    * it was specified in last_deployed_config and is not specified in new_config
    * it is specified in both last_deployed_config and new_config but the specified
      value has changed

    Args:
        new_config: Newly deployed config dict that follows ServeApplicationSchema
        last_deployed_config: Last deployed config dict that follows
            ServeApplicationSchema, which is an empty dictionary if there is no previous
            deployment
        last_deployed_versions: Dictionary of {deployment_name: str -> version: str}
            tracking the versions of deployments listed in the last deployed config

    Returns:
        Dictionary of {deployment_name: str -> version: str} containing updated
        versions for deployments listed in the new config
    """
    # If import_path or runtime_env is changed, it is considered a code change
    if last_deployed_config is None:
        last_deployed_config = {}
    if last_deployed_versions is None:
        last_deployed_versions = {}

    if last_deployed_config.get("import_path") != new_config.get(
        "import_path"
    ) or last_deployed_config.get("runtime_env") != new_config.get("runtime_env"):
        last_deployed_config, last_deployed_versions = {}, {}

    new_deployments = {d["name"]: d for d in new_config.get("deployments", [])}
    old_deployments = {
        d["name"]: d for d in last_deployed_config.get("deployments", [])
    }

    def exclude_lightweight_update_options(dict):
        # Exclude config options from dict that qualify for a lightweight config
        # update. Changes in any other config options are considered a code change,
        # and require a version change to trigger an update that tears
        # down existing replicas and replaces them with updated ones.
        lightweight_update_options = [
            "num_replicas",
            "user_config",
            "autoscaling_config",
        ]
        return {
            option: dict[option]
            for option in dict
            if option not in lightweight_update_options
        }

    updated_versions = {}
    for name in new_deployments:
        new_deployment = exclude_lightweight_update_options(new_deployments[name])
        old_deployment = exclude_lightweight_update_options(
            old_deployments.get(name, {})
        )

        # If config options haven't changed, version stays the same
        # otherwise, generate a new random version
        if old_deployment == new_deployment:
            updated_versions[name] = last_deployed_versions[name]
        else:
            updated_versions[name] = get_random_letters()

    return updated_versions


@ray.remote(num_cpus=0, max_calls=1)
def run_graph(
    import_path: str,
    graph_env: Dict,
    deployment_override_options: List[Dict],
    deployment_versions: Dict,
):
    """
    Deploys a Serve application to the controller's Ray cluster.

    Args:
        import_path: Serve deployment graph's import path
        graph_env: runtime env to run the deployment graph in
        deployment_override_options: Dictionary of options that overrides
            deployment options set in the graph's code itself.
        deployment_versions: Versions of each deployment, each of which is
            the same as the last deployment if it is a config update or
            a new randomly generated version if it is a code update
    """
    try:
        from ray import serve
        from ray.serve.api import build

        # Import and build the graph
        graph = import_attr(import_path)
        app = build(graph)

        # Override options for each deployment
        for options in deployment_override_options:
            name = options["name"]

            # Merge graph-level and deployment-level runtime_envs
            if "ray_actor_options" in options:
                # If specified, get ray_actor_options from config
                ray_actor_options = options["ray_actor_options"] or {}
            else:
                # Otherwise, get options from graph code (and default to {} if code
                # sets options to None)
                ray_actor_options = app.deployments[name].ray_actor_options or {}

            deployment_env = ray_actor_options.get("runtime_env", {})
            merged_env = override_runtime_envs_except_env_vars(
                graph_env, deployment_env
            )

            ray_actor_options.update({"runtime_env": merged_env})
            options["ray_actor_options"] = ray_actor_options

            options["version"] = deployment_versions[name]

            # Update the deployment's options
            app.deployments[name].set_options(**options, _internal=True)

        # Run the graph locally on the cluster
        serve.run(app)
    except KeyboardInterrupt:
        # Error is raised when this task is canceled with ray.cancel(), which
        # happens when deploy_app() is called.
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

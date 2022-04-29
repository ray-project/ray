import asyncio
from collections import defaultdict
from copy import copy
import json
import logging
import time
import os
from typing import Dict, Iterable, List, Optional, Tuple, Any

import ray
from ray.actor import ActorHandle

from ray.serve.autoscaling_metrics import InMemoryMetricsStore
from ray.serve.autoscaling_policy import BasicAutoscalingPolicy
from ray.serve.common import (
    DeploymentInfo,
    EndpointTag,
    EndpointInfo,
    NodeId,
    RunningReplicaInfo,
)
from ray.serve.config import DeploymentConfig, HTTPOptions, ReplicaConfig
from ray.serve.constants import (
    CONTROL_LOOP_PERIOD_S,
    SERVE_ROOT_URL_ENV_KEY,
    SERVE_LOGGER_NAME,
)
from ray.serve.deployment_state import ReplicaState, DeploymentStateManager
from ray.serve.endpoint_state import EndpointState
from ray.serve.http_state import HTTPState
from ray.serve.logging_utils import configure_component_logger
from ray.serve.long_poll import LongPollHost
from ray.serve.storage.checkpoint_path import make_kv_store
from ray.serve.storage.kv_store import RayInternalKVStore

logger = logging.getLogger(SERVE_LOGGER_NAME)

# Used for testing purposes only. If this is set, the controller will crash
# after writing each checkpoint with the specified probability.
_CRASH_AFTER_CHECKPOINT_PROBABILITY = 0

SNAPSHOT_KEY = "serve-deployments-snapshot"


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
        http_config: HTTPOptions,
        checkpoint_path: str,
        detached: bool = False,
        _override_controller_namespace: Optional[str] = None,
    ):
        configure_component_logger(
            component_name="controller", component_id=str(os.getpid())
        )

        # Used to read/write checkpoints.
        self.controller_namespace = ray.get_runtime_context().namespace
        self.controller_name = controller_name
        self.checkpoint_path = checkpoint_path
        kv_store_namespace = f"{self.controller_name}-{self.controller_namespace}"
        self.kv_store = make_kv_store(checkpoint_path, namespace=kv_store_namespace)
        self.snapshot_store = RayInternalKVStore(namespace=kv_store_namespace)

        # Dictionary of deployment_name -> proxy_name -> queue length.
        self.deployment_stats = defaultdict(lambda: defaultdict(dict))

        # Used to ensure that only a single state-changing operation happens
        # at any given time.
        self.write_lock = asyncio.Lock()

        self.long_poll_host = LongPollHost()

        self.http_state = HTTPState(
            controller_name,
            detached,
            http_config,
            _override_controller_namespace=_override_controller_namespace,
        )
        self.endpoint_state = EndpointState(self.kv_store, self.long_poll_host)
        # Fetch all running actors in current cluster as source of current
        # replica state for controller failure recovery
        all_current_actor_names = ray.util.list_named_actors()
        self.deployment_state_manager = DeploymentStateManager(
            controller_name,
            detached,
            self.kv_store,
            self.long_poll_host,
            all_current_actor_names,
            _override_controller_namespace=_override_controller_namespace,
        )

        # TODO(simon): move autoscaling related stuff into a manager.
        self.autoscaling_metrics_store = InMemoryMetricsStore()

        asyncio.get_event_loop().create_task(self.run_control_loop())

    def check_alive(self) -> None:
        """No-op to check if this controller is alive."""
        return

    def record_autoscaling_metrics(self, data: Dict[str, float], send_timestamp: float):
        self.autoscaling_metrics_store.add_metrics_point(data, send_timestamp)

    def _dump_autoscaling_metrics_for_testing(self):
        return self.autoscaling_metrics_store.data

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

    def get_checkpoint_path(self) -> str:
        return self.checkpoint_path

    def get_all_endpoints(self) -> Dict[EndpointTag, Dict[str, Any]]:
        """Returns a dictionary of deployment name to config."""
        return self.endpoint_state.get_endpoints()

    def get_http_proxies(self) -> Dict[NodeId, ActorHandle]:
        """Returns a dictionary of node ID to http_proxy actor handles."""
        return self.http_state.get_http_proxy_handles()

    def get_http_proxy_names(self) -> bytes:
        """Returns the http_proxy actor name list serialized by protobuf."""
        from ray.serve.generated.serve_pb2 import ActorNameList

        actor_name_list = ActorNameList(
            names=self.http_state.get_http_proxy_names().values()
        )
        return actor_name_list.SerializeToString()

    def autoscale(self) -> None:
        """Updates autoscaling deployments with calculated num_replicas."""
        for deployment_name, (
            deployment_info,
            route_prefix,
        ) in self.list_deployments_internal().items():
            deployment_config = deployment_info.deployment_config
            autoscaling_policy = deployment_info.autoscaling_policy

            if autoscaling_policy is None:
                continue

            replicas = self.deployment_state_manager._deployment_states[
                deployment_name
            ]._replicas
            running_replicas = replicas.get([ReplicaState.RUNNING])

            current_num_ongoing_requests = []
            for replica in running_replicas:
                replica_tag = replica.replica_tag
                num_ongoing_requests = self.autoscaling_metrics_store.window_average(
                    replica_tag,
                    time.time() - autoscaling_policy.config.look_back_period_s,
                )
                if num_ongoing_requests is not None:
                    current_num_ongoing_requests.append(num_ongoing_requests)

            if len(current_num_ongoing_requests) == 0:
                continue

            new_deployment_config = deployment_config.copy()

            decision_num_replicas = autoscaling_policy.get_decision_num_replicas(
                current_num_ongoing_requests=current_num_ongoing_requests,
                curr_target_num_replicas=deployment_config.num_replicas,
            )
            new_deployment_config.num_replicas = decision_num_replicas

            new_deployment_info = copy(deployment_info)
            new_deployment_info.deployment_config = new_deployment_config

            self.deployment_state_manager.deploy(deployment_name, new_deployment_info)

    async def run_control_loop(self) -> None:
        # NOTE(edoakes): we catch all exceptions here and simply log them,
        # because an unhandled exception would cause the main control loop to
        # halt, which should *never* happen.
        while True:
            try:
                self.autoscale()
            except Exception:
                logger.exception("Exception in autoscaling.")

            async with self.write_lock:
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
            entry["class_name"] = deployment_info.replica_config.func_or_class_name
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

    def _all_running_replicas(self) -> Dict[str, List[RunningReplicaInfo]]:
        """Used for testing."""
        return self.deployment_state_manager.get_running_replica_infos()

    def get_http_config(self):
        """Return the HTTP proxy configuration."""
        return self.http_state.get_config()

    def get_root_url(self):
        """Return the root url for the serve instance."""
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
            self.deployment_state_manager.shutdown()
            self.endpoint_state.shutdown()
            self.http_state.shutdown()

    def deploy(
        self,
        name: str,
        deployment_config_proto_bytes: bytes,
        replica_config_proto_bytes: bytes,
        route_prefix: Optional[str],
        deployer_job_id: "ray._raylet.JobID",
    ) -> bool:
        if route_prefix is not None:
            assert route_prefix.startswith("/")

        deployment_config = DeploymentConfig.from_proto_bytes(
            deployment_config_proto_bytes
        )
        version = deployment_config.version
        prev_version = deployment_config.prev_version
        replica_config = ReplicaConfig.from_proto_bytes(
            replica_config_proto_bytes, deployment_config.deployment_language
        )

        if prev_version is not None:
            existing_deployment_info = self.deployment_state_manager.get_deployment(
                name
            )
            if existing_deployment_info is None or not existing_deployment_info.version:
                raise ValueError(
                    f"prev_version '{prev_version}' is specified but "
                    "there is no existing deployment."
                )
            if existing_deployment_info.version != prev_version:
                raise ValueError(
                    f"prev_version '{prev_version}' "
                    "does not match with the existing "
                    f"version '{existing_deployment_info.version}'."
                )

        autoscaling_config = deployment_config.autoscaling_config
        if autoscaling_config is not None:
            # TODO: is this the desired behaviour? Should this be a setting?
            deployment_config.num_replicas = autoscaling_config.min_replicas

            autoscaling_policy = BasicAutoscalingPolicy(autoscaling_config)
        else:
            autoscaling_policy = None

        deployment_info = DeploymentInfo(
            actor_name=name,
            serialized_deployment_def=replica_config.serialized_deployment_def,
            version=version,
            deployment_config=deployment_config,
            replica_config=replica_config,
            deployer_job_id=deployer_job_id,
            start_time_ms=int(time.time() * 1000),
            autoscaling_policy=autoscaling_policy,
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

    def delete_deployment(self, name: str):
        self.endpoint_state.delete_endpoint(name)
        return self.deployment_state_manager.delete_deployment(name)

    def delete_deployments(self, names: Iterable[str]) -> None:
        for name in names:
            self.delete_deployment(name)

    def get_deployment_info(self, name: str) -> bytes:
        """Get the current information about a deployment.

        Args:
            name(str): the name of the deployment.

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
            include_deleted(bool): Whether to include information about
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
            include_deleted(bool): Whether to include information about
                deployments that have been deleted.

        Returns:
            DeploymentRouteList's protobuf serialized bytes
        """
        from ray.serve.generated.serve_pb2 import DeploymentRouteList, DeploymentRoute

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

    def get_deployment_statuses(self) -> bytes:
        """Gets the current status information about all deployments.

        Returns:
            DeploymentStatusInfoList's protobuf serialized bytes
        """
        from ray.serve.generated.serve_pb2 import DeploymentStatusInfoList

        deployment_status_info_list = DeploymentStatusInfoList()
        for (
            name,
            deployment_status_info,
        ) in self.deployment_state_manager.get_deployment_statuses().items():
            deployment_status_info_proto = deployment_status_info.to_proto()
            deployment_status_info_proto.name = name
            deployment_status_info_list.deployment_status_infos.append(
                deployment_status_info_proto
            )
        return deployment_status_info_list.SerializeToString()

import asyncio
import json
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Any

import ray
from ray.actor import ActorHandle
from ray.serve.async_goal_manager import AsyncGoalManager
from ray.serve.backend_state import ReplicaState, BackendStateManager
from ray.serve.backend_worker import create_backend_replica
from ray.serve.common import (
    BackendInfo,
    BackendTag,
    EndpointTag,
    EndpointInfo,
    GoalId,
    NodeId,
    ReplicaTag,
)
from ray.serve.config import BackendConfig, HTTPOptions, ReplicaConfig
from ray.serve.constants import CONTROL_LOOP_PERIOD_S
from ray.serve.endpoint_state import EndpointState
from ray.serve.http_state import HTTPState
from ray.serve.storage.kv_store import RayInternalKVStore
from ray.serve.long_poll import LongPollHost
from ray.serve.utils import logger

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

    async def __init__(self,
                       controller_name: str,
                       http_config: HTTPOptions,
                       detached: bool = False):
        # Used to read/write checkpoints.
        controller_namespace = ray.get_runtime_context().namespace
        self.kv_store = RayInternalKVStore(
            namespace=f"{controller_name}-{controller_namespace}")

        # Dictionary of backend_tag -> proxy_name -> most recent queue length.
        self.backend_stats = defaultdict(lambda: defaultdict(dict))

        # Used to ensure that only a single state-changing operation happens
        # at any given time.
        self.write_lock = asyncio.Lock()

        self.long_poll_host = LongPollHost()

        self.goal_manager = AsyncGoalManager()
        self.http_state = HTTPState(controller_name, detached, http_config)
        self.endpoint_state = EndpointState(self.kv_store, self.long_poll_host)
        self.backend_state_manager = BackendStateManager(
            controller_name, detached, self.kv_store, self.long_poll_host,
            self.goal_manager)

        asyncio.get_event_loop().create_task(self.run_control_loop())

    async def wait_for_goal(self, goal_id: GoalId) -> Optional[Exception]:
        return await self.goal_manager.wait_for_goal(goal_id)

    async def _num_pending_goals(self) -> int:
        return self.goal_manager.num_pending_goals()

    async def listen_for_change(self, keys_to_snapshot_ids: Dict[str, int]):
        """Proxy long pull client's listen request.

        Args:
            keys_to_snapshot_ids (Dict[str, int]): Snapshot IDs are used to
              determine whether or not the host should immediately return the
              data or wait for the value to be changed.
        """
        return await (
            self.long_poll_host.listen_for_change(keys_to_snapshot_ids))

    def get_all_endpoints(self) -> Dict[EndpointTag, Dict[BackendTag, Any]]:
        """Returns a dictionary of backend tag to backend config."""
        return self.endpoint_state.get_endpoints()

    def get_http_proxies(self) -> Dict[NodeId, ActorHandle]:
        """Returns a dictionary of node ID to http_proxy actor handles."""
        return self.http_state.get_http_proxy_handles()

    async def run_control_loop(self) -> None:
        while True:
            async with self.write_lock:
                try:
                    self.http_state.update()
                except Exception as e:
                    logger.error(f"Exception updating HTTP state: {e}")
                try:
                    self.backend_state_manager.update()
                except Exception as e:
                    logger.error(f"Exception updating backend state: {e}")
            self._put_serve_snapshot()
            await asyncio.sleep(CONTROL_LOOP_PERIOD_S)

    def _put_serve_snapshot(self) -> None:
        val = dict()
        for deployment_name, (backend_info,
                              route_prefix) in self.list_deployments(
                                  include_deleted=True).items():
            entry = dict()
            entry["name"] = deployment_name
            entry["namespace"] = ray.get_runtime_context().namespace
            entry["ray_job_id"] = ("None"
                                   if backend_info.deployer_job_id is None else
                                   backend_info.deployer_job_id.hex())
            entry[
                "class_name"] = backend_info.replica_config.func_or_class_name
            entry["version"] = backend_info.version or "None"
            # TODO(architkulkarni): When we add the feature to allow
            # deployments with no HTTP route, update the below line.
            # Or refactor the route_prefix logic in the Deployment class.
            entry["http_route"] = route_prefix or f"/{deployment_name}"
            entry["start_time"] = backend_info.start_time_ms
            entry["end_time"] = backend_info.end_time_ms or 0
            entry["status"] = ("DELETED"
                               if backend_info.end_time_ms else "RUNNING")
            entry["actors"] = dict()
            if entry["status"] == "RUNNING":
                replicas = self.backend_state_manager._backend_states[
                    deployment_name]._replicas
                running_replicas = replicas.get([ReplicaState.RUNNING])
                for replica in running_replicas:
                    try:
                        actor_handle = replica.actor_handle
                    except ValueError:
                        # Actor died or hasn't yet been created.
                        continue
                    actor_id = actor_handle._ray_actor_id.hex()
                    replica_tag = replica.replica_tag
                    replica_version = ("None" if replica.version.unversioned
                                       else replica.version.code_version)
                    entry["actors"][actor_id] = {
                        "replica_tag": replica_tag,
                        "version": replica_version
                    }

            val[deployment_name] = entry
        self.kv_store.put(SNAPSHOT_KEY, json.dumps(val).encode("utf-8"))

    def _all_replica_handles(
            self) -> Dict[BackendTag, Dict[ReplicaTag, ActorHandle]]:
        """Used for testing."""
        return self.backend_state_manager.get_running_replica_handles()

    def get_http_config(self):
        """Return the HTTP proxy configuration."""
        return self.http_state.get_config()

    async def shutdown(self) -> List[GoalId]:
        """Shuts down the serve instance completely."""
        async with self.write_lock:
            goal_ids = self.backend_state_manager.shutdown()
            self.endpoint_state.shutdown()
            self.http_state.shutdown()

            return goal_ids

    async def deploy(self,
                     name: str,
                     backend_config: BackendConfig,
                     replica_config: ReplicaConfig,
                     python_methods: List[str],
                     version: Optional[str],
                     prev_version: Optional[str],
                     route_prefix: Optional[str],
                     deployer_job_id: "Optional[ray._raylet.JobID]" = None
                     ) -> Tuple[Optional[GoalId], bool]:
        if route_prefix is not None:
            assert route_prefix.startswith("/")

        async with self.write_lock:
            if prev_version is not None:
                existing_backend_info = self.backend_state_manager.get_backend(
                    name)
                if (existing_backend_info is None
                        or not existing_backend_info.version):
                    raise ValueError(
                        f"prev_version '{prev_version}' is specified but "
                        "there is no existing deployment.")
                if existing_backend_info.version != prev_version:
                    raise ValueError(
                        f"prev_version '{prev_version}' "
                        "does not match with the existing "
                        f"version '{existing_backend_info.version}'.")
            backend_info = BackendInfo(
                actor_def=ray.remote(
                    create_backend_replica(
                        name, replica_config.serialized_backend_def)),
                version=version,
                backend_config=backend_config,
                replica_config=replica_config,
                deployer_job_id=deployer_job_id,
                start_time_ms=int(time.time() * 1000))

            goal_id, updating = self.backend_state_manager.deploy_backend(
                name, backend_info)
            endpoint_info = EndpointInfo(
                route=route_prefix, python_methods=python_methods)
            self.endpoint_state.update_endpoint(name, endpoint_info)
            return goal_id, updating

    def delete_deployment(self, name: str) -> Optional[GoalId]:
        self.endpoint_state.delete_endpoint(name)
        return self.backend_state_manager.delete_backend(
            name, force_kill=False)

    def get_deployment_info(self, name: str) -> Tuple[BackendInfo, str]:
        """Get the current information about a deployment.

        Args:
            name(str): the name of the deployment.

        Returns:
            (BackendInfo, route)

        Raises:
            KeyError if the deployment doesn't exist.
        """
        backend_info: BackendInfo = self.backend_state_manager.get_backend(
            name)
        if backend_info is None:
            raise KeyError(f"Deployment {name} does not exist.")

        route = self.endpoint_state.get_endpoint_route(name)

        return backend_info, route

    def list_deployments(self, include_deleted: Optional[bool] = False
                         ) -> Dict[str, Tuple[BackendInfo, str]]:
        """Gets the current information about all deployments.

        Args:
            include_deleted(bool): Whether to include information about
                deployments that have been deleted.

        Returns:
            Dict(deployment_name, (BackendInfo, route))

        Raises:
            KeyError if the deployment doesn't exist.
        """
        return {
            name: (self.backend_state_manager.get_backend(
                name, include_deleted=include_deleted),
                   self.endpoint_state.get_endpoint_route(name))
            for name in self.backend_state_manager.get_backend_configs(
                include_deleted=include_deleted)
        }

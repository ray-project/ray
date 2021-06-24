import asyncio
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

import ray
from ray.actor import ActorHandle
from ray.serve.async_goal_manager import AsyncGoalManager
from ray.serve.backend_state import BackendState
from ray.serve.backend_worker import create_backend_replica
from ray.serve.common import (
    BackendInfo,
    BackendTag,
    EndpointInfo,
    EndpointTag,
    GoalId,
    NodeId,
    ReplicaTag,
    TrafficPolicy,
)
from ray.serve.config import BackendConfig, HTTPOptions, ReplicaConfig
from ray.serve.constants import (
    ALL_HTTP_METHODS,
    RESERVED_VERSION_TAG,
)
from ray.serve.endpoint_state import EndpointState
from ray.serve.http_state import HTTPState
from ray.serve.kv_store import RayInternalKVStore
from ray.serve.long_poll import LongPollHost
from ray.serve.utils import logger

# Used for testing purposes only. If this is set, the controller will crash
# after writing each checkpoint with the specified probability.
_CRASH_AFTER_CHECKPOINT_PROBABILITY = 0

# How often to call the control loop on the controller.
CONTROL_LOOP_PERIOD_S = 0.1


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
        self.kv_store = RayInternalKVStore(namespace=controller_name)

        # Dictionary of backend_tag -> proxy_name -> most recent queue length.
        self.backend_stats = defaultdict(lambda: defaultdict(dict))

        # Used to ensure that only a single state-changing operation happens
        # at any given time.
        self.write_lock = asyncio.Lock()

        self.long_poll_host = LongPollHost()

        self.goal_manager = AsyncGoalManager()
        self.http_state = HTTPState(controller_name, detached, http_config)
        self.endpoint_state = EndpointState(self.kv_store, self.long_poll_host)
        self.backend_state = BackendState(controller_name, detached,
                                          self.kv_store, self.long_poll_host,
                                          self.goal_manager)

        asyncio.get_event_loop().create_task(self.run_control_loop())

    async def wait_for_goal(self, goal_id: GoalId) -> None:
        await self.goal_manager.wait_for_goal(goal_id)

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
                    self.backend_state.update()
                except Exception as e:
                    logger.error(f"Exception updating backend state: {e}")

            await asyncio.sleep(CONTROL_LOOP_PERIOD_S)

    def _all_replica_handles(
            self) -> Dict[BackendTag, Dict[ReplicaTag, ActorHandle]]:
        """Used for testing."""
        return self.backend_state.get_running_replica_handles()

    def get_all_backends(self) -> Dict[BackendTag, BackendConfig]:
        """Returns a dictionary of backend tag to backend config."""
        return self.backend_state.get_backend_configs()

    def get_all_endpoints(self) -> Dict[EndpointTag, Dict[BackendTag, Any]]:
        """Returns a dictionary of backend tag to backend config."""
        return self.endpoint_state.get_endpoints()

    def _validate_traffic_dict(self, traffic_dict: Dict[str, float]):
        for backend in traffic_dict:
            if self.backend_state.get_backend(backend) is None:
                raise ValueError(
                    "Attempted to assign traffic to a backend '{}' that "
                    "is not registered.".format(backend))

    async def set_traffic(self, endpoint: str,
                          traffic_dict: Dict[str, float]) -> None:
        """Sets the traffic policy for the specified endpoint."""
        async with self.write_lock:
            self._validate_traffic_dict(traffic_dict)

            logger.info("Setting traffic for endpoint "
                        f"'{endpoint}' to '{traffic_dict}'.")

            self.endpoint_state.set_traffic_policy(endpoint,
                                                   TrafficPolicy(traffic_dict))

    async def shadow_traffic(self, endpoint_name: str, backend_tag: BackendTag,
                             proportion: float) -> None:
        """Shadow traffic from the endpoint to the backend."""
        async with self.write_lock:
            if self.backend_state.get_backend(backend_tag) is None:
                raise ValueError(
                    "Attempted to shadow traffic to a backend '{}' that "
                    "is not registered.".format(backend_tag))

            logger.info(
                "Shadowing '{}' of traffic to endpoint '{}' to backend '{}'.".
                format(proportion, endpoint_name, backend_tag))

            self.endpoint_state.shadow_traffic(endpoint_name, backend_tag,
                                               proportion)

    async def create_endpoint(
            self,
            endpoint: str,
            traffic_dict: Dict[str, float],
            route: Optional[str],
            methods: Set[str],
    ) -> None:
        """Create a new endpoint with the specified route and methods.

        If the route is None, this is a "headless" endpoint that will not
        be exposed over HTTP and can only be accessed via a handle.
        """
        async with self.write_lock:
            self._validate_traffic_dict(traffic_dict)

            logger.info(
                "Registering route '{}' to endpoint '{}' with methods '{}'.".
                format(route, endpoint, methods))

            self.endpoint_state.create_endpoint(
                endpoint, EndpointInfo(methods, route=route),
                TrafficPolicy(traffic_dict))

        # TODO(simon): Use GoalID mechanism for this so client can check for
        # goal id and http_state complete the goal id.
        await self.http_state.ensure_http_route_exists(endpoint, timeout_s=30)

    async def delete_endpoint(self, endpoint: str) -> None:
        """Delete the specified endpoint.

        Does not modify any corresponding backends.
        """
        logger.info("Deleting endpoint '{}'".format(endpoint))
        async with self.write_lock:
            self.endpoint_state.delete_endpoint(endpoint)

    async def create_backend(
            self, backend_tag: BackendTag, backend_config: BackendConfig,
            replica_config: ReplicaConfig) -> Optional[GoalId]:
        """Register a new backend under the specified tag."""
        async with self.write_lock:
            backend_info = BackendInfo(
                actor_def=ray.remote(
                    create_backend_replica(
                        backend_tag, replica_config.serialized_backend_def)),
                version=RESERVED_VERSION_TAG,
                backend_config=backend_config,
                replica_config=replica_config)
            goal_id, _ = self.backend_state.deploy_backend(
                backend_tag, backend_info)
            return goal_id

    async def delete_backend(self,
                             backend_tag: BackendTag,
                             force_kill: bool = False) -> Optional[GoalId]:
        async with self.write_lock:
            # Check that the specified backend isn't used by any endpoints.
            for endpoint, info in self.endpoint_state.get_endpoints().items():
                if (backend_tag in info["traffic"]
                        or backend_tag in info["shadows"]):
                    raise ValueError(f"Backend '{backend_tag}' is used by "
                                     f"endpoint '{endpoint}' and cannot be "
                                     "deleted. Please remove the backend "
                                     "from all endpoints and try again.")
            return self.backend_state.delete_backend(backend_tag, force_kill)

    async def update_backend_config(self, backend_tag: BackendTag,
                                    config_options: BackendConfig) -> GoalId:
        """Set the config for the specified backend."""
        async with self.write_lock:
            existing_info = self.backend_state.get_backend(backend_tag)
            if existing_info is None:
                raise ValueError(f"Backend {backend_tag} is not registered.")

            backend_info = BackendInfo(
                actor_def=existing_info.actor_def,
                version=existing_info.version,
                backend_config=existing_info.backend_config.copy(
                    update=config_options.dict(exclude_unset=True)),
                replica_config=existing_info.replica_config)
            goal_id, _ = self.backend_state.deploy_backend(
                backend_tag, backend_info)
            return goal_id

    def get_backend_config(self, backend_tag: BackendTag) -> BackendConfig:
        """Get the current config for the specified backend."""
        if self.backend_state.get_backend(backend_tag) is None:
            raise ValueError(f"Backend {backend_tag} is not registered.")
        return self.backend_state.get_backend(backend_tag).backend_config

    def get_http_config(self):
        """Return the HTTP proxy configuration."""
        return self.http_state.get_config()

    async def shutdown(self) -> List[GoalId]:
        """Shuts down the serve instance completely."""
        async with self.write_lock:
            goal_ids = self.backend_state.shutdown()
            self.endpoint_state.shutdown()
            self.http_state.shutdown()

            return goal_ids

    async def deploy(
            self, name: str, backend_config: BackendConfig,
            replica_config: ReplicaConfig, python_methods: List[str],
            version: Optional[str], prev_version: Optional[str],
            route_prefix: Optional[str]) -> Tuple[Optional[GoalId], bool]:
        if route_prefix is not None:
            assert route_prefix.startswith("/")

        async with self.write_lock:
            if prev_version is not None:
                existing_backend_info = self.backend_state.get_backend(name)
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
                replica_config=replica_config)

            goal_id, updating = self.backend_state.deploy_backend(
                name, backend_info)
            endpoint_info = EndpointInfo(
                ALL_HTTP_METHODS,
                route=route_prefix,
                python_methods=python_methods,
                legacy=False)
            self.endpoint_state.update_endpoint(name, endpoint_info,
                                                TrafficPolicy({
                                                    name: 1.0
                                                }))
            return goal_id, updating

    def delete_deployment(self, name: str) -> Optional[GoalId]:
        self.endpoint_state.delete_endpoint(name)
        return self.backend_state.delete_backend(name, force_kill=False)

    def get_deployment_info(self, name: str) -> Tuple[BackendInfo, str]:
        """Get the current information about a deployment.

        Args:
            name(str): the name of the deployment.

        Returns:
            (BackendInfo, route)

        Raises:
            KeyError if the deployment doesn't exist.
        """
        backend_info: BackendInfo = self.backend_state.get_backend(name)
        if backend_info is None:
            raise KeyError(f"Deployment {name} does not exist.")

        route = self.endpoint_state.get_endpoint_route(name)

        return backend_info, route

    def list_deployments(self) -> Dict[str, Tuple[BackendInfo, str]]:
        """Gets the current information about all active deployments."""
        return {
            name: (self.backend_state.get_backend(name),
                   self.endpoint_state.get_endpoint_route(name))
            for name in self.backend_state.get_backend_configs()
        }

import asyncio
import os
import random
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
from uuid import uuid4, UUID

import ray
import ray.cloudpickle as pickle
from ray.actor import ActorHandle
from ray.serve.backend_state import BackendState
from ray.serve.backend_worker import create_backend_replica
from ray.serve.constants import LongPollKey
from ray.serve.common import (
    BackendInfo,
    BackendTag,
    EndpointTag,
    GoalId,
    NodeId,
    ReplicaTag,
    TrafficPolicy,
)
from ray.serve.config import BackendConfig, HTTPOptions, ReplicaConfig
from ray.serve.endpoint_state import EndpointState
from ray.serve.exceptions import RayServeException
from ray.serve.http_state import HTTPState
from ray.serve.kv_store import RayInternalKVStore
from ray.serve.long_poll import LongPollHost
from ray.serve.utils import logger

# Used for testing purposes only. If this is set, the controller will crash
# after writing each checkpoint with the specified probability.
_CRASH_AFTER_CHECKPOINT_PROBABILITY = 0
CHECKPOINT_KEY = "serve-controller-checkpoint"

# How often to call the control loop on the controller.
CONTROL_LOOP_PERIOD_S = 1.0


@dataclass
class FutureResult:
    # Goal requested when this future was created
    requested_goal: Dict[str, Any]


@dataclass
class Checkpoint:
    backend_state_checkpoint: bytes
    # TODO(ilr) Rename reconciler to PendingState
    inflight_reqs: Dict[uuid4, FutureResult]


@ray.remote
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

        # Map of awaiting results
        # TODO(ilr): Checkpoint this once this becomes asynchronous
        self.inflight_results: Dict[UUID, asyncio.Event] = dict()
        self._serializable_inflight_results: Dict[UUID, FutureResult] = dict()

        # NOTE(simon): Currently we do all-to-all broadcast. This means
        # any listeners will receive notification for all changes. This
        # can be problem at scale, e.g. updating a single backend config
        # will send over the entire configs. In the future, we should
        # optimize the logic to support subscription by key.
        self.long_poll_host = LongPollHost()

        self.http_state = HTTPState(controller_name, detached, http_config)
        self.endpoint_state = EndpointState(self.kv_store, self.long_poll_host)

        checkpoint_bytes = self.kv_store.get(CHECKPOINT_KEY)
        if checkpoint_bytes is None:
            logger.debug("No checkpoint found")
            self.backend_state = BackendState(controller_name, detached)
        else:
            checkpoint: Checkpoint = pickle.loads(checkpoint_bytes)
            self.backend_state = BackendState(
                controller_name,
                detached,
                checkpoint=checkpoint.backend_state_checkpoint)

            self._serializable_inflight_results = checkpoint.inflight_reqs
            for uuid, fut_result in self._serializable_inflight_results.items(
            ):
                self._create_event_with_result(fut_result.requested_goal, uuid)

        self.notify_backend_configs_changed()
        self.notify_replica_handles_changed()

        asyncio.get_event_loop().create_task(self.run_control_loop())

    async def wait_for_event(self, uuid: UUID) -> bool:
        start = time.time()
        if uuid not in self.inflight_results:
            logger.debug(f"UUID ({uuid}) not found!!!")
            return True
        event = self.inflight_results[uuid]
        await event.wait()
        self.inflight_results.pop(uuid)
        self._serializable_inflight_results.pop(uuid)
        async with self.write_lock:
            self._checkpoint()
        logger.debug(f"Waiting for {uuid} took {time.time() - start} seconds")

        return True

    def _create_event_with_result(
            self,
            goal_state: Dict[str, any],
            recreation_uuid: Optional[UUID] = None) -> UUID:
        # NOTE(ilr) Must be called before checkpointing!
        event = asyncio.Event()
        event.result = FutureResult(goal_state)
        uuid_val = recreation_uuid or uuid4()
        self.inflight_results[uuid_val] = event
        self._serializable_inflight_results[uuid_val] = event.result
        return uuid_val

    async def _num_inflight_results(self) -> int:
        return len(self.inflight_results)

    def notify_replica_handles_changed(self):
        self.long_poll_host.notify_changed(
            LongPollKey.REPLICA_HANDLES, {
                backend_tag: list(replica_dict.values())
                for backend_tag, replica_dict in
                self.backend_state.backend_replicas.items()
            })

    def notify_backend_configs_changed(self):
        self.long_poll_host.notify_changed(
            LongPollKey.BACKEND_CONFIGS,
            self.backend_state.get_backend_configs())

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

    def _checkpoint(self) -> None:
        """Checkpoint internal state and write it to the KV store."""
        assert self.write_lock.locked()
        logger.debug("Writing checkpoint")
        start = time.time()

        checkpoint = pickle.dumps(
            Checkpoint(self.backend_state.checkpoint(),
                       self._serializable_inflight_results))

        self.kv_store.put(CHECKPOINT_KEY, checkpoint)
        logger.debug("Wrote checkpoint in {:.3f}s".format(time.time() - start))

        if random.random(
        ) < _CRASH_AFTER_CHECKPOINT_PROBABILITY and self.detached:
            logger.warning("Intentionally crashing after checkpoint")
            os._exit(0)

    async def reconcile_current_and_goal_backends(self):
        pass

    def set_goal_id(self, goal_id: UUID) -> None:
        event = self.inflight_results.get(goal_id)
        logger.debug(f"Setting goal id {goal_id}")
        if event:
            event.set()

    async def run_control_loop(self) -> None:
        while True:
            async with self.write_lock:
                self.http_state.update()

                completed_ids = self.backend_state.completed_goals()
                for done_id in completed_ids:
                    self.set_goal_id(done_id)
                delta_workers = await self.backend_state.update()
                if delta_workers:
                    self.notify_replica_handles_changed()
                    self._checkpoint()

            await asyncio.sleep(CONTROL_LOOP_PERIOD_S)

    def _all_replica_handles(
            self) -> Dict[BackendTag, Dict[ReplicaTag, ActorHandle]]:
        """Used for testing."""
        return self.backend_state.get_replica_handles()

    def get_all_backends(self) -> Dict[BackendTag, BackendConfig]:
        """Returns a dictionary of backend tag to backend config."""
        return self.backend_state.get_backend_configs()

    def get_all_endpoints(self) -> Dict[EndpointTag, Dict[BackendTag, Any]]:
        """Returns a dictionary of backend tag to backend config."""
        return self.endpoint_state.get_endpoints()

    def _set_traffic(self, endpoint_name: str,
                     traffic_dict: Dict[str, float]) -> UUID:
        for backend in traffic_dict:
            if self.backend_state.get_backend(backend) is None:
                raise ValueError(
                    "Attempted to assign traffic to a backend '{}' that "
                    "is not registered.".format(backend))

        self.endpoint_state.set_traffic_policy(endpoint_name,
                                               TrafficPolicy(traffic_dict))

    def _validate_traffic_dict(self, traffic_dict: Dict[str, float]):
        for backend in traffic_dict:
            if self.backend_state.get_backend(backend) is None:
                raise ValueError(
                    "Attempted to assign traffic to a backend '{}' that "
                    "is not registered.".format(backend))

    async def set_traffic(self, endpoint_name: str,
                          traffic_dict: Dict[str, float]) -> None:
        """Sets the traffic policy for the specified endpoint."""
        async with self.write_lock:
            self._validate_traffic_dict(traffic_dict)
            self._set_traffic(endpoint_name, traffic_dict)

    async def shadow_traffic(self, endpoint_name: str, backend_tag: BackendTag,
                             proportion: float) -> UUID:
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

    # TODO(architkulkarni): add Optional for route after cloudpickle upgrade
    async def create_endpoint(self, endpoint: str,
                              traffic_dict: Dict[str, float], route,
                              methods: List[str]) -> UUID:
        """Create a new endpoint with the specified route and methods.

        If the route is None, this is a "headless" endpoint that will not
        be exposed over HTTP and can only be accessed via a handle.
        """
        async with self.write_lock:
            self._validate_traffic_dict(traffic_dict)

            logger.info(
                "Registering route '{}' to endpoint '{}' with methods '{}'.".
                format(route, endpoint, methods))

            self.endpoint_state.create_endpoint(endpoint, route, methods,
                                                TrafficPolicy(traffic_dict))

    async def delete_endpoint(self, endpoint: str) -> None:
        """Delete the specified endpoint.

        Does not modify any corresponding backends.
        """
        logger.info("Deleting endpoint '{}'".format(endpoint))
        async with self.write_lock:
            self.endpoint_state.delete_endpoint(endpoint)

    async def set_backend_goal(self, backend_tag: BackendTag,
                               backend_info: BackendInfo,
                               new_id: GoalId) -> None:
        # NOTE(ilr) Must checkpoint after doing this!
        existing_id_to_set = self.backend_state._set_backend_goal(
            backend_tag, backend_info, new_id)
        if existing_id_to_set:
            self.set_goal_id(existing_id_to_set)

    async def create_backend(self, backend_tag: BackendTag,
                             backend_config: BackendConfig,
                             replica_config: ReplicaConfig) -> UUID:
        """Register a new backend under the specified tag."""
        async with self.write_lock:
            # Ensures this method is idempotent.
            backend_info = self.backend_state.get_backend(backend_tag)
            if backend_info is not None:
                if (backend_info.backend_config == backend_config
                        and backend_info.replica_config == replica_config):
                    return

            backend_replica = create_backend_replica(
                replica_config.func_or_class)

            # Save creator that starts replicas, the arguments to be passed in,
            # and the configuration for the backends.
            backend_info = BackendInfo(
                worker_class=backend_replica,
                backend_config=backend_config,
                replica_config=replica_config)

            return_uuid = self._create_event_with_result({
                backend_tag: backend_info
            })

            await self.set_backend_goal(backend_tag, backend_info, return_uuid)

            try:
                # This call should be to run control loop
                self.backend_state.scale_backend_replicas(
                    backend_tag, backend_config.num_replicas)
            except RayServeException as e:
                del self.backend_state.backends[backend_tag]
                raise e

            # NOTE(edoakes): we must write a checkpoint before starting new
            # or pushing the updated config to avoid inconsistent state if we
            # crash while making the change.
            self._checkpoint()
            self.notify_backend_configs_changed()
            return return_uuid

    async def delete_backend(self,
                             backend_tag: BackendTag,
                             force_kill: bool = False) -> UUID:
        async with self.write_lock:
            # This method must be idempotent. We should validate that the
            # specified backend exists on the client.
            if self.backend_state.get_backend(backend_tag) is None:
                return

            # Check that the specified backend isn't used by any endpoints.
            for endpoint, info in self.endpoint_state.get_endpoints().items():
                if (backend_tag in info["traffic"]
                        or backend_tag in info["shadows"]):
                    raise ValueError("Backend '{}' is used by endpoint '{}' "
                                     "and cannot be deleted. Please remove "
                                     "the backend from all endpoints and try "
                                     "again.".format(backend_tag, endpoint))

            # Scale its replicas down to 0.
            self.backend_state.scale_backend_replicas(backend_tag, 0,
                                                      force_kill)

            # Remove the backend's metadata.
            del self.backend_state.backends[backend_tag]

            # Add the intention to remove the backend from the routers.
            self.backend_state.backends_to_remove.append(backend_tag)

            return_uuid = self._create_event_with_result({backend_tag: None})
            # Remove the backend's metadata.
            await self.set_backend_goal(backend_tag, None, return_uuid)
            # NOTE(edoakes): we must write a checkpoint before removing the
            # backend from the routers to avoid inconsistent state if we crash
            # after pushing the update.
            self._checkpoint()
            return return_uuid

    async def update_backend_config(self, backend_tag: BackendTag,
                                    config_options: BackendConfig) -> UUID:
        """Set the config for the specified backend."""
        async with self.write_lock:
            assert (self.backend_state.get_backend(backend_tag)
                    ), "Backend {} is not registered.".format(backend_tag)
            assert isinstance(config_options, BackendConfig)

            stored_backend_config = self.backend_state.get_backend(
                backend_tag).backend_config
            backend_config = stored_backend_config.copy(
                update=config_options.dict(exclude_unset=True))
            backend_config._validate_complete()
            self.backend_state.get_backend(
                backend_tag).backend_config = backend_config
            backend_info = self.backend_state.get_backend(backend_tag)

            return_uuid = self._create_event_with_result({
                backend_tag: backend_info
            })
            await self.set_backend_goal(backend_tag, backend_info, return_uuid)

            # Scale the replicas with the new configuration.

            # This should be to run the control loop
            self.backend_state.scale_backend_replicas(
                backend_tag, backend_config.num_replicas)

            # NOTE(edoakes): we must write a checkpoint before pushing the
            # update to avoid inconsistent state if we crash after pushing the
            # update.
            self._checkpoint()

            # Inform the routers and backend replicas about config changes.
            self.notify_backend_configs_changed()

            return return_uuid

    def get_backend_config(self, backend_tag: BackendTag) -> BackendConfig:
        """Get the current config for the specified backend."""
        assert (self.backend_state.get_backend(backend_tag)
                ), "Backend {} is not registered.".format(backend_tag)
        return self.backend_state.get_backend(backend_tag).backend_config

    def get_http_config(self):
        """Return the HTTP proxy configuration."""
        return self.http_state.get_config()

    async def shutdown(self) -> None:
        """Shuts down the serve instance completely."""
        async with self.write_lock:
            for proxy in self.http_state.get_http_proxy_handles().values():
                ray.kill(proxy, no_restart=True)
            for replica_dict in self.backend_state.get_replica_handles(
            ).values():
                for replica in replica_dict.values():
                    ray.kill(replica, no_restart=True)
            self.kv_store.delete(CHECKPOINT_KEY)

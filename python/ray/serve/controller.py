import asyncio
from collections import defaultdict
from itertools import chain
import os
import random
import time
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Tuple
from pydantic import BaseModel

import ray
import ray.cloudpickle as pickle
from ray.serve.autoscaling_policy import BasicAutoscalingPolicy
from ray.serve.backend_worker import create_backend_replica
from ray.serve.constants import ASYNC_CONCURRENCY, SERVE_PROXY_NAME
from ray.serve.http_proxy import HTTPProxyActor
from ray.serve.kv_store import RayInternalKVStore
from ray.serve.exceptions import RayServeException
from ray.serve.utils import (format_actor_name, get_random_letters, logger,
                             try_schedule_resources_on_nodes, get_all_node_ids)
from ray.serve.config import BackendConfig, ReplicaConfig
from ray.serve.long_poll import LongPollerHost
from ray.actor import ActorHandle

import numpy as np

# Used for testing purposes only. If this is set, the controller will crash
# after writing each checkpoint with the specified probability.
_CRASH_AFTER_CHECKPOINT_PROBABILITY = 0
CHECKPOINT_KEY = "serve-controller-checkpoint"

# Feature flag for controller resource checking. If true, controller will
# error if the desired replicas exceed current resource availability.
_RESOURCE_CHECK_ENABLED = True

# How often to call the control loop on the controller.
CONTROL_LOOP_PERIOD_S = 1.0

REPLICA_STARTUP_TIME_WARNING_S = 5

# TypeDefs
BackendTag = str
EndpointTag = str
ReplicaTag = str
NodeId = str
GoalId = int


class TrafficPolicy:
    def __init__(self, traffic_dict: Dict[str, float]) -> None:
        self.traffic_dict: Dict[str, float] = dict()
        self.shadow_dict: Dict[str, float] = dict()
        self.set_traffic_dict(traffic_dict)

    def set_traffic_dict(self, traffic_dict: Dict[str, float]) -> None:
        prob = 0
        for backend, weight in traffic_dict.items():
            if weight < 0:
                raise ValueError(
                    "Attempted to assign a weight of {} to backend '{}'. "
                    "Weights cannot be negative.".format(weight, backend))
            prob += weight

        # These weights will later be plugged into np.random.choice, which
        # uses a tolerance of 1e-8.
        if not np.isclose(prob, 1, atol=1e-8):
            raise ValueError("Traffic dictionary weights must sum to 1, "
                             "currently they sum to {}".format(prob))
        self.traffic_dict = traffic_dict

    def set_shadow(self, backend: str, proportion: float):
        if proportion == 0 and backend in self.shadow_dict:
            del self.shadow_dict[backend]
        else:
            self.shadow_dict[backend] = proportion

    def __repr__(self) -> str:
        return f"<Traffic {self.traffic_dict}; Shadow {self.shadow_dict}>"


class BackendInfo(BaseModel):
    # TODO(architkulkarni): Add type hint for worker_class after upgrading
    # cloudpickle and adding types to RayServeWrappedReplica
    worker_class: Any
    backend_config: BackendConfig
    replica_config: ReplicaConfig

    class Config:
        # TODO(architkulkarni): Remove once ReplicaConfig is a pydantic
        # model
        arbitrary_types_allowed = True


@dataclass
class SystemState:
    backends: Dict[BackendTag, BackendInfo] = field(default_factory=dict)
    traffic_policies: Dict[EndpointTag, TrafficPolicy] = field(
        default_factory=dict)
    routes: Dict[BackendTag, Tuple[EndpointTag, Any]] = field(
        default_factory=dict)

    backend_goal_ids: Dict[BackendTag, GoalId] = field(default_factory=dict)
    traffic_goal_ids: Dict[EndpointTag, GoalId] = field(default_factory=dict)
    route_goal_ids: Dict[BackendTag, GoalId] = field(default_factory=dict)

    def get_backend_configs(self) -> Dict[BackendTag, BackendConfig]:
        return {
            tag: info.backend_config
            for tag, info in self.backends.items()
        }

    def get_backend(self, backend_tag: BackendTag) -> Optional[BackendInfo]:
        return self.backends.get(backend_tag)

    def add_backend(self,
                    backend_tag: BackendTag,
                    backend_info: BackendInfo,
                    goal_id: GoalId = 0) -> None:
        self.backends[backend_tag] = backend_info
        self.backend_goal_ids = goal_id

    def get_endpoints(self) -> Dict[EndpointTag, Dict[str, Any]]:
        endpoints = {}
        for route, (endpoint, methods) in self.routes.items():
            if endpoint in self.traffic_policies:
                traffic_policy = self.traffic_policies[endpoint]
                traffic_dict = traffic_policy.traffic_dict
                shadow_dict = traffic_policy.shadow_dict
            else:
                traffic_dict = {}
                shadow_dict = {}

            endpoints[endpoint] = {
                "route": route if route.startswith("/") else None,
                "methods": methods,
                "traffic": traffic_dict,
                "shadows": shadow_dict,
            }
        return endpoints


@dataclass
class ActorStateReconciler:
    controller_name: str = field(init=True)
    detached: bool = field(init=True)

    routers_cache: Dict[NodeId, ActorHandle] = field(default_factory=dict)
    backend_replicas: Dict[BackendTag, Dict[ReplicaTag, ActorHandle]] = field(
        default_factory=lambda: defaultdict(dict))
    backend_replicas_to_start: Dict[BackendTag, List[ReplicaTag]] = field(
        default_factory=lambda: defaultdict(list))
    backend_replicas_to_stop: Dict[BackendTag, List[ReplicaTag]] = field(
        default_factory=lambda: defaultdict(list))
    backends_to_remove: List[BackendTag] = field(default_factory=list)
    endpoints_to_remove: List[EndpointTag] = field(default_factory=list)

    # TODO(edoakes): consider removing this and just using the names.

    def router_handles(self) -> List[ActorHandle]:
        return list(self.routers_cache.values())

    def get_replica_handles(self) -> List[ActorHandle]:
        return list(
            chain.from_iterable([
                replica_dict.values()
                for replica_dict in self.backend_replicas.values()
            ]))

    def get_replica_tags(self) -> List[ReplicaTag]:
        return list(
            chain.from_iterable([
                replica_dict.keys()
                for replica_dict in self.backend_replicas.values()
            ]))

    async def _start_pending_backend_replicas(
            self, current_state: SystemState) -> None:
        """Starts the pending backend replicas in self.backend_replicas_to_start.

        Waits for replicas to start up, then removes them from
        self.backend_replicas_to_start.
        """
        fut_to_replica_info = {}
        for backend_tag, replicas_to_create in self.backend_replicas_to_start.\
                items():
            for replica_tag in replicas_to_create:
                replica_handle = await self._start_backend_replica(
                    current_state, backend_tag, replica_tag)
                ready_future = replica_handle.ready.remote().as_future()
                fut_to_replica_info[ready_future] = (backend_tag, replica_tag,
                                                     replica_handle)

        start = time.time()
        prev_warning = start
        while fut_to_replica_info:
            if time.time() - prev_warning > REPLICA_STARTUP_TIME_WARNING_S:
                prev_warning = time.time()
                logger.warning("Waited {:.2f}s for replicas to start up. Make "
                               "sure there are enough resources to create the "
                               "replicas.".format(time.time() - start))

            done, pending = await asyncio.wait(
                list(fut_to_replica_info.keys()), timeout=1)
            for fut in done:
                (backend_tag, replica_tag,
                 replica_handle) = fut_to_replica_info.pop(fut)
                self.backend_replicas[backend_tag][
                    replica_tag] = replica_handle

        self.backend_replicas_to_start.clear()

    async def _start_backend_replica(self, current_state: SystemState,
                                     backend_tag: BackendTag,
                                     replica_tag: ReplicaTag) -> ActorHandle:
        """Start a replica and return its actor handle.

        Checks if the named actor already exists before starting a new one.

        Assumes that the backend configuration is already in the Goal State.
        """
        # NOTE(edoakes): the replicas may already be created if we
        # failed after creating them but before writing a
        # checkpoint.
        replica_name = format_actor_name(replica_tag, self.controller_name)
        try:
            replica_handle = ray.get_actor(replica_name)
        except ValueError:
            logger.debug("Starting replica '{}' for backend '{}'.".format(
                replica_tag, backend_tag))
            backend_info = current_state.get_backend(backend_tag)

            replica_handle = ray.remote(backend_info.worker_class).options(
                name=replica_name,
                lifetime="detached" if self.detached else None,
                max_restarts=-1,
                max_task_retries=-1,
                **backend_info.replica_config.ray_actor_options).remote(
                    backend_tag, replica_tag,
                    backend_info.replica_config.actor_init_args,
                    backend_info.backend_config, self.controller_name)

        return replica_handle

    def _scale_backend_replicas(self, backends: Dict[BackendTag, BackendInfo],
                                backend_tag: BackendTag,
                                num_replicas: int) -> None:
        """Scale the given backend to the number of replicas.

        NOTE: this does not actually start or stop the replicas, but instead
        adds the intention to start/stop them to self.backend_replicas_to_start
        and self.backend_replicas_to_stop. The caller is responsible for then
        first writing a checkpoint and then actually starting/stopping the
        intended replicas. This avoids inconsistencies with starting/stopping a
        replica and then crashing before writing a checkpoint.
        """
        logger.debug("Scaling backend '{}' to {} replicas".format(
            backend_tag, num_replicas))
        assert (backend_tag in backends
                ), "Backend {} is not registered.".format(backend_tag)
        assert num_replicas >= 0, ("Number of replicas must be"
                                   " greater than or equal to 0.")

        current_num_replicas = len(self.backend_replicas[backend_tag])
        delta_num_replicas = num_replicas - current_num_replicas

        backend_info = backends[backend_tag]
        if delta_num_replicas > 0:
            can_schedule = try_schedule_resources_on_nodes(requirements=[
                backend_info.replica_config.resource_dict
                for _ in range(delta_num_replicas)
            ])

            if _RESOURCE_CHECK_ENABLED and not all(can_schedule):
                num_possible = sum(can_schedule)
                raise RayServeException(
                    "Cannot scale backend {} to {} replicas. Ray Serve tried "
                    "to add {} replicas but the resources only allows {} "
                    "to be added. To fix this, consider scaling to replica to "
                    "{} or add more resources to the cluster. You can check "
                    "avaiable resources with ray.nodes().".format(
                        backend_tag, num_replicas, delta_num_replicas,
                        num_possible, current_num_replicas + num_possible))

            logger.debug("Adding {} replicas to backend {}".format(
                delta_num_replicas, backend_tag))
            for _ in range(delta_num_replicas):
                replica_tag = "{}#{}".format(backend_tag, get_random_letters())
                self.backend_replicas_to_start[backend_tag].append(replica_tag)

        elif delta_num_replicas < 0:
            logger.debug("Removing {} replicas from backend '{}'".format(
                -delta_num_replicas, backend_tag))
            assert len(
                self.backend_replicas[backend_tag]) >= delta_num_replicas
            for _ in range(-delta_num_replicas):
                replica_tag, _ = self.backend_replicas[backend_tag].popitem()
                if len(self.backend_replicas[backend_tag]) == 0:
                    del self.backend_replicas[backend_tag]

                self.backend_replicas_to_stop[backend_tag].append(replica_tag)

    async def _stop_pending_backend_replicas(self) -> None:
        """Stops the pending backend replicas in self.backend_replicas_to_stop.

        Removes backend_replicas from the router, kills them, and clears
        self.backend_replicas_to_stop.
        """
        for backend_tag, replicas_list in self.backend_replicas_to_stop.items(
        ):
            for replica_tag in replicas_list:
                # NOTE(edoakes): the replicas may already be stopped if we
                # failed after stopping them but before writing a checkpoint.
                replica_name = format_actor_name(replica_tag,
                                                 self.controller_name)
                try:
                    replica = ray.get_actor(replica_name)
                except ValueError:
                    continue

                # TODO(edoakes): this logic isn't ideal because there may be
                # pending tasks still executing on the replica. However, if we
                # use replica.__ray_terminate__, we may send it while the
                # replica is being restarted and there's no way to tell if it
                # successfully killed the worker or not.
                ray.kill(replica, no_restart=True)

        self.backend_replicas_to_stop.clear()

    def _start_routers_if_needed(self, http_host: str, http_port: str,
                                 http_middlewares: List[Any]) -> None:
        """Start a router on every node if it doesn't already exist."""
        if http_host is None:
            return

        for node_id, node_resource in get_all_node_ids():
            if node_id in self.routers_cache:
                continue

            router_name = format_actor_name(SERVE_PROXY_NAME,
                                            self.controller_name, node_id)
            try:
                router = ray.get_actor(router_name)
            except ValueError:
                logger.info("Starting router with name '{}' on node '{}' "
                            "listening on '{}:{}'".format(
                                router_name, node_id, http_host, http_port))
                router = HTTPProxyActor.options(
                    name=router_name,
                    lifetime="detached" if self.detached else None,
                    max_concurrency=ASYNC_CONCURRENCY,
                    max_restarts=-1,
                    max_task_retries=-1,
                    resources={
                        node_resource: 0.01
                    },
                ).remote(
                    http_host,
                    http_port,
                    controller_name=self.controller_name,
                    http_middlewares=http_middlewares)

            self.routers_cache[node_id] = router

    def _stop_routers_if_needed(self) -> bool:
        """Removes router actors from any nodes that no longer exist.

        Returns whether or not any actors were removed (a checkpoint should
        be taken).
        """
        actor_stopped = False
        all_node_ids = {node_id for node_id, _ in get_all_node_ids()}
        to_stop = []
        for node_id in self.routers_cache:
            if node_id not in all_node_ids:
                logger.info(
                    "Removing router on removed node '{}'.".format(node_id))
                to_stop.append(node_id)

        for node_id in to_stop:
            router_handle = self.routers_cache.pop(node_id)
            ray.kill(router_handle, no_restart=True)
            actor_stopped = True

        return actor_stopped

    def _recover_actor_handles(self) -> None:
        # Refresh the RouterCache
        for node_id in self.routers_cache.keys():
            router_name = format_actor_name(SERVE_PROXY_NAME,
                                            self.controller_name, node_id)
            self.routers_cache[node_id] = ray.get_actor(router_name)

        # Fetch actor handles for all of the backend replicas in the system.
        # All of these backend_replicas are guaranteed to already exist because
        #  they would not be written to a checkpoint in self.backend_replicas
        # until they were created.
        for backend_tag, replica_dict in self.backend_replicas.items():
            for replica_tag in replica_dict.keys():
                replica_name = format_actor_name(replica_tag,
                                                 self.controller_name)
                self.backend_replicas[backend_tag][
                    replica_tag] = ray.get_actor(replica_name)

    async def _recover_from_checkpoint(
            self, current_state: SystemState, controller: "ServeController"
    ) -> Dict[BackendTag, BasicAutoscalingPolicy]:
        self._recover_actor_handles()
        autoscaling_policies = dict()

        for backend, info in current_state.backends.items():
            metadata = info.backend_config.internal_metadata
            if metadata.autoscaling_config is not None:
                autoscaling_policies[backend] = BasicAutoscalingPolicy(
                    backend, metadata.autoscaling_config)

        # Start/stop any pending backend replicas.
        await self._start_pending_backend_replicas(current_state)
        await self._stop_pending_backend_replicas()

        return autoscaling_policies


@dataclass
class Checkpoint:
    goal_state: SystemState
    current_state: SystemState
    reconciler: ActorStateReconciler
    # TODO(ilr) Rename reconciler to PendingState


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
                       http_host: str,
                       http_port: str,
                       http_middlewares: List[Any],
                       detached: bool = False):
        # Used to read/write checkpoints.
        self.kv_store = RayInternalKVStore(namespace=controller_name)
        # Current State
        self.current_state = SystemState()
        # Goal State
        # TODO(ilr) This is currently *unused* until the refactor of the serve
        # controller.
        self.goal_state = SystemState()
        # ActorStateReconciler
        self.actor_reconciler = ActorStateReconciler(controller_name, detached)

        # backend -> AutoscalingPolicy
        self.autoscaling_policies = dict()

        # Dictionary of backend_tag -> router_name -> most recent queue length.
        self.backend_stats = defaultdict(lambda: defaultdict(dict))

        # Used to ensure that only a single state-changing operation happens
        # at any given time.
        self.write_lock = asyncio.Lock()

        self.http_host = http_host
        self.http_port = http_port
        self.http_middlewares = http_middlewares

        # If starting the actor for the first time, starts up the other system
        # components. If recovering, fetches their actor handles.
        self.actor_reconciler._start_routers_if_needed(
            self.http_host, self.http_port, self.http_middlewares)

        # NOTE(edoakes): unfortunately, we can't completely recover from a
        # checkpoint in the constructor because we block while waiting for
        # other actors to start up, and those actors fetch soft state from
        # this actor. Because no other tasks will start executing until after
        # the constructor finishes, if we were to run this logic in the
        # constructor it could lead to deadlock between this actor and a child.
        # However we do need to guarantee that we have fully recovered from a
        # checkpoint before any other state-changing calls run. We address this
        # by acquiring the write_lock and then posting the task to recover from
        # a checkpoint to the event loop. Other state-changing calls acquire
        # this lock and will be blocked until recovering from the checkpoint
        # finishes.
        checkpoint = self.kv_store.get(CHECKPOINT_KEY)
        if checkpoint is None:
            logger.debug("No checkpoint found")
        else:
            await self.write_lock.acquire()
            asyncio.get_event_loop().create_task(
                self._recover_from_checkpoint(checkpoint))

        # NOTE(simon): Currently we do all-to-all broadcast. This means
        # any listeners will receive notification for all changes. This
        # can be problem at scale, e.g. updating a single backend config
        # will send over the entire configs. In the future, we should
        # optimize the logic to support subscription by key.
        self.long_poll_host = LongPollerHost()
        self.notify_backend_configs_changed()
        self.notify_replica_handles_changed()
        self.notify_traffic_policies_changed()

        asyncio.get_event_loop().create_task(self.run_control_loop())

    def notify_replica_handles_changed(self):
        self.long_poll_host.notify_changed(
            "worker_handles", {
                backend_tag: list(replica_dict.values())
                for backend_tag, replica_dict in
                self.actor_reconciler.backend_replicas.items()
            })

    def notify_traffic_policies_changed(self):
        self.long_poll_host.notify_changed("traffic_policies",
                                           self.current_state.traffic_policies)

    def notify_backend_configs_changed(self):
        self.long_poll_host.notify_changed(
            "backend_configs", self.current_state.get_backend_configs())

    async def listen_for_change(self, keys_to_snapshot_ids: Dict[str, int]):
        """Proxy long pull client's listen request.

        Args:
            keys_to_snapshot_ids (Dict[str, int]): Snapshot IDs are used to
              determine whether or not the host should immediately return the
              data or wait for the value to be changed.
        """
        return await (
            self.long_poll_host.listen_for_change(keys_to_snapshot_ids))

    def get_routers(self) -> Dict[str, ActorHandle]:
        """Returns a dictionary of node ID to router actor handles."""
        return self.actor_reconciler.routers_cache

    def get_router_config(self) -> Dict[str, Tuple[str, List[str]]]:
        """Called by the router on startup to fetch required state."""
        return self.current_state.routes

    def _checkpoint(self) -> None:
        """Checkpoint internal state and write it to the KV store."""
        assert self.write_lock.locked()
        logger.debug("Writing checkpoint")
        start = time.time()

        checkpoint = pickle.dumps(
            Checkpoint(self.goal_state, self.current_state,
                       self.actor_reconciler))

        self.kv_store.put(CHECKPOINT_KEY, checkpoint)
        logger.debug("Wrote checkpoint in {:.2f}".format(time.time() - start))

        if random.random(
        ) < _CRASH_AFTER_CHECKPOINT_PROBABILITY and self.detached:
            logger.warning("Intentionally crashing after checkpoint")
            os._exit(0)

    async def _recover_from_checkpoint(self, checkpoint_bytes: bytes) -> None:
        """Recover the instance state from the provided checkpoint.

        Performs the following operations:
            1) Deserializes the internal state from the checkpoint.
            2) Pushes the latest configuration to the routers
               in case we crashed before updating them.
            3) Starts/stops any replicas that are pending creation or
               deletion.

        NOTE: this requires that self.write_lock is already acquired and will
        release it before returning.
        """
        assert self.write_lock.locked()

        start = time.time()
        logger.info("Recovering from checkpoint")

        restored_checkpoint: Checkpoint = pickle.loads(checkpoint_bytes)
        # Restore SystemState
        self.current_state = restored_checkpoint.current_state

        # Restore ActorStateReconciler
        self.actor_reconciler = restored_checkpoint.reconciler

        self.autoscaling_policies = await self.actor_reconciler.\
            _recover_from_checkpoint(self.current_state, self)

        logger.info(
            "Recovered from checkpoint in {:.3f}s".format(time.time() - start))

        self.write_lock.release()

    async def do_autoscale(self) -> None:
        for backend, info in self.current_state.backends.items():
            if backend not in self.autoscaling_policies:
                continue

            new_num_replicas = self.autoscaling_policies[backend].scale(
                self.backend_stats[backend], info.backend_config.num_replicas)
            if new_num_replicas > 0:
                await self.update_backend_config(
                    backend, BackendConfig(num_replicas=new_num_replicas))

    async def run_control_loop(self) -> None:
        while True:
            await self.do_autoscale()
            async with self.write_lock:
                self.actor_reconciler._start_routers_if_needed(
                    self.http_host, self.http_port, self.http_middlewares)
                checkpoint_required = self.actor_reconciler.\
                    _stop_routers_if_needed()
                if checkpoint_required:
                    self._checkpoint()

            await asyncio.sleep(CONTROL_LOOP_PERIOD_S)

    def get_backend_configs(self) -> Dict[str, BackendConfig]:
        """Fetched by the router on startup."""
        return self.current_state.get_backend_configs()

    def get_traffic_policies(self) -> Dict[str, TrafficPolicy]:
        """Fetched by the router on startup."""
        return self.current_state.traffic_policies

    def _list_replicas(self, backend_tag: BackendTag) -> List[ReplicaTag]:
        """Used only for testing."""
        return list(self.actor_reconciler.backend_replicas[backend_tag].keys())

    def get_traffic_policy(self, endpoint: str) -> TrafficPolicy:
        """Fetched by serve handles."""
        return self.current_state.traffic_policies[endpoint]

    def get_all_replica_handles(self) -> Dict[str, Dict[str, ActorHandle]]:
        """Fetched by the router on startup."""
        return self.actor_reconciler.backend_replicas

    def get_all_backends(self) -> Dict[str, BackendConfig]:
        """Returns a dictionary of backend tag to backend config."""
        return self.current_state.get_backend_configs()

    def get_all_endpoints(self) -> Dict[str, Dict[str, Any]]:
        return self.current_state.get_endpoints()

    async def _set_traffic(self, endpoint_name: str,
                           traffic_dict: Dict[str, float]) -> None:
        if endpoint_name not in self.current_state.get_endpoints():
            raise ValueError("Attempted to assign traffic for an endpoint '{}'"
                             " that is not registered.".format(endpoint_name))

        assert isinstance(traffic_dict,
                          dict), "Traffic policy must be a dictionary."

        for backend in traffic_dict:
            if self.current_state.get_backend(backend) is None:
                raise ValueError(
                    "Attempted to assign traffic to a backend '{}' that "
                    "is not registered.".format(backend))

        traffic_policy = TrafficPolicy(traffic_dict)
        self.current_state.traffic_policies[endpoint_name] = traffic_policy

        # NOTE(edoakes): we must write a checkpoint before pushing the
        # update to avoid inconsistent state if we crash after pushing the
        # update.
        self._checkpoint()

        self.notify_traffic_policies_changed()

    async def set_traffic(self, endpoint_name: str,
                          traffic_dict: Dict[str, float]) -> None:
        """Sets the traffic policy for the specified endpoint."""
        async with self.write_lock:
            await self._set_traffic(endpoint_name, traffic_dict)

    async def shadow_traffic(self, endpoint_name: str, backend_tag: BackendTag,
                             proportion: float) -> None:
        """Shadow traffic from the endpoint to the backend."""
        async with self.write_lock:
            if endpoint_name not in self.current_state.get_endpoints():
                raise ValueError("Attempted to shadow traffic from an "
                                 "endpoint '{}' that is not registered."
                                 .format(endpoint_name))

            if self.current_state.get_backend(backend_tag) is None:
                raise ValueError(
                    "Attempted to shadow traffic to a backend '{}' that "
                    "is not registered.".format(backend_tag))

            self.current_state.traffic_policies[endpoint_name].set_shadow(
                backend_tag, proportion)

            # NOTE(edoakes): we must write a checkpoint before pushing the
            # update to avoid inconsistent state if we crash after pushing the
            # update.
            self._checkpoint()
            self.notify_traffic_policies_changed()

    # TODO(architkulkarni): add Optional for route after cloudpickle upgrade
    async def create_endpoint(self, endpoint: str,
                              traffic_dict: Dict[str, float], route,
                              methods) -> None:
        """Create a new endpoint with the specified route and methods.

        If the route is None, this is a "headless" endpoint that will not
        be exposed over HTTP and can only be accessed via a handle.
        """
        async with self.write_lock:
            # If this is a headless endpoint with no route, key the endpoint
            # based on its name.
            # TODO(edoakes): we should probably just store routes and endpoints
            # separately.
            if route is None:
                route = endpoint

            # TODO(edoakes): move this to client side.
            err_prefix = "Cannot create endpoint."
            if route in self.current_state.routes:

                # Ensures this method is idempotent
                if self.current_state.routes[route] == (endpoint, methods):
                    return

                else:
                    raise ValueError(
                        "{} Route '{}' is already registered.".format(
                            err_prefix, route))

            if endpoint in self.current_state.get_endpoints():
                raise ValueError(
                    "{} Endpoint '{}' is already registered.".format(
                        err_prefix, endpoint))

            logger.info(
                "Registering route '{}' to endpoint '{}' with methods '{}'.".
                format(route, endpoint, methods))

            self.current_state.routes[route] = (endpoint, methods)

            # NOTE(edoakes): checkpoint is written in self._set_traffic.
            await self._set_traffic(endpoint, traffic_dict)
            await asyncio.gather(*[
                router.set_route_table.remote(self.current_state.routes)
                for router in self.actor_reconciler.router_handles()
            ])

    async def delete_endpoint(self, endpoint: str) -> None:
        """Delete the specified endpoint.

        Does not modify any corresponding backends.
        """
        logger.info("Deleting endpoint '{}'".format(endpoint))
        async with self.write_lock:
            # This method must be idempotent. We should validate that the
            # specified endpoint exists on the client.
            for route, (route_endpoint,
                        _) in self.current_state.routes.items():
                if route_endpoint == endpoint:
                    route_to_delete = route
                    break
            else:
                logger.info("Endpoint '{}' doesn't exist".format(endpoint))
                return

            # Remove the routing entry.
            del self.current_state.routes[route_to_delete]

            # Remove the traffic policy entry if it exists.
            if endpoint in self.current_state.traffic_policies:
                del self.current_state.traffic_policies[endpoint]

            self.actor_reconciler.endpoints_to_remove.append(endpoint)

            # NOTE(edoakes): we must write a checkpoint before pushing the
            # updates to the routers to avoid inconsistent state if we crash
            # after pushing the update.
            self._checkpoint()

            await asyncio.gather(*[
                router.set_route_table.remote(self.current_state.routes)
                for router in self.actor_reconciler.router_handles()
            ])

    async def create_backend(self, backend_tag: BackendTag,
                             backend_config: BackendConfig,
                             replica_config: ReplicaConfig) -> None:
        """Register a new backend under the specified tag."""
        async with self.write_lock:
            # Ensures this method is idempotent.
            backend_info = self.current_state.get_backend(backend_tag)
            if backend_info is not None:
                if (backend_info.backend_config == backend_config
                        and backend_info.replica_config == replica_config):
                    return

            backend_replica = create_backend_replica(
                replica_config.func_or_class)

            # Save creator that starts replicas, the arguments to be passed in,
            # and the configuration for the backends.
            self.current_state.add_backend(
                backend_tag,
                BackendInfo(
                    worker_class=backend_replica,
                    backend_config=backend_config,
                    replica_config=replica_config))
            metadata = backend_config.internal_metadata
            if metadata.autoscaling_config is not None:
                self.autoscaling_policies[
                    backend_tag] = BasicAutoscalingPolicy(
                        backend_tag, metadata.autoscaling_config)

            try:
                self.actor_reconciler._scale_backend_replicas(
                    self.current_state.backends, backend_tag,
                    backend_config.num_replicas)
            except RayServeException as e:
                del self.current_state.backends[backend_tag]
                raise e

            # NOTE(edoakes): we must write a checkpoint before starting new
            # or pushing the updated config to avoid inconsistent state if we
            # crash while making the change.
            self._checkpoint()
            await self.actor_reconciler._start_pending_backend_replicas(
                self.current_state)

            self.notify_replica_handles_changed()

            # Set the backend config inside the router
            # (particularly for max_concurrent_queries).
            self.notify_backend_configs_changed()

    async def delete_backend(self, backend_tag: BackendTag) -> None:
        async with self.write_lock:
            # This method must be idempotent. We should validate that the
            # specified backend exists on the client.
            if self.current_state.get_backend(backend_tag) is None:
                return

            # Check that the specified backend isn't used by any endpoints.
            for endpoint, traffic_policy in self.current_state.\
                    traffic_policies.items():
                if (backend_tag in traffic_policy.traffic_dict
                        or backend_tag in traffic_policy.shadow_dict):
                    raise ValueError("Backend '{}' is used by endpoint '{}' "
                                     "and cannot be deleted. Please remove "
                                     "the backend from all endpoints and try "
                                     "again.".format(backend_tag, endpoint))

            # Scale its replicas down to 0. This will also remove the backend
            # from self.current_state.backends and
            # self.actor_reconciler.backend_replicas.
            self.actor_reconciler._scale_backend_replicas(
                self.current_state.backends, backend_tag, 0)

            # Remove the backend's metadata.
            del self.current_state.backends[backend_tag]
            if backend_tag in self.autoscaling_policies:
                del self.autoscaling_policies[backend_tag]

            # Add the intention to remove the backend from the router.
            self.actor_reconciler.backends_to_remove.append(backend_tag)

            # NOTE(edoakes): we must write a checkpoint before removing the
            # backend from the router to avoid inconsistent state if we crash
            # after pushing the update.
            self._checkpoint()
            await self.actor_reconciler._stop_pending_backend_replicas()

            self.notify_replica_handles_changed()

    async def update_backend_config(self, backend_tag: BackendTag,
                                    config_options: BackendConfig) -> None:
        """Set the config for the specified backend."""
        async with self.write_lock:
            assert (self.current_state.get_backend(backend_tag)
                    ), "Backend {} is not registered.".format(backend_tag)
            assert isinstance(config_options, BackendConfig)

            stored_backend_config = self.current_state.get_backend(
                backend_tag).backend_config
            backend_config = stored_backend_config.copy(
                update=config_options.dict(exclude_unset=True))
            backend_config._validate_complete()
            self.current_state.get_backend(
                backend_tag).backend_config = backend_config

            # Scale the replicas with the new configuration.
            self.actor_reconciler._scale_backend_replicas(
                self.current_state.backends, backend_tag,
                backend_config.num_replicas)

            # NOTE(edoakes): we must write a checkpoint before pushing the
            # update to avoid inconsistent state if we crash after pushing the
            # update.
            self._checkpoint()

            # Inform the router about change in configuration
            # (particularly for setting max_batch_size).

            await self.actor_reconciler._start_pending_backend_replicas(
                self.current_state)
            await self.actor_reconciler._stop_pending_backend_replicas()

            self.notify_replica_handles_changed()
            self.notify_backend_configs_changed()

    def get_backend_config(self, backend_tag: BackendTag) -> BackendConfig:
        """Get the current config for the specified backend."""
        assert (self.current_state.get_backend(backend_tag)
                ), "Backend {} is not registered.".format(backend_tag)
        return self.current_state.get_backend(backend_tag).backend_config

    async def shutdown(self) -> None:
        """Shuts down the serve instance completely."""
        async with self.write_lock:
            for router in self.actor_reconciler.router_handles():
                ray.kill(router, no_restart=True)
            for replica in self.actor_reconciler.get_replica_handles():
                ray.kill(replica, no_restart=True)
            self.kv_store.delete(CHECKPOINT_KEY)

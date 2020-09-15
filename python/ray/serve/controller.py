import asyncio
from collections import defaultdict
import os
import random
import time
from typing import Union, Dict, Any, List, Tuple
from pydantic import BaseModel

import ray
import ray.cloudpickle as pickle
from ray.serve.autoscaling_policy import BasicAutoscalingPolicy
from ray.serve.backend_worker import create_backend_worker
from ray.serve.constants import ASYNC_CONCURRENCY, SERVE_PROXY_NAME
from ray.serve.http_proxy import HTTPProxyActor
from ray.serve.kv_store import RayInternalKVStore
from ray.serve.exceptions import RayServeException
from ray.serve.utils import (format_actor_name, get_random_letters, logger,
                             try_schedule_resources_on_nodes, get_all_node_ids)
from ray.serve.config import BackendConfig, ReplicaConfig
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


class TrafficPolicy:
    def __init__(self, traffic_dict: Dict[str, float]) -> None:
        self.traffic_dict = dict()
        self.shadow_dict = dict()
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


class BackendInfo(BaseModel):
    # TODO(architkulkarni): Add type hint for worker_class after upgrading
    # cloudpickle and adding types to RayServeWrappedWorker
    worker_class: Any
    backend_config: BackendConfig
    replica_config: ReplicaConfig

    class Config:
        # TODO(architkulkarni): Remove once ReplicaConfig is a pydantic
        # model
        arbitrary_types_allowed = True


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
        self.detached = detached
        # Name of this controller actor.
        self.controller_name = controller_name
        # Used to read/write checkpoints.
        self.kv_store = RayInternalKVStore(namespace=controller_name)
        # path -> (endpoint, methods).
        self.routes = dict()
        # backend -> BackendInfo.
        self.backends = dict()
        # backend -> AutoscalingPolicy
        self.autoscaling_policies = dict()
        # backend -> replica_tags.
        self.replicas = defaultdict(list)
        # replicas that should be started if recovering from a checkpoint.
        self.replicas_to_start = defaultdict(list)
        # replicas that should be stopped if recovering from a checkpoint.
        self.replicas_to_stop = defaultdict(list)
        # backends that should be removed from the router if recovering from a
        # checkpoint.
        self.backends_to_remove = list()
        # endpoints that should be removed from the router if recovering from a
        # checkpoint.
        self.endpoints_to_remove = list()
        # endpoint -> TrafficPolicy
        self.traffic_policies = dict()
        # Dictionary of backend tag to dictionaries of replica tag to worker.
        # TODO(edoakes): consider removing this and just using the names.
        self.workers = defaultdict(dict)
        # Dictionary of backend_tag -> router_name -> most recent queue length.
        self.backend_stats = defaultdict(lambda: defaultdict(dict))

        # Used to ensure that only a single state-changing operation happens
        # at any given time.
        self.write_lock = asyncio.Lock()

        # Cached handles to actors in the system.
        # node_id -> actor_handle
        self.routers = dict()

        self.http_host = http_host
        self.http_port = http_port
        self.http_middlewares = http_middlewares

        # If starting the actor for the first time, starts up the other system
        # components. If recovering, fetches their actor handles.
        self._start_routers_if_needed()

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

        asyncio.get_event_loop().create_task(self.run_control_loop())

    def _start_routers_if_needed(self) -> None:
        """Start a router on every node if it doesn't already exist."""
        for node_id, node_resource in get_all_node_ids():
            if node_id in self.routers:
                continue

            router_name = format_actor_name(SERVE_PROXY_NAME,
                                            self.controller_name, node_id)
            try:
                router = ray.get_actor(router_name)
            except ValueError:
                logger.info("Starting router with name '{}' on node '{}' "
                            "listening on '{}:{}'".format(
                                router_name, node_id, self.http_host,
                                self.http_port))
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
                    node_id,
                    self.http_host,
                    self.http_port,
                    controller_name=self.controller_name,
                    http_middlewares=self.http_middlewares)

            self.routers[node_id] = router

    def _stop_routers_if_needed(self) -> bool:
        """Removes router actors from any nodes that no longer exist.

        Returns whether or not any actors were removed (a checkpoint should
        be taken).
        """
        checkpoint_required = False
        all_node_ids = {node_id for node_id, _ in get_all_node_ids()}
        to_stop = []
        for node_id in self.routers:
            if node_id not in all_node_ids:
                logger.info(
                    "Removing router on removed node '{}'.".format(node_id))
                to_stop.append(node_id)

        for node_id in to_stop:
            router_handle = self.routers.pop(node_id)
            ray.kill(router_handle, no_restart=True)
            checkpoint_required = True

        return checkpoint_required

    def get_routers(self) -> Dict[str, ActorHandle]:
        """Returns a dictionary of node ID to router actor handles."""
        return self.routers

    def get_router_config(self) -> Dict[str, Dict[str, Tuple[str, List[str]]]]:
        """Called by the router on startup to fetch required state."""
        return self.routes

    def _checkpoint(self) -> None:
        """Checkpoint internal state and write it to the KV store."""
        assert self.write_lock.locked()
        logger.debug("Writing checkpoint")
        start = time.time()
        checkpoint = pickle.dumps(
            (self.routes, list(
                self.routers.keys()), self.backends, self.traffic_policies,
             self.replicas, self.replicas_to_start, self.replicas_to_stop,
             self.backends_to_remove, self.endpoints_to_remove))

        self.kv_store.put(CHECKPOINT_KEY, checkpoint)
        logger.debug("Wrote checkpoint in {:.2f}".format(time.time() - start))

        if random.random() < _CRASH_AFTER_CHECKPOINT_PROBABILITY:
            logger.warning("Intentionally crashing after checkpoint")
            os._exit(0)

    async def _recover_from_checkpoint(self, checkpoint_bytes: bytes) -> None:
        """Recover the instance state from the provided checkpoint.

        Performs the following operations:
            1) Deserializes the internal state from the checkpoint.
            2) Pushes the latest configuration to the routers
               in case we crashed before updating them.
            3) Starts/stops any worker replicas that are pending creation or
               deletion.

        NOTE: this requires that self.write_lock is already acquired and will
        release it before returning.
        """
        assert self.write_lock.locked()

        start = time.time()
        logger.info("Recovering from checkpoint")

        # Load internal state from the checkpoint data.
        (
            self.routes,
            router_node_ids,
            self.backends,
            self.traffic_policies,
            self.replicas,
            self.replicas_to_start,
            self.replicas_to_stop,
            self.backends_to_remove,
            self.endpoints_to_remove,
        ) = pickle.loads(checkpoint_bytes)

        for node_id in router_node_ids:
            router_name = format_actor_name(SERVE_PROXY_NAME,
                                            self.controller_name, node_id)
            self.routers[node_id] = ray.get_actor(router_name)

        # Fetch actor handles for all of the backend replicas in the system.
        # All of these workers are guaranteed to already exist because they
        # would not be written to a checkpoint in self.workers until they
        # were created.
        for backend_tag, replica_tags in self.replicas.items():
            for replica_tag in replica_tags:
                replica_name = format_actor_name(replica_tag,
                                                 self.controller_name)
                self.workers[backend_tag][replica_tag] = ray.get_actor(
                    replica_name)

        # Push configuration state to the router.
        # TODO(edoakes): should we make this a pull-only model for simplicity?
        for endpoint, traffic_policy in self.traffic_policies.items():
            await asyncio.gather(*[
                router.set_traffic.remote(endpoint, traffic_policy)
                for router in self.routers.values()
            ])

        for backend_tag, replica_dict in self.workers.items():
            for replica_tag, worker in replica_dict.items():
                await asyncio.gather(*[
                    router.add_new_worker.remote(backend_tag, replica_tag,
                                                 worker)
                    for router in self.routers.values()
                ])

        for backend, info in self.backends.items():
            await asyncio.gather(*[
                router.set_backend_config.remote(backend, info.backend_config)
                for router in self.routers.values()
            ])
            await self.broadcast_backend_config(backend)
            metadata = info.backend_config.internal_metadata
            if metadata.autoscaling_config is not None:
                self.autoscaling_policies[backend] = BasicAutoscalingPolicy(
                    backend, metadata.autoscaling_config)

        # Push configuration state to the routers.
        await asyncio.gather(*[
            router.set_route_table.remote(self.routes)
            for router in self.routers.values()
        ])

        # Start/stop any pending backend replicas.
        await self._start_pending_replicas()
        await self._stop_pending_replicas()

        # Remove any pending backends and endpoints.
        await self._remove_pending_backends()
        await self._remove_pending_endpoints()

        logger.info(
            "Recovered from checkpoint in {:.3f}s".format(time.time() - start))

        self.write_lock.release()

    async def do_autoscale(self) -> None:
        for backend in self.backends:
            if backend not in self.autoscaling_policies:
                continue

            new_num_replicas = self.autoscaling_policies[backend].scale(
                self.backend_stats[backend],
                self.backends[backend].backend_config.num_replicas)
            if new_num_replicas > 0:
                await self.update_backend_config(
                    backend, {"num_replicas": new_num_replicas})

    async def run_control_loop(self) -> None:
        while True:
            await self.do_autoscale()
            async with self.write_lock:
                self._start_routers_if_needed()
                checkpoint_required = self._stop_routers_if_needed()
                if checkpoint_required:
                    self._checkpoint()

            await asyncio.sleep(CONTROL_LOOP_PERIOD_S)

    def get_backend_configs(self) -> Dict[str, BackendConfig]:
        """Fetched by the router on startup."""
        backend_configs = {}
        for backend, info in self.backends.items():
            backend_configs[backend] = info.backend_config
        return backend_configs

    def get_traffic_policies(self) -> Dict[str, TrafficPolicy]:
        """Fetched by the router on startup."""
        return self.traffic_policies

    def _list_replicas(self, backend_tag: str) -> List[str]:
        """Used only for testing."""
        return self.replicas[backend_tag]

    def get_traffic_policy(self, endpoint: str) -> TrafficPolicy:
        """Fetched by serve handles."""
        return self.traffic_policies[endpoint]

    async def _start_backend_worker(self, backend_tag: str, replica_tag: str,
                                    replica_name: str) -> ActorHandle:
        """Creates a backend worker and waits for it to start up.

        Assumes that the backend configuration has already been registered
        in self.backends.
        """
        logger.debug("Starting worker '{}' for backend '{}'.".format(
            replica_tag, backend_tag))
        backend_info = self.backends[backend_tag]

        worker_handle = ray.remote(backend_info.worker_class).options(
            name=replica_name,
            lifetime="detached" if self.detached else None,
            max_restarts=-1,
            max_task_retries=-1,
            **backend_info.replica_config.ray_actor_options).remote(
                backend_tag, replica_tag,
                backend_info.replica_config.actor_init_args,
                backend_info.backend_config, self.controller_name)
        # TODO(edoakes): we should probably have a timeout here.
        await worker_handle.ready.remote()
        return worker_handle

    async def _start_replica(self, backend_tag: str, replica_tag: str) -> None:
        # NOTE(edoakes): the replicas may already be created if we
        # failed after creating them but before writing a
        # checkpoint.
        replica_name = format_actor_name(replica_tag, self.controller_name)
        try:
            worker_handle = ray.get_actor(replica_name)
        except ValueError:
            worker_handle = await self._start_backend_worker(
                backend_tag, replica_tag, replica_name)

        self.replicas[backend_tag].append(replica_tag)
        self.workers[backend_tag][replica_tag] = worker_handle

        # Register the worker with the router.
        await asyncio.gather(*[
            router.add_new_worker.remote(backend_tag, replica_tag,
                                         worker_handle)
            for router in self.routers.values()
        ])

    async def _start_pending_replicas(self) -> None:
        """Starts the pending backend replicas in self.replicas_to_start.

        Starts the worker, then pushes an update to the router to add it to
        the proper backend. If the worker has already been started, only
        updates the router.

        Clears self.replicas_to_start.
        """
        replica_started_futures = []
        for backend_tag, replicas_to_create in self.replicas_to_start.items():
            for replica_tag in replicas_to_create:
                replica_started_futures.append(
                    self._start_replica(backend_tag, replica_tag))

        # Wait on all creation task futures together.
        await asyncio.gather(*replica_started_futures)

        self.replicas_to_start.clear()

    async def _stop_pending_replicas(self) -> None:
        """Stops the pending backend replicas in self.replicas_to_stop.

        Removes workers from the router, kills them, and clears
        self.replicas_to_stop.
        """
        for backend_tag, replicas_to_stop in self.replicas_to_stop.items():
            for replica_tag in replicas_to_stop:
                # NOTE(edoakes): the replicas may already be stopped if we
                # failed after stopping them but before writing a checkpoint.
                replica_name = format_actor_name(replica_tag,
                                                 self.controller_name)
                try:
                    replica = ray.get_actor(replica_name)
                except ValueError:
                    continue

                # Remove the replica from router. This call is idempotent.
                await asyncio.gather(*[
                    router.remove_worker.remote(backend_tag, replica_tag)
                    for router in self.routers.values()
                ])

                # TODO(edoakes): this logic isn't ideal because there may be
                # pending tasks still executing on the replica. However, if we
                # use replica.__ray_terminate__, we may send it while the
                # replica is being restarted and there's no way to tell if it
                # successfully killed the worker or not.
                ray.kill(replica, no_restart=True)

        self.replicas_to_stop.clear()

    async def _remove_pending_backends(self) -> None:
        """Removes the pending backends in self.backends_to_remove.

        Clears self.backends_to_remove.
        """
        for backend_tag in self.backends_to_remove:
            await asyncio.gather(*[
                router.remove_backend.remote(backend_tag)
                for router in self.routers.values()
            ])
        self.backends_to_remove.clear()

    async def _remove_pending_endpoints(self) -> None:
        """Removes the pending endpoints in self.endpoints_to_remove.

        Clears self.endpoints_to_remove.
        """
        for endpoint_tag in self.endpoints_to_remove:
            await asyncio.gather(*[
                router.remove_endpoint.remote(endpoint_tag)
                for router in self.routers.values()
            ])
        self.endpoints_to_remove.clear()

    def _scale_replicas(self, backend_tag: str, num_replicas: int) -> None:
        """Scale the given backend to the number of replicas.

        NOTE: this does not actually start or stop the replicas, but instead
        adds the intention to start/stop them to self.workers_to_start and
        self.workers_to_stop. The caller is responsible for then first writing
        a checkpoint and then actually starting/stopping the intended replicas.
        This avoids inconsistencies with starting/stopping a worker and then
        crashing before writing a checkpoint.
        """
        logger.debug("Scaling backend '{}' to {} replicas".format(
            backend_tag, num_replicas))
        assert (backend_tag in self.backends
                ), "Backend {} is not registered.".format(backend_tag)
        assert num_replicas >= 0, ("Number of replicas must be"
                                   " greater than or equal to 0.")

        current_num_replicas = len(self.replicas[backend_tag])
        delta_num_replicas = num_replicas - current_num_replicas

        backend_info = self.backends[backend_tag]
        if delta_num_replicas > 0:
            can_schedule = try_schedule_resources_on_nodes(
                requirements=[
                    backend_info.replica_config.resource_dict
                    for _ in range(delta_num_replicas)
                ],
                ray_nodes=ray.nodes())
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
                self.replicas_to_start[backend_tag].append(replica_tag)

        elif delta_num_replicas < 0:
            logger.debug("Removing {} replicas from backend '{}'".format(
                -delta_num_replicas, backend_tag))
            assert len(self.replicas[backend_tag]) >= delta_num_replicas
            for _ in range(-delta_num_replicas):
                replica_tag = self.replicas[backend_tag].pop()
                if len(self.replicas[backend_tag]) == 0:
                    del self.replicas[backend_tag]
                del self.workers[backend_tag][replica_tag]
                if len(self.workers[backend_tag]) == 0:
                    del self.workers[backend_tag]

                self.replicas_to_stop[backend_tag].append(replica_tag)

    def get_all_worker_handles(self) -> Dict[str, Dict[str, ActorHandle]]:
        """Fetched by the router on startup."""
        return self.workers

    def get_all_backends(self) -> Dict[str, Dict[str, Any]]:
        """Returns a dictionary of backend tag to backend config dict."""
        backends = {}
        for backend_tag, backend_info in self.backends.items():
            backends[backend_tag] = backend_info.backend_config.__dict__
        return backends

    def get_all_endpoints(self) -> Dict[str, Dict[str, Any]]:
        """Returns a dictionary of endpoint to endpoint config."""
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

    async def _set_traffic(self, endpoint_name: str,
                           traffic_dict: Dict[str, float]) -> None:
        if endpoint_name not in self.get_all_endpoints():
            raise ValueError("Attempted to assign traffic for an endpoint '{}'"
                             " that is not registered.".format(endpoint_name))

        assert isinstance(traffic_dict,
                          dict), "Traffic policy must be a dictionary."

        for backend in traffic_dict:
            if backend not in self.backends:
                raise ValueError(
                    "Attempted to assign traffic to a backend '{}' that "
                    "is not registered.".format(backend))

        traffic_policy = TrafficPolicy(traffic_dict)
        self.traffic_policies[endpoint_name] = traffic_policy

        # NOTE(edoakes): we must write a checkpoint before pushing the
        # update to avoid inconsistent state if we crash after pushing the
        # update.
        self._checkpoint()
        await asyncio.gather(*[
            router.set_traffic.remote(endpoint_name, traffic_policy)
            for router in self.routers.values()
        ])

    async def set_traffic(self, endpoint_name: str,
                          traffic_dict: Dict[str, float]) -> None:
        """Sets the traffic policy for the specified endpoint."""
        async with self.write_lock:
            await self._set_traffic(endpoint_name, traffic_dict)

    async def shadow_traffic(self, endpoint_name: str, backend_tag: str,
                             proportion: float) -> None:
        """Shadow traffic from the endpoint to the backend."""
        async with self.write_lock:
            if endpoint_name not in self.get_all_endpoints():
                raise ValueError("Attempted to shadow traffic from an "
                                 "endpoint '{}' that is not registered."
                                 .format(endpoint_name))

            if backend_tag not in self.backends:
                raise ValueError(
                    "Attempted to shadow traffic to a backend '{}' that "
                    "is not registered.".format(backend_tag))

            self.traffic_policies[endpoint_name].set_shadow(
                backend_tag, proportion)

            # NOTE(edoakes): we must write a checkpoint before pushing the
            # update to avoid inconsistent state if we crash after pushing the
            # update.
            self._checkpoint()
            await asyncio.gather(*[
                router.set_traffic.remote(
                    endpoint_name,
                    self.traffic_policies[endpoint_name],
                ) for router in self.routers.values()
            ])

    # TODO(architkulkarni): add optional type hints after upgrading cloudpickle
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
            if route in self.routes:

                # Ensures this method is idempotent
                if self.routes[route] == (endpoint, methods):
                    return

                else:
                    raise ValueError(
                        "{} Route '{}' is already registered.".format(
                            err_prefix, route))

            if endpoint in self.get_all_endpoints():
                raise ValueError(
                    "{} Endpoint '{}' is already registered.".format(
                        err_prefix, endpoint))

            logger.info(
                "Registering route '{}' to endpoint '{}' with methods '{}'.".
                format(route, endpoint, methods))

            self.routes[route] = (endpoint, methods)

            # NOTE(edoakes): checkpoint is written in self._set_traffic.
            await self._set_traffic(endpoint, traffic_dict)
            await asyncio.gather(*[
                router.set_route_table.remote(self.routes)
                for router in self.routers.values()
            ])

    async def delete_endpoint(self, endpoint: str) -> None:
        """Delete the specified endpoint.

        Does not modify any corresponding backends.
        """
        logger.info("Deleting endpoint '{}'".format(endpoint))
        async with self.write_lock:
            # This method must be idempotent. We should validate that the
            # specified endpoint exists on the client.
            for route, (route_endpoint, _) in self.routes.items():
                if route_endpoint == endpoint:
                    route_to_delete = route
                    break
            else:
                logger.info("Endpoint '{}' doesn't exist".format(endpoint))
                return

            # Remove the routing entry.
            del self.routes[route_to_delete]

            # Remove the traffic policy entry if it exists.
            if endpoint in self.traffic_policies:
                del self.traffic_policies[endpoint]

            self.endpoints_to_remove.append(endpoint)

            # NOTE(edoakes): we must write a checkpoint before pushing the
            # updates to the routers to avoid inconsistent state if we crash
            # after pushing the update.
            self._checkpoint()

            await asyncio.gather(*[
                router.set_route_table.remote(self.routes)
                for router in self.routers.values()
            ])
            await self._remove_pending_endpoints()

    async def create_backend(self, backend_tag: str,
                             backend_config: BackendConfig,
                             replica_config: ReplicaConfig) -> None:
        """Register a new backend under the specified tag."""
        async with self.write_lock:
            # Ensures this method is idempotent.
            if backend_tag in self.backends:
                backend_info = self.backends[backend_tag]
                if (backend_info.backend_config == backend_config
                        and backend_info.replica_config == replica_config):
                    return

            backend_worker = create_backend_worker(
                replica_config.func_or_class)

            # Save creator that starts replicas, the arguments to be passed in,
            # and the configuration for the backends.
            self.backends[backend_tag] = BackendInfo(
                worker_class=backend_worker,
                backend_config=backend_config,
                replica_config=replica_config)
            metadata = backend_config.internal_metadata
            if metadata.autoscaling_config is not None:
                self.autoscaling_policies[
                    backend_tag] = BasicAutoscalingPolicy(
                        backend_tag, metadata.autoscaling_config)

            try:
                self._scale_replicas(backend_tag, backend_config.num_replicas)
            except RayServeException as e:
                del self.backends[backend_tag]
                raise e

            # NOTE(edoakes): we must write a checkpoint before starting new
            # or pushing the updated config to avoid inconsistent state if we
            # crash while making the change.
            self._checkpoint()
            await self._start_pending_replicas()

            # Set the backend config inside the router
            # (particularly for max-batch-size).
            await asyncio.gather(*[
                router.set_backend_config.remote(backend_tag, backend_config)
                for router in self.routers.values()
            ])
            await self.broadcast_backend_config(backend_tag)

    async def delete_backend(self, backend_tag: str) -> None:
        async with self.write_lock:
            # This method must be idempotent. We should validate that the
            # specified backend exists on the client.
            if backend_tag not in self.backends:
                return

            # Check that the specified backend isn't used by any endpoints.
            for endpoint, traffic_policy in self.traffic_policies.items():
                if (backend_tag in traffic_policy.traffic_dict
                        or backend_tag in traffic_policy.shadow_dict):
                    raise ValueError("Backend '{}' is used by endpoint '{}' "
                                     "and cannot be deleted. Please remove "
                                     "the backend from all endpoints and try "
                                     "again.".format(backend_tag, endpoint))

            # Scale its replicas down to 0. This will also remove the backend
            # from self.backends and self.replicas.
            self._scale_replicas(backend_tag, 0)

            # Remove the backend's metadata.
            del self.backends[backend_tag]
            if backend_tag in self.autoscaling_policies:
                del self.autoscaling_policies[backend_tag]

            # Add the intention to remove the backend from the router.
            self.backends_to_remove.append(backend_tag)

            # NOTE(edoakes): we must write a checkpoint before removing the
            # backend from the router to avoid inconsistent state if we crash
            # after pushing the update.
            self._checkpoint()
            await self._stop_pending_replicas()
            await self._remove_pending_backends()

    async def update_backend_config(
            self, backend_tag: str,
            config_options: "Union[BackendConfig, Dict[str, Any]]") -> None:
        """Set the config for the specified backend."""
        async with self.write_lock:
            assert (backend_tag in self.backends
                    ), "Backend {} is not registered.".format(backend_tag)
            assert isinstance(config_options, BackendConfig) or isinstance(
                config_options, dict)

            if isinstance(config_options, BackendConfig):
                update_data = config_options.dict(exclude_unset=True)
            elif isinstance(config_options, dict):
                update_data = config_options

            stored_backend_config = self.backends[backend_tag].backend_config
            backend_config = stored_backend_config.copy(update=update_data)
            backend_config._validate_complete()
            self.backends[backend_tag].backend_config = backend_config

            # Scale the replicas with the new configuration.
            self._scale_replicas(backend_tag, backend_config.num_replicas)

            # NOTE(edoakes): we must write a checkpoint before pushing the
            # update to avoid inconsistent state if we crash after pushing the
            # update.
            self._checkpoint()

            # Inform the router about change in configuration
            # (particularly for setting max_batch_size).
            await asyncio.gather(*[
                router.set_backend_config.remote(backend_tag, backend_config)
                for router in self.routers.values()
            ])

            await self._start_pending_replicas()
            await self._stop_pending_replicas()

            await self.broadcast_backend_config(backend_tag)

    async def broadcast_backend_config(self, backend_tag: str) -> None:
        backend_config = self.backends[backend_tag].backend_config
        broadcast_futures = []
        for replica_tag in self.replicas[backend_tag]:
            try:
                replica = ray.get_actor(replica_tag)
            except ValueError:
                continue

            future = replica.update_config.remote(backend_config).as_future()
            broadcast_futures.append(future)
        if len(broadcast_futures) > 0:
            await asyncio.gather(*broadcast_futures)

    def get_backend_config(self, backend_tag: str) -> BackendConfig:
        """Get the current config for the specified backend."""
        assert (backend_tag in self.backends
                ), "Backend {} is not registered.".format(backend_tag)
        return self.backends[backend_tag].backend_config

    async def shutdown(self) -> None:
        """Shuts down the serve instance completely."""
        async with self.write_lock:
            for router in self.routers.values():
                ray.kill(router, no_restart=True)
            for replica_dict in self.workers.values():
                for replica in replica_dict.values():
                    ray.kill(replica, no_restart=True)
            self.kv_store.delete(CHECKPOINT_KEY)

    async def report_queue_lengths(self, router_name: str,
                                   queue_lengths: Dict[str, int]):
        # TODO: remove old router stats when removing them.
        for backend, queue_length in queue_lengths.items():
            self.backend_stats[backend][router_name] = queue_length

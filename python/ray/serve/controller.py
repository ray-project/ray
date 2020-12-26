import asyncio
from collections import defaultdict
from itertools import chain
import os
import random
import time
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Tuple
from uuid import uuid4, UUID
from pydantic import BaseModel

import ray
import ray.cloudpickle as pickle
from ray.serve.autoscaling_policy import BasicAutoscalingPolicy
from ray.serve.backend_worker import create_backend_replica
from ray.serve.constants import (ASYNC_CONCURRENCY, SERVE_PROXY_NAME,
                                 LongPollKey)
from ray.serve.http_proxy import HTTPProxyActor
from ray.serve.kv_store import RayInternalKVStore
from ray.serve.exceptions import RayServeException
from ray.serve.utils import (format_actor_name, get_random_letters, logger,
                             try_schedule_resources_on_nodes, get_all_node_ids)
from ray.serve.config import BackendConfig, ReplicaConfig, HTTPConfig
from ray.serve.long_poll import LongPollHost
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


class HTTPState:
    def __init__(self, controller_name: str, detached: bool,
                 config: HTTPConfig):
        self._controller_name = controller_name
        self._detached = detached
        self._config = config
        self._proxy_actors: Dict[NodeId, ActorHandle] = dict()

        # Will populate self.proxy_actors with existing actors.
        self._start_proxies_if_needed()

    def get_config(self):
        return self._config

    def get_http_proxy_handles(self) -> Dict[NodeId, ActorHandle]:
        return self._proxy_actors

    def update(self):
        self._start_proxies_if_needed()
        self._stop_proxies_if_needed()

    def _start_proxies_if_needed(self) -> None:
        """Start a proxy on every node if it doesn't already exist."""
        if self._config.host is None:
            return

        for node_id, node_resource in get_all_node_ids():
            if node_id in self._proxy_actors:
                continue

            name = format_actor_name(SERVE_PROXY_NAME, self._controller_name,
                                     node_id)
            try:
                proxy = ray.get_actor(name)
            except ValueError:
                logger.info("Starting HTTP proxy with name '{}' on node '{}' "
                            "listening on '{}:{}'".format(
                                name, node_id, self._config.host,
                                self._config.port))
                proxy = HTTPProxyActor.options(
                    name=name,
                    lifetime="detached" if self._detached else None,
                    max_concurrency=ASYNC_CONCURRENCY,
                    max_restarts=-1,
                    max_task_retries=-1,
                    resources={
                        node_resource: 0.01
                    },
                ).remote(
                    self._config.host,
                    self._config.port,
                    controller_name=self._controller_name,
                    http_middlewares=self._config.middlewares)

            self._proxy_actors[node_id] = proxy

    def _stop_proxies_if_needed(self) -> bool:
        """Removes proxy actors from any nodes that no longer exist."""
        all_node_ids = {node_id for node_id, _ in get_all_node_ids()}
        to_stop = []
        for node_id in self._proxy_actors:
            if node_id not in all_node_ids:
                logger.info("Removing HTTP proxy on removed node '{}'.".format(
                    node_id))
                to_stop.append(node_id)

        for node_id in to_stop:
            proxy = self._proxy_actors.pop(node_id)
            ray.kill(proxy, no_restart=True)


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


class BackendState:
    def __init__(self, checkpoint: bytes = None):
        self.backends: Dict[BackendTag, BackendInfo] = dict()

        if checkpoint is not None:
            self.backends = pickle.loads(checkpoint)

    def checkpoint(self):
        return pickle.dumps(self.backends)

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


class EndpointState:
    def __init__(self, checkpoint: bytes = None):
        self.routes: Dict[BackendTag, Tuple[EndpointTag, Any]] = dict()
        self.traffic_policies: Dict[EndpointTag, TrafficPolicy] = dict()

        if checkpoint is not None:
            self.routes, self.traffic_policies = pickle.loads(checkpoint)

    def checkpoint(self):
        return pickle.dumps((self.routes, self.traffic_policies))

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

    backend_replicas: Dict[BackendTag, Dict[ReplicaTag, ActorHandle]] = field(
        default_factory=lambda: defaultdict(dict))
    backend_replicas_to_start: Dict[BackendTag, List[ReplicaTag]] = field(
        default_factory=lambda: defaultdict(list))
    backend_replicas_to_stop: Dict[BackendTag, List[ReplicaTag]] = field(
        default_factory=lambda: defaultdict(list))
    backends_to_remove: List[BackendTag] = field(default_factory=list)

    # NOTE(ilr): These are not checkpointed, but will be recreated by
    # `_enqueue_pending_scale_changes_loop`.
    currently_starting_replicas: Dict[asyncio.Future, Tuple[
        BackendTag, ReplicaTag, ActorHandle]] = field(default_factory=dict)
    currently_stopping_replicas: Dict[asyncio.Future, Tuple[
        BackendTag, ReplicaTag]] = field(default_factory=dict)

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["currently_stopping_replicas"]
        del state["currently_starting_replicas"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.currently_stopping_replicas = {}
        self.currently_starting_replicas = {}

    # TODO(edoakes): consider removing this and just using the names.

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

    async def _start_backend_replica(self, backend_state: BackendState,
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
            backend_info = backend_state.get_backend(backend_tag)

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

    async def _enqueue_pending_scale_changes_loop(self,
                                                  backend_state: BackendState):
        for backend_tag, replicas_to_create in self.backend_replicas_to_start.\
                items():
            for replica_tag in replicas_to_create:
                replica_handle = await self._start_backend_replica(
                    backend_state, backend_tag, replica_tag)
                ready_future = replica_handle.ready.remote().as_future()
                self.currently_starting_replicas[ready_future] = (
                    backend_tag, replica_tag, replica_handle)

        for backend_tag, replicas_to_stop in self.backend_replicas_to_stop.\
                items():
            for replica_tag in replicas_to_stop:
                replica_name = format_actor_name(replica_tag,
                                                 self.controller_name)

                async def kill_actor(replica_name_to_use):
                    # NOTE: the replicas may already be stopped if we failed
                    # after stopping them but before writing a checkpoint.
                    try:
                        replica = ray.get_actor(replica_name_to_use)
                    except ValueError:
                        return

                    # TODO(edoakes): this logic isn't ideal because there may
                    # be pending tasks still executing on the replica. However,
                    # if we use replica.__ray_terminate__, we may send it while
                    # the replica is being restarted and there's no way to tell
                    # if it successfully killed the worker or not.
                    ray.kill(replica, no_restart=True)

                self.currently_stopping_replicas[asyncio.ensure_future(
                    kill_actor(replica_name))] = (backend_tag, replica_tag)

    async def _check_currently_starting_replicas(self) -> bool:
        """Returns a boolean specifying if there are more replicas to start"""
        in_flight = list()

        if self.currently_starting_replicas:
            done, in_flight = await asyncio.wait(
                list(self.currently_starting_replicas.keys()), timeout=0)
            for fut in done:
                (backend_tag, replica_tag,
                 replica_handle) = self.currently_starting_replicas.pop(fut)
                self.backend_replicas[backend_tag][
                    replica_tag] = replica_handle

                backend = self.backend_replicas_to_start.get(backend_tag)
                if backend:
                    try:
                        backend.remove(replica_tag)
                    except ValueError:
                        pass
                    if len(backend) == 0:
                        del self.backend_replicas_to_start[backend_tag]
        return len(in_flight) > 0

    async def _check_currently_stopping_replicas(self) -> bool:
        """Returns a boolean specifying if there are more replicas to stop"""
        in_flight = list()
        if self.currently_stopping_replicas:
            done_stoppping, in_flight = await asyncio.wait(
                list(self.currently_stopping_replicas.keys()), timeout=0)
            for fut in done_stoppping:
                (backend_tag,
                 replica_tag) = self.currently_stopping_replicas.pop(fut)

                backend = self.backend_replicas_to_stop.get(backend_tag)

                if backend:
                    try:
                        backend.remove(replica_tag)
                    except ValueError:
                        pass
                    if len(backend) == 0:
                        del self.backend_replicas_to_stop[backend_tag]

        return len(in_flight) > 0

    async def backend_control_loop(self):
        start = time.time()
        prev_warning = start
        need_to_continue = True
        while need_to_continue:
            if time.time() - prev_warning > REPLICA_STARTUP_TIME_WARNING_S:
                prev_warning = time.time()
                logger.warning("Waited {:.2f}s for replicas to start up. Make "
                               "sure there are enough resources to create the "
                               "replicas.".format(time.time() - start))

            need_to_continue = (
                await self._check_currently_starting_replicas()
                or await self._check_currently_stopping_replicas())

            asyncio.sleep(1)

    def _recover_actor_handles(self) -> None:
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
            self, backend_state: BackendState, controller: "ServeController"
    ) -> Dict[BackendTag, BasicAutoscalingPolicy]:
        self._recover_actor_handles()
        autoscaling_policies = dict()

        for backend, info in backend_state.backends.items():
            metadata = info.backend_config.internal_metadata
            if metadata.autoscaling_config is not None:
                autoscaling_policies[backend] = BasicAutoscalingPolicy(
                    backend, metadata.autoscaling_config)

        # Start/stop any pending backend replicas.
        await self._enqueue_pending_scale_changes_loop(backend_state)
        await self.backend_control_loop()

        return autoscaling_policies


@dataclass
class FutureResult:
    # Goal requested when this future was created
    requested_goal: Dict[str, Any]


@dataclass
class Checkpoint:
    endpoint_state_checkpoint: bytes
    backend_state_checkpoint: bytes
    reconciler: ActorStateReconciler
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
                       http_config: HTTPConfig,
                       detached: bool = False):
        # Used to read/write checkpoints.
        self.kv_store = RayInternalKVStore(namespace=controller_name)
        self.actor_reconciler = ActorStateReconciler(controller_name, detached)

        # backend -> AutoscalingPolicy
        self.autoscaling_policies = dict()

        # Dictionary of backend_tag -> proxy_name -> most recent queue length.
        self.backend_stats = defaultdict(lambda: defaultdict(dict))

        # Used to ensure that only a single state-changing operation happens
        # at any given time.
        self.write_lock = asyncio.Lock()

        # Map of awaiting results
        # TODO(ilr): Checkpoint this once this becomes asynchronous
        self.inflight_results: Dict[UUID, asyncio.Event] = dict()
        self._serializable_inflight_results: Dict[UUID, FutureResult] = dict()

        # HTTP state doesn't currently require a checkpoint.
        self.http_state = HTTPState(controller_name, detached, http_config)

        checkpoint_bytes = self.kv_store.get(CHECKPOINT_KEY)
        if checkpoint_bytes is None:
            logger.debug("No checkpoint found")
            self.backend_state = BackendState()
            self.endpoint_state = EndpointState()
        else:
            checkpoint: Checkpoint = pickle.loads(checkpoint_bytes)
            self.backend_state = BackendState(
                checkpoint=checkpoint.backend_state_checkpoint)
            self.endpoint_state = EndpointState(
                checkpoint=checkpoint.endpoint_state_checkpoint)
            await self._recover_from_checkpoint(checkpoint)

        # NOTE(simon): Currently we do all-to-all broadcast. This means
        # any listeners will receive notification for all changes. This
        # can be problem at scale, e.g. updating a single backend config
        # will send over the entire configs. In the future, we should
        # optimize the logic to support subscription by key.
        self.long_poll_host = LongPollHost()

        # The configs pushed out here get updated by
        # self._recover_from_checkpoint in the failure scenario, so that must
        # be run before we notify the changes.
        self.notify_backend_configs_changed()
        self.notify_replica_handles_changed()
        self.notify_traffic_policies_changed()
        self.notify_route_table_changed()

        asyncio.get_event_loop().create_task(self.run_control_loop())

    async def wait_for_event(self, uuid: UUID) -> bool:
        if uuid not in self.inflight_results:
            return True
        event = self.inflight_results[uuid]
        await event.wait()
        self.inflight_results.pop(uuid)
        self._serializable_inflight_results.pop(uuid)
        async with self.write_lock:
            self._checkpoint()

        return True

    def _create_event_with_result(
            self,
            goal_state: Dict[str, any],
            recreation_uuid: Optional[UUID] = None) -> UUID:
        # NOTE(ilr) Must be called before checkpointing!
        event = asyncio.Event()
        event.result = FutureResult(goal_state)
        event.set()
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
                self.actor_reconciler.backend_replicas.items()
            })

    def notify_traffic_policies_changed(self):
        self.long_poll_host.notify_changed(
            LongPollKey.TRAFFIC_POLICIES,
            self.endpoint_state.traffic_policies,
        )

    def notify_backend_configs_changed(self):
        self.long_poll_host.notify_changed(
            LongPollKey.BACKEND_CONFIGS,
            self.backend_state.get_backend_configs())

    def notify_route_table_changed(self):
        self.long_poll_host.notify_changed(LongPollKey.ROUTE_TABLE,
                                           self.endpoint_state.routes)

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
            Checkpoint(self.endpoint_state.checkpoint(),
                       self.backend_state.checkpoint(), self.actor_reconciler,
                       self._serializable_inflight_results))

        self.kv_store.put(CHECKPOINT_KEY, checkpoint)
        logger.debug("Wrote checkpoint in {:.3f}s".format(time.time() - start))

        if random.random(
        ) < _CRASH_AFTER_CHECKPOINT_PROBABILITY and self.detached:
            logger.warning("Intentionally crashing after checkpoint")
            os._exit(0)

    async def _recover_from_checkpoint(self, checkpoint: Checkpoint) -> None:
        """Recover the instance state from the provided checkpoint.

        This should be called in the constructor to ensure that the internal
        state is updated before any other operations run. After running this,
        internal state will be updated and long-poll clients may be notified.

        Performs the following operations:
            1) Deserializes the internal state from the checkpoint.
            2) Starts/stops any replicas that are pending creation or
               deletion.
        """
        start = time.time()
        logger.info("Recovering from checkpoint")

        self.actor_reconciler = checkpoint.reconciler

        self._serializable_inflight_results = checkpoint.inflight_reqs
        for uuid, fut_result in self._serializable_inflight_results.items():
            self._create_event_with_result(fut_result.requested_goal, uuid)

        # NOTE(edoakes): unfortunately, we can't completely recover from a
        # checkpoint in the constructor because we block while waiting for
        # other actors to start up, and those actors fetch soft state from
        # this actor. Because no other tasks will start executing until after
        # the constructor finishes, if we were to run this logic in the
        # constructor it could lead to deadlock between this actor and a child.
        # However, we do need to guarantee that we have fully recovered from a
        # checkpoint before any other state-changing calls run. We address this
        # by acquiring the write_lock and then posting the task to recover from
        # a checkpoint to the event loop. Other state-changing calls acquire
        # this lock and will be blocked until recovering from the checkpoint
        # finishes. This can be removed once we move to the async control loop.

        async def finish_recover_from_checkpoint():
            assert self.write_lock.locked()
            self.autoscaling_policies = await self.actor_reconciler.\
                _recover_from_checkpoint(self.backend_state, self)
            self.write_lock.release()
            logger.info(
                "Recovered from checkpoint in {:.3f}s".format(time.time() -
                                                              start))

        await self.write_lock.acquire()
        asyncio.get_event_loop().create_task(finish_recover_from_checkpoint())

    async def do_autoscale(self) -> None:
        for backend, info in self.backend_state.backends.items():
            if backend not in self.autoscaling_policies:
                continue

            new_num_replicas = self.autoscaling_policies[backend].scale(
                self.backend_stats[backend], info.backend_config.num_replicas)
            if new_num_replicas > 0:
                await self.update_backend_config(
                    backend, BackendConfig(num_replicas=new_num_replicas))

    async def reconcile_current_and_goal_backends(self):
        pass

    async def run_control_loop(self) -> None:
        while True:
            await self.do_autoscale()
            async with self.write_lock:
                self.http_state.update()

            await asyncio.sleep(CONTROL_LOOP_PERIOD_S)

    def _all_replica_handles(
            self) -> Dict[BackendTag, Dict[ReplicaTag, ActorHandle]]:
        """Used for testing."""
        return self.actor_reconciler.backend_replicas

    def get_all_backends(self) -> Dict[BackendTag, BackendConfig]:
        """Returns a dictionary of backend tag to backend config."""
        return self.backend_state.get_backend_configs()

    def get_all_endpoints(self) -> Dict[EndpointTag, Dict[BackendTag, Any]]:
        """Returns a dictionary of backend tag to backend config."""
        return self.endpoint_state.get_endpoints()

    async def _set_traffic(self, endpoint_name: str,
                           traffic_dict: Dict[str, float]) -> UUID:
        if endpoint_name not in self.endpoint_state.get_endpoints():
            raise ValueError("Attempted to assign traffic for an endpoint '{}'"
                             " that is not registered.".format(endpoint_name))

        assert isinstance(traffic_dict,
                          dict), "Traffic policy must be a dictionary."

        for backend in traffic_dict:
            if self.backend_state.get_backend(backend) is None:
                raise ValueError(
                    "Attempted to assign traffic to a backend '{}' that "
                    "is not registered.".format(backend))

        traffic_policy = TrafficPolicy(traffic_dict)
        self.endpoint_state.traffic_policies[endpoint_name] = traffic_policy

        return_uuid = self._create_event_with_result({
            endpoint_name: traffic_policy
        })
        # NOTE(edoakes): we must write a checkpoint before pushing the
        # update to avoid inconsistent state if we crash after pushing the
        # update.
        self._checkpoint()
        self.notify_traffic_policies_changed()
        return return_uuid

    async def set_traffic(self, endpoint_name: str,
                          traffic_dict: Dict[str, float]) -> UUID:
        """Sets the traffic policy for the specified endpoint."""
        async with self.write_lock:
            return_uuid = await self._set_traffic(endpoint_name, traffic_dict)
        return return_uuid

    async def shadow_traffic(self, endpoint_name: str, backend_tag: BackendTag,
                             proportion: float) -> UUID:
        """Shadow traffic from the endpoint to the backend."""
        async with self.write_lock:
            if endpoint_name not in self.endpoint_state.get_endpoints():
                raise ValueError("Attempted to shadow traffic from an "
                                 "endpoint '{}' that is not registered."
                                 .format(endpoint_name))

            if self.backend_state.get_backend(backend_tag) is None:
                raise ValueError(
                    "Attempted to shadow traffic to a backend '{}' that "
                    "is not registered.".format(backend_tag))

            self.endpoint_state.traffic_policies[endpoint_name].set_shadow(
                backend_tag, proportion)

            traffic_policy = self.endpoint_state.traffic_policies[
                endpoint_name]

            return_uuid = self._create_event_with_result({
                endpoint_name: traffic_policy
            })
            # NOTE(edoakes): we must write a checkpoint before pushing the
            # update to avoid inconsistent state if we crash after pushing the
            # update.
            self._checkpoint()
            self.notify_traffic_policies_changed()
            return return_uuid

    # TODO(architkulkarni): add Optional for route after cloudpickle upgrade
    async def create_endpoint(self, endpoint: str,
                              traffic_dict: Dict[str, float], route,
                              methods) -> UUID:
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
            if route in self.endpoint_state.routes:

                # Ensures this method is idempotent
                if self.endpoint_state.routes[route] == (endpoint, methods):
                    return

                else:
                    raise ValueError(
                        "{} Route '{}' is already registered.".format(
                            err_prefix, route))

            if endpoint in self.endpoint_state.get_endpoints():
                raise ValueError(
                    "{} Endpoint '{}' is already registered.".format(
                        err_prefix, endpoint))

            logger.info(
                "Registering route '{}' to endpoint '{}' with methods '{}'.".
                format(route, endpoint, methods))

            self.endpoint_state.routes[route] = (endpoint, methods)

            # NOTE(edoakes): checkpoint is written in self._set_traffic.
            return_uuid = await self._set_traffic(endpoint, traffic_dict)
            self.notify_route_table_changed()
            return return_uuid

    async def delete_endpoint(self, endpoint: str) -> UUID:
        """Delete the specified endpoint.

        Does not modify any corresponding backends.
        """
        logger.info("Deleting endpoint '{}'".format(endpoint))
        async with self.write_lock:
            # This method must be idempotent. We should validate that the
            # specified endpoint exists on the client.
            for route, (route_endpoint,
                        _) in self.endpoint_state.routes.items():
                if route_endpoint == endpoint:
                    route_to_delete = route
                    break
            else:
                logger.info("Endpoint '{}' doesn't exist".format(endpoint))
                return

            # Remove the routing entry.
            del self.endpoint_state.routes[route_to_delete]

            # Remove the traffic policy entry if it exists.
            if endpoint in self.endpoint_state.traffic_policies:
                del self.endpoint_state.traffic_policies[endpoint]

            return_uuid = self._create_event_with_result({
                route_to_delete: None,
                endpoint: None
            })
            # NOTE(edoakes): we must write a checkpoint before pushing the
            # updates to the proxies to avoid inconsistent state if we crash
            # after pushing the update.
            self._checkpoint()
            self.notify_route_table_changed()
            return return_uuid

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
            self.backend_state.add_backend(backend_tag, backend_info)
            metadata = backend_config.internal_metadata
            if metadata.autoscaling_config is not None:
                self.autoscaling_policies[
                    backend_tag] = BasicAutoscalingPolicy(
                        backend_tag, metadata.autoscaling_config)

            try:
                # This call should be to run control loop
                self.actor_reconciler._scale_backend_replicas(
                    self.backend_state.backends, backend_tag,
                    backend_config.num_replicas)
            except RayServeException as e:
                del self.backend_state.backends[backend_tag]
                raise e

            return_uuid = self._create_event_with_result({
                backend_tag: backend_info
            })
            # NOTE(edoakes): we must write a checkpoint before starting new
            # or pushing the updated config to avoid inconsistent state if we
            # crash while making the change.
            self._checkpoint()
            await self.actor_reconciler._enqueue_pending_scale_changes_loop(
                self.backend_state)
            await self.actor_reconciler.backend_control_loop()

            self.notify_replica_handles_changed()

            # Set the backend config inside routers
            # (particularly for max_concurrent_queries).
            self.notify_backend_configs_changed()
            return return_uuid

    async def delete_backend(self, backend_tag: BackendTag) -> UUID:
        async with self.write_lock:
            # This method must be idempotent. We should validate that the
            # specified backend exists on the client.
            if self.backend_state.get_backend(backend_tag) is None:
                return

            # Check that the specified backend isn't used by any endpoints.
            for endpoint, traffic_policy in self.endpoint_state.\
                    traffic_policies.items():
                if (backend_tag in traffic_policy.traffic_dict
                        or backend_tag in traffic_policy.shadow_dict):
                    raise ValueError("Backend '{}' is used by endpoint '{}' "
                                     "and cannot be deleted. Please remove "
                                     "the backend from all endpoints and try "
                                     "again.".format(backend_tag, endpoint))

            # Scale its replicas down to 0. This will also remove the backend
            # from self.backend_state.backends and
            # self.actor_reconciler.backend_replicas.

            # This should be a call to the control loop
            self.actor_reconciler._scale_backend_replicas(
                self.backend_state.backends, backend_tag, 0)

            # Remove the backend's metadata.
            del self.backend_state.backends[backend_tag]
            if backend_tag in self.autoscaling_policies:
                del self.autoscaling_policies[backend_tag]

            # Add the intention to remove the backend from the routers.
            self.actor_reconciler.backends_to_remove.append(backend_tag)

            return_uuid = self._create_event_with_result({backend_tag: None})
            # NOTE(edoakes): we must write a checkpoint before removing the
            # backend from the routers to avoid inconsistent state if we crash
            # after pushing the update.
            self._checkpoint()
            await self.actor_reconciler._enqueue_pending_scale_changes_loop(
                self.backend_state)
            await self.actor_reconciler.backend_control_loop()

            self.notify_replica_handles_changed()
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

            # Scale the replicas with the new configuration.

            # This should be to run the control loop
            self.actor_reconciler._scale_backend_replicas(
                self.backend_state.backends, backend_tag,
                backend_config.num_replicas)

            return_uuid = self._create_event_with_result({
                backend_tag: backend_info
            })
            # NOTE(edoakes): we must write a checkpoint before pushing the
            # update to avoid inconsistent state if we crash after pushing the
            # update.
            self._checkpoint()

            # Inform the routers about change in configuration
            # (particularly for setting max_batch_size).

            await self.actor_reconciler._enqueue_pending_scale_changes_loop(
                self.backend_state)
            await self.actor_reconciler.backend_control_loop()

            self.notify_replica_handles_changed()
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
            for replica in self.actor_reconciler.get_replica_handles():
                ray.kill(replica, no_restart=True)
            self.kv_store.delete(CHECKPOINT_KEY)

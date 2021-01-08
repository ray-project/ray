import asyncio
from asyncio.futures import Future
from collections import defaultdict
from typing import Dict, Any, List, Optional, Set, Tuple

import ray
import ray.cloudpickle as pickle
from ray.actor import ActorHandle
from ray.serve.common import (
    BackendInfo, BackendTag, Duration, GoalId,
    ReplicaTag,
)
from ray.serve.config import BackendConfig
from ray.serve.exceptions import RayServeException
from ray.serve.kv_store import RayInternalKVStore
from ray.serve.long_poll import LongPollHost
from ray.serve.utils import (format_actor_name, get_random_letters, logger,
                             try_schedule_resources_on_nodes)

# Feature flag for controller resource checking. If true, controller will
# error if the desired replicas exceed current resource availability.
_RESOURCE_CHECK_ENABLED = True


class BackendState:
    """Manages all state for backends in the system.

    This class is *not* thread safe, so any state-modifying methods should be
    called with a lock held.
    """

    def __init__(self,
                 controller_name: str,
                 detached: bool,
                 kv_store: RayInternalKVStore,
                 long_poll_host: LongPollHost):
        self.controller_name = controller_name
        self.detached = detached

        # Non-checkpointed state.
        self.currently_starting_replicas: Dict[asyncio.Future, Tuple[
            BackendTag, ReplicaTag, ActorHandle]] = dict()
        self.currently_stopping_replicas: Dict[asyncio.Future, Tuple[
            BackendTag, ReplicaTag]] = dict()

        # Checkpointed state.
        self.backends: Dict[BackendTag, BackendInfo] = dict()
        self.backend_replicas: Dict[BackendTag, Dict[
            ReplicaTag, ActorHandle]] = defaultdict(dict)
        self.goals: Dict[BackendTag, GoalId] = dict()
        self.backend_replicas_to_start: Dict[BackendTag, List[
            ReplicaTag]] = defaultdict(list)
        self.backend_replicas_to_stop: Dict[BackendTag, List[Tuple[
            ReplicaTag, Duration]]] = defaultdict(list)
        self.backends_to_remove: List[BackendTag] = list()

        if checkpoint is not None:
            (self.backends, self.backend_replicas, self.goals,
             self.backend_replicas_to_start, self.backend_replicas_to_stop,
             self.backend_to_remove) = pickle.loads(checkpoint)

        # Fetch actor handles for all of the backend replicas in the system.
        # All of these backend_replicas are guaranteed to already exist because
        # they would not be written to a checkpoint in self.backend_replicas
        # until they were created.
        for backend_tag, replica_dict in self.backend_replicas.items():
            for replica_tag in replica_dict.keys():
                replica_name = format_actor_name(replica_tag,
                                                 self.controller_name)
                self.backend_replicas[backend_tag][
                    replica_tag] = ray.get_actor(replica_name)

    def checkpoint(self):
        return pickle.dumps(
            (self.backends, self.backend_replicas, self.goals,
             self.backend_replicas_to_start, self.backend_replicas_to_stop,
             self.backends_to_remove))

    def get_backend_configs(self) -> Dict[BackendTag, BackendConfig]:
        return {
            tag: info.backend_config
            for tag, info in self.backends.items()
        }

    def get_replica_handles(
            self) -> Dict[BackendTag, Dict[ReplicaTag, ActorHandle]]:
        return self.backend_replicas

    def get_backend(self, backend_tag: BackendTag) -> Optional[BackendInfo]:
        return self.backends.get(backend_tag)

    def _set_backend_goal(self, backend_tag: BackendTag,
                          backend_info: Optional[BackendInfo],
                          goal_id: GoalId) -> Optional[GoalId]:
        existing_goal = self.goals.get(backend_tag)
        self.backends[backend_tag] = backend_info
        if not backend_info:
            del self.backends[backend_tag]
        self.goals[backend_tag] = goal_id
        return existing_goal

    def completed_goals(self) -> List[GoalId]:
        completed_goals = []
        all_tags = set(self.backend_replicas.keys()).union(
            set(self.backends.keys()))

        for backend_tag in all_tags:
            desired_info = self.backends.get(backend_tag)
            existing_info = self.backend_replicas.get(backend_tag)
            # Check for deleting
            if (not desired_info or
                    desired_info.backend_config.num_replicas == 0) and \
                    (not existing_info or len(existing_info) == 0):
                completed_goals.append(self.goals[backend_tag])

            # Check for a non-zero number of backends
            if desired_info and existing_info and desired_info.backend_config.\
                    num_replicas == len(existing_info):
                completed_goals.append(self.goals[backend_tag])
        return completed_goals

    def _start_backend_replica(self, backend_tag: BackendTag,
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
            backend_info = self.get_backend(backend_tag)

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

    def scale_backend_replicas(
            self,
            backend_tag: BackendTag,
            num_replicas: int,
            force_kill: bool = False,
    ) -> None:
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
        assert (backend_tag in self.backends
                ), "Backend {} is not registered.".format(backend_tag)
        assert num_replicas >= 0, ("Number of replicas must be"
                                   " greater than or equal to 0.")

        current_num_replicas = len(self.backend_replicas[backend_tag])
        delta_num_replicas = num_replicas - current_num_replicas

        backend_info: BackendInfo = self.backends[backend_tag]
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
            replicas_copy = self.backend_replicas.copy()
            for _ in range(-delta_num_replicas):
                replica_tag, _ = replicas_copy[backend_tag].popitem()

                graceful_timeout_s = (backend_info.backend_config.
                                      experimental_graceful_shutdown_timeout_s)
                if force_kill:
                    graceful_timeout_s = 0
                self.backend_replicas_to_stop[backend_tag].append((
                    replica_tag,
                    graceful_timeout_s,
                ))

    def _start_pending_replicas(self):
        for backend_tag, replicas_to_create in self.backend_replicas_to_start.\
                items():
            for replica_tag in replicas_to_create:
                replica_handle = self._start_backend_replica(
                    backend_tag, replica_tag)
                ready_future = replica_handle.ready.remote().as_future()
                self.currently_starting_replicas[ready_future] = (
                    backend_tag, replica_tag, replica_handle)

    def _stop_pending_replicas(self):
        for backend_tag, replicas_to_stop in (
                self.backend_replicas_to_stop.items()):
            for replica_tag, shutdown_timeout in replicas_to_stop:
                replica_name = format_actor_name(replica_tag,
                                                 self.controller_name)

                async def kill_actor(replica_name_to_use):
                    # NOTE: the replicas may already be stopped if we failed
                    # after stopping them but before writing a checkpoint.
                    try:
                        replica = ray.get_actor(replica_name_to_use)
                    except ValueError:
                        return

                    try:
                        await asyncio.wait_for(
                            replica.drain_pending_queries.remote(),
                            timeout=shutdown_timeout)
                    except asyncio.TimeoutError:
                        # Graceful period passed, kill it forcefully.
                        logger.debug(
                            f"{replica_name_to_use} did not shutdown after "
                            f"{shutdown_timeout}s, killing.")
                    finally:
                        ray.kill(replica, no_restart=True)

                self.currently_stopping_replicas[asyncio.ensure_future(
                    kill_actor(replica_name))] = (backend_tag, replica_tag)

    async def _check_currently_starting_replicas(self) -> int:
        """Returns the number of pending replicas waiting to start"""
        in_flight: Set[Future[Any]] = set()

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

    async def _check_currently_stopping_replicas(self) -> int:
        """Returns the number of replicas waiting to stop"""
        in_flight: Set[Future[Any]] = set()

        if self.currently_stopping_replicas:
            done_stoppping, in_flight = await asyncio.wait(
                list(self.currently_stopping_replicas.keys()), timeout=0)
            for fut in done_stoppping:
                (backend_tag,
                 replica_tag) = self.currently_stopping_replicas.pop(fut)

                backend_to_stop = self.backend_replicas_to_stop.get(
                    backend_tag)

                if backend_to_stop:
                    try:
                        backend_to_stop.remove(replica_tag)
                    except ValueError:
                        pass
                    if len(backend_to_stop) == 0:
                        del self.backend_replicas_to_stop[backend_tag]

                backend = self.backend_replicas.get(backend_tag)
                if backend:
                    try:
                        del backend[replica_tag]
                    except KeyError:
                        pass

                    if len(self.backend_replicas[backend_tag]) == 0:
                        del self.backend_replicas[backend_tag]

    async def update(self) -> bool:
        """Returns whether the number of backends has changed."""
        self._start_pending_replicas()
        self._stop_pending_replicas()

        num_starting = len(self.currently_starting_replicas)
        num_stopping = len(self.currently_stopping_replicas)

        await self._check_currently_starting_replicas()
        await self._check_currently_stopping_replicas()

        return (len(self.currently_starting_replicas) != num_starting) or \
            (len(self.currently_stopping_replicas) != num_stopping)

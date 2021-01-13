import asyncio
from asyncio.futures import Future
from collections import defaultdict
from enum import Enum
import time
from typing import Dict, Any, List, Optional, Set, Tuple
from uuid import uuid4

import ray
import ray.cloudpickle as pickle
from ray.actor import ActorHandle
from ray.serve.backend_worker import create_backend_replica
from ray.serve.common import (
    BackendInfo,
    BackendTag,
    Duration,
    GoalId,
    ReplicaTag,
)
from ray.serve.config import BackendConfig, ReplicaConfig
from ray.serve.constants import LongPollKey
from ray.serve.exceptions import RayServeException
from ray.serve.kv_store import RayInternalKVStore
from ray.serve.long_poll import LongPollHost
from ray.serve.utils import (format_actor_name, get_random_letters, logger,
                             try_schedule_resources_on_nodes)

CHECKPOINT_KEY = "serve-backend-state-checkpoint"

# Feature flag for controller resource checking. If true, controller will
# error if the desired replicas exceed current resource availability.
_RESOURCE_CHECK_ENABLED = True

class ReplicaState(Enum):
    NEW = 1 #todo(ilr) Drop
    SHOULD_START = 2
    STARTING = 3
    RUNNING = 4
    SHOULD_STOP = 5
    STOPPING = 6
    STOPPED = 7

class BackendReplica:
    def __init__(self, controller_name, replica_tag, backend_tag):
        self._actor_name = format_actor_name(replica_tag, controller_name)
        self._actor_handle = None
        self._startup_obj_ref = None
        self._shutdown_future = None
        self._state = ReplicaState.SHOULD_START

    def recover_from_checkpoint(self):
        try:
            replica_handle = ray.get_actor(replica_name)
        except ValueError:
            logger.debug("Starting replica '{}' for backend '{}'.".format(
                replica_tag, backend_tag))
            backend_info = self.get_backend(backend_tag)

    def start(self):
        assert self._state == ReplicaState.SHOULD_START
        try:
            self._actor_handle = ray.get_actor(replica_name)
        except ValueError:
            logger.debug("Starting replica '{}' for backend '{}'.".format(
                replica_tag, backend_tag))
            self._actor_handle = ray.remote(backend_info.worker_class).options(
                name=replica_name,
                lifetime="detached" if self._detached else None,
                max_restarts=-1,
                max_task_retries=-1,
                **backend_info.replica_config.ray_actor_options).remote(
                    backend_tag, replica_tag,
                    backend_info.replica_config.actor_init_args,
                    backend_info.backend_config, self._controller_name)
        self._startup_obj_ref = self._actor_handle.ready.remote()
        self._state = ReplicaState.STARTING


    def check_started(self):
        if self._state == ReplicaState.RUNNING:
            return True
        assert self._state == ReplicaState.STARTING
        ready, _ = ray.wait([self._startup_object_ref], timeout=0)
        if len(ready) == 1:
            self._state = ReplicaState.RUNNING
            return True
        return False

    def set_should_stop(self, graceful_shutdown_timeout_s):
        self._state = ReplicaState.SHOULD_STOP
        self._graceful_shutdown_timeout_s = graceful_shutdown_timeout_s 

    def stop(self):
        # We need to handle transitions from SHOULD_START -> SHOULD_STOP -> STOPPING
        # This means that the replica_handle may not have been created.

        assert self._state == ReplicaState.SHOULD_STOP 
        # TODO(edoakes): this can be done without using an asyncio future.
        async def kill_actor(actor_name):
            # NOTE: the replicas may already be stopped if we failed
            # after stopping them but before writing a checkpoint.
            try:
                replica = ray.get_actor(actor_name)
            except ValueError:
                return
            try:
                await asyncio.wait_for(
                    replica.drain_pending_queries.remote(),
                    timeout=graceful_shutdown_timeout_s)
            except asyncio.TimeoutError:
                # Graceful period passed, kill it forcefully.
                logger.debug(
                    f"{actor_name} did not shutdown after "
                    f"{graceful_shutdown_timeout_s}s, force-killing.")
            finally:
                ray.kill(replica, no_restart=True)
        self._shutdown_future = asyncio.ensure_future(kill_actor(self._actor_name))

    async def check_stopped(self):
        if self._state == ReplicaState.STOPPED:
            return True
        assert self._state == ReplicaState.STOPPING
        try:
            await asyncio.wait_for(self._shutdown_future, timeout=0)
            return True
        except asyncio.TimeoutError:
            return False


class BackendState:
    """Manages all state for backends in the system.

    This class is *not* thread safe, so any state-modifying methods should be
    called with a lock held.
    """

    def __init__(self, controller_name: str, detached: bool,
                 kv_store: RayInternalKVStore, long_poll_host: LongPollHost):
        self._controller_name = controller_name
        self._detached = detached
        self._kv_store = kv_store
        self._long_poll_host = long_poll_host

        self._replicas: Dict[BackendTag, Dict[ReplicaState, List[BackendReplica]]] = defaultdict(lambda: defaultdict(list))
        self._backend_metadata: Dict[BackendTag, BackendInfo] = dict()
        self._target_replicas: Dict[BackendTag, int] = dict()


        # Checkpointed state.
        self.backends: Dict[BackendTag, BackendInfo] = dict()
        self.backend_replicas: Dict[BackendTag, Dict[
            ReplicaTag, ActorHandle]] = defaultdict(dict)
        self.goals: Dict[BackendTag, GoalId] = dict()
        self.backend_replicas_to_start: Dict[BackendTag, List[
            ReplicaTag]] = defaultdict(list)
        self.backend_replicas_to_stop: Dict[BackendTag, List[Tuple[
            ReplicaTag, Duration]]] = defaultdict(list)
        self.pending_goals: Dict[GoalId, asyncio.Event] = dict()

        checkpoint = self._kv_store.get(CHECKPOINT_KEY)
        if checkpoint is not None:
            # (self.backends, self.backend_replicas, self.goals,
            #  self.backend_replicas_to_start, self.backend_replicas_to_stop,
            #  self.backend_to_remove,
            #  pending_goal_ids) = pickle.loads(checkpoint)
            (self._replicas, self._backend_metadata, self._target_replicas) = pickle.loads(checkpoint)

            for goal_id in pending_goal_ids:
                self._create_goal(goal_id)

            # Fetch actor handles for all backend replicas in the system.
            # All of these backend_replicas are guaranteed to already exist
            # because they would not be written to a checkpoint in
            # self.backend_replicas until they were created.
            for backend_tag, replica_dict in self.backend_replicas.items():
                for replica_tag in replica_dict.keys():
                    replica_name = format_actor_name(replica_tag,
                                                     self._controller_name)
                    self.backend_replicas[backend_tag][
                        replica_tag] = ray.get_actor(replica_name)

        self._notify_backend_configs_changed()
        self._notify_replica_handles_changed()

    def _checkpoint(self) -> None:
        self._kv_store.put(
            CHECKPOINT_KEY,
            pickle.dumps(
                (self.backends, self.backend_replicas, self.goals,
                 self.backend_replicas_to_start, self.backend_replicas_to_stop,
                 list(self.pending_goals.keys()))))

    def _notify_backend_configs_changed(self) -> None:
        self._long_poll_host.notify_changed(LongPollKey.BACKEND_CONFIGS,
                                            self.get_backend_configs())

    def _notify_replica_handles_changed(self) -> None:
        self._long_poll_host.notify_changed(
            LongPollKey.REPLICA_HANDLES, {
                backend_tag: list(replica_dict.values())
                for backend_tag, replica_dict in self.backend_replicas.items()
            })

    def get_backend_configs(self) -> Dict[BackendTag, BackendConfig]:
        return {
            tag: info.backend_config
            for tag, info in self._backend_metadata.items()
        }

    def get_replica_handles(
            self) -> Dict[BackendTag, Dict[ReplicaTag, ActorHandle]]:
        return self.backend_replicas

    def get_backend(self, backend_tag: BackendTag) -> Optional[BackendInfo]:
        return self._backend_metadata.get(backend_tag)

    def num_pending_goals(self) -> int:
        return len(self.pending_goals)

    async def wait_for_goal(self, goal_id: GoalId) -> bool:
        start = time.time()
        if goal_id not in self.pending_goals:
            logger.debug(f"Goal {goal_id} not found")
            return True
        event = self.pending_goals[goal_id]
        await event.wait()
        logger.debug(
            f"Waiting for goal {goal_id} took {time.time() - start} seconds")
        return True

    def _complete_goal(self, goal_id: GoalId) -> None:
        logger.debug(f"Completing goal {goal_id}")
        event = self.pending_goals.pop(goal_id, None)
        if event:
            event.set()

    def _create_goal(self, goal_id: Optional[GoalId] = None) -> GoalId:
        if goal_id is None:
            goal_id = uuid4()
        event = asyncio.Event()
        self.pending_goals[goal_id] = event
        return goal_id

    def _set_backend_goal(self, backend_tag: BackendTag,
                          backend_info: Optional[BackendInfo]) -> None:
        existing_goal = self.goals.get(backend_tag)
        new_goal = self._create_goal()

        if backend_info is not None:
            self._backend_metadata[backend_tag] = backend_info
            self._target_replicas[backend_tag] = backend_info.backend_config.num_replicas

        self.goals[backend_tag] = new_goal

        return new_goal, existing_goal

    def create_backend(self, backend_tag: BackendTag,
                       backend_config: BackendConfig,
                       replica_config: ReplicaConfig) -> Optional[GoalId]:
        # Ensures this method is idempotent.
        backend_info = self._backend_metadata.get(backend_tag)
        if backend_info is not None:
            if (backend_info.backend_config == backend_config
                    and backend_info.replica_config == replica_config):
                return None

        backend_replica_class = create_backend_replica(replica_config.func_or_class)

        # Save creator that starts replicas, the arguments to be passed in,
        # and the configuration for the backends.
        backend_info = BackendInfo(
            worker_class=backendbackend_replica_class_replica,
            backend_config=backend_config,
            replica_config=replica_config)

        new_goal, existing_goal = self._set_backend_goal(
            backend_tag, backend_info)

        try:
            self.scale_backend_replicas(backend_tag,
                                        backend_config.num_replicas)
        except RayServeException as e:
            del self._backend_metadata[backend_tag]
            raise e

        # NOTE(edoakes): we must write a checkpoint before starting new
        # or pushing the updated config to avoid inconsistent state if we
        # crash while making the change.
        self._checkpoint()
        self._notify_backend_configs_changed()

        if existing_goal is not None:
            self._complete_goal(existing_goal)
        return new_goal

    def delete_backend(self, backend_tag: BackendTag,
                       force_kill: bool = False) -> Optional[GoalId]:
        # This method must be idempotent. We should validate that the
        # specified backend exists on the client.
        if backend_tag not in self._backend_metadata:
            return None

        # Scale its replicas down to 0.
        self.scale_backend_replicas(backend_tag, 0, force_kill)

        # Remove the backend's metadata.
        del self._backend_metadata[backend_tag]
        del self._target_replicas[backend_tag]


        new_goal, existing_goal = self._set_backend_goal(backend_tag, None)

        self._checkpoint()
        if existing_goal is not None:
            self._complete_goal(existing_goal)
        return new_goal

    def update_backend_config(self, backend_tag: BackendTag,
                              config_options: BackendConfig):
        if backend_tag not in self._backend_metadata:
            raise ValueError(f"Backend {backend_tag} is not registered")

        stored_backend_config = self._backend_metadata[backend_tag].backend_config
        updated_config = stored_backend_config.copy(
            update=config_options.dict(exclude_unset=True))
        updated_config._validate_complete()
        self._backend_metadata[backend_tag].backend_config = updated_config

        new_goal, existing_goal = self._set_backend_goal(
            backend_tag, self._backend_metadata[backend_tag])

        # Scale the replicas with the new configuration.
        self.scale_backend_replicas(backend_tag, updated_config.num_replicas)

        # NOTE(edoakes): we must write a checkpoint before pushing the
        # update to avoid inconsistent state if we crash after pushing the
        # update.
        self._checkpoint()
        if existing_goal is not None:
            self._complete_goal(existing_goal)

        # Inform the routers and backend replicas about config changes.
        # TODO(edoakes): this should only happen if we change something other
        # than num_replicas.
        self._notify_backend_configs_changed()

        return new_goal

    def _start_backend_replica(self, backend_tag: BackendTag,
                               replica_tag: ReplicaTag) -> ActorHandle:
        """Start a replica and return its actor handle.

        Checks if the named actor already exists before starting a new one.

        Assumes that the backend configuration is already in the Goal State.
        """
        # NOTE(edoakes): the replicas may already be created if we
        # failed after creating them but before writing a
        # checkpoint.
        replica_name = format_actor_name(replica_tag, self._controller_name)
        try:
            replica_handle = ray.get_actor(replica_name)
        except ValueError:
            logger.debug("Starting replica '{}' for backend '{}'.".format(
                replica_tag, backend_tag))
            backend_info = self.get_backend(backend_tag)

            replica_handle = ray.remote(backend_info.worker_class).options(
                name=replica_name,
                lifetime="detached" if self._detached else None,
                max_restarts=-1,
                max_task_retries=-1,
                **backend_info.replica_config.ray_actor_options).remote(
                    backend_tag, replica_tag,
                    backend_info.replica_config.actor_init_args,
                    backend_info.backend_config, self._controller_name)

        return replica_handle

    def scale_backend_replicas(
            self,
            backend_tag: BackendTag,
            num_replicas: int,
            force_kill: bool = False,
    ) -> None:
        """Scale the given backend to the number of replicas.

        NOTE: this does not actually start or stop the replicas, but instead
        adds them to ReplicaState.SHOULD_START or ReplicaState.SHOULD_STOP.
        The caller is responsible for then first writing a checkpoint and then
        actually starting/stopping the intended replicas. This avoids
        inconsistencies with starting/stopping a replica and then crashing
        before writing a checkpoint.
        """

        logger.debug("Scaling backend '{}' to {} replicas".format(
            backend_tag, num_replicas))
        assert (backend_tag in self._backend_metadata
                ), "Backend {} is not registered.".format(backend_tag)
        assert num_replicas >= 0, ("Number of replicas must be"
                                   " greater than or equal to 0.")

        current_num_replicas = len(self._target_replicas[backend_tag])

        delta_num_replicas = num_replicas - current_num_replicas

        backend_info: BackendInfo = self._backend_metadata[backend_tag]
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
                self._replicas[backend_tag][ReplicaState.SHOULD_START].append(BackendReplica(self._controller_name, replica_tag, backend_tag)

        elif delta_num_replicas < 0:
            logger.debug("Removing {} replicas from backend '{}'".format(
                -delta_num_replicas, backend_tag))
            assert len(
                self._target_replicas[backend_tag]) >= delta_num_replicas

            for _ in range(-delta_num_replicas):
                list_to_use = self._replicas[backend_tag][ReplicaState.SHOULD_START] or \ 
                    self._replicas[backend_tag][ReplicaState.STARTING] or \
                    self._replicas[backend_tag][ReplicaState.RUNNING]

                replica_to_stop = list_to_use.pop()


                graceful_timeout_s = (backend_info.backend_config.
                                      experimental_graceful_shutdown_timeout_s)
                if force_kill:
                    graceful_timeout_s = 0

                replica_to_stop.set_should_stop(graceful_timeout_s)
                self._replicas[backend_tag][ReplicaState.SHOULD_STOP].append(replica_to_stop)



    def _pop_replicas_of_state(self, state: ReplicaState) -> List[Tuple[ReplicaState, BackendTag]]:
        replicas = []
        for backend_tag, state_to_replica_dict in self._replicas.items():
            replicas.extend(tuple([replica, backend_tag]) for replica in state_to_replica_dict.pop(state))
        
        return replicas

    def _completed_goals(self) -> List[GoalId]:
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
                completed_goals.append(self.goals.get(backend_tag))

            # Check for a non-zero number of backends
            if desired_info and existing_info and desired_info.backend_config.\
                    num_replicas == len(existing_info):
                completed_goals.append(self.goals.get(backend_tag))
        return [goal for goal in completed_goals if goal]

    async def update(self) -> bool:
        for goal_id in self._completed_goals():
            self._complete_goal(goal_id)


        for replica_state, backend_tag in self._pop_replicas_of_state(ReplicaState.SHOULD_START):
            replica_state.start()
            self._replicas[backend_tag][ReplicaState.STARTING].append(replica_state)

        for replica_state, backend_tag in self._pop_replicas_of_state(ReplicaState.SHOULD_STOP):
            replica_state.stop()
            self._replicas[backend_tag][ReplicaState.STOPPING].append(replica_state)


        transition_triggered = False

        for replica_state, backend_tag in self._pop_replicas_of_state(ReplicaState.STARTING):
            if replica_state.check_started():
                replica_state.set_state(ReplicaState.RUNNING)
                self._replicas[backend_tag][ReplicaState.RUNNING].append(replica_state)
                transition_triggered = True
            else:
                self._replicas[backend_tag][ReplicaState.STARTING].append(replica_state)

        for replica_state, backend_tag in self._pop_replicas_of_state(ReplicaState.STOPPING):
            if replica_state.check_stopped():
                transition_triggered = True
            else:
                self._replicas[backend_tag][ReplicaState.STOPPING].append(replica_state)
        
        for backend_tag in self._replicas.keys().copy():
            if not any(self._replicas[backend_tag]):
                del self._replicas[backend_tag]
                del self._backend_metadata[backend_tag]
                del self._target_replicas[backend_tag]

        if transition_triggered:
            self._checkpoint()
            self._notify_replica_handles_changed()

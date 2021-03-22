import math
import time
from abc import ABC
from collections import defaultdict
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import ray.cloudpickle as pickle
from ray.actor import ActorHandle
from ray.serve.async_goal_manager import AsyncGoalManager
from ray.serve.backend_worker import create_backend_replica
from ray.serve.common import (BackendInfo, BackendTag, Duration, GoalId,
                              ReplicaTag)
from ray.serve.config import BackendConfig, ReplicaConfig
from ray.serve.kv_store import RayInternalKVStore
from ray.serve.long_poll import LongPollHost, LongPollNamespace
from ray.serve.utils import (format_actor_name, get_random_letters, logger)

import ray

CHECKPOINT_KEY = "serve-backend-state-checkpoint"
SLOW_STARTUP_WARNING_S = 30
SLOW_STARTUP_WARNING_PERIOD_S = 30


class ReplicaState(Enum):
    SHOULD_START = 1
    STARTING = 2
    RUNNING = 3
    SHOULD_STOP = 4
    STOPPING = 5
    STOPPED = 6


ALL_REPLICA_STATES = list(ReplicaState)


class ActorReplicaWrapper:
    """Wraps a Ray actor for a backend replica.

    This is primarily defined so that we can mock out actual Ray operations
    for unit testing.

    *All Ray API calls should be made here, not in BackendState.*
    """

    def __init__(self, actor_name: str, detached: bool, controller_name: str,
                 replica_tag: ReplicaTag, backend_tag: BackendTag):
        self._actor_name = actor_name
        self._placement_group_name = self._actor_name + "_placement_group"
        self._detached = detached
        self._controller_name = controller_name
        self._replica_tag = replica_tag
        self._backend_tag = backend_tag

        self._startup_obj_ref = None
        self._drain_obj_ref = None
        self._stopped = False
        self._actor_resources = None

        # Storing the handles is necessary to keep the actor and PG alive in
        # the non-detached case.
        self._actor_handle = None
        self._placement_group = None

    def __get_state__(self) -> Dict[Any, Any]:
        clean_dict = self.__dict__.copy()
        del clean_dict["_startup_obj_ref"]
        del clean_dict["_drain_obj_ref"]
        return clean_dict

    def __set_state__(self, d: Dict[Any, Any]) -> None:
        self.__dict__ = d
        self._startup_obj_ref = None
        self._drain_obj_ref = None

    @property
    def actor_handle(self) -> ActorHandle:
        return ray.get_actor(self._actor_name)

    def start(self, backend_info: BackendInfo):
        self._actor_resources = backend_info.replica_config.resource_dict

        try:
            self._placement_group = ray.util.get_placement_group(
                self._placement_group_name)
        except ValueError:
            logger.debug(
                "Creating placement group '{}' for backend '{}'".format(
                    self._placement_group_name, self._backend_tag))
            self._placement_group = ray.util.placement_group(
                [self._actor_resources],
                lifetime="detached",
                name=self._placement_group_name)

        try:
            self._actor_handle = ray.get_actor(self._actor_name)
        except ValueError:
            logger.debug("Starting replica '{}' for backend '{}'.".format(
                self._replica_tag, self._backend_tag))
            self._actor_handle = ray.remote(backend_info.worker_class).options(
                name=self._actor_name,
                lifetime="detached" if self._detached else None,
                max_restarts=-1,
                max_task_retries=-1,
                placement_group=self._placement_group,
                **backend_info.replica_config.ray_actor_options).remote(
                    self._backend_tag, self._replica_tag,
                    backend_info.replica_config.init_args,
                    backend_info.backend_config, self._controller_name)
        self._startup_obj_ref = self._actor_handle.ready.remote()

    def check_ready(self) -> bool:
        ready, _ = ray.wait([self._startup_obj_ref], timeout=0)
        return len(ready) == 1

    def resource_requirements(
            self) -> Tuple[Dict[str, float], Dict[str, float]]:
        """Returns required and currently available resources.

        Only resources with nonzero requirements will be included in the
        required dict and only resources in the required dict will be
        included in the available dict (filtered for relevance).
        """
        required = {k: v for k, v in self._actor_resources.items() if v > 0}
        available = {
            k: v
            for k, v in ray.available_resources().items() if k in required
        }
        return required, available

    def graceful_stop(self) -> None:
        """Request the actor to exit gracefully."""
        # NOTE: the replicas may already be stopped if we failed
        # after stopping them but before writing a checkpoint.
        if self._stopped:
            return

        try:
            handle = ray.get_actor(self._actor_name)
            self._drain_obj_ref = handle.drain_pending_queries.remote()
        except ValueError:
            self._stopped = True

    def check_stopped(self) -> bool:
        """Check if the actor has exited."""
        if self._stopped:
            return True

        try:
            ray.get_actor(self._actor_name)
            ready, _ = ray.wait([self._drain_obj_ref], timeout=0)
            self._stopped = len(ready) == 1
        except ValueError:
            self._stopped = True

        return self._stopped

    def force_stop(self):
        """Force the actor to exit without shutting down gracefully."""
        try:
            ray.kill(ray.get_actor(self._actor_name))
        except ValueError:
            pass

    def cleanup(self):
        """Clean up any remaining resources after the actor has exited.

        Currently, this just removes the placement group.
        """
        try:
            ray.util.remove_placement_group(
                ray.util.get_placement_group(self._placement_group_name))
        except ValueError:
            pass


class VersionedReplica(ABC):
    @property
    def version(self):
        return self.version


class BackendReplica(VersionedReplica):
    """Manages state transitions for backend replicas.

    This is basically a checkpointable lightweight state machine.
    """

    def __init__(self, controller_name: str, detached: bool,
                 replica_tag: ReplicaTag, backend_tag: BackendTag,
                 version: str):
        self._actor = ActorReplicaWrapper(
            format_actor_name(replica_tag, controller_name), detached,
            controller_name, replica_tag, backend_tag)
        self._controller_name = controller_name
        self._replica_tag = replica_tag
        self._backend_tag = backend_tag
        self._version = version
        self._start_time = None
        self._prev_slow_startup_warning_time = None
        self._state = ReplicaState.SHOULD_START

    def __get_state__(self) -> Dict[Any, Any]:
        return self.__dict__.copy()

    def __set_state__(self, d: Dict[Any, Any]) -> None:
        self.__dict__ = d
        self._recover_from_checkpoint()

    def _recover_from_checkpoint(self) -> None:
        if self._state == ReplicaState.STARTING:
            # We do not need to pass in the class here because the actor
            # creation has already been started if this class was checkpointed
            # in the STARTING state.
            self.start()
        elif self._state == ReplicaState.STOPPING:
            self.stop()

    @property
    def replica_tag(self) -> ReplicaTag:
        return self._replica_tag

    @property
    def version(self):
        return self._version

    @property
    def actor_handle(self) -> ActorHandle:
        assert self._state is not ReplicaState.SHOULD_START, (
            f"State must not be {ReplicaState.SHOULD_START}")
        return self._actor.actor_handle

    def start(self, backend_info: Optional[BackendInfo]) -> None:
        """Transition from SHOULD_START -> STARTING.

        Should handle the case where it's already STARTING.
        """
        assert self._state in {
            ReplicaState.SHOULD_START, ReplicaState.STARTING
        }, (f"State must be {ReplicaState.SHOULD_START} or "
            f"{ReplicaState.STARTING}, *not* {self._state}")

        self._actor.start(backend_info)
        self._start_time = time.time()
        self._prev_slow_startup_warning_time = time.time()
        self._state = ReplicaState.STARTING

    def check_started(self) -> bool:
        """Check if the replica has started. If so, transition to RUNNING.

        Should handle the case where the replica has already stopped.
        """
        if self._state == ReplicaState.RUNNING:
            return True
        assert self._state == ReplicaState.STARTING, (
            f"State must be {ReplicaState.STARTING}, *not* {self._state}")

        if self._actor.check_ready():
            self._state = ReplicaState.RUNNING
            return True

        time_since_start = time.time() - self._start_time
        if (time_since_start > SLOW_STARTUP_WARNING_S
                and time.time() - self._prev_slow_startup_warning_time >
                SLOW_STARTUP_WARNING_PERIOD_S):
            required, available = self._actor.resource_requirements()
            logger.warning(
                f"Replica '{self._replica_tag}' for backend "
                f"'{self._backend_tag}' has taken more than "
                f"{time_since_start:.0f}s to start up. This may be "
                "caused by waiting for the cluster to auto-scale or "
                "because the backend constructor is slow. Resources required: "
                f"{required}, resources available: {available}.")
            self._prev_slow_startup_warning_time = time.time()

        return False

    def set_should_stop(self, graceful_shutdown_timeout_s: Duration) -> None:
        """Mark the replica to be stopped in the future.

        Should handle the case where the replica has already been marked to
        stop.
        """
        self._state = ReplicaState.SHOULD_STOP
        self._graceful_shutdown_timeout_s = graceful_shutdown_timeout_s

    def stop(self) -> None:
        """Stop the replica.

        Should handle the case where the replica is already stopped.
        """
        # We need to handle transitions from:
        #  SHOULD_START -> SHOULD_STOP -> STOPPING
        # This means that the replica_handle may not have been created.

        assert self._state in {
            ReplicaState.SHOULD_STOP, ReplicaState.STOPPING
        }, (f"State must be {ReplicaState.SHOULD_STOP} or "
            f"{ReplicaState.STOPPING}, *not* {self._state}")

        self._actor.graceful_stop()
        self._state = ReplicaState.STOPPING
        self._shutdown_deadline = time.time(
        ) + self._graceful_shutdown_timeout_s

    def check_stopped(self) -> bool:
        """Check if the replica has stopped. If so, transition to STOPPED.

        Should handle the case where the replica has already stopped.
        """
        if self._state == ReplicaState.STOPPED:
            return True
        assert self._state == ReplicaState.STOPPING, (
            f"State must be {ReplicaState.STOPPING}, *not* {self._state}")

        stopped = self._actor.check_stopped()
        if stopped:
            self._state = ReplicaState.STOPPED
            # Clean up any associated resources (e.g., placement group).
            self._actor.cleanup()
            return True

        timeout_passed = time.time() > self._shutdown_deadline

        if timeout_passed:
            # Graceful period passed, kill it forcefully.
            # This will be called repeatedly until the replica shuts down.
            logger.debug(
                f"Replica {self._replica_tag} did not shutdown after "
                f"{self._graceful_shutdown_timeout_s}s, force-killing.")

            self._actor.force_stop()
        return False


class ReplicaStateContainer:
    """Container for mapping ReplicaStates to lists of BackendReplicas."""

    def __init__(self):
        self._replicas: Dict[ReplicaState, List[BackendReplica]] = defaultdict(
            list)

    def add(self, state: ReplicaState, replica: VersionedReplica):
        """Add the provided replica under the provided state.

        Args:
            state (ReplicaState): state to add the replica under.
            replica (VersionedReplica): replica to add.
        """
        assert isinstance(state, ReplicaState)
        assert isinstance(replica, VersionedReplica)
        self._replicas[state].append(replica)

    def get(self, states: Optional[List[ReplicaState]] = None
            ) -> List[BackendReplica]:
        """Get all replicas of the given states.

        This does not remove them from the container. Replicas are returned
        in order of state as passed in.

        Args:
            states (str): states to consider. If not specified, all replicas
                are considered.
        """
        if states is None:
            states = ALL_REPLICA_STATES

        assert isinstance(states, list)

        return sum((self._replicas[state] for state in states), [])

    def pop(self,
            exclude_version: Optional[str] = None,
            states: Optional[List[ReplicaState]] = None,
            max_replicas: Optional[int] = math.inf) -> List[VersionedReplica]:
        """Get and remove all replicas of the given states.

        This removes the replicas from the container. Replicas are returned
        in order of state as passed in.

        Args:
            exclude_version (str): if specified, replicas of the provided
                version will *not* be removed.
            states (str): states to consider. If not specified, all replicas
                are considered.
            max_replicas (int): max number of replicas to return. If not
                specified, will pop all replicas matching the criteria.
        """
        if states is None:
            states = ALL_REPLICA_STATES

        assert exclude_version is None or isinstance(exclude_version, str)
        assert isinstance(states, list)

        replicas = []
        for state in states:
            popped = []
            remaining = []
            for replica in self._replicas[state]:
                if len(replicas) + len(popped) == max_replicas:
                    remaining.append(replica)
                elif (exclude_version is not None
                      and replica.version == exclude_version):
                    remaining.append(replica)
                else:
                    popped.append(replica)
            self._replicas[state] = remaining
            replicas.extend(popped)

        return replicas

    def count(self, states: Optional[List[ReplicaState]] = None):
        """Get the total count of replicas of the given states.

        Args:
            states (str): states to consider. If not specified, all replicas
                are considered.
        """
        if states is None:
            states = ALL_REPLICA_STATES
        assert isinstance(states, list)
        return sum(len(self._replicas[state]) for state in states)

    def __str__(self):
        return str(self._replicas)

    def __repr__(self):
        return repr(self._replicas)


class BackendState:
    """Manages all state for backends in the system.

    This class is *not* thread safe, so any state-modifying methods should be
    called with a lock held.
    """

    def __init__(self, controller_name: str, detached: bool,
                 kv_store: RayInternalKVStore, long_poll_host: LongPollHost,
                 goal_manager: AsyncGoalManager):

        self._controller_name = controller_name
        self._detached = detached
        self._kv_store = kv_store
        self._long_poll_host = long_poll_host
        self._goal_manager = goal_manager
        self._replicas: Dict[BackendTag, ReplicaStateContainer] = dict()
        self._backend_metadata: Dict[BackendTag, BackendInfo] = dict()
        self._target_replicas: Dict[BackendTag, int] = defaultdict(int)
        self._backend_goals: Dict[BackendTag, GoalId] = dict()
        self._target_versions: Dict[BackendTag, str] = dict()

        checkpoint = self._kv_store.get(CHECKPOINT_KEY)
        if checkpoint is not None:
            (self._replicas, self._backend_metadata, self._target_replicas,
             self._target_versions,
             self._backend_goals) = pickle.loads(checkpoint)

            for goal_id in self._backend_goals.values():
                self._goal_manager.create_goal(goal_id)

        self._notify_backend_configs_changed()
        self._notify_replica_handles_changed()

    def _checkpoint(self) -> None:
        self._kv_store.put(
            CHECKPOINT_KEY,
            pickle.dumps(
                (self._replicas, self._backend_metadata, self._target_replicas,
                 self._target_versions, self._backend_goals)))

    def _notify_backend_configs_changed(
            self, key: Optional[BackendTag] = None) -> None:
        for key, config in self.get_backend_configs(key).items():
            self._long_poll_host.notify_changed(
                (LongPollNamespace.BACKEND_CONFIGS, key),
                config,
            )

    def get_running_replica_handles(
            self,
            filter_tag: Optional[BackendTag] = None,
    ) -> Dict[BackendTag, Dict[ReplicaTag, ActorHandle]]:
        return {
            backend_tag: {
                backend_replica.replica_tag: backend_replica.actor_handle
                for backend_replica in replicas_container.get(
                    [ReplicaState.RUNNING])
            }
            for backend_tag, replicas_container in self._replicas.items()
            if filter_tag is None or backend_tag == filter_tag
        }

    def _notify_replica_handles_changed(
            self, key: Optional[BackendTag] = None) -> None:
        for key, replica_dict in self.get_running_replica_handles(key).items():
            self._long_poll_host.notify_changed(
                (LongPollNamespace.REPLICA_HANDLES, key),
                list(replica_dict.values()),
            )

    def get_backend_configs(
            self,
            filter_tag: Optional[BackendTag] = None,
    ) -> Dict[BackendTag, BackendConfig]:
        return {
            tag: info.backend_config
            for tag, info in self._backend_metadata.items()
            if filter_tag is None or tag == filter_tag
        }

    def get_backend(self, backend_tag: BackendTag) -> Optional[BackendInfo]:
        return self._backend_metadata.get(backend_tag)

    def _set_backend_goal(self, backend_tag: BackendTag,
                          backend_info: BackendInfo,
                          version: Optional[str]) -> None:
        existing_goal_id = self._backend_goals.get(backend_tag)
        new_goal_id = self._goal_manager.create_goal()

        if backend_info is not None:
            self._backend_metadata[backend_tag] = backend_info
            self._target_replicas[
                backend_tag] = backend_info.backend_config.num_replicas
        else:
            self._target_replicas[backend_tag] = 0

        self._backend_goals[backend_tag] = new_goal_id
        self._target_versions[backend_tag] = version

        return new_goal_id, existing_goal_id

    def deploy_backend(self,
                       backend_tag: BackendTag,
                       backend_config: BackendConfig,
                       replica_config: ReplicaConfig,
                       version: Optional[str] = None) -> Optional[GoalId]:
        # Ensures this method is idempotent.
        backend_info = self._backend_metadata.get(backend_tag)
        if backend_info is not None:
            # Old codepath.
            if version is None:
                if (backend_info.backend_config == backend_config
                        and backend_info.replica_config == replica_config):
                    return self._backend_goals.get(backend_tag, None)
            # New codepath: treat version as ground truth for implementation.
            else:
                if (backend_info.backend_config == backend_config
                        and self._target_versions[backend_tag] == version):
                    return self._backend_goals.get(backend_tag, None)

        if backend_tag not in self._replicas:
            self._replicas[backend_tag] = ReplicaStateContainer()

        backend_replica_class = create_backend_replica(
            replica_config.backend_def)

        # Save creator that starts replicas, the arguments to be passed in,
        # and the configuration for the backends.
        backend_info = BackendInfo(
            worker_class=backend_replica_class,
            backend_config=backend_config,
            replica_config=replica_config)

        new_goal_id, existing_goal_id = self._set_backend_goal(
            backend_tag, backend_info, version)

        # NOTE(edoakes): we must write a checkpoint before starting new
        # or pushing the updated config to avoid inconsistent state if we
        # crash while making the change.
        self._checkpoint()
        self._notify_backend_configs_changed(backend_tag)

        if existing_goal_id is not None:
            self._goal_manager.complete_goal(existing_goal_id)
        return new_goal_id

    def delete_backend(self, backend_tag: BackendTag,
                       force_kill: bool = False) -> Optional[GoalId]:
        # This method must be idempotent. We should validate that the
        # specified backend exists on the client.
        if backend_tag not in self._backend_metadata:
            return None

        new_goal_id, existing_goal_id = self._set_backend_goal(
            backend_tag, None, None)
        if force_kill:
            self._backend_metadata[
                backend_tag].backend_config.\
                    experimental_graceful_shutdown_timeout_s = 0

        self._checkpoint()
        if existing_goal_id is not None:
            self._goal_manager.complete_goal(existing_goal_id)
        return new_goal_id

    def _stop_wrong_version_replicas(
            self, backend_tag: BackendTag, version: str,
            graceful_shutdown_timeout_s: float) -> int:
        # NOTE(edoakes): this short-circuits when using the legacy
        # `create_backend` codepath -- it can be removed once we deprecate
        # that as the version should never be None.
        if version is None:
            return

        # TODO(edoakes): to implement rolling upgrade, all we should need to
        # do is cap the number of old version replicas that are stopped here.
        replicas_to_stop = self._replicas[backend_tag].pop(
            exclude_version=version,
            states=[
                ReplicaState.SHOULD_START, ReplicaState.STARTING,
                ReplicaState.RUNNING
            ])

        # Inform the routers and backend replicas about config changes.
        # TODO(edoakes): this should only happen if we change something other
        # than num_replicas.
        self._notify_backend_configs_changed(backend_tag)
        if len(replicas_to_stop) > 0:
            logger.info(f"Stopping {len(replicas_to_stop)} replicas of "
                        f"backend '{backend_tag}' with outdated versions.")

        for replica in replicas_to_stop:
            replica.set_should_stop(graceful_shutdown_timeout_s)
            self._replicas[backend_tag].add(ReplicaState.SHOULD_STOP, replica)

    def _scale_backend_replicas(
            self,
            backend_tag: BackendTag,
            num_replicas: int,
            version: str,
    ) -> bool:
        """Scale the given backend to the number of replicas.

        NOTE: this does not actually start or stop the replicas, but instead
        adds them to ReplicaState.SHOULD_START or ReplicaState.SHOULD_STOP.
        The caller is responsible for then first writing a checkpoint and then
        actually starting/stopping the intended replicas. This avoids
        inconsistencies with starting/stopping a replica and then crashing
        before writing a checkpoint.
        """
        assert (backend_tag in self._backend_metadata
                ), "Backend {} is not registered.".format(backend_tag)
        assert num_replicas >= 0, ("Number of replicas must be"
                                   " greater than or equal to 0.")

        backend_info: BackendInfo = self._backend_metadata[backend_tag]
        graceful_shutdown_timeout_s = (
            backend_info.backend_config.
            experimental_graceful_shutdown_timeout_s)

        self._stop_wrong_version_replicas(backend_tag, version,
                                          graceful_shutdown_timeout_s)

        current_num_replicas = self._replicas[backend_tag].count(states=[
            ReplicaState.SHOULD_START, ReplicaState.STARTING,
            ReplicaState.RUNNING
        ])

        delta_num_replicas = num_replicas - current_num_replicas

        if delta_num_replicas == 0:
            return False

        logger.debug(
            f"Scaling backend '{backend_tag}' from {current_num_replicas} "
            f"to {num_replicas} replicas")

        if delta_num_replicas > 0:
            logger.debug("Adding {} replicas to backend {}".format(
                delta_num_replicas, backend_tag))
            for _ in range(delta_num_replicas):
                replica_tag = "{}#{}".format(backend_tag, get_random_letters())
                self._replicas[backend_tag].add(
                    ReplicaState.SHOULD_START,
                    BackendReplica(self._controller_name, self._detached,
                                   replica_tag, backend_tag, version))

        elif delta_num_replicas < 0:
            logger.debug("Removing {} replicas from backend '{}'".format(
                -delta_num_replicas, backend_tag))
            assert self._target_replicas[backend_tag] >= delta_num_replicas

            for _ in range(-delta_num_replicas):
                replicas_to_stop = self._replicas[backend_tag].pop(
                    states=[
                        ReplicaState.SHOULD_START, ReplicaState.STARTING,
                        ReplicaState.RUNNING
                    ],
                    max_replicas=-delta_num_replicas)

                for replica in replicas_to_stop:
                    replica.set_should_stop(graceful_shutdown_timeout_s)
                    self._replicas[backend_tag].add(ReplicaState.SHOULD_STOP,
                                                    replica)

        return True

    def _scale_all_backends(self):
        checkpoint_needed = False
        for backend_tag, num_replicas in list(self._target_replicas.items()):
            checkpoint_needed |= self._scale_backend_replicas(
                backend_tag, num_replicas, self._target_versions[backend_tag])

        if checkpoint_needed:
            self._checkpoint()

    def _completed_goals(self) -> List[GoalId]:
        completed_goals = []
        deleted_backends = []
        for backend_tag in self._replicas:
            target_count = self._target_replicas.get(backend_tag, 0)

            # If we have pending ops, the current goal is *not* ready.
            if (self._replicas[backend_tag].count(states=[
                    ReplicaState.SHOULD_START,
                    ReplicaState.STARTING,
                    ReplicaState.SHOULD_STOP,
                    ReplicaState.STOPPING,
            ]) > 0):
                continue

            running = self._replicas[backend_tag].get(
                states=[ReplicaState.RUNNING])
            running_count = len(running)

            # Check for deleting.
            if target_count == 0 and running_count == 0:
                deleted_backends.append(backend_tag)
                completed_goals.append(
                    self._backend_goals.pop(backend_tag, None))

            # Check for a non-zero number of backends.
            elif target_count == running_count:
                # Check that all running replicas are the target version.
                target_version = self._target_versions[backend_tag]
                if all(r.version == target_version for r in running):
                    completed_goals.append(
                        self._backend_goals.pop(backend_tag, None))

        for backend_tag in deleted_backends:
            del self._replicas[backend_tag]
            del self._backend_metadata[backend_tag]
            del self._target_replicas[backend_tag]
            del self._target_versions[backend_tag]

        return [goal for goal in completed_goals if goal]

    def update(self) -> bool:
        """Updates the state of all running replicas to match the goal state.
        """
        self._scale_all_backends()

        for goal_id in self._completed_goals():
            self._goal_manager.complete_goal(goal_id)

        transitioned_backend_tags = set()
        for backend_tag, replicas in self._replicas.items():
            for replica in replicas.pop(states=[ReplicaState.SHOULD_START]):
                replica.start(self._backend_metadata[backend_tag])
                replicas.add(ReplicaState.STARTING, replica)

            for replica in replicas.pop(states=[ReplicaState.SHOULD_STOP]):
                replica.stop()
                replicas.add(ReplicaState.STOPPING, replica)

            for replica in replicas.pop(states=[ReplicaState.STARTING]):
                if replica.check_started():
                    replicas.add(ReplicaState.RUNNING, replica)
                    transitioned_backend_tags.add(backend_tag)
                else:
                    replicas.add(ReplicaState.STARTING, replica)

            for replica in replicas.pop(states=[ReplicaState.STOPPING]):
                if replica.check_stopped():
                    transitioned_backend_tags.add(backend_tag)
                else:
                    replicas.add(ReplicaState.STOPPING, replica)

        if len(transitioned_backend_tags) > 0:
            self._checkpoint()
            [
                self._notify_replica_handles_changed(tag)
                for tag in transitioned_backend_tags
            ]

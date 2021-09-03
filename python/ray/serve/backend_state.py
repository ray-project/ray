import math
import time
from abc import ABC
from collections import defaultdict, OrderedDict
from collections.abc import Iterable
from enum import Enum
import os
from typing import Any, Callable, Dict, List, Optional, Tuple

import ray
from ray import cloudpickle
from ray.actor import ActorHandle
from ray.serve.async_goal_manager import AsyncGoalManager
from ray.serve.common import (BackendInfo, BackendTag, Duration, GoalId,
                              ReplicaTag)
from ray.serve.config import BackendConfig
from ray.serve.constants import (MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT,
                                 MAX_NUM_DELETED_DEPLOYMENTS)
from ray.serve.storage.kv_store import RayInternalKVStore
from ray.serve.long_poll import LongPollHost, LongPollNamespace
from ray.serve.utils import format_actor_name, get_random_letters, logger

CHECKPOINT_KEY = "serve-backend-state-checkpoint"
SLOW_STARTUP_WARNING_S = 30
SLOW_STARTUP_WARNING_PERIOD_S = 30


class ReplicaState(Enum):
    SHOULD_START = 1
    STARTING_OR_UPDATING = 2
    RUNNING = 3
    SHOULD_STOP = 4
    STOPPING = 5


class ReplicaStartupStatus(Enum):
    PENDING = 1
    PENDING_SLOW_START = 2
    SUCCEEDED = 3
    FAILED = 4


class GoalStatus(Enum):
    NONE = 1
    PENDING = 2
    SUCCEEDED = 3
    SUCCESSFULLY_DELETED = 4
    FAILED = 5


ALL_REPLICA_STATES = list(ReplicaState)
USE_PLACEMENT_GROUP = os.environ.get("SERVE_USE_PLACEMENT_GROUP", "1") != "0"


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

        self._ready_obj_ref = None
        self._drain_obj_ref = None
        self._actor_resources = None
        self._health_check_ref = None

        # Storing the handles is necessary to keep the actor and PG alive in
        # the non-detached case.
        self._actor_handle = None
        self._placement_group = None

    def __get_state__(self) -> Dict[Any, Any]:
        clean_dict = self.__dict__.copy()
        del clean_dict["_ready_obj_ref"]
        del clean_dict["_drain_obj_ref"]
        return clean_dict

    def __set_state__(self, d: Dict[Any, Any]) -> None:
        self.__dict__ = d
        self._ready_obj_ref = None
        self._drain_obj_ref = None

    @property
    def actor_handle(self) -> ActorHandle:
        return ray.get_actor(self._actor_name)

    def start_or_update(self, backend_info: BackendInfo):
        self._actor_resources = backend_info.replica_config.resource_dict

        # Feature flagging because of placement groups doesn't handle
        # newly added nodes.
        # https://github.com/ray-project/ray/issues/15801
        if USE_PLACEMENT_GROUP:
            try:
                self._placement_group = ray.util.get_placement_group(
                    self._placement_group_name)
            except ValueError:
                logger.debug(
                    "Creating placement group '{}' for deployment '{}'".format(
                        self._placement_group_name, self._backend_tag) +
                    f" component=serve deployment={self._backend_tag}")
                self._placement_group = ray.util.placement_group(
                    [self._actor_resources],
                    lifetime="detached" if self._detached else None,
                    name=self._placement_group_name)

        try:
            self._actor_handle = ray.get_actor(self._actor_name)
        except ValueError:
            logger.debug("Starting replica '{}' for deployment '{}'.".format(
                self._replica_tag, self._backend_tag) +
                         f" component=serve deployment={self._backend_tag} "
                         f"replica={self._replica_tag}")

            self._actor_handle = backend_info.actor_def.options(
                name=self._actor_name,
                lifetime="detached" if self._detached else None,
                placement_group=self._placement_group,
                placement_group_capture_child_tasks=False,
                **backend_info.replica_config.ray_actor_options).remote(
                    self._backend_tag, self._replica_tag,
                    backend_info.replica_config.init_args,
                    backend_info.backend_config, self._controller_name,
                    self._detached)

        self._ready_obj_ref = self._actor_handle.reconfigure.remote(
            backend_info.backend_config.user_config)

    def check_ready(self) -> ReplicaStartupStatus:
        """
        Check if current replica has started by making ray API calls on
        relevant actor / object ref.

        Returns:
            state (ReplicaStartupStatus):
                PENDING:
                    - replica reconfigure() haven't returned.
                FAILED:
                    - replica __init__() failed.
                SUCCEEDED:
                    - replica __init__() and reconfigure() succeeded.
        """
        ready, _ = ray.wait([self._ready_obj_ref], timeout=0)
        # In case of deployment constructor failure, ray.get will help to
        # surface exception to each update() cycle.
        if len(ready) == 0:
            return ReplicaStartupStatus.PENDING
        elif len(ready) > 0:
            try:
                ray.get(ready)
            except Exception:
                return ReplicaStartupStatus.FAILED

        return ReplicaStartupStatus.SUCCEEDED

    @property
    def actor_resources(self) -> Dict[str, float]:
        return self._actor_resources

    @property
    def available_resources(self) -> Dict[str, float]:
        return ray.available_resources()

    def graceful_stop(self) -> None:
        """Request the actor to exit gracefully."""
        try:
            handle = ray.get_actor(self._actor_name)
            self._drain_obj_ref = handle.drain_pending_queries.remote()
        except ValueError:
            pass

    def check_stopped(self) -> bool:
        """Check if the actor has exited."""
        try:
            handle = ray.get_actor(self._actor_name)
            ready, _ = ray.wait([self._drain_obj_ref], timeout=0)
            stopped = len(ready) == 1
            if stopped:
                ray.kill(handle, no_restart=True)
        except ValueError:
            stopped = True

        return stopped

    def check_health(self) -> bool:
        """Check if the actor is healthy."""
        if self._health_check_ref is None:
            self._health_check_ref = self._actor_handle.run_forever.remote()

        ready, _ = ray.wait([self._health_check_ref], timeout=0)

        return len(ready) == 0

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
        if not USE_PLACEMENT_GROUP:
            return

        try:
            ray.util.remove_placement_group(
                ray.util.get_placement_group(self._placement_group_name))
        except ValueError:
            pass


class BackendVersion:
    def __init__(self,
                 code_version: Optional[str],
                 user_config: Optional[Any] = None):
        if code_version is not None and not isinstance(code_version, str):
            raise TypeError(
                f"code_version must be str, got {type(code_version)}.")
        if code_version is None:
            self.unversioned = True
            self.code_version = get_random_letters()
        else:
            self.unversioned = False
            self.code_version = code_version
        self.user_config_hash = self._hash_user_config(user_config)
        self._hash = hash((self.code_version, self.user_config_hash))

    def _hash_user_config(self, user_config: Any) -> int:
        """Hash the user config.

        We want users to be able to pass lists and dictionaries for
        convenience, but these are not hashable types because they're mutable.

        This supports lists and dictionaries by recursively converting them
        into immutable tuples and then hashing them.
        """
        try:
            return hash(user_config)
        except TypeError:
            pass

        if isinstance(user_config, dict):
            keys = tuple(sorted(user_config))
            val_hashes = tuple(
                self._hash_user_config(user_config[k]) for k in keys)
            return hash((hash(keys), hash(val_hashes)))
        elif isinstance(user_config, Iterable):
            return hash(
                tuple(self._hash_user_config(item) for item in user_config))
        else:
            raise TypeError(
                "user_config must contain only lists, dicts, or hashable "
                f"types. Got {type(user_config)}.")

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: Any) -> bool:
        return self._hash == other._hash


class VersionedReplica(ABC):
    @property
    def version(self) -> BackendVersion:
        pass


class BackendReplica(VersionedReplica):
    """Manages state transitions for backend replicas.

    This is basically a checkpointable lightweight state machine.
    """

    def __init__(self, controller_name: str, detached: bool,
                 replica_tag: ReplicaTag, backend_tag: BackendTag,
                 version: BackendVersion):
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
        if self._state == ReplicaState.STARTING_OR_UPDATING:
            # We do not need to pass in the class here because the actor
            # creation has already been started if this class was checkpointed
            # in the STARTING_OR_UPDATING state.
            self.start_or_update()
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

    def start_or_update(self, backend_info: BackendInfo,
                        version: BackendVersion) -> None:
        """Transition from SHOULD_START -> STARTING_OR_UPDATING.

        Should handle the case where it's already STARTING_OR_UPDATING.
        """
        assert self._state in {
            ReplicaState.SHOULD_START,
            ReplicaState.STARTING_OR_UPDATING,
            ReplicaState.RUNNING,
        }, (f"State must be {ReplicaState.SHOULD_START}, "
            f"{ReplicaState.STARTING_OR_UPDATING}, or"
            f"{ReplicaState.RUNNING}, *not* {self._state}.")

        self._actor.start_or_update(backend_info)
        self._start_time = time.time()
        self._prev_slow_startup_warning_time = time.time()
        self._state = ReplicaState.STARTING_OR_UPDATING
        self._version = version

    def check_started(self) -> ReplicaStartupStatus:
        """Check if the replica has started. If so, transition to RUNNING.

        Should handle the case where the replica has already stopped.

        Returns:
            status (ReplicaStartupStatus): Most recent state of replica by
                querying actor obj ref
        """
        if self._state == ReplicaState.RUNNING:
            return ReplicaStartupStatus.SUCCEEDED

        assert self._state == ReplicaState.STARTING_OR_UPDATING, (
            f"State must be {ReplicaState.STARTING_OR_UPDATING}, "
            f"*not* {self._state}.")

        status = self._actor.check_ready()
        if status == ReplicaStartupStatus.SUCCEEDED:
            self._state = ReplicaState.RUNNING
        elif status == ReplicaStartupStatus.PENDING:
            if time.time() - self._start_time > SLOW_STARTUP_WARNING_S:
                status = ReplicaStartupStatus.PENDING_SLOW_START
        elif status == ReplicaStartupStatus.FAILED:
            self._state = ReplicaState.SHOULD_STOP

        return status

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
        """Check if the replica has finished stopping."""
        assert self._state == ReplicaState.STOPPING, (
            f"State must be {ReplicaState.STOPPING}, *not* {self._state}")

        if self._actor.check_stopped():
            # Clean up any associated resources (e.g., placement group).
            self._actor.cleanup()
            return True

        timeout_passed = time.time() >= self._shutdown_deadline

        if timeout_passed:
            # Graceful period passed, kill it forcefully.
            # This will be called repeatedly until the replica shuts down.
            logger.debug(
                f"Replica {self._replica_tag} did not shutdown after "
                f"{self._graceful_shutdown_timeout_s}s, force-killing. "
                f"component=serve deployment={self._backend_tag} "
                f"replica={self._replica_tag}")

            self._actor.force_stop()
        return False

    def check_health(self) -> bool:
        """Check if the replica is still alive.

        Returns `True` if the replica is healthy, else `False`.
        """
        return self._actor.check_health()

    def resource_requirements(
            self) -> Tuple[Dict[str, float], Dict[str, float]]:
        """Returns required and currently available resources.

        Only resources with nonzero requirements will be included in the
        required dict and only resources in the required dict will be
        included in the available dict (filtered for relevance).
        """
        required = {
            k: v
            for k, v in self._actor.actor_resources.items() if v > 0
        }
        available = {
            k: v
            for k, v in self._actor.available_resources.items()
            if k in required
        }
        return required, available


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
            exclude_version: Optional[BackendVersion] = None,
            states: Optional[List[ReplicaState]] = None,
            max_replicas: Optional[int] = math.inf) -> List[VersionedReplica]:
        """Get and remove all replicas of the given states.

        This removes the replicas from the container. Replicas are returned
        in order of state as passed in.

        Args:
            exclude_version (BackendVersion): if specified, replicas of the
                provided version will *not* be removed.
            states (str): states to consider. If not specified, all replicas
                are considered.
            max_replicas (int): max number of replicas to return. If not
                specified, will pop all replicas matching the criteria.
        """
        if states is None:
            states = ALL_REPLICA_STATES

        assert (exclude_version is None
                or isinstance(exclude_version, BackendVersion))
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

    def count(self,
              exclude_version: Optional[BackendVersion] = None,
              version: Optional[BackendVersion] = None,
              states: Optional[List[ReplicaState]] = None):
        """Get the total count of replicas of the given states.

        Args:
            exclude_version(BackendVersion): version to exclude. If not
                specified, all versions are considered.
            version(BackendVersion): version to filter to. If not specified,
                all versions are considered.
            states (str): states to consider. If not specified, all replicas
                are considered.
        """
        if states is None:
            states = ALL_REPLICA_STATES
        assert isinstance(states, list)
        assert (exclude_version is None
                or isinstance(exclude_version, BackendVersion))
        assert version is None or isinstance(version, BackendVersion)
        if exclude_version is None and version is None:
            return sum(len(self._replicas[state]) for state in states)
        elif exclude_version is None and version is not None:
            return sum(
                len(
                    list(
                        filter(lambda r: r.version == version, self._replicas[
                            state]))) for state in states)
        elif exclude_version is not None and version is None:
            return sum(
                len(
                    list(
                        filter(lambda r: r.version != exclude_version,
                               self._replicas[state]))) for state in states)
        else:
            raise ValueError(
                "Only one of `version` or `exclude_version` may be provided.")

    def __str__(self):
        return str(self._replicas)

    def __repr__(self):
        return repr(self._replicas)


class BackendState:
    """Manages the target state and replicas for a single backend."""

    def __init__(self, name: str, controller_name: str, detached: bool,
                 long_poll_host: LongPollHost, goal_manager: AsyncGoalManager,
                 checkpoint_fn: Callable):
        self._name = name
        self._controller_name: str = controller_name
        self._detached: bool = detached
        self._long_poll_host: LongPollHost = long_poll_host
        self._goal_manager: AsyncGoalManager = goal_manager
        self._checkpoint_fn = checkpoint_fn

        # Each time we set a new backend goal, we're trying to save new
        # BackendInfo and bring current deployment to meet new status.
        # In case the new backend goal failed to complete, we keep track of
        # previous BackendInfo and rollback to it.
        self._target_info: BackendInfo = None
        self._rollback_info: BackendInfo = None
        self._target_replicas: int = -1
        self._curr_goal: Optional[GoalId] = None
        self._target_version: BackendVersion = None
        self._prev_startup_warning: float = -1
        self._replica_constructor_retry_counter: int = 0
        self._replicas: ReplicaStateContainer = ReplicaStateContainer()

    def get_checkpoint_data(self):
        return (self._target_info, self._rollback_info, self._target_replicas,
                self._curr_goal, self._target_version,
                self._prev_startup_warning,
                self._replica_constructor_retry_counter, self._replicas)

    def recover_from_checkpoint_data(self, data):
        (self._target_info, self._rollback_info, self._target_replicas,
         self._curr_goal, self._target_version, self._prev_startup_warning,
         self._replica_constructor_retry_counter, self._replicas) = data
        if self._curr_goal is not None:
            self._goal_manager.create_goal(self._curr_goal)

        self._notify_backend_configs_changed()
        self._notify_replica_handles_changed()

    @property
    def target_info(self) -> BackendInfo:
        return self._target_info

    @property
    def curr_goal(self) -> Optional[GoalId]:
        return self._curr_goal

    def get_running_replica_handles(self) -> Dict[ReplicaTag, ActorHandle]:
        return {
            replica.replica_tag: replica.actor_handle
            for replica in self._replicas.get([ReplicaState.RUNNING])
        }

    def _notify_replica_handles_changed(self):
        self._long_poll_host.notify_changed(
            (LongPollNamespace.REPLICA_HANDLES, self._name),
            list(self.get_running_replica_handles().values()),
        )

    def _notify_backend_configs_changed(self) -> None:
        self._long_poll_host.notify_changed(
            (LongPollNamespace.BACKEND_CONFIGS, self._name),
            self._target_info.backend_config,
        )

    def _set_backend_goal(self, backend_info: Optional[BackendInfo]) -> None:
        """
        Set desirable state for a given backend, identified by tag.

        Args:
            backend_info (Optional[BackendInfo]): Contains backend and
                replica config, if passed in as None, we're marking
                target backend as shutting down.
        """
        existing_goal_id = self._curr_goal
        new_goal_id = self._goal_manager.create_goal()

        if backend_info is not None:
            self._target_info = backend_info
            self._target_replicas = backend_info.backend_config.num_replicas
            self._target_version = BackendVersion(
                backend_info.version,
                user_config=backend_info.backend_config.user_config)

        else:
            self._target_replicas = 0

        self._curr_goal = new_goal_id
        logger.debug(
            f"Set backend goal for {self._name} with version "
            f"{backend_info if backend_info is None else backend_info.version}"
        )
        return new_goal_id, existing_goal_id

    def deploy(self,
               backend_info: BackendInfo) -> Tuple[Optional[GoalId], bool]:
        """Deploy the backend.

        If the backend already exists with the same version, this is a no-op
        and returns the GoalId corresponding to the existing update if there
        is one.

        Returns:
            GoalId, bool: The GoalId for the client to wait for and whether or
            not the backend is being updated.
        """
        # Ensures this method is idempotent.
        existing_info = self._target_info
        if existing_info is not None:
            # Redeploying should not reset the deployment's start time.
            backend_info.start_time_ms = existing_info.start_time_ms

            if (existing_info.backend_config == backend_info.backend_config
                    and backend_info.version is not None
                    and existing_info.version == backend_info.version):
                return self._curr_goal, False

        # Keep a copy of previous backend info in case goal failed to
        # complete to initiate rollback.
        self._rollback_info = self._target_info

        # Reset constructor retry counter.
        self._replica_constructor_retry_counter = 0

        new_goal_id, existing_goal_id = self._set_backend_goal(backend_info)

        # NOTE(edoakes): we must write a checkpoint before starting new
        # or pushing the updated config to avoid inconsistent state if we
        # crash while making the change.
        self._checkpoint_fn()
        self._notify_backend_configs_changed()

        if existing_goal_id is not None:
            self._goal_manager.complete_goal(existing_goal_id)
        return new_goal_id, True

    def delete(self, force_kill: bool = False) -> Optional[GoalId]:
        new_goal_id, existing_goal_id = self._set_backend_goal(None)
        if force_kill:
            self._target_info.backend_config.\
                experimental_graceful_shutdown_timeout_s = 0

        self._checkpoint_fn()
        self._notify_backend_configs_changed()
        if existing_goal_id is not None:
            self._goal_manager.complete_goal(existing_goal_id)
        return new_goal_id

    def _stop_wrong_version_replicas(self) -> int:
        """Stops replicas with outdated versions to implement rolling updates.

        This includes both explicit code version updates and changes to the
        user_config.
        """
        # Short circuit if target replicas is 0 (the backend is being deleted)
        # because this will be handled in the main loop.
        if self._target_replicas == 0:
            return 0

        # We include SHOULD_START and STARTING_OR_UPDATING replicas here
        # because if there are replicas still pending startup, we may as well
        # terminate them and start new version replicas instead.
        old_running_replicas = self._replicas.count(
            exclude_version=self._target_version,
            states=[
                ReplicaState.SHOULD_START, ReplicaState.STARTING_OR_UPDATING,
                ReplicaState.RUNNING
            ])
        old_stopping_replicas = self._replicas.count(
            exclude_version=self._target_version,
            states=[ReplicaState.SHOULD_STOP, ReplicaState.STOPPING])
        new_running_replicas = self._replicas.count(
            version=self._target_version, states=[ReplicaState.RUNNING])

        # If the backend is currently scaling down, let the scale down
        # complete before doing a rolling update.
        if (self._target_replicas <
                old_running_replicas + old_stopping_replicas):
            return 0

        # The number of replicas that are currently in transition between
        # an old version and the new version. Note that we cannot directly
        # count the number of stopping replicas because once replicas finish
        # stopping, they are removed from the data structure.
        pending_replicas = (self._target_replicas - new_running_replicas -
                            old_running_replicas)

        # Maximum number of replicas that can be updating at any given time.
        # There should never be more than rollout_size old replicas stopping
        # or rollout_size new replicas starting.
        rollout_size = max(int(0.2 * self._target_replicas), 1)
        max_to_stop = max(rollout_size - pending_replicas, 0)

        replicas_to_update = self._replicas.pop(
            exclude_version=self._target_version,
            states=[
                ReplicaState.SHOULD_START, ReplicaState.STARTING_OR_UPDATING,
                ReplicaState.RUNNING
            ],
            max_replicas=max_to_stop)

        graceful_shutdown_timeout_s = (
            self._target_info.backend_config.
            experimental_graceful_shutdown_timeout_s)
        code_version_changes = 0
        user_config_changes = 0
        for replica in replicas_to_update:
            # If the code version is a mismatch, we stop the replica. A new one
            # with the correct version will be started later as part of the
            # normal scale-up process.
            if (replica.version.code_version !=
                    self._target_version.code_version):
                code_version_changes += 1
                replica.set_should_stop(graceful_shutdown_timeout_s)
                self._replicas.add(ReplicaState.SHOULD_STOP, replica)
            # If only the user_config is a mismatch, we update it dynamically
            # without restarting the replica.
            elif (replica.version.user_config_hash !=
                  self._target_version.user_config_hash):
                user_config_changes += 1
                replica.start_or_update(self._target_info,
                                        self._target_version)
                self._replicas.add(ReplicaState.STARTING_OR_UPDATING, replica)
            else:
                assert False, "Update must be code version or user config."

        if code_version_changes > 0:
            logger.info(f"Stopping {code_version_changes} replicas of "
                        f"deployment '{self._name}' with outdated versions. "
                        f"component=serve deployment={self._name}")

        if user_config_changes > 0:
            logger.info(f"Updating {user_config_changes} replicas of "
                        f"deployment '{self._name}' with outdated "
                        f"user_configs. component=serve "
                        f"deployment={self._name}")

        return len(replicas_to_update)

    def _scale_backend_replicas(self) -> bool:
        """Scale the given backend to the number of replicas.

        NOTE: this does not actually start or stop the replicas, but instead
        adds them to ReplicaState.SHOULD_START or ReplicaState.SHOULD_STOP.
        The caller is responsible for then first writing a checkpoint and then
        actually starting/stopping the intended replicas. This avoids
        inconsistencies with starting/stopping a replica and then crashing
        before writing a checkpoint.
        """
        assert self._target_replicas >= 0, ("Number of replicas must be"
                                            " greater than or equal to 0.")

        graceful_shutdown_timeout_s = (
            self._target_info.backend_config.
            experimental_graceful_shutdown_timeout_s)

        self._stop_wrong_version_replicas()

        current_replicas = self._replicas.count(states=[
            ReplicaState.SHOULD_START, ReplicaState.STARTING_OR_UPDATING,
            ReplicaState.RUNNING
        ])

        delta_replicas = self._target_replicas - current_replicas
        if delta_replicas == 0:
            return False

        elif delta_replicas > 0:
            # Don't ever exceed self._target_replicas.
            stopping_replicas = self._replicas.count(states=[
                ReplicaState.SHOULD_STOP,
                ReplicaState.STOPPING,
            ])
            to_add = max(delta_replicas - stopping_replicas, 0)
            if to_add > 0:
                logger.info(f"Adding {to_add} replicas to deployment "
                            f"'{self._name}'. component=serve "
                            f"deployment={self._name}")
            for _ in range(to_add):
                replica_tag = "{}#{}".format(self._name, get_random_letters())
                self._replicas.add(
                    ReplicaState.SHOULD_START,
                    BackendReplica(self._controller_name, self._detached,
                                   replica_tag, self._name,
                                   self._target_version))
                logger.debug(
                    f"Adding SHOULD_START to replica_tag: {replica_tag}, "
                    f"backend_tag: {self._name}")

        elif delta_replicas < 0:
            to_remove = -delta_replicas
            logger.info(f"Removing {to_remove} replicas from deployment "
                        f"'{self._name}'. component=serve "
                        f"deployment={self._name}")
            replicas_to_stop = self._replicas.pop(
                states=[
                    ReplicaState.SHOULD_START,
                    ReplicaState.STARTING_OR_UPDATING, ReplicaState.RUNNING
                ],
                max_replicas=to_remove)

            for replica in replicas_to_stop:
                logger.debug(f"Adding SHOULD_STOP to replica_tag: {replica}, "
                             f"backend_tag: {self._name}")
                replica.set_should_stop(graceful_shutdown_timeout_s)
                self._replicas.add(ReplicaState.SHOULD_STOP, replica)

        return True

    def _check_curr_goal_status(self) -> GoalStatus:
        """
        In each update() cycle, upon finished calling _scale_all_backends(),
        check difference between target vs. running relica count for each
        backend and return whether or not the current goal is complete.

        Returns:
            AsyncGoalStatus
        """

        if self._curr_goal is None:
            return GoalStatus.NONE

        target_version = self._target_version
        target_replica_count = self._target_replicas

        all_running_replica_cnt = self._replicas.count(
            states=[ReplicaState.RUNNING])
        running_at_target_version_replica_cnt = self._replicas.count(
            states=[ReplicaState.RUNNING], version=target_version)

        failed_to_start_count = self._replica_constructor_retry_counter
        failed_to_start_threshold = min(MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT,
                                        target_replica_count * 3)

        # Got to make a call to complete current deploy() goal after
        # start failure threshold reached, while we might still have
        # pending replicas in current goal.
        if (failed_to_start_count >= failed_to_start_threshold
                and failed_to_start_threshold != 0):
            if running_at_target_version_replica_cnt > 0:
                # At least one RUNNING replica at target state, partial
                # success; We can stop tracking constructor failures and
                # leave it to the controller to fully scale to target
                # number of replicas and only return as completed once
                # reached target replica count
                self._replica_constructor_retry_counter = -1
            else:
                return GoalStatus.FAILED

        # If we have pending ops, the current goal is *not* ready.
        if (self._replicas.count(states=[
                ReplicaState.SHOULD_START,
                ReplicaState.STARTING_OR_UPDATING,
                ReplicaState.SHOULD_STOP,
                ReplicaState.STOPPING,
        ]) == 0):
            # Check for deleting.
            if target_replica_count == 0 and all_running_replica_cnt == 0:
                return GoalStatus.SUCCESSFULLY_DELETED

            # Check for a non-zero number of backends.
            elif target_replica_count == running_at_target_version_replica_cnt:
                return GoalStatus.SUCCEEDED

        return GoalStatus.PENDING

    def _check_and_update_replicas(self) -> bool:
        transitioned = False
        for replica in self._replicas.pop(states=[ReplicaState.RUNNING]):
            if replica.check_health():
                self._replicas.add(ReplicaState.RUNNING, replica)
            else:
                logger.warning(
                    f"Replica {replica.replica_tag} of deployment "
                    f"{self._name} failed health check, stopping it. "
                    f"component=serve deployment={self._name} "
                    f"replica={replica.replica_tag}")
                replica.set_should_stop(0)
                self._replicas.add(ReplicaState.SHOULD_STOP, replica)

        for replica in self._replicas.pop(states=[ReplicaState.SHOULD_START]):
            replica.start_or_update(self._target_info, self._target_version)
            self._replicas.add(ReplicaState.STARTING_OR_UPDATING, replica)

        for replica in self._replicas.pop(states=[ReplicaState.SHOULD_STOP]):
            # This replica should be taken off handle's replica set.
            transitioned = True
            replica.stop()
            self._replicas.add(ReplicaState.STOPPING, replica)

        slow_start_replicas = []
        for replica in self._replicas.pop(
                states=[ReplicaState.STARTING_OR_UPDATING]):
            start_status = replica.check_started()
            if start_status == ReplicaStartupStatus.SUCCEEDED:
                # This replica should be now be added to handle's replica
                # set.
                self._replicas.add(ReplicaState.RUNNING, replica)
                transitioned = True
            elif start_status == ReplicaStartupStatus.FAILED:
                # Replica reconfigure (deploy / upgrade) failed
                if self._replica_constructor_retry_counter >= 0:  # noqa: E501 line too long
                    # Increase startup failure counter if we're tracking it
                    self._replica_constructor_retry_counter += 1

                replica.set_should_stop(0)
                self._replicas.add(ReplicaState.SHOULD_STOP, replica)
                transitioned = True
            elif start_status == ReplicaStartupStatus.PENDING:
                # Not done yet, remain at same state
                self._replicas.add(ReplicaState.STARTING_OR_UPDATING, replica)
            else:
                # Slow start, remain at same state but also add to
                # slow start replicas.
                self._replicas.add(ReplicaState.STARTING_OR_UPDATING, replica)
                slow_start_replicas.append(replica)

        if (len(slow_start_replicas)
                and time.time() - self._prev_startup_warning >
                SLOW_STARTUP_WARNING_PERIOD_S):
            required, available = slow_start_replicas[
                0].resource_requirements()
            logger.warning(
                f"Deployment '{self._name}' has "
                f"{len(slow_start_replicas)} replicas that have taken "
                f"more than {SLOW_STARTUP_WARNING_S}s to start up. This "
                "may be caused by waiting for the cluster to auto-scale "
                "or because the constructor is slow. Resources required "
                f"for each replica: {required}, resources available: "
                f"{available}. component=serve deployment={self._name}")

            self._prev_startup_warning = time.time()

        for replica in self._replicas.pop(states=[ReplicaState.STOPPING]):
            stopped = replica.check_stopped()
            if not stopped:
                self._replicas.add(ReplicaState.STOPPING, replica)

        return transitioned

    def update(self) -> bool:
        """Updates the state of all backends to match their goal state."""
        # Add or remove BackendReplica instances in self._replicas.
        # This should be the only place we adjust total number of replicas
        # we manage.
        self._scale_backend_replicas()

        transitioned = self._check_and_update_replicas()
        if transitioned:
            self._notify_replica_handles_changed()
            self._checkpoint_fn()

        status = self._check_curr_goal_status()
        if status == GoalStatus.SUCCEEDED:
            # Deployment successul, complete the goal and clear the
            # backup backend_info.
            self._goal_manager.complete_goal(self._curr_goal)
            self._rollback_info = None
        elif status == GoalStatus.FAILED:
            # Roll back or delete the deployment if it failed.
            if self._rollback_info is None:
                # No info to roll back to, delete it.
                self._goal_manager.complete_goal(
                    self._curr_goal,
                    RuntimeError(f"Deployment '{self._name}' failed, "
                                 "deleting it asynchronously."))
                self.delete()
            else:
                # Roll back to the previous version.
                rollback_info = self._rollback_info
                self._goal_manager.complete_goal(
                    self._curr_goal,
                    RuntimeError(
                        f"Deployment '{self._name}' failed, rolling back to "
                        "version {rollback_info.version} asynchronously."))

                self._curr_goal = None
                self._rollback_info = None
                self.deploy(rollback_info)
        elif status == GoalStatus.SUCCESSFULLY_DELETED:
            self._goal_manager.complete_goal(self._curr_goal)

        return status == GoalStatus.SUCCESSFULLY_DELETED


class BackendStateManager:
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
        self._create_backend_state: Callable = lambda name: BackendState(
            name, controller_name, detached,
            long_poll_host, goal_manager, self._checkpoint)
        self._backend_states: Dict[BackendTag, BackendState] = dict()
        self._deleted_backend_metadata: Dict[BackendTag,
                                             BackendInfo] = OrderedDict()

        checkpoint = self._kv_store.get(CHECKPOINT_KEY)
        if checkpoint is not None:
            (backend_state_info,
             self._deleted_backend_metadata) = cloudpickle.loads(checkpoint)

            for backend_tag, checkpoint_data in backend_state_info.items():
                backend_state = self._create_backend_state(backend_tag)
                backend_state.recover_from_checkpoint_data(checkpoint_data)
                self._backend_states[backend_tag] = backend_state

    def shutdown(self) -> List[GoalId]:
        """
        Shutdown all running replicas by notifying the controller, and leave
        it to the controller event loop to take actions afterwards.

        Once shutdown signal is received, it will also prevent any new
        deployments or replicas from being created.

        One can send multiple shutdown signals but won't effectively make any
        difference compare to calling it once.
        """

        shutdown_goals = []
        for backend_state in self._backend_states.values():
            goal = backend_state.delete(force_kill=True)
            if goal is not None:
                shutdown_goals.append(goal)

        # TODO(jiaodong): This might not be 100% safe since we deleted
        # everything without ensuring all shutdown goals are completed
        # yet. Need to address in follow-up PRs.
        self._kv_store.delete(CHECKPOINT_KEY)

        # TODO(jiaodong): Need to add some logic to prevent new replicas
        # from being created once shutdown signal is sent.
        return shutdown_goals

    def _checkpoint(self) -> None:
        backend_state_info = {
            backend_tag: backend_state.get_checkpoint_data()
            for backend_tag, backend_state in self._backend_states.items()
        }
        self._kv_store.put(
            CHECKPOINT_KEY,
            cloudpickle.dumps((backend_state_info,
                               self._deleted_backend_metadata)))

    def get_running_replica_handles(
            self,
            filter_tag: Optional[BackendTag] = None,
    ) -> Dict[BackendTag, Dict[ReplicaTag, ActorHandle]]:
        replicas = {}
        for backend_tag, backend_state in self._backend_states.items():
            if filter_tag is None or backend_tag == filter_tag:
                replicas[
                    backend_tag] = backend_state.get_running_replica_handles()

        return replicas

    def get_backend_configs(self,
                            filter_tag: Optional[BackendTag] = None,
                            include_deleted: Optional[bool] = False
                            ) -> Dict[BackendTag, BackendConfig]:
        configs: Dict[BackendTag, BackendConfig] = {}
        for backend_tag, backend_state in self._backend_states.items():
            if filter_tag is None or backend_tag == filter_tag:
                configs[backend_tag] = backend_state.target_info.backend_config

        if include_deleted:
            for backend_tag, info in self._deleted_backend_metadata.items():
                if filter_tag is None or backend_tag == filter_tag:
                    configs[backend_tag] = info.backend_config

        return configs

    def get_backend(self,
                    backend_tag: BackendTag,
                    include_deleted: Optional[bool] = False
                    ) -> Optional[BackendInfo]:
        if backend_tag in self._backend_states:
            return self._backend_states[backend_tag].target_info
        elif include_deleted and backend_tag in self._deleted_backend_metadata:
            return self._deleted_backend_metadata[backend_tag]
        else:
            return None

    def deploy_backend(self, backend_tag: BackendTag, backend_info: BackendInfo
                       ) -> Tuple[Optional[GoalId], bool]:
        """Deploy the backend.

        If the backend already exists with the same version, this is a no-op
        and returns the GoalId corresponding to the existing update if there
        is one.

        Returns:
            GoalId, bool: The GoalId for the client to wait for and whether or
            not the backend is being updated.
        """
        if backend_tag in self._deleted_backend_metadata:
            del self._deleted_backend_metadata[backend_tag]

        if backend_tag not in self._backend_states:
            self._backend_states[backend_tag] = self._create_backend_state(
                backend_tag)

        return self._backend_states[backend_tag].deploy(backend_info)

    def delete_backend(self, backend_tag: BackendTag,
                       force_kill: bool = False) -> Optional[GoalId]:
        # This method must be idempotent. We should validate that the
        # specified backend exists on the client.
        if backend_tag not in self._backend_states:
            return None

        backend_state = self._backend_states[backend_tag]
        return backend_state.delete(force_kill=force_kill)

    def update(self) -> bool:
        """Updates the state of all backends to match their goal state."""
        deleted_tags = []
        for backend_tag, backend_state in self._backend_states.items():
            deleted = backend_state.update()
            if deleted:
                deleted_tags.append(backend_tag)
                backend_info = backend_state.target_info
                backend_info.end_time_ms = int(time.time() * 1000)
                if (len(self._deleted_backend_metadata) >
                        MAX_NUM_DELETED_DEPLOYMENTS):
                    self._deleted_backend_metadata.popitem(last=False)
                self._deleted_backend_metadata[backend_tag] = backend_info

        for tag in deleted_tags:
            del self._backend_states[tag]

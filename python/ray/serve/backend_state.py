import math
import time
from abc import ABC
from collections import defaultdict
from collections.abc import Iterable
from enum import Enum
import os
from typing import Any, Dict, List, Optional, Tuple

import ray
from ray import cloudpickle
from ray.actor import ActorHandle
from ray.serve.async_goal_manager import AsyncGoalManager
from ray.serve.common import (BackendInfo, BackendTag, Duration, GoalId,
                              ReplicaTag)
from ray.serve.config import BackendConfig
from ray.serve.constants import RESERVED_VERSION_TAG
from ray.serve.kv_store import RayInternalKVStore
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
                    "Creating placement group '{}' for backend '{}'".format(
                        self._placement_group_name, self._backend_tag))
                self._placement_group = ray.util.placement_group(
                    [self._actor_resources],
                    lifetime="detached" if self._detached else None,
                    name=self._placement_group_name)

        try:
            self._actor_handle = ray.get_actor(self._actor_name)
        except ValueError:
            logger.debug("Starting replica '{}' for backend '{}'.".format(
                self._replica_tag, self._backend_tag))
            self._actor_handle = backend_info.actor_def.options(
                name=self._actor_name,
                lifetime="detached" if self._detached else None,
                placement_group=self._placement_group,
                placement_group_capture_child_tasks=False,
                **backend_info.replica_config.ray_actor_options).remote(
                    self._backend_tag, self._replica_tag,
                    backend_info.replica_config.init_args,
                    backend_info.backend_config, self._controller_name)

        self._ready_obj_ref = self._actor_handle.reconfigure.remote(
            backend_info.backend_config.user_config)

    def check_ready(self) -> bool:
        ready, _ = ray.wait([self._ready_obj_ref], timeout=0)
        return len(ready) == 1

    @property
    def actor_resources(self) -> Dict[str, float]:
        return self._actor_resources

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
            f"{ReplicaState.STARTING_OR_UPDATING}, or "
            f"{ReplicaState.RUNNING} *not* {self._state}.")

        self._actor.start_or_update(backend_info)
        self._start_time = time.time()
        self._prev_slow_startup_warning_time = time.time()
        self._state = ReplicaState.STARTING_OR_UPDATING
        self._version = version

    def check_started(self) -> Tuple[bool, bool]:
        """Check if the replica has started. If so, transition to RUNNING.

        Should handle the case where the replica has already stopped.

        Returns:
            (ready, past_slow_startup_threshold)
        """
        if self._state == ReplicaState.RUNNING:
            return True, False
        assert self._state == ReplicaState.STARTING_OR_UPDATING, (
            f"State must be {ReplicaState.STARTING_OR_UPDATING}, "
            f"*not* {self._state}.")

        if self._actor.check_ready():
            self._state = ReplicaState.RUNNING
            return True, False

        time_since_start = time.time() - self._start_time
        return False, time_since_start > SLOW_STARTUP_WARNING_S

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
                f"{self._graceful_shutdown_timeout_s}s, force-killing.")

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
            for k, v in ray.available_resources().items() if k in required
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
        self._target_versions: Dict[BackendTag, BackendVersion] = dict()
        self._prev_startup_warnings: Dict[BackendTag, float] = defaultdict(
            float)

        checkpoint = self._kv_store.get(CHECKPOINT_KEY)
        if checkpoint is not None:
            (self._replicas, self._backend_metadata, self._target_replicas,
             self._target_versions,
             self._backend_goals) = cloudpickle.loads(checkpoint)

            for goal_id in self._backend_goals.values():
                self._goal_manager.create_goal(goal_id)

        self._notify_backend_configs_changed()
        self._notify_replica_handles_changed()

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
        for backend_tag, _ in self._replicas.items():
            goal = self.delete_backend(backend_tag, force_kill=True)
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
        self._kv_store.put(
            CHECKPOINT_KEY,
            cloudpickle.dumps(
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
                          backend_info: Optional[BackendInfo]) -> None:
        """
        Set desirable state for a given backend, identified by tag.

        Args:
            backend_tag (BackendTag): Identifier of a backend
            backend_info (Optional[BackendInfo]): Contains backend and
                replica config, if passed in as None, we're marking
                target backend as shutting down.
        """
        existing_goal_id = self._backend_goals.get(backend_tag)
        new_goal_id = self._goal_manager.create_goal()

        if backend_info is not None:
            self._backend_metadata[backend_tag] = backend_info
            self._target_replicas[
                backend_tag] = backend_info.backend_config.num_replicas

            if backend_info.version is not None:
                code_version = backend_info.version
            else:
                code_version = get_random_letters()

            self._target_versions[backend_tag] = BackendVersion(
                code_version,
                user_config=backend_info.backend_config.user_config)

        else:
            self._target_replicas[backend_tag] = 0

        self._backend_goals[backend_tag] = new_goal_id

        return new_goal_id, existing_goal_id

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
        # Ensures this method is idempotent.
        existing_info = self._backend_metadata.get(backend_tag)
        if existing_info is not None:
            # Old codepath. We use RESERVED_VERSION_TAG to distinguish that
            # we shouldn't use versions at all to determine redeployment
            # because `None` is used to indicate always redeploying.
            if backend_info.version == RESERVED_VERSION_TAG:
                if (existing_info.backend_config == backend_info.backend_config
                        and existing_info.replica_config ==
                        backend_info.replica_config):
                    return self._backend_goals.get(backend_tag, None), False
            # New codepath: treat version as ground truth for implementation.
            elif (existing_info.backend_config == backend_info.backend_config
                  and backend_info.version is not None
                  and existing_info.version == backend_info.version):
                return self._backend_goals.get(backend_tag, None), False

        if backend_tag not in self._replicas:
            self._replicas[backend_tag] = ReplicaStateContainer()

        new_goal_id, existing_goal_id = self._set_backend_goal(
            backend_tag, backend_info)

        # NOTE(edoakes): we must write a checkpoint before starting new
        # or pushing the updated config to avoid inconsistent state if we
        # crash while making the change.
        self._checkpoint()
        self._notify_backend_configs_changed(backend_tag)

        if existing_goal_id is not None:
            self._goal_manager.complete_goal(existing_goal_id)
        return new_goal_id, True

    def delete_backend(self, backend_tag: BackendTag,
                       force_kill: bool = False) -> Optional[GoalId]:
        # This method must be idempotent. We should validate that the
        # specified backend exists on the client.
        if backend_tag not in self._backend_metadata:
            return None

        new_goal_id, existing_goal_id = self._set_backend_goal(
            backend_tag, None)
        if force_kill:
            self._backend_metadata[
                backend_tag].backend_config.\
                    experimental_graceful_shutdown_timeout_s = 0

        self._checkpoint()
        if existing_goal_id is not None:
            self._goal_manager.complete_goal(existing_goal_id)
        return new_goal_id

    def _stop_wrong_version_replicas(
            self, backend_tag: BackendTag, replicas: ReplicaStateContainer,
            target_replicas: int, target_version: str,
            graceful_shutdown_timeout_s: float) -> int:
        """Stops replicas with outdated versions to implement rolling updates.

        This includes both explicit code version updates and changes to the
        user_config.
        """
        # NOTE(edoakes): this short-circuits when using the legacy
        # `create_backend` codepath -- it can be removed once we deprecate
        # that as the version should never be None.
        if target_version is None:
            return 0

        # Short circuit if target replicas is 0 (the backend is being deleted)
        # because this will be handled in the main loop.
        if target_replicas == 0:
            return 0

        # We include SHOULD_START and STARTING_OR_UPDATING replicas here
        # because if there are replicas still pending startup, we may as well
        # terminate them and start new version replicas instead.
        old_running_replicas = replicas.count(
            exclude_version=target_version,
            states=[
                ReplicaState.SHOULD_START, ReplicaState.STARTING_OR_UPDATING,
                ReplicaState.RUNNING
            ])
        old_stopping_replicas = replicas.count(
            exclude_version=target_version,
            states=[ReplicaState.SHOULD_STOP, ReplicaState.STOPPING])
        new_running_replicas = replicas.count(
            version=target_version, states=[ReplicaState.RUNNING])

        # If the backend is currently scaling down, let the scale down
        # complete before doing a rolling update.
        if target_replicas < old_running_replicas + old_stopping_replicas:
            return 0

        # The number of replicas that are currently in transition between
        # an old version and the new version. Note that we cannot directly
        # count the number of stopping replicas because once replicas finish
        # stopping, they are removed from the data structure.
        pending_replicas = (
            target_replicas - new_running_replicas - old_running_replicas)

        # Maximum number of replicas that can be updating at any given time.
        # There should never be more than rollout_size old replicas stopping
        # or rollout_size new replicas starting.
        rollout_size = max(int(0.2 * target_replicas), 1)
        max_to_stop = max(rollout_size - pending_replicas, 0)

        replicas_to_update = replicas.pop(
            exclude_version=target_version,
            states=[
                ReplicaState.SHOULD_START, ReplicaState.STARTING_OR_UPDATING,
                ReplicaState.RUNNING
            ],
            max_replicas=max_to_stop)

        code_version_changes = 0
        user_config_changes = 0
        for replica in replicas_to_update:
            # If the code version is a mismatch, we stop the replica. A new one
            # with the correct version will be started later as part of the
            # normal scale-up process.
            if replica.version.code_version != target_version.code_version:
                code_version_changes += 1
                replica.set_should_stop(graceful_shutdown_timeout_s)
                replicas.add(ReplicaState.SHOULD_STOP, replica)
            # If only the user_config is a mismatch, we update it dynamically
            # without restarting the replica.
            elif (replica.version.user_config_hash !=
                  target_version.user_config_hash):
                user_config_changes += 1
                replica.start_or_update(self._backend_metadata[backend_tag],
                                        target_version)
                replicas.add(ReplicaState.STARTING_OR_UPDATING, replica)
            else:
                assert False, "Update must be code version or user config."

        if code_version_changes > 0:
            logger.info(f"Stopping {code_version_changes} replicas of backend "
                        f"'{backend_tag}' with outdated versions.")

        if user_config_changes > 0:
            logger.info(f"Updating {user_config_changes} replicas of backend "
                        f"'{backend_tag}' with outdated user_configs.")

        return len(replicas_to_update)

    def _scale_backend_replicas(
            self,
            backend_tag: BackendTag,
            target_replicas: int,
            target_version: str,
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
        assert target_replicas >= 0, ("Number of replicas must be"
                                      " greater than or equal to 0.")

        backend_info: BackendInfo = self._backend_metadata[backend_tag]
        graceful_shutdown_timeout_s = (
            backend_info.backend_config.
            experimental_graceful_shutdown_timeout_s)

        self._stop_wrong_version_replicas(
            backend_tag, self._replicas[backend_tag], target_replicas,
            target_version, graceful_shutdown_timeout_s)

        current_replicas = self._replicas[backend_tag].count(states=[
            ReplicaState.SHOULD_START, ReplicaState.STARTING_OR_UPDATING,
            ReplicaState.RUNNING
        ])

        delta_replicas = target_replicas - current_replicas
        if delta_replicas == 0:
            return False

        elif delta_replicas > 0:
            # Don't ever exceed target_replicas.
            stopping_replicas = self._replicas[backend_tag].count(states=[
                ReplicaState.SHOULD_STOP,
                ReplicaState.STOPPING,
            ])
            to_add = max(delta_replicas - stopping_replicas, 0)
            if to_add > 0:
                logger.info(f"Adding {to_add} replicas "
                            f"to backend '{backend_tag}'.")
            for _ in range(to_add):
                replica_tag = "{}#{}".format(backend_tag, get_random_letters())
                self._replicas[backend_tag].add(
                    ReplicaState.SHOULD_START,
                    BackendReplica(self._controller_name, self._detached,
                                   replica_tag, backend_tag, target_version))

        elif delta_replicas < 0:
            to_remove = -delta_replicas
            logger.info(f"Removing {to_remove} replicas "
                        f"from backend '{backend_tag}'.")
            replicas_to_stop = self._replicas[backend_tag].pop(
                states=[
                    ReplicaState.SHOULD_START,
                    ReplicaState.STARTING_OR_UPDATING, ReplicaState.RUNNING
                ],
                max_replicas=to_remove)

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
                    ReplicaState.STARTING_OR_UPDATING,
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
            for replica in replicas.pop(states=[ReplicaState.RUNNING]):
                if replica.check_health():
                    replicas.add(ReplicaState.RUNNING, replica)
                else:
                    logger.warning(
                        f"Replica {replica.replica_tag} of backend "
                        f"{backend_tag} failed health check, stopping it.")
                    replica.set_should_stop(0)
                    replicas.add(ReplicaState.SHOULD_STOP, replica)

            for replica in replicas.pop(states=[ReplicaState.SHOULD_START]):
                replica.start_or_update(self._backend_metadata[backend_tag],
                                        self._target_versions[backend_tag])
                replicas.add(ReplicaState.STARTING_OR_UPDATING, replica)

            for replica in replicas.pop(states=[ReplicaState.SHOULD_STOP]):
                # This replica should be taken off handle's replica set.
                transitioned_backend_tags.add(backend_tag)
                replica.stop()
                replicas.add(ReplicaState.STOPPING, replica)

            slow_start_replicas = []
            for replica in replicas.pop(
                    states=[ReplicaState.STARTING_OR_UPDATING]):
                ready, slow_start = replica.check_started()
                if ready:
                    # This replica should be now be added to handle's replica
                    # set.
                    replicas.add(ReplicaState.RUNNING, replica)
                    transitioned_backend_tags.add(backend_tag)
                else:
                    replicas.add(ReplicaState.STARTING_OR_UPDATING, replica)
                    if slow_start:
                        slow_start_replicas.append(replica)

            if (len(slow_start_replicas)
                    and time.time() - self._prev_startup_warnings[backend_tag]
                    > SLOW_STARTUP_WARNING_PERIOD_S):
                required, available = slow_start_replicas[
                    0].resource_requirements()
                logger.warning(
                    f"Backend '{backend_tag}' has {len(slow_start_replicas)} "
                    "replicas that have taken more than "
                    f"{SLOW_STARTUP_WARNING_S}s to start up. This may be "
                    "caused by waiting for the cluster to auto-scale or "
                    "because the constructor is slow. Resources required "
                    f"for each replica: {required}, resources available: "
                    f"{available}.")

                self._prev_startup_warnings[backend_tag] = time.time()

            for replica in replicas.pop(states=[ReplicaState.STOPPING]):
                if not replica.check_stopped():
                    replicas.add(ReplicaState.STOPPING, replica)

        if len(transitioned_backend_tags) > 0:
            self._checkpoint()
            [
                self._notify_replica_handles_changed(tag)
                for tag in transitioned_backend_tags
            ]

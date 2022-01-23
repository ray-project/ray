import math
import json
import pickle
import time
from collections import defaultdict, OrderedDict
from enum import Enum
import os
from typing import Any, Callable, Dict, List, Optional, Tuple

import ray
from ray import ObjectRef
from ray.actor import ActorHandle
from ray.serve.async_goal_manager import AsyncGoalManager
from ray.serve.common import (DeploymentInfo, Duration, GoalId, ReplicaTag,
                              ReplicaName, RunningReplicaInfo)
from ray.serve.config import DeploymentConfig
from ray.serve.constants import (MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT,
                                 MAX_NUM_DELETED_DEPLOYMENTS)
from ray.serve.storage.kv_store import KVStoreBase
from ray.serve.long_poll import LongPollHost, LongPollNamespace
from ray.serve.utils import (format_actor_name, get_random_letters, logger,
                             wrap_to_ray_error)
from ray.serve.version import DeploymentVersion, VersionedReplica
from ray.util.placement_group import PlacementGroup


class ReplicaState(Enum):
    STARTING = 1
    UPDATING = 2
    RECOVERING = 3
    RUNNING = 4
    STOPPING = 5


class ReplicaStartupStatus(Enum):
    PENDING_ALLOCATION = 1
    PENDING_INITIALIZATION = 2
    SUCCEEDED = 3
    FAILED = 4


class GoalStatus(Enum):
    NONE = 1
    PENDING = 2
    SUCCEEDED = 3
    SUCCESSFULLY_DELETED = 4
    FAILED = 5


CHECKPOINT_KEY = "serve-deployment-state-checkpoint"
SLOW_STARTUP_WARNING_S = 30
SLOW_STARTUP_WARNING_PERIOD_S = 30

ALL_REPLICA_STATES = list(ReplicaState)
USE_PLACEMENT_GROUP = os.environ.get("SERVE_USE_PLACEMENT_GROUP", "1") != "0"
_SCALING_LOG_ENABLED = os.environ.get("SERVE_ENABLE_SCALING_LOG", "0") != "0"


def print_verbose_scaling_log():
    assert _SCALING_LOG_ENABLED

    log_path = "/tmp/ray/session_latest/logs/monitor.log"
    last_n_lines = 50
    autoscaler_log_last_n_lines = []
    if os.path.exists(log_path):
        with open(log_path) as f:
            autoscaler_log_last_n_lines = f.readlines()[-last_n_lines:]

    debug_info = {
        "nodes": ray.nodes(),
        "available_resources": ray.available_resources(),
        "total_resources": ray.cluster_resources(),
        "autoscaler_logs": autoscaler_log_last_n_lines,
    }
    logger.error(f"Scaling information\n{json.dumps(debug_info, indent=2)}")


class ActorReplicaWrapper:
    """Wraps a Ray actor for a deployment replica.

    This is primarily defined so that we can mock out actual Ray operations
    for unit testing.

    *All Ray API calls should be made here, not in DeploymentState.*
    """

    def __init__(self, actor_name: str, detached: bool, controller_name: str,
                 replica_tag: ReplicaTag, deployment_name: str):
        self._actor_name = actor_name
        self._placement_group_name = self._actor_name + "_placement_group"
        self._detached = detached
        self._controller_name = controller_name
        self._controller_namespace = ray.serve.api._get_controller_namespace(
            detached)

        self._replica_tag = replica_tag
        self._deployment_name = deployment_name

        # Populated in either self.start() or self.recover()
        self._allocated_obj_ref: ObjectRef = None
        self._ready_obj_ref: ObjectRef = None

        self._actor_resources: Dict[str, float] = None
        self._max_concurrent_queries: int = None
        self._graceful_shutdown_timeout_s: float = 0.0
        self._health_check_ref: ObjectRef = None
        # NOTE: storing these is necessary to keep the actor and PG alive in
        # the non-detached case.
        self._actor_handle: ActorHandle = None
        self._placement_group: PlacementGroup = None

        # Populated in self.stop().
        self._graceful_shutdown_ref: ObjectRef = None

    @property
    def replica_tag(self) -> str:
        return self._replica_tag

    @property
    def deployment_name(self) -> str:
        return self._deployment_name

    @property
    def actor_handle(self) -> Optional[ActorHandle]:
        if not self._actor_handle:
            try:
                self._actor_handle = ray.get_actor(
                    self._actor_name, namespace=self._controller_namespace)
            except ValueError:
                self._actor_handle = None

        return self._actor_handle

    @property
    def max_concurrent_queries(self) -> int:
        return self._max_concurrent_queries

    def create_placement_group(self, placement_group_name: str,
                               actor_resources: dict) -> PlacementGroup:
        # Only need one placement group per actor
        if self._placement_group:
            return self._placement_group

        logger.debug("Creating placement group '{}' for deployment '{}'".
                     format(placement_group_name, self.deployment_name) +
                     f" component=serve deployment={self.deployment_name}")

        self._placement_group = ray.util.placement_group(
            [actor_resources],
            lifetime="detached" if self._detached else None,
            name=placement_group_name)

        return self._placement_group

    def get_placement_group(self,
                            placement_group_name) -> Optional[PlacementGroup]:
        if not self._placement_group:
            try:
                self._placement_group = ray.util.get_placement_group(
                    placement_group_name)
            except ValueError:
                self._placement_group = None

        return self._placement_group

    def start(self, deployment_info: DeploymentInfo,
              version: DeploymentVersion):
        """
        Start a new actor for current DeploymentReplica instance.
        """
        self._max_concurrent_queries = (
            deployment_info.deployment_config.max_concurrent_queries)
        self._graceful_shutdown_timeout_s = (
            deployment_info.deployment_config.graceful_shutdown_timeout_s)

        self._actor_resources = deployment_info.replica_config.resource_dict
        # it is currently not possiible to create a placement group
        # with no resources (https://github.com/ray-project/ray/issues/20401)
        has_resources_assigned = all(
            (r > 0 for r in self._actor_resources.values()))
        if USE_PLACEMENT_GROUP and has_resources_assigned:
            self._placement_group = self.create_placement_group(
                self._placement_group_name, self._actor_resources)

        logger.debug(f"Starting replica {self.replica_tag} for deployment "
                     f"{self.deployment_name} component=serve deployment="
                     f"{self.deployment_name} replica={self.replica_tag}")

        self._actor_handle = deployment_info.actor_def.options(
            name=self._actor_name,
            namespace=self._controller_namespace,
            lifetime="detached" if self._detached else None,
            placement_group=self._placement_group,
            placement_group_capture_child_tasks=False,
            **deployment_info.replica_config.ray_actor_options).remote(
                self.deployment_name, self.replica_tag,
                deployment_info.replica_config.init_args,
                deployment_info.replica_config.init_kwargs,
                deployment_info.deployment_config.to_proto_bytes(), version,
                self._controller_name, self._detached)

        self._allocated_obj_ref = self._actor_handle.is_allocated.remote()
        self._ready_obj_ref = self._actor_handle.reconfigure.remote(
            deployment_info.deployment_config.user_config)

    def update_user_config(self, user_config: Any):
        """
        Update user config of existing actor behind current
        DeploymentReplica instance.
        """
        logger.debug(f"Updating replica {self.replica_tag} for deployment "
                     f"{self.deployment_name} component=serve deployment="
                     f"{self.deployment_name} replica={self.replica_tag} "
                     f"with user_config {user_config}")

        self._ready_obj_ref = self._actor_handle.reconfigure.remote(
            user_config)

    def recover(self):
        """
        Recover states in DeploymentReplica instance by fetching running actor
        status
        """
        logger.debug(f"Recovering replica {self.replica_tag} for deployment "
                     f"{self.deployment_name} component=serve deployment="
                     f"{self.deployment_name} replica={self.replica_tag}")

        self._actor_handle = self.actor_handle
        if USE_PLACEMENT_GROUP:
            self._placement_group = self.get_placement_group(
                self._placement_group_name)

        # Re-fetch initialization proof
        self._allocated_obj_ref = self._actor_handle.is_allocated.remote()

        # Running actor handle already has all info needed, thus successful
        # starting simply means retrieving replica version hash from actor
        self._ready_obj_ref = self._actor_handle.get_metadata.remote()

    def check_ready(
            self) -> Tuple[ReplicaStartupStatus, Optional[DeploymentVersion]]:
        """
        Check if current replica has started by making ray API calls on
        relevant actor / object ref.

        Returns:
            state (ReplicaStartupStatus):
                PENDING_ALLOCATION:
                    - replica is waiting for a worker to start
                PENDING_INITIALIZATION
                    - replica reconfigure() haven't returned.
                FAILED:
                    - replica __init__() failed.
                SUCCEEDED:
                    - replica __init__() and reconfigure() succeeded.
            version (DeploymentVersion):
                None:
                    - replica reconfigure() haven't returned OR
                    - replica __init__() failed.
                version:
                    - replica __init__() and reconfigure() succeeded.
        """

        # check whether the replica has been allocated
        ready, _ = ray.wait([self._allocated_obj_ref], timeout=0)
        if len(ready) == 0:
            return ReplicaStartupStatus.PENDING_ALLOCATION, None

        # check whether relica initialization has completed
        ready, _ = ray.wait([self._ready_obj_ref], timeout=0)
        # In case of deployment constructor failure, ray.get will help to
        # surface exception to each update() cycle.
        if len(ready) == 0:
            return ReplicaStartupStatus.PENDING_INITIALIZATION, None
        elif len(ready) > 0:
            try:
                deployment_config, version = ray.get(ready)[0]
                self._max_concurrent_queries = (
                    deployment_config.max_concurrent_queries)
                self._graceful_shutdown_timeout_s = (
                    deployment_config.graceful_shutdown_timeout_s)
            except Exception:
                logger.exception(
                    f"Exception in deployment '{self._deployment_name}'")
                return ReplicaStartupStatus.FAILED, None

        return ReplicaStartupStatus.SUCCEEDED, version

    @property
    def actor_resources(self) -> Optional[Dict[str, float]]:
        return self._actor_resources

    @property
    def available_resources(self) -> Dict[str, float]:
        return ray.available_resources()

    def graceful_stop(self) -> Duration:
        """Request the actor to exit gracefully.

        Returns the timeout after which to kill the actor.
        """
        try:
            handle = ray.get_actor(self._actor_name)
            self._graceful_shutdown_ref = handle.prepare_for_shutdown.remote()
        except ValueError:
            pass

        return self._graceful_shutdown_timeout_s

    def check_stopped(self) -> bool:
        """Check if the actor has exited."""
        try:
            handle = ray.get_actor(self._actor_name)
            ready, _ = ray.wait([self._graceful_shutdown_ref], timeout=0)
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
            if self._placement_group is not None:
                ray.util.remove_placement_group(self._placement_group)
        except ValueError:
            pass


class DeploymentReplica(VersionedReplica):
    """Manages state transitions for deployment replicas.

    This is basically a checkpointable lightweight state machine.
    """

    def __init__(self, controller_name: str, detached: bool,
                 replica_tag: ReplicaTag, deployment_name: str,
                 version: DeploymentVersion):
        self._actor = ActorReplicaWrapper(
            f"{ReplicaName.prefix}{format_actor_name(replica_tag)}", detached,
            controller_name, replica_tag, deployment_name)
        self._controller_name = controller_name
        self._deployment_name = deployment_name
        self._replica_tag = replica_tag
        self._version = version
        self._start_time = None
        self._prev_slow_startup_warning_time = None

    def get_running_replica_info(self) -> RunningReplicaInfo:
        return RunningReplicaInfo(
            deployment_name=self._deployment_name,
            replica_tag=self._replica_tag,
            actor_handle=self._actor.actor_handle,
            max_concurrent_queries=self._actor.max_concurrent_queries,
        )

    @property
    def replica_tag(self) -> ReplicaTag:
        return self._replica_tag

    @property
    def deployment_name(self) -> str:
        return self._deployment_name

    @property
    def version(self):
        return self._version

    @property
    def actor_handle(self) -> ActorHandle:
        return self._actor.actor_handle

    def start(self, deployment_info: DeploymentInfo,
              version: DeploymentVersion):
        """
        Start a new actor for current DeploymentReplica instance.
        """
        self._actor.start(deployment_info, version)
        self._start_time = time.time()
        self._prev_slow_startup_warning_time = time.time()
        self._version = version

    def update_user_config(self, user_config: Any):
        """
        Update user config of existing actor behind current
        DeploymentReplica instance.
        """
        self._actor.update_user_config(user_config)
        self._version = DeploymentVersion(
            self._version.code_version, user_config=user_config)

    def recover(self):
        """
        Recover states in DeploymentReplica instance by fetching running actor
        status
        """
        self._actor.recover()
        self._start_time = time.time()
        # Replica version is fetched from recovered replica dynamically in
        # check_started() below

    def check_started(self) -> ReplicaStartupStatus:
        """Check if the replica has started. If so, transition to RUNNING.

        Should handle the case where the replica has already stopped.

        Returns:
            status (ReplicaStartupStatus): Most recent state of replica by
                querying actor obj ref
        """
        status, version = self._actor.check_ready()

        if status == ReplicaStartupStatus.SUCCEEDED:
            # Re-assign DeploymentVersion if start / update / recover succeeded
            # by reading re-computed version in RayServeReplica
            if version is not None:
                self._version = version

        return status

    def stop(self, graceful: bool = True) -> None:
        """Stop the replica.

        Should handle the case where the replica is already stopped.
        """
        timeout_s = self._actor.graceful_stop()
        if not graceful:
            timeout_s = 0
        self._shutdown_deadline = time.time() + timeout_s

    def check_stopped(self) -> bool:
        """Check if the replica has finished stopping."""
        if self._actor.check_stopped():
            # Clean up any associated resources (e.g., placement group).
            self._actor.cleanup()
            return True

        timeout_passed = time.time() > self._shutdown_deadline
        if timeout_passed:
            # Graceful period passed, kill it forcefully.
            # This will be called repeatedly until the replica shuts down.
            logger.debug(
                f"Replica {self.replica_tag} did not shut down after grace "
                "period, force-killing it. "
                f"component=serve deployment={self.deployment_name} "
                f"replica={self.replica_tag}")

            self._actor.force_stop()
        return False

    def check_health(self) -> bool:
        """Check if the replica is still alive.

        Returns `True` if the replica is healthy, else `False`.
        """
        return self._actor.check_health()

    def resource_requirements(self) -> Tuple[str, str]:
        """Returns required and currently available resources.

        Only resources with nonzero requirements will be included in the
        required dict and only resources in the required dict will be
        included in the available dict (filtered for relevance).
        """
        # NOTE(edoakes):
        if self._actor.actor_resources is None:
            return "UNKNOWN", "UNKNOWN"

        required = {
            k: v
            for k, v in self._actor.actor_resources.items() if v > 0
        }
        available = {
            k: v
            for k, v in self._actor.available_resources.items()
            if k in required
        }
        return str(required), str(available)


class ReplicaStateContainer:
    """Container for mapping ReplicaStates to lists of DeploymentReplicas."""

    def __init__(self):
        self._replicas: Dict[ReplicaState, List[
            DeploymentReplica]] = defaultdict(list)

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
            ) -> List[DeploymentReplica]:
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
            exclude_version: Optional[DeploymentVersion] = None,
            states: Optional[List[ReplicaState]] = None,
            max_replicas: Optional[int] = math.inf) -> List[VersionedReplica]:
        """Get and remove all replicas of the given states.

        This removes the replicas from the container. Replicas are returned
        in order of state as passed in.

        Args:
            exclude_version (DeploymentVersion): if specified, replicas of the
                provided version will *not* be removed.
            states (str): states to consider. If not specified, all replicas
                are considered.
            max_replicas (int): max number of replicas to return. If not
                specified, will pop all replicas matching the criteria.
        """
        if states is None:
            states = ALL_REPLICA_STATES

        assert (exclude_version is None
                or isinstance(exclude_version, DeploymentVersion))
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
              exclude_version: Optional[DeploymentVersion] = None,
              version: Optional[DeploymentVersion] = None,
              states: Optional[List[ReplicaState]] = None):
        """Get the total count of replicas of the given states.

        Args:
            exclude_version(DeploymentVersion): version to exclude. If not
                specified, all versions are considered.
            version(DeploymentVersion): version to filter to. If not specified,
                all versions are considered.
            states (str): states to consider. If not specified, all replicas
                are considered.
        """
        if states is None:
            states = ALL_REPLICA_STATES
        assert isinstance(states, list)
        assert (exclude_version is None
                or isinstance(exclude_version, DeploymentVersion))
        assert version is None or isinstance(version, DeploymentVersion)
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


class DeploymentState:
    """Manages the target state and replicas for a single deployment."""

    def __init__(self, name: str, controller_name: str, detached: bool,
                 long_poll_host: LongPollHost, goal_manager: AsyncGoalManager,
                 _save_checkpoint_func: Callable):

        self._name = name
        self._controller_name: str = controller_name
        self._detached: bool = detached
        self._long_poll_host: LongPollHost = long_poll_host
        self._goal_manager: AsyncGoalManager = goal_manager
        self._save_checkpoint_func = _save_checkpoint_func

        # Each time we set a new deployment goal, we're trying to save new
        # DeploymentInfo and bring current deployment to meet new status.
        # In case the new deployment goal failed to complete, we keep track of
        # previous DeploymentInfo and rollback to it.
        self._target_info: DeploymentInfo = None
        self._rollback_info: DeploymentInfo = None
        self._target_replicas: int = -1
        self._curr_goal: Optional[GoalId] = None
        self._target_version: DeploymentVersion = None
        self._prev_startup_warning: float = time.time()
        self._replica_constructor_retry_counter: int = 0
        self._replicas: ReplicaStateContainer = ReplicaStateContainer()

    def get_target_state_checkpoint_data(self):
        """
        Return deployment's target state submitted by user's deployment call.
        Should be persisted and outlive current ray cluster.
        """
        return (self._target_info, self._target_replicas, self._target_version)

    def get_checkpoint_data(self):
        return self.get_target_state_checkpoint_data()

    def recover_target_state_from_checkpoint(self, target_state_checkpoint):
        logger.info("Recovering target state for deployment "
                    f"{self._name} from checkpoint..")
        (self._target_info, self._target_replicas,
         self._target_version) = target_state_checkpoint

    def recover_current_state_from_replica_actor_names(
            self, replica_actor_names: List[str]):
        assert (
            self._target_info is not None and self._target_replicas != -1
            and self._target_version is not None), (
                "Target state should be recovered successfully first before "
                "recovering current state from replica actor names.")

        logger.debug("Recovering current state for deployment "
                     f"{self._name} from {len(replica_actor_names)} actors in "
                     "current ray cluster..")
        # All current states use default value, only attach running replicas.
        for replica_actor_name in replica_actor_names:
            replica_name: ReplicaName = ReplicaName.from_str(
                replica_actor_name)
            new_deployment_replica = DeploymentReplica(
                self._controller_name, self._detached,
                replica_name.replica_tag, replica_name.deployment_tag, None)
            new_deployment_replica.recover()
            self._replicas.add(ReplicaState.RECOVERING, new_deployment_replica)
            logger.debug(
                f"RECOVERING replica: {new_deployment_replica.replica_tag}, "
                f"deployment: {self._name}.")

        # TODO(jiaodong): this currently halts all traffic in the cluster
        # briefly because we will broadcast a replica update with everything in
        # RECOVERING. We should have a grace period where we recover the state
        # of the replicas before doing this update.

    @property
    def target_info(self) -> DeploymentInfo:
        return self._target_info

    @property
    def curr_goal(self) -> Optional[GoalId]:
        return self._curr_goal

    def get_running_replica_infos(self) -> List[RunningReplicaInfo]:
        return [
            replica.get_running_replica_info()
            for replica in self._replicas.get([ReplicaState.RUNNING])
        ]

    def _notify_running_replicas_changed(self):
        self._long_poll_host.notify_changed(
            (LongPollNamespace.RUNNING_REPLICAS, self._name),
            self.get_running_replica_infos(),
        )

    def _set_deployment_goal(
            self, deployment_info: Optional[DeploymentInfo]) -> None:
        """
        Set desirable state for a given deployment, identified by tag.

        Args:
            deployment_info (Optional[DeploymentInfo]): Contains deployment and
                replica config, if passed in as None, we're marking
                target deployment as shutting down.
        """
        existing_goal_id = self._curr_goal
        new_goal_id = self._goal_manager.create_goal()

        if deployment_info is not None:
            self._target_info = deployment_info
            self._target_replicas = (
                deployment_info.deployment_config.num_replicas)

            self._target_version = DeploymentVersion(
                deployment_info.version,
                user_config=deployment_info.deployment_config.user_config)

        else:
            self._target_replicas = 0

        self._curr_goal = new_goal_id
        version_str = (deployment_info
                       if deployment_info is None else deployment_info.version)
        logger.debug(
            f"Set deployment goal for {self._name} with version {version_str}")
        return new_goal_id, existing_goal_id

    def deploy(self, deployment_info: DeploymentInfo
               ) -> Tuple[Optional[GoalId], bool]:
        """Deploy the deployment.

        If the deployment already exists with the same version and config,
        this is a no-op and returns the GoalId corresponding to the existing
        update if there is one.

        Returns:
            GoalId, bool: The GoalId for the client to wait for and whether or
            not the deployment is being updated.
        """
        # Ensures this method is idempotent.
        existing_info = self._target_info
        if existing_info is not None:
            # Redeploying should not reset the deployment's start time.
            deployment_info.start_time_ms = existing_info.start_time_ms

            if (existing_info.deployment_config ==
                    deployment_info.deployment_config
                    and deployment_info.version is not None
                    and existing_info.version == deployment_info.version):
                return self._curr_goal, False

        # Keep a copy of previous deployment info in case goal failed to
        # complete to initiate rollback.
        self._rollback_info = self._target_info

        # Reset constructor retry counter.
        self._replica_constructor_retry_counter = 0

        new_goal_id, existing_goal_id = self._set_deployment_goal(
            deployment_info)

        # NOTE(edoakes): we must write a checkpoint before starting new
        # or pushing the updated config to avoid inconsistent state if we
        # crash while making the change.
        self._save_checkpoint_func()

        if existing_goal_id is not None:
            self._goal_manager.complete_goal(existing_goal_id)
        return new_goal_id, True

    def delete(self) -> Optional[GoalId]:
        new_goal_id, existing_goal_id = self._set_deployment_goal(None)

        self._save_checkpoint_func()
        if existing_goal_id is not None:
            self._goal_manager.complete_goal(existing_goal_id)
        return new_goal_id

    def _stop_wrong_version_replicas(self) -> bool:
        """Stops replicas with outdated versions to implement rolling updates.

        This includes both explicit code version updates and changes to the
        user_config.

        Returns whether any replicas were stopped.
        """
        # Short circuit if target replicas is 0 (the deployment is being
        # deleted) because this will be handled in the main loop.
        if self._target_replicas == 0:
            return False

        # We include STARTING and UPDATING replicas here
        # because if there are replicas still pending startup, we may as well
        # terminate them and start new version replicas instead.
        old_running_replicas = self._replicas.count(
            exclude_version=self._target_version,
            states=[
                ReplicaState.STARTING, ReplicaState.UPDATING,
                ReplicaState.RUNNING
            ])
        old_stopping_replicas = self._replicas.count(
            exclude_version=self._target_version,
            states=[ReplicaState.STOPPING])
        new_running_replicas = self._replicas.count(
            version=self._target_version, states=[ReplicaState.RUNNING])

        # If the deployment is currently scaling down, let the scale down
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
            states=[ReplicaState.STARTING, ReplicaState.RUNNING],
            max_replicas=max_to_stop)

        replicas_stopped = False
        code_version_changes = 0
        user_config_changes = 0
        for replica in replicas_to_update:
            # If the code version is a mismatch, we stop the replica. A new one
            # with the correct version will be started later as part of the
            # normal scale-up process.
            if (replica.version.code_version !=
                    self._target_version.code_version):
                code_version_changes += 1
                replica.stop()
                self._replicas.add(ReplicaState.STOPPING, replica)
                replicas_stopped = True
            # If only the user_config is a mismatch, we update it dynamically
            # without restarting the replica.
            elif (replica.version.user_config_hash !=
                  self._target_version.user_config_hash):
                user_config_changes += 1
                replica.update_user_config(self._target_version.user_config)
                self._replicas.add(ReplicaState.UPDATING, replica)
                logger.debug(
                    "Adding UPDATING to replica_tag: "
                    f"{replica.replica_tag}, deployment_name: {self._name}")
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

        return replicas_stopped

    def _scale_deployment_replicas(self) -> bool:
        """Scale the given deployment to the number of replicas."""

        assert self._target_replicas >= 0, ("Number of replicas must be"
                                            " greater than or equal to 0.")

        replicas_stopped = self._stop_wrong_version_replicas()

        current_replicas = self._replicas.count(states=[
            ReplicaState.STARTING, ReplicaState.UPDATING, ReplicaState.RUNNING
        ])
        recovering_replicas = self._replicas.count(
            states=[ReplicaState.RECOVERING])

        delta_replicas = (
            self._target_replicas - current_replicas - recovering_replicas)
        if delta_replicas == 0:
            return False

        elif delta_replicas > 0:
            # Don't ever exceed self._target_replicas.
            stopping_replicas = self._replicas.count(states=[
                ReplicaState.STOPPING,
            ])
            to_add = max(delta_replicas - stopping_replicas, 0)
            if to_add > 0:
                logger.info(f"Adding {to_add} replicas to deployment "
                            f"'{self._name}'. component=serve "
                            f"deployment={self._name}")
            for _ in range(to_add):
                replica_name = ReplicaName(self._name, get_random_letters())
                new_deployment_replica = DeploymentReplica(
                    self._controller_name, self._detached,
                    replica_name.replica_tag, replica_name.deployment_tag,
                    self._target_version)
                new_deployment_replica.start(self._target_info,
                                             self._target_version)

                self._replicas.add(ReplicaState.STARTING,
                                   new_deployment_replica)
                logger.debug("Adding STARTING to replica_tag: "
                             f"{replica_name}, deployment_name: {self._name}")

        elif delta_replicas < 0:
            replicas_stopped = True
            to_remove = -delta_replicas
            logger.info(f"Removing {to_remove} replicas from deployment "
                        f"'{self._name}'. component=serve "
                        f"deployment={self._name}")
            replicas_to_stop = self._replicas.pop(
                states=[
                    ReplicaState.STARTING, ReplicaState.UPDATING,
                    ReplicaState.RECOVERING, ReplicaState.RUNNING
                ],
                max_replicas=to_remove)

            for replica in replicas_to_stop:
                logger.debug(f"Adding STOPPING to replica_tag: {replica}, "
                             f"deployment_name: {self._name}")
                replica.stop()
                self._replicas.add(ReplicaState.STOPPING, replica)

        return replicas_stopped

    def _check_curr_goal_status(self) -> GoalStatus:
        """
        In each update() cycle, upon finished calling _scale_all_deployments(),
        check difference between target vs. running relica count for each
        deployment and return whether or not the current goal is complete.

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
                ReplicaState.STARTING,
                ReplicaState.UPDATING,
                ReplicaState.RECOVERING,
                ReplicaState.STOPPING,
        ]) == 0):
            # Check for deleting.
            if target_replica_count == 0 and all_running_replica_cnt == 0:
                return GoalStatus.SUCCESSFULLY_DELETED

            # Check for a non-zero number of deployments.
            elif target_replica_count == running_at_target_version_replica_cnt:
                return GoalStatus.SUCCEEDED

        return GoalStatus.PENDING

    def _check_startup_replicas(
            self, original_state: ReplicaState, stop_on_slow=False
    ) -> Tuple[List[Tuple[DeploymentReplica, ReplicaStartupStatus]], bool]:
        """
        Common helper function for startup actions tracking and status
        transition: STARTING, UPDATING and RECOVERING.

        Args:
            stop_on_slow: If we consider a replica failed upon observing it's
                slow to reach running state.
        """
        slow_replicas = []
        transitioned_to_running = False
        for replica in self._replicas.pop(states=[original_state]):
            start_status = replica.check_started()
            if start_status == ReplicaStartupStatus.SUCCEEDED:
                # This replica should be now be added to handle's replica
                # set.
                self._replicas.add(ReplicaState.RUNNING, replica)
                transitioned_to_running = True
            elif start_status == ReplicaStartupStatus.FAILED:
                # Replica reconfigure (deploy / upgrade) failed
                if self._replica_constructor_retry_counter >= 0:
                    # Increase startup failure counter if we're tracking it
                    self._replica_constructor_retry_counter += 1

                replica.stop(graceful=False)
                self._replicas.add(ReplicaState.STOPPING, replica)
            elif start_status in [
                    ReplicaStartupStatus.PENDING_ALLOCATION,
                    ReplicaStartupStatus.PENDING_INITIALIZATION,
            ]:

                is_slow = time.time(
                ) - replica._start_time > SLOW_STARTUP_WARNING_S

                if is_slow:
                    slow_replicas.append((replica, start_status))

                # Does it make sense to stop replicas in PENDING_ALLOCATION
                # state?
                if is_slow and stop_on_slow:
                    replica.stop(graceful=False)
                    self._replicas.add(ReplicaState.STOPPING, replica)
                else:
                    self._replicas.add(original_state, replica)

        return slow_replicas, transitioned_to_running

    def _check_and_update_replicas(self) -> bool:
        """
        Check current state of all DeploymentReplica being tracked, and compare
        with state container from previous update() cycle to see if any state
        transition happened.

        Returns if any running replicas transitioned to another state.
        """
        running_replicas_changed = False
        for replica in self._replicas.pop(states=[ReplicaState.RUNNING]):
            if replica.check_health():
                self._replicas.add(ReplicaState.RUNNING, replica)
            else:
                running_replicas_changed = True
                logger.warning(
                    f"Replica {replica.replica_tag} of deployment "
                    f"{self._name} failed health check, stopping it. "
                    f"component=serve deployment={self._name} "
                    f"replica={replica.replica_tag}")
                replica.stop(graceful=False)
                self._replicas.add(ReplicaState.STOPPING, replica)

        slow_start_replicas = []
        slow_start, starting_to_running = self._check_startup_replicas(
            ReplicaState.STARTING)
        slow_update, updating_to_running = self._check_startup_replicas(
            ReplicaState.UPDATING)
        slow_recover, recovering_to_running = self._check_startup_replicas(
            ReplicaState.RECOVERING, stop_on_slow=True)

        slow_start_replicas = slow_start + slow_update + slow_recover
        running_replicas_changed = (running_replicas_changed
                                    or starting_to_running
                                    or updating_to_running
                                    or recovering_to_running)

        if (len(slow_start_replicas)
                and time.time() - self._prev_startup_warning >
                SLOW_STARTUP_WARNING_PERIOD_S):

            pending_allocation = []
            pending_initialization = []

            for replica, startup_status in slow_start_replicas:
                if startup_status == ReplicaStartupStatus.PENDING_ALLOCATION:
                    pending_allocation.append(replica)
                if startup_status \
                        == ReplicaStartupStatus.PENDING_INITIALIZATION:
                    pending_initialization.append(replica)

            if len(pending_allocation) > 0:
                required, available = slow_start_replicas[0][
                    0].resource_requirements()
                logger.warning(
                    f"Deployment '{self._name}' has "
                    f"{len(pending_allocation)} replicas that have taken "
                    f"more than {SLOW_STARTUP_WARNING_S}s to be scheduled. "
                    f"This may be caused by waiting for the cluster to "
                    f"auto-scale, or waiting for a runtime environment "
                    f"to install. "
                    f"Resources required for each replica: {required}, "
                    f"resources available: {available}. "
                    f"component=serve deployment={self._name}")
                if _SCALING_LOG_ENABLED:
                    print_verbose_scaling_log()

            if len(pending_initialization) > 0:
                logger.warning(
                    f"Deployment '{self._name}' has "
                    f"{len(pending_initialization)} replicas that have taken "
                    f"more than {SLOW_STARTUP_WARNING_S}s to initialize. This "
                    f"may be caused by a slow __init__ or reconfigure method."
                    f"component=serve deployment={self._name}")

            self._prev_startup_warning = time.time()

        for replica in self._replicas.pop(states=[ReplicaState.STOPPING]):
            stopped = replica.check_stopped()
            if not stopped:
                self._replicas.add(ReplicaState.STOPPING, replica)

        return running_replicas_changed

    def update(self) -> bool:
        """Updates the state of all deployments to match their goal state."""
        try:
            # Add or remove DeploymentReplica instances in self._replicas.
            # This should be the only place we adjust total number of replicas
            # we manage.
            running_replicas_changed = self._scale_deployment_replicas()

            # Check the state of existing replicas and transition if necessary.
            running_replicas_changed |= self._check_and_update_replicas()

            if running_replicas_changed:
                self._notify_running_replicas_changed()

            status = self._check_curr_goal_status()
            exception = None
        except Exception as e:
            status = GoalStatus.FAILED
            exception = e

        if status == GoalStatus.SUCCEEDED:
            # Deployment successul, complete the goal and clear the
            # backup deployment_info.
            self._goal_manager.complete_goal(self._curr_goal)
            self._rollback_info = None
        elif status == GoalStatus.FAILED:
            if exception is None:
                # report a generic exception if none was caught
                # TODO: in which cases can we get the exception
                # that caused the failure?
                exception = RuntimeError("Failed to reach deployment goal. "
                                         "Check the serve logs for details.")
            # wrap this exception so that it can be sent across the cluster
            exception = wrap_to_ray_error("unknown", exception)

            # Roll back or delete the deployment if it failed.
            if self._rollback_info is None:
                # No info to roll back to, delete it.
                self._goal_manager.complete_goal(
                    self._curr_goal,
                    exception,
                )
                logger.info(f"Deployment '{self._name}' failed, deleting it. "
                            f"component=serve deployment={self._name}")
                self.delete()
            else:
                # Roll back to the previous version.
                rollback_info = self._rollback_info
                self._goal_manager.complete_goal(
                    self._curr_goal,
                    exception,
                )
                self._curr_goal = None
                self._rollback_info = None
                logger.info(
                    f"Updating deployment '{self._name}' failed, rolling "
                    f"back to version {rollback_info.version}. "
                    f"component=serve deployment={self._name}")
                self.deploy(rollback_info)
        elif status == GoalStatus.SUCCESSFULLY_DELETED:
            self._goal_manager.complete_goal(self._curr_goal)

        return status == GoalStatus.SUCCESSFULLY_DELETED

    def _stop_one_running_replica_for_testing(self):
        running_replicas = self._replicas.pop(states=[ReplicaState.RUNNING])
        replica_to_stop = running_replicas.pop()
        replica_to_stop.stop(graceful=False)
        self._replicas.add(ReplicaState.STOPPING, replica_to_stop)
        for replica in running_replicas:
            self._replicas.add(ReplicaState.RUNNING, replica)


class DeploymentStateManager:
    """Manages all state for deployments in the system.

    This class is *not* thread safe, so any state-modifying methods should be
    called with a lock held.
    """

    def __init__(self, controller_name: str, detached: bool,
                 kv_store: KVStoreBase, long_poll_host: LongPollHost,
                 goal_manager: AsyncGoalManager,
                 all_current_actor_names: List[str]):

        self._controller_name = controller_name
        self._detached = detached
        self._kv_store = kv_store
        self._long_poll_host = long_poll_host
        self._goal_manager = goal_manager
        self._create_deployment_state: Callable = lambda name: DeploymentState(
            name, controller_name, detached,
            long_poll_host, goal_manager, self._save_checkpoint_func)
        self._deployment_states: Dict[str, DeploymentState] = dict()
        self._deleted_deployment_metadata: Dict[
            str, DeploymentInfo] = OrderedDict()

        self._recover_from_checkpoint(all_current_actor_names)

    def _map_actor_names_to_deployment(
            self, all_current_actor_names: List[str]) -> Dict[str, List[str]]:
        """
        Given a list of all actor names queried from current ray cluster,
        map them to corresponding deployments.

        Example:
            Args:
                [A#zxc123, B#xcv234, A#qwe234]
            Returns:
                {
                    A: [A#zxc123, A#qwe234]
                    B: [B#xcv234]
                }
        """
        all_replica_names = [
            actor_name for actor_name in all_current_actor_names
            if ReplicaName.is_replica_name(actor_name)
        ]
        deployment_to_current_replicas = defaultdict(list)
        if len(all_replica_names) > 0:
            # Each replica tag is formatted as "deployment_name#random_letter"
            for replica_name in all_replica_names:
                replica_tag = ReplicaName.from_str(replica_name)
                deployment_to_current_replicas[
                    replica_tag.deployment_tag].append(replica_name)

        return deployment_to_current_replicas

    def _recover_from_checkpoint(self,
                                 all_current_actor_names: List[str]) -> None:
        """
        Recover from checkpoint upon controller failure with all actor names
        found in current cluster.

        Each deployment resumes target state from checkpoint if available.

        For current state it will prioritize reconstructing from current
        actor names found that matches deployment tag if applicable.
        """
        deployment_to_current_replicas = self._map_actor_names_to_deployment(
            all_current_actor_names)
        checkpoint = self._kv_store.get(CHECKPOINT_KEY)
        if checkpoint is not None:
            (deployment_state_info,
             self._deleted_deployment_metadata) = pickle.loads(checkpoint)

            for deployment_tag, checkpoint_data in deployment_state_info.items(
            ):
                deployment_state = self._create_deployment_state(
                    deployment_tag)

                target_state_checkpoint = checkpoint_data
                deployment_state.recover_target_state_from_checkpoint(
                    target_state_checkpoint)
                if len(deployment_to_current_replicas[deployment_tag]) > 0:
                    deployment_state.recover_current_state_from_replica_actor_names(  # noqa: E501
                        deployment_to_current_replicas[deployment_tag])
                self._deployment_states[deployment_tag] = deployment_state

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
        for deployment_state in self._deployment_states.values():
            goal = deployment_state.delete()
            if goal is not None:
                shutdown_goals.append(goal)

        # TODO(jiaodong): This might not be 100% safe since we deleted
        # everything without ensuring all shutdown goals are completed
        # yet. Need to address in follow-up PRs.
        self._kv_store.delete(CHECKPOINT_KEY)

        # TODO(jiaodong): Need to add some logic to prevent new replicas
        # from being created once shutdown signal is sent.
        return shutdown_goals

    def _save_checkpoint_func(self) -> None:
        deployment_state_info = {
            deployment_name: deployment_state.get_checkpoint_data()
            for deployment_name, deployment_state in
            self._deployment_states.items()
        }
        self._kv_store.put(
            CHECKPOINT_KEY,
            # NOTE(simon): Make sure to use pickle so we don't save any ray
            # object that relies on external state (e.g. gcs). For code object,
            # we are explicitly using cloudpickle to serialize them.
            pickle.dumps((deployment_state_info,
                          self._deleted_deployment_metadata)))

    def get_running_replica_infos(
            self,
            filter_tag: Optional[str] = None,
    ) -> Dict[str, List[RunningReplicaInfo]]:
        replicas = {}
        for deployment_name, deployment_state in self._deployment_states.items(
        ):
            if filter_tag is None or deployment_name == filter_tag:
                replicas[deployment_name] = (
                    deployment_state.get_running_replica_infos())

        return replicas

    def get_deployment_configs(self,
                               filter_tag: Optional[str] = None,
                               include_deleted: Optional[bool] = False
                               ) -> Dict[str, DeploymentConfig]:
        configs: Dict[str, DeploymentConfig] = {}
        for deployment_name, deployment_state in self._deployment_states.items(
        ):
            if filter_tag is None or deployment_name == filter_tag:
                configs[deployment_name] = (
                    deployment_state.target_info.deployment_config)

        if include_deleted:
            for name, info in self._deleted_deployment_metadata.items():
                if filter_tag is None or name == filter_tag:
                    configs[name] = info.deployment_config

        return configs

    def get_deployment(self,
                       deployment_name: str,
                       include_deleted: Optional[bool] = False
                       ) -> Optional[DeploymentInfo]:
        if deployment_name in self._deployment_states:
            return self._deployment_states[deployment_name].target_info
        elif (include_deleted
              and deployment_name in self._deleted_deployment_metadata):
            return self._deleted_deployment_metadata[deployment_name]
        else:
            return None

    def deploy(self, deployment_name: str, deployment_info: DeploymentInfo
               ) -> Tuple[Optional[GoalId], bool]:
        """Deploy the deployment.

        If the deployment already exists with the same version and config,
        this is a no-op and returns the GoalId corresponding to the existing
        update if there is one.

        Returns:
            GoalId, bool: The GoalId for the client to wait for and whether or
            not the deployment is being updated.
        """
        if deployment_name in self._deleted_deployment_metadata:
            del self._deleted_deployment_metadata[deployment_name]

        if deployment_name not in self._deployment_states:
            self._deployment_states[
                deployment_name] = self._create_deployment_state(
                    deployment_name)

        return self._deployment_states[deployment_name].deploy(deployment_info)

    def delete_deployment(self, deployment_name: str) -> Optional[GoalId]:
        # This method must be idempotent. We should validate that the
        # specified deployment exists on the client.
        if deployment_name not in self._deployment_states:
            return None

        deployment_state = self._deployment_states[deployment_name]
        return deployment_state.delete()

    def update(self) -> bool:
        """Updates the state of all deployments to match their goal state."""
        deleted_tags = []
        for deployment_name, deployment_state in self._deployment_states.items(
        ):
            deleted = deployment_state.update()
            if deleted:
                deleted_tags.append(deployment_name)
                deployment_info = deployment_state.target_info
                deployment_info.end_time_ms = int(time.time() * 1000)
                if (len(self._deleted_deployment_metadata) >
                        MAX_NUM_DELETED_DEPLOYMENTS):
                    self._deleted_deployment_metadata.popitem(last=False)
                self._deleted_deployment_metadata[
                    deployment_name] = deployment_info

        for tag in deleted_tags:
            del self._deployment_states[tag]

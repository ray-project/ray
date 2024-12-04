import json
import logging
import math
import os
import random
import time
import traceback
from collections import defaultdict
from copy import copy
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import ray
from ray import ObjectRef, cloudpickle
from ray._private import ray_constants
from ray.actor import ActorHandle
from ray.exceptions import RayActorError, RayError, RayTaskError, RuntimeEnvSetupError
from ray.serve import metrics
from ray.serve._private import default_impl
from ray.serve._private.autoscaling_state import AutoscalingStateManager
from ray.serve._private.cluster_node_info_cache import ClusterNodeInfoCache
from ray.serve._private.common import (
    DeploymentID,
    DeploymentStatus,
    DeploymentStatusInfo,
    DeploymentStatusInternalTrigger,
    DeploymentStatusTrigger,
    Duration,
    MultiplexedReplicaInfo,
    ReplicaID,
    ReplicaState,
    RunningReplicaInfo,
)
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.constants import (
    MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT,
    RAY_SERVE_EAGERLY_START_REPLACEMENT_REPLICAS,
    RAY_SERVE_ENABLE_TASK_EVENTS,
    RAY_SERVE_FORCE_STOP_UNHEALTHY_REPLICAS,
    RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY,
    REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve._private.deployment_scheduler import (
    DeploymentDownscaleRequest,
    DeploymentScheduler,
    ReplicaSchedulingRequest,
    ReplicaSchedulingRequestStatus,
    SpreadDeploymentSchedulingPolicy,
)
from ray.serve._private.long_poll import LongPollHost, LongPollNamespace
from ray.serve._private.storage.kv_store import KVStoreBase
from ray.serve._private.usage import ServeUsageTag
from ray.serve._private.utils import (
    JavaActorHandleProxy,
    check_obj_ref_ready_nowait,
    get_capacity_adjusted_num_replicas,
    get_random_string,
    msgpack_deserialize,
    msgpack_serialize,
)
from ray.serve._private.version import DeploymentVersion
from ray.serve.generated.serve_pb2 import DeploymentLanguage
from ray.serve.schema import (
    DeploymentDetails,
    ReplicaDetails,
    _deployment_info_to_schema,
)
from ray.util.placement_group import PlacementGroup

logger = logging.getLogger(SERVE_LOGGER_NAME)


class ReplicaStartupStatus(Enum):
    PENDING_ALLOCATION = 1
    PENDING_INITIALIZATION = 2
    SUCCEEDED = 3
    FAILED = 4


class ReplicaHealthCheckResponse(Enum):
    NONE = 1
    SUCCEEDED = 2
    APP_FAILURE = 3
    ACTOR_CRASHED = 4


@dataclass
class DeploymentTargetState:
    """The current goal state for a deployment.

    info: contains the information needed to initialize a replica.
    target_num_replicas: the number of replicas to run. This should already
        be adjusted by the target_capacity.
    version: the goal version of the deployment.
    deleting: whether the deployment is being deleted.
    """

    info: Optional[DeploymentInfo]
    target_num_replicas: int
    version: Optional[DeploymentVersion]
    deleting: bool

    @classmethod
    def default(cls) -> "DeploymentTargetState":
        return cls(None, -1, None, False)

    @classmethod
    def create(
        cls,
        info: DeploymentInfo,
        target_num_replicas: int,
        *,
        deleting: bool = False,
    ) -> "DeploymentTargetState":
        if deleting:
            if target_num_replicas != 0:
                raise ValueError(
                    "target_num_replicas must be 0 when setting target state "
                    f"to deleting. Got {target_num_replicas} instead."
                )

        version = DeploymentVersion(
            info.version,
            deployment_config=info.deployment_config,
            ray_actor_options=info.replica_config.ray_actor_options,
            placement_group_bundles=info.replica_config.placement_group_bundles,
            placement_group_strategy=info.replica_config.placement_group_strategy,
            max_replicas_per_node=info.replica_config.max_replicas_per_node,
        )

        return cls(info, target_num_replicas, version, deleting)

    def is_scaled_copy_of(self, other_target_state: "DeploymentTargetState") -> bool:
        """Checks if this target state is a scaled copy of another target state.

        A target state is a scaled copy of another target state if all
        configurable info is identical, other than target_num_replicas.

        Returns: True if this target state contains a non-None DeploymentInfo
            and is a scaled copy of the other target state.
        """

        if other_target_state.info is None:
            return False

        return all(
            [
                self.info.replica_config.ray_actor_options
                == other_target_state.info.replica_config.ray_actor_options,
                self.info.replica_config.placement_group_bundles
                == other_target_state.info.replica_config.placement_group_bundles,
                self.info.replica_config.placement_group_strategy
                == other_target_state.info.replica_config.placement_group_strategy,
                self.info.replica_config.max_replicas_per_node
                == other_target_state.info.replica_config.max_replicas_per_node,
                self.info.deployment_config.dict(exclude={"num_replicas"})
                == other_target_state.info.deployment_config.dict(
                    exclude={"num_replicas"}
                ),
                # TODO(zcin): version can be None, this is from an outdated codepath.
                # We should remove outdated code, so version can never be None.
                self.version,
                self.version == other_target_state.version,
            ]
        )


@dataclass
class DeploymentStateUpdateResult:
    deleted: bool
    any_replicas_recovering: bool
    upscale: List[ReplicaSchedulingRequest]
    downscale: Optional[DeploymentDownscaleRequest]


CHECKPOINT_KEY = "serve-deployment-state-checkpoint"
SLOW_STARTUP_WARNING_S = int(os.environ.get("SERVE_SLOW_STARTUP_WARNING_S", 30))
SLOW_STARTUP_WARNING_PERIOD_S = int(
    os.environ.get("SERVE_SLOW_STARTUP_WARNING_PERIOD_S", 30)
)

EXPONENTIAL_BACKOFF_FACTOR = float(os.environ.get("EXPONENTIAL_BACKOFF_FACTOR", 2.0))
MAX_BACKOFF_TIME_S = int(os.environ.get("SERVE_MAX_BACKOFF_TIME_S", 64))

ALL_REPLICA_STATES = list(ReplicaState)
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

    def __init__(
        self,
        replica_id: ReplicaID,
        version: DeploymentVersion,
    ):
        self._replica_id = replica_id
        self._deployment_id = replica_id.deployment_id
        self._actor_name = replica_id.to_full_id_str()

        # Populated in either self.start() or self.recover()
        self._allocated_obj_ref: ObjectRef = None
        self._ready_obj_ref: ObjectRef = None

        self._actor_resources: Dict[str, float] = None
        # If the replica is being started, this will be the true version
        # If the replica is being recovered, this will be the target
        # version, which may be inconsistent with the actual replica
        # version. If so, the actual version will be updated later after
        # recover() and check_ready()
        self._version: DeploymentVersion = version
        self._healthy: bool = True
        self._health_check_ref: Optional[ObjectRef] = None
        self._last_health_check_time: float = 0.0
        self._consecutive_health_check_failures = 0
        self._initialization_latency_s: Optional[float] = None
        self._port: Optional[int] = None

        # Populated in `on_scheduled` or `recover`.
        self._actor_handle: ActorHandle = None
        self._placement_group: PlacementGroup = None

        # Populated after replica is allocated.
        self._pid: int = None
        self._actor_id: str = None
        self._worker_id: str = None
        self._node_id: str = None
        self._node_ip: str = None
        self._log_file_path: str = None

        # Populated in self.stop().
        self._graceful_shutdown_ref: ObjectRef = None

        # todo: will be confused with deployment_config.is_cross_language
        self._is_cross_language = False
        self._deployment_is_cross_language = False

    @property
    def replica_id(self) -> str:
        return self._replica_id

    @property
    def deployment_name(self) -> str:
        return self._deployment_id.name

    @property
    def app_name(self) -> str:
        return self._deployment_id.app_name

    @property
    def is_cross_language(self) -> bool:
        return self._is_cross_language

    @property
    def actor_handle(self) -> Optional[ActorHandle]:
        if not self._actor_handle:
            try:
                self._actor_handle = ray.get_actor(
                    self._actor_name, namespace=SERVE_NAMESPACE
                )
            except ValueError:
                self._actor_handle = None

        if self._is_cross_language:
            assert isinstance(self._actor_handle, JavaActorHandleProxy)
            return self._actor_handle.handle

        return self._actor_handle

    @property
    def placement_group_bundles(self) -> Optional[List[Dict[str, float]]]:
        if not self._placement_group:
            return None

        return self._placement_group.bundle_specs

    @property
    def version(self) -> DeploymentVersion:
        """Replica version. This can be incorrect during state recovery.

        If the controller crashes and the deployment state is being
        recovered, this will temporarily be the deployment-wide target
        version, which may be inconsistent with the actual version
        running on the replica actor. If so, the actual version will be
        updated when the replica transitions from RECOVERING -> RUNNING
        """
        return self._version

    @property
    def deployment_config(self) -> DeploymentConfig:
        """Deployment config. This can return an incorrect config during state recovery.

        If the controller hasn't yet recovered the up-to-date version
        from the running replica actor, this property will return the
        current target config for the deployment.
        """
        return self._version.deployment_config

    @property
    def max_ongoing_requests(self) -> int:
        return self.deployment_config.max_ongoing_requests

    @property
    def max_queued_requests(self) -> int:
        return self.deployment_config.max_queued_requests

    @property
    def graceful_shutdown_timeout_s(self) -> float:
        return self.deployment_config.graceful_shutdown_timeout_s

    @property
    def health_check_period_s(self) -> float:
        return self.deployment_config.health_check_period_s

    @property
    def health_check_timeout_s(self) -> float:
        return self.deployment_config.health_check_timeout_s

    @property
    def pid(self) -> Optional[int]:
        """Returns the pid of the actor, None if not started."""
        return self._pid

    @property
    def actor_id(self) -> Optional[str]:
        """Returns the actor id, None if not started."""
        return self._actor_id

    @property
    def worker_id(self) -> Optional[str]:
        """Returns the worker id, None if not started."""
        return self._worker_id

    @property
    def node_id(self) -> Optional[str]:
        """Returns the node id of the actor, None if not placed."""
        return self._node_id

    @property
    def node_ip(self) -> Optional[str]:
        """Returns the node ip of the actor, None if not placed."""
        return self._node_ip

    @property
    def log_file_path(self) -> Optional[str]:
        """Returns the relative log file path of the actor, None if not placed."""
        return self._log_file_path

    @property
    def initialization_latency_s(self) -> Optional[float]:
        """Returns the initialization latency for the replica actor.

        Returns None if the replica hasn't started yet.

        Note: this value isn't checkpointed, so if the controller restarts,
        this value goes back to None.
        """

        return self._initialization_latency_s

    def start(self, deployment_info: DeploymentInfo) -> ReplicaSchedulingRequest:
        """Start the current DeploymentReplica instance.

        The replica will be in the STARTING and PENDING_ALLOCATION states
        until the deployment scheduler schedules the underlying actor.
        """
        self._actor_resources = deployment_info.replica_config.resource_dict
        # it is currently not possible to create a placement group
        # with no resources (https://github.com/ray-project/ray/issues/20401)
        self._deployment_is_cross_language = (
            deployment_info.deployment_config.is_cross_language
        )

        logger.info(
            f"Starting {self.replica_id}.",
            extra={"log_to_stderr": False},
        )

        actor_def = deployment_info.actor_def
        if (
            deployment_info.deployment_config.deployment_language
            == DeploymentLanguage.PYTHON
        ):
            if deployment_info.replica_config.serialized_init_args is None:
                serialized_init_args = cloudpickle.dumps(())
            else:
                serialized_init_args = (
                    cloudpickle.dumps(
                        msgpack_deserialize(
                            deployment_info.replica_config.serialized_init_args
                        )
                    )
                    if self._deployment_is_cross_language
                    else deployment_info.replica_config.serialized_init_args
                )
            init_args = (
                self.replica_id,
                cloudpickle.dumps(deployment_info.replica_config.deployment_def)
                if self._deployment_is_cross_language
                else deployment_info.replica_config.serialized_deployment_def,
                serialized_init_args,
                deployment_info.replica_config.serialized_init_kwargs
                if deployment_info.replica_config.serialized_init_kwargs
                else cloudpickle.dumps({}),
                deployment_info.deployment_config.to_proto_bytes(),
                self._version,
            )
        # TODO(simon): unify the constructor arguments across language
        elif (
            deployment_info.deployment_config.deployment_language
            == DeploymentLanguage.JAVA
        ):
            self._is_cross_language = True
            actor_def = ray.cross_language.java_actor_class(
                "io.ray.serve.replica.RayServeWrappedReplica"
            )
            init_args = (
                # String deploymentName,
                self.deployment_name,
                # String replicaID,
                self.replica_id.to_full_id_str(),
                # String deploymentDef
                deployment_info.replica_config.deployment_def_name,
                # byte[] initArgsbytes
                msgpack_serialize(
                    cloudpickle.loads(
                        deployment_info.replica_config.serialized_init_args
                    )
                )
                if self._deployment_is_cross_language
                else deployment_info.replica_config.serialized_init_args,
                # byte[] deploymentConfigBytes,
                deployment_info.deployment_config.to_proto_bytes(),
                # byte[] deploymentVersionBytes,
                self._version.to_proto().SerializeToString(),
                # String controllerName
                # String appName
                self.app_name,
            )

        actor_options = {
            "name": self._actor_name,
            "namespace": SERVE_NAMESPACE,
            "lifetime": "detached",
            "enable_task_events": RAY_SERVE_ENABLE_TASK_EVENTS,
        }
        actor_options.update(deployment_info.replica_config.ray_actor_options)

        # A replica's default `max_concurrency` value can prevent it from
        # respecting the configured `max_ongoing_requests`. To avoid this
        # unintentional behavior, use `max_ongoing_requests` to override
        # the Actor's `max_concurrency` if it is larger.
        if (
            deployment_info.deployment_config.max_ongoing_requests
            > ray_constants.DEFAULT_MAX_CONCURRENCY_ASYNC
        ):
            actor_options[
                "max_concurrency"
            ] = deployment_info.deployment_config.max_ongoing_requests

        return ReplicaSchedulingRequest(
            replica_id=self.replica_id,
            actor_def=actor_def,
            actor_resources=self._actor_resources,
            actor_options=actor_options,
            actor_init_args=init_args,
            placement_group_bundles=(
                deployment_info.replica_config.placement_group_bundles
            ),
            placement_group_strategy=(
                deployment_info.replica_config.placement_group_strategy
            ),
            max_replicas_per_node=(
                deployment_info.replica_config.max_replicas_per_node
            ),
            on_scheduled=self.on_scheduled,
        )

    def on_scheduled(
        self,
        actor_handle: ActorHandle,
        placement_group: Optional[PlacementGroup] = None,
    ):
        self._actor_handle = actor_handle
        self._placement_group = placement_group

        # Perform auto method name translation for java handles.
        # See https://github.com/ray-project/ray/issues/21474
        deployment_config = copy(self._version.deployment_config)
        deployment_config.user_config = self._format_user_config(
            deployment_config.user_config
        )
        if self._is_cross_language:
            self._actor_handle = JavaActorHandleProxy(self._actor_handle)
            self._allocated_obj_ref = self._actor_handle.is_allocated.remote()
            self._ready_obj_ref = self._actor_handle.is_initialized.remote(
                deployment_config.to_proto_bytes()
            )
        else:
            self._allocated_obj_ref = self._actor_handle.is_allocated.remote()
            replica_ready_check_func = self._actor_handle.initialize_and_get_metadata
            self._ready_obj_ref = replica_ready_check_func.remote(
                deployment_config,
                # Ensure that `is_allocated` will execute
                # before `initialize_and_get_metadata`,
                # because `initialize_and_get_metadata` runs
                # user code that could block the replica
                # asyncio loop. If that happens before `is_allocated` is executed,
                # the `is_allocated` call won't be able to run.
                self._allocated_obj_ref,
            )

    def _format_user_config(self, user_config: Any):
        temp = copy(user_config)
        if user_config is not None and self._deployment_is_cross_language:
            if self._is_cross_language:
                temp = msgpack_serialize(temp)
            else:
                temp = msgpack_deserialize(temp)
        return temp

    def reconfigure(self, version: DeploymentVersion) -> bool:
        """
        Update replica version. Also, updates the deployment config on the actor
        behind this DeploymentReplica instance if necessary.

        Returns: whether the actor is being updated.
        """
        updating = False
        if self._version.requires_actor_reconfigure(version):
            # Call into replica actor reconfigure() with updated user config and
            # graceful_shutdown_wait_loop_s
            updating = True
            deployment_config = copy(version.deployment_config)
            deployment_config.user_config = self._format_user_config(
                deployment_config.user_config
            )
            self._ready_obj_ref = self._actor_handle.reconfigure.remote(
                deployment_config
            )

        self._version = version
        return updating

    def recover(self) -> bool:
        """Recover replica version from a live replica actor.

        When controller dies, the deployment state loses the info on the version that's
        running on each individual replica actor, so as part of the recovery process, we
        need to recover the version that is running on the replica actor.

        Also confirm that actor is allocated and initialized before marking as running.

        Returns: False if the replica actor is no longer alive; the
            actor could have been killed in the time between when the
            controller fetching all Serve actors in the cluster and when
            the controller tries to recover it. Otherwise, return True.
        """
        logger.info(f"Recovering {self.replica_id}.")
        try:
            self._actor_handle = ray.get_actor(
                self._actor_name, namespace=SERVE_NAMESPACE
            )
        except ValueError:
            logger.warning(
                f"Failed to get handle to replica {self._actor_name} "
                "during controller recovery. Marking as dead."
            )
            return False

        try:
            self._placement_group = ray.util.get_placement_group(
                self._actor_name,
            )
        except ValueError:
            # ValueError is raised if the placement group does not exist.
            self._placement_group = None

        # Re-fetch initialization proof
        self._allocated_obj_ref = self._actor_handle.is_allocated.remote()

        # Running actor handle already has all info needed, thus successful
        # starting simply means retrieving replica version hash from actor
        if self._is_cross_language:
            self._ready_obj_ref = self._actor_handle.check_health.remote()
        else:
            self._ready_obj_ref = (
                self._actor_handle.initialize_and_get_metadata.remote()
            )

        return True

    def check_ready(self) -> Tuple[ReplicaStartupStatus, Optional[str]]:
        """
        Check if current replica has started by making ray API calls on
        relevant actor / object ref.

        Replica initialization calls __init__(), reconfigure(), and check_health().

        Returns:
            state (ReplicaStartupStatus):
                PENDING_ALLOCATION: replica is waiting for a worker to start
                PENDING_INITIALIZATION: replica initialization hasn't finished.
                FAILED: replica initialization failed.
                SUCCEEDED: replica initialization succeeded.
            error_msg:
                None: for PENDING_ALLOCATION, PENDING_INITIALIZATION or SUCCEEDED states
                str: for FAILED state
        """

        # Check whether the replica has been allocated.
        if self._allocated_obj_ref is None or not check_obj_ref_ready_nowait(
            self._allocated_obj_ref
        ):
            return ReplicaStartupStatus.PENDING_ALLOCATION, None

        if not self._is_cross_language:
            try:
                (
                    self._pid,
                    self._actor_id,
                    self._worker_id,
                    self._node_id,
                    self._node_ip,
                    self._log_file_path,
                ) = ray.get(self._allocated_obj_ref)
            except RayTaskError as e:
                logger.exception(
                    f"Exception in {self._replica_id}, the replica will be stopped."
                )
                return ReplicaStartupStatus.FAILED, str(e.as_instanceof_cause())
            except RuntimeEnvSetupError as e:
                msg = f"Exception when allocating {self._replica_id}: {str(e)}"
                logger.exception(msg)
                return ReplicaStartupStatus.FAILED, msg
            except Exception:
                msg = (
                    f"Exception when allocating {self._replica_id}:\n"
                    + traceback.format_exc()
                )
                logger.exception(msg)
                return ReplicaStartupStatus.FAILED, msg

        # Check whether replica initialization has completed.
        replica_ready = check_obj_ref_ready_nowait(self._ready_obj_ref)
        # In case of deployment constructor failure, ray.get will help to
        # surface exception to each update() cycle.
        if not replica_ready:
            return ReplicaStartupStatus.PENDING_INITIALIZATION, None
        else:
            try:
                # TODO(simon): fully implement reconfigure for Java replicas.
                if self._is_cross_language:
                    return ReplicaStartupStatus.SUCCEEDED, None

                # todo: The replica's userconfig whitch java client created
                #  is different from the controller's userconfig
                if not self._deployment_is_cross_language:
                    # This should only update version if the replica is being recovered.
                    # If this is checking on a replica that is newly started, this
                    # should return a version that is identical to what's already stored
                    (
                        _,
                        self._version,
                        self._initialization_latency_s,
                        self._port,
                    ) = ray.get(self._ready_obj_ref)
            except RayTaskError as e:
                logger.exception(
                    f"Exception in {self._replica_id}, the replica will be stopped."
                )
                # NOTE(zcin): we should use str(e) instead of traceback.format_exc()
                # here because the full details of the error is not displayed properly
                # with traceback.format_exc().
                return ReplicaStartupStatus.FAILED, str(e.as_instanceof_cause())
            except Exception as e:
                logger.exception(
                    f"Exception in {self._replica_id}, the replica will be stopped."
                )
                return ReplicaStartupStatus.FAILED, repr(e)

        return ReplicaStartupStatus.SUCCEEDED, None

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
            handle = ray.get_actor(self._actor_name, namespace=SERVE_NAMESPACE)
            if self._is_cross_language:
                handle = JavaActorHandleProxy(handle)
            self._graceful_shutdown_ref = handle.perform_graceful_shutdown.remote()
        except ValueError:
            # ValueError thrown from ray.get_actor means actor has already been deleted.
            pass

        return self.graceful_shutdown_timeout_s

    def check_stopped(self) -> bool:
        """Check if the actor has exited."""
        try:
            handle = ray.get_actor(self._actor_name, namespace=SERVE_NAMESPACE)
            stopped = check_obj_ref_ready_nowait(self._graceful_shutdown_ref)
            if stopped:
                try:
                    ray.get(self._graceful_shutdown_ref)
                except Exception:
                    logger.exception(
                        "Exception when trying to gracefully shutdown replica:\n"
                        + traceback.format_exc()
                    )

                ray.kill(handle, no_restart=True)
        except ValueError:
            # ValueError thrown from ray.get_actor means actor has already been deleted.
            stopped = True
        finally:
            # Remove the placement group both if the actor has already been deleted or
            # it was just killed above.
            if stopped and self._placement_group is not None:
                ray.util.remove_placement_group(self._placement_group)

        return stopped

    def _check_active_health_check(self) -> ReplicaHealthCheckResponse:
        """Check the active health check (if any).

        self._health_check_ref will be reset to `None` when the active health
        check is deemed to have succeeded or failed. This method *does not*
        start a new health check, that's up to the caller.

        Returns:
            - NONE if there's no active health check, or it hasn't returned
              yet and the timeout is not up.
            - SUCCEEDED if the active health check succeeded.
            - APP_FAILURE if the active health check failed (or didn't return
              before the timeout).
            - ACTOR_CRASHED if the underlying actor crashed.
        """
        if self._health_check_ref is None:
            # There is no outstanding health check.
            response = ReplicaHealthCheckResponse.NONE
        elif check_obj_ref_ready_nowait(self._health_check_ref):
            # Object ref is ready, ray.get it to check for exceptions.
            try:
                ray.get(self._health_check_ref)
                # Health check succeeded without exception.
                response = ReplicaHealthCheckResponse.SUCCEEDED
            except RayActorError:
                # Health check failed due to actor crashing.
                response = ReplicaHealthCheckResponse.ACTOR_CRASHED
            except RayError as e:
                # Health check failed due to application-level exception.
                logger.warning(f"Health check for {self._replica_id} failed: {e}")
                response = ReplicaHealthCheckResponse.APP_FAILURE
        elif time.time() - self._last_health_check_time > self.health_check_timeout_s:
            # Health check hasn't returned and the timeout is up, consider it failed.
            logger.warning(
                "Didn't receive health check response for replica "
                f"{self._replica_id} after "
                f"{self.health_check_timeout_s}s, marking it unhealthy."
            )
            response = ReplicaHealthCheckResponse.APP_FAILURE
        else:
            # Health check hasn't returned and the timeout isn't up yet.
            response = ReplicaHealthCheckResponse.NONE

        if response is not ReplicaHealthCheckResponse.NONE:
            self._health_check_ref = None

        return response

    def _should_start_new_health_check(self) -> bool:
        """Determines if a new health check should be kicked off.

        A health check will be started if:
            1) There is not already an active health check.
            2) It has been more than health_check_period_s since the
               previous health check was *started*.

        This assumes that self._health_check_ref is reset to `None` when an
        active health check succeeds or fails (due to returning or timeout).
        """
        if self._health_check_ref is not None:
            # There's already an active health check.
            return False

        # If there's no active health check, kick off another and reset
        # the timer if it's been long enough since the last health
        # check. Add some randomness to avoid synchronizing across all
        # replicas.
        time_since_last = time.time() - self._last_health_check_time
        randomized_period = self.health_check_period_s * random.uniform(0.9, 1.1)
        return time_since_last > randomized_period

    def check_health(self) -> bool:
        """Check if the actor is healthy.

        self._healthy should *only* be modified in this method.

        This is responsible for:
            1) Checking the outstanding health check (if any).
            2) Determining the replica health based on the health check results.
            3) Kicking off a new health check if needed.
        """
        response: ReplicaHealthCheckResponse = self._check_active_health_check()
        if response is ReplicaHealthCheckResponse.NONE:
            # No info; don't update replica health.
            pass
        elif response is ReplicaHealthCheckResponse.SUCCEEDED:
            # Health check succeeded. Reset the consecutive failure counter
            # and mark the replica healthy.
            if self._consecutive_health_check_failures > 0:
                logger.info(
                    f"{self._replica_id} passed the health check after "
                    f"{self._consecutive_health_check_failures} consecutive failures."
                )
            self._consecutive_health_check_failures = 0
            self._healthy = True
        elif response is ReplicaHealthCheckResponse.APP_FAILURE:
            # Health check failed. If it has failed more than N times in a row,
            # mark the replica unhealthy.
            self._consecutive_health_check_failures += 1
            if (
                self._consecutive_health_check_failures
                >= REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD
            ):
                logger.warning(
                    f"Replica {self._replica_id} failed the health "
                    f"check {self._consecutive_health_check_failures} "
                    "times in a row, marking it unhealthy."
                )
                self._healthy = False
        elif response is ReplicaHealthCheckResponse.ACTOR_CRASHED:
            # Actor crashed, mark the replica unhealthy immediately.
            logger.warning(
                f"Actor for {self._replica_id} crashed, marking "
                "it unhealthy immediately."
            )
            self._healthy = False
        else:
            assert False, f"Unknown response type: {response}."

        if self._should_start_new_health_check():
            self._last_health_check_time = time.time()
            self._health_check_ref = self._actor_handle.check_health.remote()

        return self._healthy

    def force_stop(self):
        """Force the actor to exit without shutting down gracefully."""
        try:
            ray.kill(ray.get_actor(self._actor_name, namespace=SERVE_NAMESPACE))
        except ValueError:
            pass


class DeploymentReplica:
    """Manages state transitions for deployment replicas.

    This is basically a checkpointable lightweight state machine.
    """

    def __init__(
        self,
        replica_id: ReplicaID,
        version: DeploymentVersion,
    ):
        self._replica_id = replica_id
        self._actor = ActorReplicaWrapper(replica_id, version)
        self._start_time = None
        self._actor_details = ReplicaDetails(
            actor_name=replica_id.to_full_id_str(),
            replica_id=self._replica_id.unique_id,
            state=ReplicaState.STARTING,
            start_time_s=0,
        )
        self._multiplexed_model_ids: List = []

    def get_running_replica_info(
        self, cluster_node_info_cache: ClusterNodeInfoCache
    ) -> RunningReplicaInfo:
        return RunningReplicaInfo(
            replica_id=self._replica_id,
            node_id=self.actor_node_id,
            node_ip=self._actor.node_ip,
            availability_zone=cluster_node_info_cache.get_node_az(self.actor_node_id),
            actor_handle=self._actor.actor_handle,
            max_ongoing_requests=self._actor.max_ongoing_requests,
            is_cross_language=self._actor.is_cross_language,
            multiplexed_model_ids=self.multiplexed_model_ids,
            port=self._actor._port,
        )

    def record_multiplexed_model_ids(self, multiplexed_model_ids: List[str]):
        """Record the multiplexed model ids for this replica."""
        self._multiplexed_model_ids = multiplexed_model_ids

    @property
    def multiplexed_model_ids(self) -> List[str]:
        return self._multiplexed_model_ids

    @property
    def actor_details(self) -> ReplicaDetails:
        return self._actor_details

    @property
    def replica_id(self) -> ReplicaID:
        return self._replica_id

    @property
    def deployment_name(self) -> str:
        return self._replica_id.deployment_id.name

    @property
    def app_name(self) -> str:
        return self._replica_id.deployment_id.app_name

    @property
    def version(self):
        return self._actor.version

    @property
    def actor_id(self) -> str:
        return self._actor.actor_id

    @property
    def actor_handle(self) -> ActorHandle:
        return self._actor.actor_handle

    @property
    def actor_node_id(self) -> Optional[str]:
        """Returns the node id of the actor, None if not placed."""
        return self._actor.node_id

    @property
    def initialization_latency_s(self) -> Optional[float]:
        """Returns how long the replica took to initialize."""

        return self._actor.initialization_latency_s

    def start(self, deployment_info: DeploymentInfo) -> ReplicaSchedulingRequest:
        """
        Start a new actor for current DeploymentReplica instance.
        """
        replica_scheduling_request = self._actor.start(deployment_info)
        self._start_time = time.time()
        self.update_actor_details(start_time_s=self._start_time)
        return replica_scheduling_request

    def reconfigure(self, version: DeploymentVersion) -> bool:
        """
        Update replica version. Also, updates the deployment config on the actor
        behind this DeploymentReplica instance if necessary.

        Returns: whether the actor is being updated.
        """
        return self._actor.reconfigure(version)

    def recover(self) -> bool:
        """
        Recover states in DeploymentReplica instance by fetching running actor
        status

        Returns: False if the replica is no longer alive at the time
            when this method is called.
        """
        # If replica is no longer alive
        if not self._actor.recover():
            return False

        self._start_time = time.time()
        self.update_actor_details(start_time_s=self._start_time)
        return True

    def check_started(
        self,
    ) -> Tuple[ReplicaStartupStatus, Optional[str], Optional[float]]:
        """Check if the replica has started. If so, transition to RUNNING.

        Should handle the case where the replica has already stopped.

        Returns:
            status: Most recent state of replica by
                querying actor obj ref
        """
        is_ready = self._actor.check_ready()
        self.update_actor_details(
            pid=self._actor.pid,
            node_id=self._actor.node_id,
            node_ip=self._actor.node_ip,
            actor_id=self._actor.actor_id,
            worker_id=self._actor.worker_id,
            log_file_path=self._actor.log_file_path,
        )

        return is_ready

    def stop(self, graceful: bool = True) -> None:
        """Stop the replica.

        Should handle the case where the replica is already stopped.
        """
        state = self._actor_details.state
        logger.info(
            f"Stopping {self.replica_id} (currently {state}).",
            extra={"log_to_stderr": False},
        )
        timeout_s = self._actor.graceful_stop()
        if not graceful:
            timeout_s = 0
        self._shutdown_deadline = time.time() + timeout_s

    def check_stopped(self) -> bool:
        """Check if the replica has finished stopping."""
        if self._actor.check_stopped():
            return True

        timeout_passed = time.time() >= self._shutdown_deadline
        if timeout_passed:
            # Graceful period passed, kill it forcefully.
            # This will be called repeatedly until the replica shuts down.
            logger.info(
                f"{self.replica_id} did not shut down after grace "
                "period, force-killing it. "
            )

            self._actor.force_stop()
        return False

    def check_health(self) -> bool:
        """Check if the replica is healthy.

        Returns `True` if the replica is healthy, else `False`.
        """
        return self._actor.check_health()

    def update_state(self, state: ReplicaState) -> None:
        """Updates state in actor details."""
        self.update_actor_details(state=state)

    def update_actor_details(self, **kwargs) -> None:
        details_kwargs = self._actor_details.dict()
        details_kwargs.update(kwargs)
        self._actor_details = ReplicaDetails(**details_kwargs)

    def resource_requirements(self) -> Tuple[str, str]:
        """Returns required and currently available resources.

        Only resources with nonzero requirements will be included in the
        required dict and only resources in the required dict will be
        included in the available dict (filtered for relevance).
        """
        if self._actor.actor_resources is None:
            return "UNKNOWN", "UNKNOWN"

        if self._actor.placement_group_bundles is not None:
            required = self._actor.placement_group_bundles
        else:
            required = {
                k: v
                for k, v in self._actor.actor_resources.items()
                if v is not None and v > 0
            }

        available = {
            k: v for k, v in self._actor.available_resources.items() if k in required
        }

        # Use json.dumps() instead of str() here to avoid double-quoting keys
        # when dumping these objects. See
        # https://github.com/ray-project/ray/issues/26210 for the issue.
        return json.dumps(required), json.dumps(available)


class ReplicaStateContainer:
    """Container for mapping ReplicaStates to lists of DeploymentReplicas."""

    def __init__(self):
        self._replicas: Dict[ReplicaState, List[DeploymentReplica]] = defaultdict(list)

    def add(self, state: ReplicaState, replica: DeploymentReplica):
        """Add the provided replica under the provided state.

        Args:
            state: state to add the replica under.
            replica: replica to add.
        """
        assert isinstance(state, ReplicaState), f"Type: {type(state)}"
        replica.update_state(state)
        self._replicas[state].append(replica)

    def get(
        self, states: Optional[List[ReplicaState]] = None
    ) -> List[DeploymentReplica]:
        """Get all replicas of the given states.

        This does not remove them from the container. Replicas are returned
        in order of state as passed in.

        Args:
            states: states to consider. If not specified, all replicas
                are considered.
        """
        if states is None:
            states = ALL_REPLICA_STATES

        assert isinstance(states, list)

        return sum((self._replicas[state] for state in states), [])

    def pop(
        self,
        exclude_version: Optional[DeploymentVersion] = None,
        states: Optional[List[ReplicaState]] = None,
        max_replicas: Optional[int] = math.inf,
    ) -> List[DeploymentReplica]:
        """Get and remove all replicas of the given states.

        This removes the replicas from the container. Replicas are returned
        in order of state as passed in.

        Args:
            exclude_version: if specified, replicas of the
                provided version will *not* be removed.
            states: states to consider. If not specified, all replicas
                are considered.
            max_replicas: max number of replicas to return. If not
                specified, will pop all replicas matching the criteria.
        """
        if states is None:
            states = ALL_REPLICA_STATES

        assert exclude_version is None or isinstance(exclude_version, DeploymentVersion)
        assert isinstance(states, list)

        replicas = []
        for state in states:
            popped = []
            remaining = []

            for replica in self._replicas[state]:
                if len(replicas) + len(popped) == max_replicas:
                    remaining.append(replica)
                elif exclude_version is not None and replica.version == exclude_version:
                    remaining.append(replica)
                else:
                    popped.append(replica)

            self._replicas[state] = remaining
            replicas.extend(popped)

        return replicas

    def count(
        self,
        exclude_version: Optional[DeploymentVersion] = None,
        version: Optional[DeploymentVersion] = None,
        states: Optional[List[ReplicaState]] = None,
    ):
        """Get the total count of replicas of the given states.

        Args:
            exclude_version: version to exclude. If not
                specified, all versions are considered.
            version: version to filter to. If not specified,
                all versions are considered.
            states: states to consider. If not specified, all replicas
                are considered.
        """
        if states is None:
            states = ALL_REPLICA_STATES
        assert isinstance(states, list)
        assert exclude_version is None or isinstance(exclude_version, DeploymentVersion)
        assert version is None or isinstance(version, DeploymentVersion)
        if exclude_version is None and version is None:
            return sum(len(self._replicas[state]) for state in states)
        elif exclude_version is None and version is not None:
            return sum(
                len(list(filter(lambda r: r.version == version, self._replicas[state])))
                for state in states
            )
        elif exclude_version is not None and version is None:
            return sum(
                len(
                    list(
                        filter(
                            lambda r: r.version != exclude_version,
                            self._replicas[state],
                        )
                    )
                )
                for state in states
            )
        else:
            raise ValueError(
                "Only one of `version` or `exclude_version` may be provided."
            )

    def __str__(self):
        return str(self._replicas)

    def __repr__(self):
        return repr(self._replicas)


class DeploymentState:
    """Manages the target state and replicas for a single deployment."""

    FORCE_STOP_UNHEALTHY_REPLICAS = RAY_SERVE_FORCE_STOP_UNHEALTHY_REPLICAS

    def __init__(
        self,
        id: DeploymentID,
        long_poll_host: LongPollHost,
        deployment_scheduler: DeploymentScheduler,
        cluster_node_info_cache: ClusterNodeInfoCache,
        autoscaling_state_manager: AutoscalingStateManager,
        _save_checkpoint_func: Callable,
    ):
        self._id = id
        self._long_poll_host: LongPollHost = long_poll_host
        self._deployment_scheduler = deployment_scheduler
        self._cluster_node_info_cache = cluster_node_info_cache
        self._autoscaling_state_manager = autoscaling_state_manager
        self._save_checkpoint_func = _save_checkpoint_func

        # Each time we set a new deployment goal, we're trying to save new
        # DeploymentInfo and bring current deployment to meet new status.
        self._target_state: DeploymentTargetState = DeploymentTargetState.default()
        self._prev_startup_warning: float = time.time()
        # Exponential backoff when retrying a consistently failing deployment
        self._last_retry: float = 0.0
        self._backoff_time_s: int = 1
        self._replica_constructor_retry_counter: int = 0
        self._replica_constructor_error_msg: Optional[str] = None
        self._replicas: ReplicaStateContainer = ReplicaStateContainer()
        self._curr_status_info: DeploymentStatusInfo = DeploymentStatusInfo(
            self._id.name,
            DeploymentStatus.UPDATING,
            DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
        )

        self.replica_average_ongoing_requests: Dict[str, float] = dict()

        self.health_check_gauge = metrics.Gauge(
            "serve_deployment_replica_healthy",
            description=(
                "Tracks whether this deployment replica is healthy. 1 means "
                "healthy, 0 means unhealthy."
            ),
            tag_keys=("deployment", "replica", "application"),
        )

        # Whether the multiplexed model ids have been updated since the last
        # time we checked.
        self._multiplexed_model_ids_updated = False

        self._last_broadcasted_running_replica_infos: List[RunningReplicaInfo] = []
        self._last_broadcasted_deployment_config = None

    def should_autoscale(self) -> bool:
        """
        Check if the deployment is under autoscaling
        """
        return self._id in self._autoscaling_state_manager._autoscaling_states

    def get_checkpoint_data(self) -> DeploymentTargetState:
        """
        Return deployment's target state submitted by user's deployment call.
        Should be persisted and outlive current ray cluster.
        """
        return self._target_state

    def recover_target_state_from_checkpoint(
        self, target_state_checkpoint: DeploymentTargetState
    ):
        logger.info(f"Recovering target state for {self._id} from checkpoint.")
        self._target_state = target_state_checkpoint
        self._deployment_scheduler.on_deployment_deployed(
            self._id, self._target_state.info.replica_config
        )
        if self._target_state.info.deployment_config.autoscaling_config:
            self._autoscaling_state_manager.register_deployment(
                self._id,
                self._target_state.info,
                self._target_state.target_num_replicas,
            )

    def recover_current_state_from_replica_actor_names(
        self, replica_actor_names: List[str]
    ):
        """Recover deployment state from live replica actors found in the cluster."""

        assert self._target_state is not None, (
            "Target state should be recovered successfully first before "
            "recovering current state from replica actor names."
        )
        logger.info(
            f"Recovering current state for {self._id} "
            f"from {len(replica_actor_names)} live actors."
        )
        # All current states use default value, only attach running replicas.
        for replica_actor_name in replica_actor_names:
            replica_id = ReplicaID.from_full_id_str(replica_actor_name)
            new_deployment_replica = DeploymentReplica(
                replica_id,
                self._target_state.version,
            )
            # If replica is no longer alive, simply don't add it to the
            # deployment state manager to track.
            if not new_deployment_replica.recover():
                logger.warning(f"{replica_id} died before controller could recover it.")
                continue

            self._replicas.add(ReplicaState.RECOVERING, new_deployment_replica)
            self._deployment_scheduler.on_replica_recovering(replica_id)
            logger.debug(f"RECOVERING {replica_id}.")

        # TODO(jiaodong): this currently halts all traffic in the cluster
        # briefly because we will broadcast a replica update with everything in
        # RECOVERING. We should have a grace period where we recover the state
        # of the replicas before doing this update.

    @property
    def target_info(self) -> DeploymentInfo:
        return self._target_state.info

    @property
    def target_version(self) -> DeploymentVersion:
        return self._target_state.version

    @property
    def target_num_replicas(self) -> int:
        return self._target_state.target_num_replicas

    @property
    def curr_status_info(self) -> DeploymentStatusInfo:
        return self._curr_status_info

    @property
    def deployment_name(self) -> str:
        return self._id.name

    @property
    def app_name(self) -> str:
        return self._id.app_name

    @property
    def _failed_to_start_threshold(self) -> int:
        return min(
            MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT,
            self._target_state.target_num_replicas * 3,
        )

    def get_alive_replica_actor_ids(self) -> Set[str]:
        return {replica.actor_id for replica in self._replicas.get()}

    def get_running_replica_ids(self) -> List[ReplicaID]:
        return [
            replica.replica_id
            for replica in self._replicas.get(
                [ReplicaState.RUNNING, ReplicaState.PENDING_MIGRATION]
            )
        ]

    def get_running_replica_infos(self) -> List[RunningReplicaInfo]:
        return [
            replica.get_running_replica_info(self._cluster_node_info_cache)
            for replica in self._replicas.get(
                [ReplicaState.RUNNING, ReplicaState.PENDING_MIGRATION]
            )
        ]

    def get_num_running_replicas(self, version: DeploymentVersion = None) -> int:
        return self._replicas.count(states=[ReplicaState.RUNNING], version=version)

    def get_active_node_ids(self) -> Set[str]:
        """Get the node ids of all running replicas in this deployment.

        This is used to determine which node has replicas. Only nodes with replicas and
        head node should have active proxies.
        """
        active_states = [
            ReplicaState.STARTING,
            ReplicaState.UPDATING,
            ReplicaState.RECOVERING,
            ReplicaState.RUNNING,
            # NOTE(zcin): We still want a proxy to run on a draining
            # node before all the replicas are migrated.
            ReplicaState.PENDING_MIGRATION,
        ]
        return {
            replica.actor_node_id
            for replica in self._replicas.get(active_states)
            if replica.actor_node_id is not None
        }

    def list_replica_details(self) -> List[ReplicaDetails]:
        return [replica.actor_details for replica in self._replicas.get()]

    def broadcast_running_replicas_if_changed(self) -> None:
        """Broadcasts the set of running replicas over long poll if it has changed.

        Keeps an in-memory record of the last set of running replicas that was broadcast
        to determine if it has changed.

        The set will also be broadcast if any replicas have an updated set of
        multiplexed model IDs.
        """
        running_replica_infos = self.get_running_replica_infos()
        if (
            set(self._last_broadcasted_running_replica_infos)
            == set(running_replica_infos)
            and not self._multiplexed_model_ids_updated
        ):
            return

        self._long_poll_host.notify_changed(
            {
                (LongPollNamespace.RUNNING_REPLICAS, self._id): running_replica_infos,
                # NOTE(zcin): notify changed for Java routers. Since Java only
                # supports 1.x API, there is no concept of applications in Java,
                # so the key should remain a string describing the deployment
                # name. If there are no Java routers, this is a no-op.
                (
                    LongPollNamespace.RUNNING_REPLICAS,
                    self._id.name,
                ): running_replica_infos,
            }
        )
        self._last_broadcasted_running_replica_infos = running_replica_infos
        self._multiplexed_model_ids_updated = False

    def broadcast_deployment_config_if_changed(self) -> None:
        """Broadcasts the deployment config over long poll if it has changed.

        Keeps an in-memory record of the last config that was broadcast to determine
        if it has changed.
        """
        current_deployment_config = self._target_state.info.deployment_config
        if self._last_broadcasted_deployment_config == current_deployment_config:
            return

        self._long_poll_host.notify_changed(
            {(LongPollNamespace.DEPLOYMENT_CONFIG, self._id): current_deployment_config}
        )

        self._last_broadcasted_deployment_config = current_deployment_config

    def _set_target_state_deleting(self) -> None:
        """Set the target state for the deployment to be deleted."""

        # We must write ahead the target state in case of GCS failure (we don't
        # want to set the target state, then fail because we can't checkpoint it).
        target_state = DeploymentTargetState.create(
            info=self._target_state.info,
            target_num_replicas=0,
            deleting=True,
        )
        self._save_checkpoint_func(writeahead_checkpoints={self._id: target_state})

        self._target_state = target_state
        self._curr_status_info = self._curr_status_info.handle_transition(
            trigger=DeploymentStatusInternalTrigger.DELETE
        )
        logger.info(
            f"Deleting {self._id}",
            extra={"log_to_stderr": False},
        )

    def _set_target_state(
        self,
        target_info: DeploymentInfo,
        target_num_replicas: int,
    ) -> None:
        """Set the target state for the deployment to the provided info.

        Args:
            target_info: The info with which to set the target state.
            target_num_replicas: The number of replicas that this deployment
                should attempt to run.
            status_trigger: The driver that triggered this change of state.
        """

        # We must write ahead the target state in case of GCS failure (we don't
        # want to set the target state, then fail because we can't checkpoint it).
        new_target_state = DeploymentTargetState.create(
            target_info, target_num_replicas, deleting=False
        )
        self._save_checkpoint_func(writeahead_checkpoints={self._id: new_target_state})

        if self._target_state.version == new_target_state.version:
            # Record either num replica or autoscaling config lightweight update
            if (
                self._target_state.version.deployment_config.autoscaling_config
                != new_target_state.version.deployment_config.autoscaling_config
            ):
                ServeUsageTag.AUTOSCALING_CONFIG_LIGHTWEIGHT_UPDATED.record("True")
            elif (
                self._target_state.version.deployment_config.num_replicas
                != new_target_state.version.deployment_config.num_replicas
            ):
                ServeUsageTag.NUM_REPLICAS_LIGHTWEIGHT_UPDATED.record("True")

        self._target_state = new_target_state

    def deploy(self, deployment_info: DeploymentInfo) -> bool:
        """Deploy the deployment.

        If the deployment already exists with the same version, config,
        target_capacity, and target_capacity_direction,
        this method returns False.

        Returns:
            bool: Whether or not the deployment is being updated.
        """

        curr_deployment_info = self._target_state.info
        if curr_deployment_info is not None:
            # Redeploying should not reset the deployment's start time.
            if not self._target_state.deleting:
                deployment_info.start_time_ms = curr_deployment_info.start_time_ms

            deployment_settings_changed = (
                self._target_state.deleting
                or curr_deployment_info.deployment_config
                != deployment_info.deployment_config
                or curr_deployment_info.replica_config.ray_actor_options
                != deployment_info.replica_config.ray_actor_options
                or deployment_info.version is None
                or curr_deployment_info.version != deployment_info.version
            )
            target_capacity_changed = (
                curr_deployment_info.target_capacity != deployment_info.target_capacity
                or curr_deployment_info.target_capacity_direction
                != deployment_info.target_capacity_direction
            )
        else:
            deployment_settings_changed = True
            target_capacity_changed = True

        # Exit early if the deployment info hasn't changed. Ensures this method
        # is idempotent.
        if not deployment_settings_changed and not target_capacity_changed:
            return False

        if deployment_info.deployment_config.autoscaling_config:
            target_num_replicas = self._autoscaling_state_manager.register_deployment(
                self._id, deployment_info, self._target_state.target_num_replicas
            )
        else:
            self._autoscaling_state_manager.deregister_deployment(self._id)
            target_num_replicas = get_capacity_adjusted_num_replicas(
                deployment_info.deployment_config.num_replicas,
                deployment_info.target_capacity,
            )

        old_target_state = self._target_state
        self._set_target_state(deployment_info, target_num_replicas=target_num_replicas)
        self._deployment_scheduler.on_deployment_deployed(
            self._id, deployment_info.replica_config
        )

        # Determine if the updated target state simply scales the current state.
        if self._target_state.is_scaled_copy_of(old_target_state):
            old_num = old_target_state.target_num_replicas
            new_num = self._target_state.target_num_replicas

            if new_num > old_num:
                self._curr_status_info = self._curr_status_info.handle_transition(
                    trigger=DeploymentStatusInternalTrigger.MANUALLY_INCREASE_NUM_REPLICAS,  # noqa: E501
                    message=f"Upscaling from {old_num} to {new_num} replicas.",
                )
            elif new_num < old_num:
                self._curr_status_info = self._curr_status_info.handle_transition(
                    trigger=DeploymentStatusInternalTrigger.MANUALLY_DECREASE_NUM_REPLICAS,  # noqa: E501
                    message=f"Downscaling from {old_num} to {new_num} replicas.",
                )
        else:
            # Otherwise, the deployment configuration has actually been updated.
            self._curr_status_info = self._curr_status_info.handle_transition(
                trigger=DeploymentStatusInternalTrigger.CONFIG_UPDATE
            )

        logger.info(
            f"Deploying new version of {self._id} "
            f"(initial target replicas: {target_num_replicas})."
        )
        self._replica_constructor_retry_counter = 0
        self._backoff_time_s = 1
        return True

    def autoscale(self) -> int:
        """Autoscale the deployment based on metrics."""

        if self._target_state.deleting:
            return

        decision_num_replicas = self._autoscaling_state_manager.get_target_num_replicas(
            deployment_id=self._id,
            curr_target_num_replicas=self._target_state.target_num_replicas,
        )

        if (
            decision_num_replicas is None
            or decision_num_replicas == self._target_state.target_num_replicas
        ):
            return

        new_info = copy(self._target_state.info)
        new_info.version = self._target_state.version.code_version

        old_num = self._target_state.target_num_replicas
        self._set_target_state(new_info, decision_num_replicas)

        # The deployment should only transition to UPSCALING/DOWNSCALING
        # if it's within the autoscaling bounds
        if not self._autoscaling_state_manager.is_within_bounds(
            self._id,
            self._replicas.count(
                states=[ReplicaState.RUNNING], version=self._target_state.version
            ),
        ):
            return

        curr_stats_str = (
            f"Current ongoing requests: "
            f"{self._autoscaling_state_manager.get_total_num_requests(self._id):.2f}, "
            f"current running replicas: "
            f"{self._replicas.count(states=[ReplicaState.RUNNING])}."
        )
        new_num = self._target_state.target_num_replicas
        if new_num > old_num:
            logger.info(
                f"Upscaling {self._id} from {old_num} to {new_num} replicas. "
                f"{curr_stats_str}"
            )
            self._curr_status_info = self._curr_status_info.handle_transition(
                trigger=DeploymentStatusInternalTrigger.AUTOSCALE_UP,
                message=f"Upscaling from {old_num} to {new_num} replicas.",
            )
        elif new_num < old_num:
            logger.info(
                f"Downscaling {self._id} from {old_num} to {new_num} replicas. "
                f"{curr_stats_str}"
            )
            self._curr_status_info = self._curr_status_info.handle_transition(
                trigger=DeploymentStatusInternalTrigger.AUTOSCALE_DOWN,
                message=f"Downscaling from {old_num} to {new_num} replicas.",
            )

    def delete(self) -> None:
        if not self._target_state.deleting:
            self._set_target_state_deleting()

    def _stop_or_update_outdated_version_replicas(self, max_to_stop=math.inf) -> bool:
        """Stop or update replicas with outdated versions.

        Stop replicas with versions that require the actor to be restarted, and
        reconfigure replicas that require refreshing deployment config values.

        Args:
            max_to_stop: max number of replicas to stop, by default,
            it will stop all replicas with outdated version.
        """
        replicas_to_update = self._replicas.pop(
            exclude_version=self._target_state.version,
            states=[
                ReplicaState.STARTING,
                ReplicaState.PENDING_MIGRATION,
                ReplicaState.RUNNING,
            ],
        )
        replicas_changed = False
        code_version_changes = 0
        reconfigure_changes = 0
        for replica in replicas_to_update:
            if (code_version_changes + reconfigure_changes) >= max_to_stop:
                self._replicas.add(replica.actor_details.state, replica)
            # If the new version requires the actors to be restarted, stop the replica.
            # A new one with the correct version will be started later as part of the
            # normal scale-up process.
            elif replica.version.requires_actor_restart(self._target_state.version):
                code_version_changes += 1
                # If the replica is still `STARTING`, we don't need to go through the
                # graceful stop period.
                graceful_stop = replica.actor_details.state == ReplicaState.RUNNING
                self._stop_replica(replica, graceful_stop=graceful_stop)
                replicas_changed = True
            # Otherwise, only lightweight options in deployment config is a mismatch, so
            # we update it dynamically without restarting the replica.
            elif replica.actor_details.state == ReplicaState.RUNNING:
                reconfigure_changes += 1
                if replica.version.requires_long_poll_broadcast(
                    self._target_state.version
                ):
                    replicas_changed = True
                actor_updating = replica.reconfigure(self._target_state.version)
                if actor_updating:
                    self._replicas.add(ReplicaState.UPDATING, replica)
                else:
                    self._replicas.add(ReplicaState.RUNNING, replica)
            # We don't allow going from STARTING, PENDING_MIGRATION to UPDATING.
            else:
                self._replicas.add(replica.actor_details.state, replica)

        if code_version_changes > 0:
            logger.info(
                f"Stopping {code_version_changes} replicas of {self._id} "
                "with outdated versions."
            )

        if reconfigure_changes > 0:
            logger.info(
                f"Updating {reconfigure_changes} replicas of {self._id} "
                "with outdated deployment configs."
            )
            # Record user config lightweight update
            ServeUsageTag.USER_CONFIG_LIGHTWEIGHT_UPDATED.record("True")

        return replicas_changed

    def _check_and_stop_outdated_version_replicas(self) -> bool:
        """Stops replicas with outdated versions to implement rolling updates.

        This includes both explicit code version updates and changes to the
        user_config.

        Returns whether any replicas were stopped.
        """
        # Short circuit if target replicas is 0 (the deployment is being
        # deleted) because this will be handled in the main loop.
        if self._target_state.target_num_replicas == 0:
            return False

        # We include STARTING and UPDATING replicas here
        # because if there are replicas still pending startup, we may as well
        # terminate them and start new version replicas instead.
        old_running_replicas = self._replicas.count(
            exclude_version=self._target_state.version,
            states=[
                ReplicaState.STARTING,
                ReplicaState.UPDATING,
                ReplicaState.RUNNING,
            ],
        )
        old_stopping_replicas = self._replicas.count(
            exclude_version=self._target_state.version, states=[ReplicaState.STOPPING]
        )
        new_running_replicas = self._replicas.count(
            version=self._target_state.version, states=[ReplicaState.RUNNING]
        )

        # If the deployment is currently scaling down, let the scale down
        # complete before doing a rolling update.
        if (
            self._target_state.target_num_replicas
            < old_running_replicas + old_stopping_replicas
        ):
            return False

        # The number of replicas that are currently in transition between
        # an old version and the new version. Note that we cannot directly
        # count the number of stopping replicas because once replicas finish
        # stopping, they are removed from the data structure.
        pending_replicas = (
            self._target_state.target_num_replicas
            - new_running_replicas
            - old_running_replicas
        )

        # Maximum number of replicas that can be updating at any given time.
        # There should never be more than rollout_size old replicas stopping
        # or rollout_size new replicas starting.
        rollout_size = max(int(0.2 * self._target_state.target_num_replicas), 1)
        max_to_stop = max(rollout_size - pending_replicas, 0)

        return self._stop_or_update_outdated_version_replicas(max_to_stop)

    def scale_deployment_replicas(
        self,
    ) -> Tuple[List[ReplicaSchedulingRequest], DeploymentDownscaleRequest]:
        """Scale the given deployment to the number of replicas."""

        assert (
            self._target_state.target_num_replicas >= 0
        ), "Target number of replicas must be greater than or equal to 0."

        upscale = []
        downscale = None

        self._check_and_stop_outdated_version_replicas()

        current_replicas = self._replicas.count(
            states=[ReplicaState.STARTING, ReplicaState.UPDATING, ReplicaState.RUNNING]
        )
        recovering_replicas = self._replicas.count(states=[ReplicaState.RECOVERING])

        delta_replicas = (
            self._target_state.target_num_replicas
            - current_replicas
            - recovering_replicas
        )
        if delta_replicas == 0:
            return (upscale, downscale)

        elif delta_replicas > 0:
            to_add = delta_replicas
            if not RAY_SERVE_EAGERLY_START_REPLACEMENT_REPLICAS:
                # Don't ever exceed target_num_replicas.
                stopping_replicas = self._replicas.count(states=[ReplicaState.STOPPING])
                to_add = max(delta_replicas - stopping_replicas, 0)

            if to_add > 0:
                # Exponential backoff
                if (
                    self._replica_constructor_retry_counter
                    >= self._failed_to_start_threshold
                ):
                    # Wait 1, 2, 4, ... seconds before consecutive retries, with random
                    # offset added to avoid synchronization
                    if (
                        time.time() - self._last_retry
                        < self._backoff_time_s + random.uniform(0, 3)
                    ):
                        return upscale, downscale

                self._last_retry = time.time()
                added_replicas = f"{to_add} replica{'s' if to_add > 1 else ''}"
                logger.info(f"Adding {added_replicas} to {self._id}.")
                for _ in range(to_add):
                    replica_id = ReplicaID(get_random_string(), deployment_id=self._id)
                    new_deployment_replica = DeploymentReplica(
                        replica_id,
                        self._target_state.version,
                    )
                    upscale.append(
                        new_deployment_replica.start(self._target_state.info)
                    )

                    self._replicas.add(ReplicaState.STARTING, new_deployment_replica)

        elif delta_replicas < 0:
            to_remove = -delta_replicas
            removed_replicas = f"{to_remove} replica{'s' if to_remove > 1 else ''}"
            logger.info(f"Removing {removed_replicas} from {self._id}.")
            downscale = DeploymentDownscaleRequest(
                deployment_id=self._id, num_to_stop=to_remove
            )

        return upscale, downscale

    def check_curr_status(self) -> Tuple[bool, bool]:
        """Check the current deployment status.

        Checks the difference between the target vs. running replica count for
        the target version.

        This will update the current deployment status depending on the state
        of the replicas.

        Returns (deleted, any_replicas_recovering).
        """
        # TODO(edoakes): we could make this more efficient in steady-state by
        # having a "healthy" flag that gets flipped if an update or replica
        # failure happens.

        target_version = self._target_state.version

        any_replicas_recovering = (
            self._replicas.count(states=[ReplicaState.RECOVERING]) > 0
        )
        all_running_replica_cnt = self._replicas.count(states=[ReplicaState.RUNNING])
        running_at_target_version_replica_cnt = self._replicas.count(
            states=[ReplicaState.RUNNING], version=target_version
        )

        failed_to_start_count = self._replica_constructor_retry_counter

        # Got to make a call to complete current deploy() goal after
        # start failure threshold reached, while we might still have
        # pending replicas in current goal.
        if (
            failed_to_start_count >= self._failed_to_start_threshold
            and self._failed_to_start_threshold != 0
        ):
            if running_at_target_version_replica_cnt > 0:
                # At least one RUNNING replica at target state, partial
                # success; We can stop tracking constructor failures and
                # leave it to the controller to fully scale to target
                # number of replicas and only return as completed once
                # reached target replica count
                self._replica_constructor_retry_counter = -1
            else:
                self._curr_status_info = self._curr_status_info.handle_transition(
                    trigger=DeploymentStatusInternalTrigger.REPLICA_STARTUP_FAILED,
                    message=(
                        f"The deployment failed to start {failed_to_start_count} times "
                        "in a row. This may be due to a problem with its "
                        "constructor or initial health check failing. See "
                        "controller logs for details. Retrying after "
                        f"{self._backoff_time_s} seconds. Error:\n"
                        f"{self._replica_constructor_error_msg}"
                    ),
                )
                return False, any_replicas_recovering

        # If we have pending ops, the current goal is *not* ready.
        if (
            self._replicas.count(
                states=[
                    ReplicaState.STARTING,
                    ReplicaState.UPDATING,
                    ReplicaState.RECOVERING,
                    ReplicaState.STOPPING,
                ]
            )
            == 0
        ):
            # Check for deleting and a non-zero number of deployments.
            if self._target_state.deleting and all_running_replica_cnt == 0:
                return True, any_replicas_recovering

            if (
                self._target_state.target_num_replicas
                == running_at_target_version_replica_cnt
                and running_at_target_version_replica_cnt == all_running_replica_cnt
            ):
                self._curr_status_info = self._curr_status_info.handle_transition(
                    trigger=DeploymentStatusInternalTrigger.HEALTHY
                )
                return False, any_replicas_recovering

        return False, any_replicas_recovering

    def _check_startup_replicas(
        self, original_state: ReplicaState, stop_on_slow=False
    ) -> List[Tuple[DeploymentReplica, ReplicaStartupStatus]]:
        """
        Common helper function for startup actions tracking and status
        transition: STARTING, UPDATING and RECOVERING.

        Args:
            stop_on_slow: If we consider a replica failed upon observing it's
                slow to reach running state.
        """
        slow_replicas = []
        replicas_failed = False
        for replica in self._replicas.pop(states=[original_state]):
            start_status, error_msg = replica.check_started()
            if start_status == ReplicaStartupStatus.SUCCEEDED:
                # This replica should be now be added to handle's replica
                # set.
                self._replicas.add(ReplicaState.RUNNING, replica)
                self._deployment_scheduler.on_replica_running(
                    replica.replica_id, replica.actor_node_id
                )

                # Log the startup latency.
                e2e_replica_start_latency = time.time() - replica._start_time
                replica_startup_message = (
                    f"{replica.replica_id} started successfully "
                    f"on node '{replica.actor_node_id}' after "
                    f"{e2e_replica_start_latency:.1f}s."
                )
                if replica.initialization_latency_s is not None:
                    # This condition should always be True. The initialization
                    # latency is only None before the replica has initialized.
                    replica_startup_message += (
                        " Replica constructor, "
                        "reconfigure method, and initial health check took "
                        f"{replica.initialization_latency_s:.1f}s."
                    )
                logger.info(replica_startup_message, extra={"log_to_stderr": False})

            elif start_status == ReplicaStartupStatus.FAILED:
                # Replica reconfigure (deploy / upgrade) failed
                replicas_failed = True
                self.record_replica_startup_failure(error_msg)
                self._stop_replica(replica)
            elif start_status in [
                ReplicaStartupStatus.PENDING_ALLOCATION,
                ReplicaStartupStatus.PENDING_INITIALIZATION,
            ]:
                is_slow = time.time() - replica._start_time > SLOW_STARTUP_WARNING_S
                if is_slow:
                    slow_replicas.append((replica, start_status))

                # Does it make sense to stop replicas in PENDING_ALLOCATION
                # state?
                if is_slow and stop_on_slow:
                    self._stop_replica(replica, graceful_stop=False)
                else:
                    self._replicas.add(original_state, replica)

        if replicas_failed:
            self.update_replica_startup_backoff_time()

        return slow_replicas

    def record_replica_startup_failure(self, error_msg: str):
        """Record that a replica failed to start."""

        if self._replica_constructor_retry_counter >= 0:
            # Increase startup failure counter if we're tracking it
            self._replica_constructor_retry_counter += 1
            self._replica_constructor_error_msg = error_msg

            retrying_msg = "Retrying"
            if self._failed_to_start_threshold != 0:
                remaining_retries = (
                    self._failed_to_start_threshold
                    - self._replica_constructor_retry_counter
                )
                retrying_msg += f" {remaining_retries} more time(s)"

            message = (
                f"A replica failed to start with exception. {retrying_msg}. Error:\n"
                f"{error_msg}"
            )
            self._curr_status_info = self._curr_status_info.update_message(message)

    def update_replica_startup_backoff_time(self):
        """Updates the replica startup backoff time."""

        # If replicas have failed enough times, execute exponential backoff
        # Wait 1, 2, 4, ... seconds before consecutive retries (or use a custom
        # backoff factor by setting EXPONENTIAL_BACKOFF_FACTOR)
        if self._replica_constructor_retry_counter > self._failed_to_start_threshold:
            self._backoff_time_s = min(
                EXPONENTIAL_BACKOFF_FACTOR * self._backoff_time_s, MAX_BACKOFF_TIME_S
            )

    def stop_replicas(self, replicas_to_stop) -> None:
        for replica in self._replicas.pop():
            if replica.replica_id in replicas_to_stop:
                self._stop_replica(replica)
            else:
                self._replicas.add(replica.actor_details.state, replica)

    def _stop_replica(self, replica: DeploymentReplica, graceful_stop=True):
        """Stop replica
        1. Stop the replica.
        2. Change the replica into stopping state.
        3. Set the health replica stats to 0.
        """
        logger.debug(f"Adding STOPPING to replica: {replica.replica_id}.")
        replica.stop(graceful=graceful_stop)
        self._replicas.add(ReplicaState.STOPPING, replica)
        self._deployment_scheduler.on_replica_stopping(replica.replica_id)
        self.health_check_gauge.set(
            0,
            tags={
                "deployment": self.deployment_name,
                "replica": replica.replica_id.unique_id,
                "application": self.app_name,
            },
        )

    def check_and_update_replicas(self):
        """
        Check current state of all DeploymentReplica being tracked, and compare
        with state container from previous update() cycle to see if any state
        transition happened.
        """

        for replica in self._replicas.pop(
            states=[ReplicaState.RUNNING, ReplicaState.PENDING_MIGRATION]
        ):
            if replica.check_health():
                self._replicas.add(replica.actor_details.state, replica)
                self.health_check_gauge.set(
                    1,
                    tags={
                        "deployment": self.deployment_name,
                        "replica": replica.replica_id.unique_id,
                        "application": self.app_name,
                    },
                )
            else:
                logger.warning(
                    f"Replica {replica.replica_id} failed health check, stopping it."
                )
                self.health_check_gauge.set(
                    0,
                    tags={
                        "deployment": self.deployment_name,
                        "replica": replica.replica_id.unique_id,
                        "application": self.app_name,
                    },
                )
                self._stop_replica(
                    replica, graceful_stop=not self.FORCE_STOP_UNHEALTHY_REPLICAS
                )
                # If this is a replica of the target version, the deployment
                # enters the "UNHEALTHY" status until the replica is
                # recovered or a new deploy happens.
                if replica.version == self._target_state.version:
                    self._curr_status_info = self._curr_status_info.handle_transition(
                        trigger=DeploymentStatusInternalTrigger.HEALTH_CHECK_FAILED,
                        message="A replica's health check failed. This "
                        "deployment will be UNHEALTHY until the replica "
                        "recovers or a new deploy happens.",
                    )

        slow_start_replicas = []
        slow_start = self._check_startup_replicas(ReplicaState.STARTING)
        slow_update = self._check_startup_replicas(ReplicaState.UPDATING)
        slow_recover = self._check_startup_replicas(
            ReplicaState.RECOVERING, stop_on_slow=True
        )

        slow_start_replicas = slow_start + slow_update + slow_recover

        if (
            len(slow_start_replicas)
            and time.time() - self._prev_startup_warning > SLOW_STARTUP_WARNING_PERIOD_S
        ):
            pending_allocation = []
            pending_initialization = []

            for replica, startup_status in slow_start_replicas:
                if startup_status == ReplicaStartupStatus.PENDING_ALLOCATION:
                    pending_allocation.append(replica)
                if startup_status == ReplicaStartupStatus.PENDING_INITIALIZATION:
                    pending_initialization.append(replica)

            if len(pending_allocation) > 0:
                required, available = pending_allocation[0].resource_requirements()
                message = (
                    f"Deployment '{self.deployment_name}' in application "
                    f"'{self.app_name}' has {len(pending_allocation)} replicas that "
                    f"have taken more than {SLOW_STARTUP_WARNING_S}s to be scheduled.\n"
                    "This may be due to waiting for the cluster to auto-scale or for a "
                    "runtime environment to be installed.\n"
                    "Resources required for each replica:\n"
                    f"{required}\n"
                    "Total resources available:\n"
                    f"{available}\n"
                    "Use `ray status` for more details."
                )
                logger.warning(message)
                if _SCALING_LOG_ENABLED:
                    print_verbose_scaling_log()
                # If status is UNHEALTHY, leave the status and message as is.
                # The issue that caused the deployment to be unhealthy should be
                # prioritized over this resource availability issue.
                if self._curr_status_info.status != DeploymentStatus.UNHEALTHY:
                    self._curr_status_info = self._curr_status_info.update_message(
                        message
                    )

            if len(pending_initialization) > 0:
                message = (
                    f"Deployment '{self.deployment_name}' in application "
                    f"'{self.app_name}' has {len(pending_initialization)} replicas "
                    f"that have taken more than {SLOW_STARTUP_WARNING_S}s to "
                    "initialize.\n"
                    "This may be caused by a slow __init__ or reconfigure method."
                )
                logger.warning(message)
                # If status is UNHEALTHY, leave the status and message as is.
                # The issue that caused the deployment to be unhealthy should be
                # prioritized over this resource availability issue.
                if self._curr_status_info.status != DeploymentStatus.UNHEALTHY:
                    self._curr_status_info = self._curr_status_info.update_message(
                        message
                    )

            self._prev_startup_warning = time.time()

        for replica in self._replicas.pop(states=[ReplicaState.STOPPING]):
            stopped = replica.check_stopped()
            if not stopped:
                self._replicas.add(ReplicaState.STOPPING, replica)
            else:
                logger.info(f"{replica.replica_id} is stopped.")
                self._autoscaling_state_manager.on_replica_stopped(replica.replica_id)

    def _choose_pending_migration_replicas_to_stop(
        self,
        replicas: List[DeploymentReplica],
        deadlines: Dict[str, int],
        min_replicas_to_stop: int,
    ) -> Tuple[List[DeploymentReplica], List[DeploymentReplica]]:
        """Returns a partition of replicas to stop and to keep.

        Args:
            replicas: The current list of replicas pending migration.
            deadlines: The current draining node deadlines.
            min_replicas_to_stop: The minimum number of replicas to stop.
        """
        to_stop = list()
        remaining = list()

        # Stop replicas whose deadline is up
        for replica in replicas:
            assert replica.actor_node_id in deadlines

            curr_timestamp_ms = time.time() * 1000
            timeout_ms = replica._actor.graceful_shutdown_timeout_s * 1000
            if curr_timestamp_ms >= deadlines[replica.actor_node_id] - timeout_ms:
                to_stop.append(replica)
            else:
                remaining.append(replica)

        # Stop excess PENDING_MIGRATION replicas when new "replacement"
        # replicas have transitioned to RUNNING. The replicas with the
        # earliest deadlines should be chosen greedily.
        remaining.sort(key=lambda r: deadlines[r.actor_node_id])
        num_excess = min_replicas_to_stop - len(to_stop)

        if num_excess > 0:
            to_stop.extend(remaining[:num_excess])
            remaining = remaining[num_excess:]

        return to_stop, remaining

    def migrate_replicas_on_draining_nodes(self, draining_nodes: Dict[str, int]):
        # Move replicas back to running if they are no longer on a draining node.
        # If this causes the number of replicas to exceed the target state,
        # they will be scaled down because `scale_deployment_replicas` is called on
        # each deployment after this
        for replica in self._replicas.pop(states=[ReplicaState.PENDING_MIGRATION]):
            if replica.actor_node_id not in draining_nodes:
                self._replicas.add(ReplicaState.RUNNING, replica)
            else:
                self._replicas.add(ReplicaState.PENDING_MIGRATION, replica)

        # Migrate replicas on draining nodes
        for replica in self._replicas.pop(
            states=[ReplicaState.UPDATING, ReplicaState.RUNNING, ReplicaState.STARTING]
        ):
            if replica.actor_node_id in draining_nodes:
                # For RUNNING replicas, migrate them safely by starting
                # a replacement replica first.
                if replica.actor_details.state == ReplicaState.RUNNING:
                    logger.info(
                        f"Migrating {replica.replica_id} from draining node "
                        f"'{replica.actor_node_id}'. A new replica will be created on "
                        "another node."
                    )
                    self._replicas.add(ReplicaState.PENDING_MIGRATION, replica)
                # For replicas that are STARTING or UPDATING, might as
                # well terminate them immediately to allow replacement
                # replicas to start. Otherwise we need to wait for them
                # to transition to RUNNING before starting migration.
                else:
                    self._stop_replica(replica, graceful_stop=True)
            else:
                self._replicas.add(replica.actor_details.state, replica)

        num_running = self._replicas.count(states=[ReplicaState.RUNNING])
        num_draining = self._replicas.count(states=[ReplicaState.PENDING_MIGRATION])
        num_pending_migration_replicas_to_stop = (
            num_running + num_draining - self._target_state.target_num_replicas
        )

        (
            replicas_to_stop,
            replicas_to_keep,
        ) = self._choose_pending_migration_replicas_to_stop(
            self._replicas.pop(states=[ReplicaState.PENDING_MIGRATION]),
            draining_nodes,
            num_pending_migration_replicas_to_stop,
        )
        for replica in replicas_to_stop:
            logger.info(
                f"Stopping {replica.replica_id} "
                f"on draining node {replica.actor_node_id}."
            )
            self._stop_replica(replica, graceful_stop=True)

        for replica in replicas_to_keep:
            self._replicas.add(ReplicaState.PENDING_MIGRATION, replica)

    def record_multiplexed_model_ids(
        self, replica_id: ReplicaID, multiplexed_model_ids: List[str]
    ) -> None:
        """Records the multiplexed model IDs of a replica.

        Args:
            replica_name: Name of the replica.
            multiplexed_model_ids: List of model IDs that replica is serving.
        """
        # Find the replica
        for replica in self._replicas.get():
            if replica.replica_id == replica_id:
                replica.record_multiplexed_model_ids(multiplexed_model_ids)
                self._multiplexed_model_ids_updated = True
                return

        logger.warning(f"{replica_id} not found.")

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

    def __init__(
        self,
        kv_store: KVStoreBase,
        long_poll_host: LongPollHost,
        all_current_actor_names: List[str],
        all_current_placement_group_names: List[str],
        cluster_node_info_cache: ClusterNodeInfoCache,
        autoscaling_state_manager: AutoscalingStateManager,
        head_node_id_override: Optional[str] = None,
        create_placement_group_fn_override: Optional[Callable] = None,
    ):
        self._kv_store = kv_store
        self._long_poll_host = long_poll_host
        self._cluster_node_info_cache = cluster_node_info_cache
        self._deployment_scheduler = default_impl.create_deployment_scheduler(
            cluster_node_info_cache,
            head_node_id_override,
            create_placement_group_fn_override,
        )
        self._autoscaling_state_manager = autoscaling_state_manager

        self._deployment_states: Dict[DeploymentID, DeploymentState] = dict()

        self._recover_from_checkpoint(
            all_current_actor_names, all_current_placement_group_names
        )

    def _create_deployment_state(self, deployment_id):
        self._deployment_scheduler.on_deployment_created(
            deployment_id, SpreadDeploymentSchedulingPolicy()
        )

        return DeploymentState(
            deployment_id,
            self._long_poll_host,
            self._deployment_scheduler,
            self._cluster_node_info_cache,
            self._autoscaling_state_manager,
            self._save_checkpoint_func,
        )

    def _map_actor_names_to_deployment(
        self, all_current_actor_names: List[str]
    ) -> Dict[str, List[str]]:
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
            actor_name
            for actor_name in all_current_actor_names
            if ReplicaID.is_full_id_str(actor_name)
        ]
        deployment_to_current_replicas = defaultdict(list)
        if len(all_replica_names) > 0:
            for replica_name in all_replica_names:
                replica_id = ReplicaID.from_full_id_str(replica_name)
                deployment_to_current_replicas[replica_id.deployment_id].append(
                    replica_name
                )

        return deployment_to_current_replicas

    def _detect_and_remove_leaked_placement_groups(
        self,
        all_current_actor_names: List[str],
        all_current_placement_group_names: List[str],
    ):
        """Detect and remove any placement groups not associated with a replica.

        This can happen under certain rare circumstances:
            - The controller creates a placement group then crashes before creating
            the associated replica actor.
            - While the controller is down, a replica actor crashes but its placement
            group still exists.

        In both of these (or any other unknown cases), we simply need to remove the
        leaked placement groups.
        """
        leaked_pg_names = []
        for pg_name in all_current_placement_group_names:
            if (
                ReplicaID.is_full_id_str(pg_name)
                and pg_name not in all_current_actor_names
            ):
                leaked_pg_names.append(pg_name)

        if len(leaked_pg_names) > 0:
            logger.warning(
                f"Detected leaked placement groups: {leaked_pg_names}. "
                "The placement groups will be removed. This can happen in rare "
                "circumstances when the controller crashes and should not cause any "
                "issues. If this happens repeatedly, please file an issue on GitHub."
            )

        for leaked_pg_name in leaked_pg_names:
            try:
                pg = ray.util.get_placement_group(leaked_pg_name)
                ray.util.remove_placement_group(pg)
            except Exception:
                logger.exception(
                    f"Failed to remove leaked placement group {leaked_pg_name}."
                )

    def _recover_from_checkpoint(
        self,
        all_current_actor_names: List[str],
        all_current_placement_group_names: List[str],
    ):
        """
        Recover from checkpoint upon controller failure with all actor names
        found in current cluster.

        Each deployment resumes target state from checkpoint if available.

        For current state it will prioritize reconstructing from current
        actor names found that matches deployment tag if applicable.
        """
        self._detect_and_remove_leaked_placement_groups(
            all_current_actor_names,
            all_current_placement_group_names,
        )

        deployment_to_current_replicas = self._map_actor_names_to_deployment(
            all_current_actor_names
        )
        checkpoint = self._kv_store.get(CHECKPOINT_KEY)
        if checkpoint is not None:
            deployment_state_info = cloudpickle.loads(checkpoint)

            for deployment_id, checkpoint_data in deployment_state_info.items():
                deployment_state = self._create_deployment_state(deployment_id)
                deployment_state.recover_target_state_from_checkpoint(checkpoint_data)
                if len(deployment_to_current_replicas[deployment_id]) > 0:
                    deployment_state.recover_current_state_from_replica_actor_names(  # noqa: E501
                        deployment_to_current_replicas[deployment_id]
                    )
                self._deployment_states[deployment_id] = deployment_state

    def shutdown(self):
        """
        Shutdown all running replicas by notifying the controller, and leave
        it to the controller event loop to take actions afterwards.

        Once shutdown signal is received, it will also prevent any new
        deployments or replicas from being created.

        One can send multiple shutdown signals but won't effectively make any
        difference compare to calling it once.
        """

        for deployment_state in self._deployment_states.values():
            deployment_state.delete()

        # TODO(jiaodong): This might not be 100% safe since we deleted
        # everything without ensuring all shutdown goals are completed
        # yet. Need to address in follow-up PRs.
        self._kv_store.delete(CHECKPOINT_KEY)

        # TODO(jiaodong): Need to add some logic to prevent new replicas
        # from being created once shutdown signal is sent.

    def is_ready_for_shutdown(self) -> bool:
        """Return whether all deployments are shutdown.

        Check there are no deployment states and no checkpoints.
        """
        return (
            len(self._deployment_states) == 0
            and self._kv_store.get(CHECKPOINT_KEY) is None
        )

    def _save_checkpoint_func(
        self, *, writeahead_checkpoints: Optional[Dict[str, Tuple]]
    ) -> None:
        """Write a checkpoint of all deployment states.
        By default, this checkpoints the current in-memory state of each
        deployment. However, these can be overwritten by passing
        `writeahead_checkpoints` in order to checkpoint an update before
        applying it to the in-memory state.
        """

        deployment_state_info = {
            deployment_id: deployment_state.get_checkpoint_data()
            for deployment_id, deployment_state in self._deployment_states.items()
        }

        if writeahead_checkpoints is not None:
            deployment_state_info.update(writeahead_checkpoints)

        self._kv_store.put(
            CHECKPOINT_KEY,
            cloudpickle.dumps(deployment_state_info),
        )

    def get_running_replica_infos(
        self,
    ) -> Dict[DeploymentID, List[RunningReplicaInfo]]:
        return {
            id: deployment_state.get_running_replica_infos()
            for id, deployment_state in self._deployment_states.items()
        }

    def get_deployment_infos(self) -> Dict[DeploymentID, DeploymentInfo]:
        infos: Dict[DeploymentID, DeploymentInfo] = {}
        for deployment_id, deployment_state in self._deployment_states.items():
            infos[deployment_id] = deployment_state.target_info

        return infos

    def get_deployment(self, deployment_id: DeploymentID) -> Optional[DeploymentInfo]:
        if deployment_id in self._deployment_states:
            return self._deployment_states[deployment_id].target_info
        else:
            return None

    def get_deployment_details(self, id: DeploymentID) -> Optional[DeploymentDetails]:
        """Gets detailed info on a deployment.

        Returns:
            DeploymentDetails: if the deployment is live.
            None: if the deployment is deleted.
        """
        statuses = self.get_deployment_statuses([id])
        if len(statuses) == 0:
            return None
        else:
            status_info = statuses[0]
            deployment_state = self._deployment_states[id]
            return DeploymentDetails(
                name=id.name,
                status=status_info.status,
                status_trigger=status_info.status_trigger,
                message=status_info.message,
                deployment_config=_deployment_info_to_schema(
                    id.name, self.get_deployment(id)
                ),
                target_num_replicas=deployment_state._target_state.target_num_replicas,
                replicas=deployment_state.list_replica_details(),
            )

    def get_deployment_statuses(
        self, ids: Optional[List[DeploymentID]] = None
    ) -> List[DeploymentStatusInfo]:
        """
        Return the statuses of the deployments with the given `ids`.
        If `ids` is `None`, returns the status of all deployments.
        """
        if ids is None:
            # fast path for returning all deployments,
            # avoids checking `if ids is None` in a loop
            return [
                state.curr_status_info for state in self._deployment_states.values()
            ]
        else:
            statuses = []
            for id in ids:
                state = self._deployment_states.get(id)
                if state is not None:
                    statuses.append(state.curr_status_info)
            return statuses

    def get_alive_replica_actor_ids(self) -> Set[str]:
        alive_replica_actor_ids = set()
        for ds in self._deployment_states.values():
            alive_replica_actor_ids |= ds.get_alive_replica_actor_ids()

        return alive_replica_actor_ids

    def deploy(
        self,
        deployment_id: DeploymentID,
        deployment_info: DeploymentInfo,
    ) -> bool:
        """Deploy the deployment.

        If the deployment already exists with the same version and config,
        this is a no-op and returns False.

        Returns:
            bool: Whether or not the deployment is being updated.
        """
        if deployment_id not in self._deployment_states:
            self._deployment_states[deployment_id] = self._create_deployment_state(
                deployment_id
            )
            self._record_deployment_usage()

        return self._deployment_states[deployment_id].deploy(deployment_info)

    def get_deployments_in_application(self, app_name: str) -> List[str]:
        """Return list of deployment names in application."""

        deployments = []
        for deployment_id in self._deployment_states:
            if deployment_id.app_name == app_name:
                deployments.append(deployment_id.name)

        return deployments

    def delete_deployment(self, id: DeploymentID):
        # This method must be idempotent. We should validate that the
        # specified deployment exists on the client.
        if id in self._deployment_states:
            self._deployment_states[id].delete()

    def update(self) -> bool:
        """Updates the state of all deployments to match their goal state.

        Returns True if any of the deployments have replicas in the RECOVERING state.
        """

        deleted_ids = []
        any_recovering = False
        upscales: Dict[DeploymentID, List[ReplicaSchedulingRequest]] = {}
        downscales: Dict[DeploymentID, DeploymentDownscaleRequest] = {}

        # STEP 1: Update current state
        for deployment_state in self._deployment_states.values():
            if deployment_state.should_autoscale():
                deployment_state.autoscale()

            deployment_state.check_and_update_replicas()

        # STEP 2: Check current status
        for deployment_state in self._deployment_states.values():
            deployment_state.check_curr_status()

        # STEP 3: Drain nodes
        draining_nodes = self._cluster_node_info_cache.get_draining_nodes()
        allow_new_compaction = len(draining_nodes) == 0 and all(
            ds.curr_status_info.status == DeploymentStatus.HEALTHY
            # TODO(zcin): Make sure that status should never be healthy if
            # the number of running replicas at target version is not at
            # target number, so we can remove this defensive check.
            and ds.get_num_running_replicas(ds.target_version) == ds.target_num_replicas
            # To be extra conservative, only actively compact if there
            # are no non-running replicas
            and len(ds._replicas.get()) == ds.target_num_replicas
            for ds in self._deployment_states.values()
        )
        if RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY:
            # Tuple of target node to compact, and its draining deadline
            node_info: Optional[
                Tuple[str, float]
            ] = self._deployment_scheduler.get_node_to_compact(
                allow_new_compaction=allow_new_compaction
            )
            if node_info:
                target_node_id, deadline = node_info
                draining_nodes = {target_node_id: deadline}

        for deployment_id, deployment_state in self._deployment_states.items():
            deployment_state.migrate_replicas_on_draining_nodes(draining_nodes)

        # STEP 4: Scale replicas
        for deployment_id, deployment_state in self._deployment_states.items():
            upscale, downscale = deployment_state.scale_deployment_replicas()

            if upscale:
                upscales[deployment_id] = upscale
            if downscale:
                downscales[deployment_id] = downscale

        # STEP 5: Update status
        for deployment_id, deployment_state in self._deployment_states.items():
            deleted, any_replicas_recovering = deployment_state.check_curr_status()

            if deleted:
                deleted_ids.append(deployment_id)
            any_recovering |= any_replicas_recovering

        # STEP 6: Schedule all STARTING replicas and stop all STOPPING replicas
        deployment_to_replicas_to_stop = self._deployment_scheduler.schedule(
            upscales, downscales
        )
        for deployment_id, replicas_to_stop in deployment_to_replicas_to_stop.items():
            self._deployment_states[deployment_id].stop_replicas(replicas_to_stop)
        for deployment_id, scheduling_requests in upscales.items():
            self._handle_scheduling_request_failures(deployment_id, scheduling_requests)

        # STEP 7: Broadcast long poll information
        for deployment_id, deployment_state in self._deployment_states.items():
            deployment_state.broadcast_running_replicas_if_changed()
            deployment_state.broadcast_deployment_config_if_changed()
            if deployment_state.should_autoscale():
                self._autoscaling_state_manager.update_running_replica_ids(
                    deployment_id=deployment_id,
                    running_replicas=deployment_state.get_running_replica_ids(),
                )

        # STEP 8: Cleanup
        for deployment_id in deleted_ids:
            self._deployment_scheduler.on_deployment_deleted(deployment_id)
            self._autoscaling_state_manager.deregister_deployment(deployment_id)
            del self._deployment_states[deployment_id]

        if len(deleted_ids):
            self._record_deployment_usage()

        return any_recovering

    def _handle_scheduling_request_failures(
        self,
        deployment_id: DeploymentID,
        scheduling_requests: List[ReplicaSchedulingRequest],
    ):
        """Updates internal datastructures when replicas fail to be scheduled."""
        failed_replicas: List[ReplicaID] = []
        for scheduling_request in scheduling_requests:
            if (
                scheduling_request.status
                == ReplicaSchedulingRequestStatus.PLACEMENT_GROUP_CREATION_FAILED
            ):
                failed_replicas.append(scheduling_request.replica_id)
                self._deployment_states[deployment_id].record_replica_startup_failure(
                    "Replica scheduling failed. Failed to create a placement "
                    f"group for replica {scheduling_request.replica_id}. "
                    "See Serve controller logs for more details."
                )
            elif (
                scheduling_request.status
                == ReplicaSchedulingRequestStatus.ACTOR_CREATION_FAILED
            ):
                failed_replicas.append(scheduling_request.replica_id)
                self._deployment_states[deployment_id].record_replica_startup_failure(
                    "Replica scheduling failed. Failed to create an actor "
                    f"for replica {scheduling_request.replica_id}. "
                    "See Serve controller logs for more details."
                )
        if failed_replicas:
            self._deployment_states[deployment_id].stop_replicas(failed_replicas)
            self._deployment_states[deployment_id].update_replica_startup_backoff_time()

    def _record_deployment_usage(self):
        ServeUsageTag.NUM_DEPLOYMENTS.record(str(len(self._deployment_states)))

        num_gpu_deployments = 0
        for deployment_state in self._deployment_states.values():
            if (
                deployment_state.target_info is not None
                and deployment_state.target_info.replica_config is not None
                and deployment_state.target_info.replica_config.ray_actor_options
                is not None
                and (
                    deployment_state.target_info.replica_config.ray_actor_options.get(
                        "num_gpus", 0
                    )
                    > 0
                )
            ):
                num_gpu_deployments += 1
        ServeUsageTag.NUM_GPU_DEPLOYMENTS.record(str(num_gpu_deployments))

    def record_multiplexed_replica_info(self, info: MultiplexedReplicaInfo):
        """
        Record multiplexed model ids for a multiplexed replica.

        Args:
            info: Multiplexed replica info including deployment name,
                replica tag and model ids.
        """
        deployment_id = info.replica_id.deployment_id
        if deployment_id not in self._deployment_states:
            app_msg = f" in application '{deployment_id.app_name}'"
            logger.error(
                f"Deployment '{deployment_id.name}'{app_msg} not found in state "
                "manager."
            )
            return
        self._deployment_states[deployment_id].record_multiplexed_model_ids(
            info.replica_id, info.model_ids
        )

    def get_active_node_ids(self) -> Set[str]:
        """Return set of node ids with running replicas of any deployment.

        This is used to determine which node has replicas. Only nodes with replicas and
        head node should have active proxies.
        """
        node_ids = set()
        for deployment_state in self._deployment_states.values():
            node_ids.update(deployment_state.get_active_node_ids())
        return node_ids

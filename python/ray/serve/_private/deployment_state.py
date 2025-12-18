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
from ray._common import ray_constants
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
    DeploymentTargetInfo,
    Duration,
    ReplicaID,
    ReplicaState,
    RequestRoutingInfo,
    RunningReplicaInfo,
)
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.constants import (
    DEFAULT_LATENCY_BUCKET_MS,
    MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT,
    MAX_PER_REPLICA_RETRY_COUNT,
    RAY_SERVE_ENABLE_TASK_EVENTS,
    RAY_SERVE_FAIL_ON_RANK_ERROR,
    RAY_SERVE_FORCE_STOP_UNHEALTHY_REPLICAS,
    RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY,
    REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD,
    REPLICA_STARTUP_SHUTDOWN_LATENCY_BUCKETS_MS,
    REQUEST_LATENCY_BUCKETS_MS,
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
from ray.serve._private.exceptions import DeploymentIsBeingDeletedError
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
    ReplicaRank,
    _deployment_info_to_schema,
)
from ray.util import metrics as ray_metrics
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
            route_prefix=info.route_prefix,
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

ALL_REPLICA_STATES = list(ReplicaState)
_SCALING_LOG_ENABLED = os.environ.get("SERVE_ENABLE_SCALING_LOG", "0") != "0"
# Feature flag to disable forcibly shutting down replicas.
RAY_SERVE_DISABLE_SHUTTING_DOWN_INGRESS_REPLICAS_FORCEFULLY = (
    os.environ.get("RAY_SERVE_DISABLE_SHUTTING_DOWN_INGRESS_REPLICAS_FORCEFULLY", "0")
    == "1"
)


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
        self._last_health_check_latency_ms: Optional[float] = None
        self._last_health_check_failed: Optional[bool] = None
        self._initialization_latency_s: Optional[float] = None
        self._reconfigure_start_time: Optional[float] = None
        self._internal_grpc_port: Optional[int] = None
        self._docs_path: Optional[str] = None
        self._route_patterns: Optional[List[str]] = None
        # Rank assigned to the replica.
        self._assign_rank_callback: Optional[Callable[[ReplicaID], ReplicaRank]] = None
        self._rank: Optional[ReplicaRank] = None
        # Populated in `on_scheduled` or `recover`.
        self._actor_handle: ActorHandle = None
        self._placement_group: PlacementGroup = None

        # Populated after replica is allocated.
        self._pid: int = None
        self._actor_id: str = None
        self._worker_id: str = None
        self._node_id: str = None
        self._node_ip: str = None
        self._node_instance_id: str = None
        self._log_file_path: str = None
        self._http_port: int = None
        self._grpc_port: int = None

        # Populated in self.stop().
        self._graceful_shutdown_ref: ObjectRef = None

        # todo: will be confused with deployment_config.is_cross_language
        self._is_cross_language = False
        self._deployment_is_cross_language = False
        self._routing_stats: Dict[str, Any] = {}
        self._record_routing_stats_ref: Optional[ObjectRef] = None
        self._last_record_routing_stats_time: float = 0.0
        self._ingress: bool = False

        # Outbound deployments polling state
        self._outbound_deployments: Optional[List[DeploymentID]] = None

        # Histogram to track routing stats delay from replica to controller
        self._routing_stats_delay_histogram = metrics.Histogram(
            "serve_routing_stats_delay_ms",
            description=(
                "The delay in milliseconds for routing stats to propagate "
                "from replica to controller."
            ),
            boundaries=DEFAULT_LATENCY_BUCKET_MS,
            tag_keys=("deployment", "replica", "application"),
        )
        self._routing_stats_delay_histogram.set_default_tags(
            {
                "deployment": self._deployment_id.name,
                "replica": self._replica_id.unique_id,
                "application": self._deployment_id.app_name,
            }
        )

        # Counter to track exceptions/timeouts when getting routing stats
        self._routing_stats_error_counter = metrics.Counter(
            "serve_routing_stats_error",
            description=(
                "The number of errors (exceptions or timeouts) when getting "
                "routing stats from replica."
            ),
            tag_keys=("deployment", "replica", "application", "error_type"),
        )
        self._routing_stats_error_counter.set_default_tags(
            {
                "deployment": self._deployment_id.name,
                "replica": self._replica_id.unique_id,
                "application": self._deployment_id.app_name,
            }
        )

    @property
    def replica_id(self) -> str:
        return self._replica_id

    @property
    def deployment_name(self) -> str:
        return self._deployment_id.name

    @property
    def rank(self) -> Optional[ReplicaRank]:
        return self._rank

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
    def docs_path(self) -> Optional[str]:
        return self._docs_path

    @property
    def route_patterns(self) -> Optional[List[str]]:
        return self._route_patterns

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
    def http_port(self) -> Optional[int]:
        return self._http_port

    @property
    def grpc_port(self) -> Optional[int]:
        return self._grpc_port

    @property
    def request_routing_stats_period_s(self) -> float:
        return (
            self.deployment_config.request_router_config.request_routing_stats_period_s
        )

    @property
    def request_routing_stats_timeout_s(self) -> float:
        return (
            self.deployment_config.request_router_config.request_routing_stats_timeout_s
        )

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
    def node_instance_id(self) -> Optional[str]:
        """Returns the node instance id of the actor, None if not placed."""
        return self._node_instance_id

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

    @property
    def reconfigure_start_time(self) -> Optional[float]:
        """Returns the start time of the last reconfigure operation.

        Returns None if no reconfigure operation has started.
        """
        return self._reconfigure_start_time

    @property
    def last_health_check_latency_ms(self) -> Optional[float]:
        """Returns the latency of the last completed health check in milliseconds.

        Returns None if no health check has completed in the current check cycle.
        """
        return self._last_health_check_latency_ms

    @property
    def last_health_check_failed(self) -> Optional[bool]:
        """Returns whether the last completed health check failed.

        Returns False if no health check has completed in the current check cycle.
        """
        return self._last_health_check_failed

    def start(
        self,
        deployment_info: DeploymentInfo,
        assign_rank_callback: Callable[[ReplicaID], ReplicaRank],
    ) -> ReplicaSchedulingRequest:
        """Start the current DeploymentReplica instance.

        The replica will be in the STARTING and PENDING_ALLOCATION states
        until the deployment scheduler schedules the underlying actor.
        """
        self._assign_rank_callback = assign_rank_callback
        self._actor_resources = deployment_info.replica_config.resource_dict
        self._ingress = deployment_info.ingress
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
                deployment_info.ingress,
                deployment_info.route_prefix,
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

        if self._is_cross_language:
            self._actor_handle = JavaActorHandleProxy(self._actor_handle)
            self._allocated_obj_ref = self._actor_handle.is_allocated.remote()
        else:
            self._allocated_obj_ref = self._actor_handle.is_allocated.remote()

    def _format_user_config(self, user_config: Any):
        temp = copy(user_config)
        if user_config is not None and self._deployment_is_cross_language:
            if self._is_cross_language:
                temp = msgpack_serialize(temp)
            else:
                temp = msgpack_deserialize(temp)
        return temp

    def reconfigure(self, version: DeploymentVersion, rank: ReplicaRank) -> bool:
        """
        Update replica version. Also, updates the deployment config on the actor
        behind this DeploymentReplica instance if necessary.

        Returns: whether the actor is being updated.
        """
        updating = False

        # Determine if we need heavyweight reconfiguration
        # vs lightweight updates
        needs_actor_reconfigure = self._version.requires_actor_reconfigure(version)
        has_rank_changes = self._rank != rank

        if needs_actor_reconfigure or has_rank_changes:
            # Call into replica actor reconfigure() with updated user config and
            # graceful_shutdown_wait_loop_s
            # Setting updating=True because we want to transition to UPDATING state
            # when rank is updated or deployment config changes.
            updating = True
            self._reconfigure_start_time = time.time()
            deployment_config = copy(version.deployment_config)
            deployment_config.user_config = self._format_user_config(
                deployment_config.user_config
            )
            self._ready_obj_ref = self._actor_handle.reconfigure.remote(
                deployment_config,
                rank,
                version.route_prefix,
            )

        self._version = version
        self._rank = rank
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
                    self._node_instance_id,
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

        if self._ready_obj_ref is None:
            # Perform auto method name translation for java handles.
            # See https://github.com/ray-project/ray/issues/21474
            deployment_config = copy(self._version.deployment_config)
            deployment_config.user_config = self._format_user_config(
                deployment_config.user_config
            )
            if self._is_cross_language:
                self._ready_obj_ref = self._actor_handle.is_initialized.remote(
                    deployment_config.to_proto_bytes()
                )
            else:
                replica_ready_check_func = (
                    self._actor_handle.initialize_and_get_metadata
                )
                # this guarantees that node_id is set before rank is assigned
                self._rank = self._assign_rank_callback(
                    self._replica_id.unique_id, self._node_id
                )
                self._ready_obj_ref = replica_ready_check_func.remote(
                    deployment_config, self._rank
                )

            return ReplicaStartupStatus.PENDING_INITIALIZATION, None

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
                        self._internal_grpc_port,
                        self._docs_path,
                        self._http_port,
                        self._grpc_port,
                        self._rank,
                        self._route_patterns,
                        self._outbound_deployments,
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
        # Reset the last health check status for this check cycle.
        # We do this because _check_active_health_check is being called in a loop,
        # and we want to avoid accumulating latency and failure metrics over multiple
        # check cycles.
        self._last_health_check_latency_ms = None
        self._last_health_check_failed = None

        if self._health_check_ref is None:
            # There is no outstanding health check.
            response = ReplicaHealthCheckResponse.NONE
        elif check_obj_ref_ready_nowait(self._health_check_ref):
            # Object ref is ready, ray.get it to check for exceptions.
            try:
                ray.get(self._health_check_ref)
                # Calculate health check latency.
                self._last_health_check_latency_ms = (
                    time.time() - self._last_health_check_time
                ) * 1000
                self._last_health_check_failed = False
                # Health check succeeded without exception.
                response = ReplicaHealthCheckResponse.SUCCEEDED
            except RayActorError:
                # Health check failed due to actor crashing.
                response = ReplicaHealthCheckResponse.ACTOR_CRASHED
                self._last_health_check_failed = True
            except RayError as e:
                # Health check failed due to application-level exception.
                logger.warning(f"Health check for {self._replica_id} failed: {e}")
                response = ReplicaHealthCheckResponse.APP_FAILURE
                self._last_health_check_failed = True
        elif time.time() - self._last_health_check_time > self.health_check_timeout_s:
            # Health check hasn't returned and the timeout is up, consider it failed.
            logger.warning(
                "Didn't receive health check response for replica "
                f"{self._replica_id} after "
                f"{self.health_check_timeout_s}s, marking it unhealthy."
            )
            response = ReplicaHealthCheckResponse.APP_FAILURE
            # Calculate latency for timeout case.
            self._last_health_check_latency_ms = (
                time.time() - self._last_health_check_time
            ) * 1000
            self._last_health_check_failed = True
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

    def _should_record_routing_stats(self) -> bool:
        """Determines if a new record routing stats should be kicked off.

        A record routing stats will be started if:
            1) There is not already an active record routing stats.
            2) It has been more than request_routing_stats_period_s since
               the previous record routing stats was *started*.

        This assumes that self._record_routing_stats_ref is reset to `None`
        when an active record routing stats succeeds or fails (due to
        returning or timeout).
        """
        if self._record_routing_stats_ref is not None:
            # There's already an active record routing stats.
            return False

        # If there's no active record routing stats, kick off another and
        # reset the timer if it's been long enough since the last record
        # routing stats. Add some randomness to avoid synchronizing across
        # all replicas.
        time_since_last = time.time() - self._last_record_routing_stats_time
        randomized_period = self.request_routing_stats_period_s * random.uniform(
            0.9, 1.1
        )
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

    def get_routing_stats(self) -> Dict[str, Any]:
        """Get the routing stats for the replica."""
        if self._record_routing_stats_ref is None:
            # There's no active record routing stats.
            pass
        elif check_obj_ref_ready_nowait(self._record_routing_stats_ref):
            # Object ref is ready, ray.get it to check for exceptions.
            try:
                self._routing_stats = ray.get(self._record_routing_stats_ref)
                # Record the round-trip delay for routing stats
                delay_ms = (time.time() - self._last_record_routing_stats_time) * 1000
                self._routing_stats_delay_histogram.observe(delay_ms)
            except Exception:
                logger.exception(
                    "Exception when trying to get routing stats:\n"
                    + traceback.format_exc()
                )
                self._routing_stats_error_counter.inc(tags={"error_type": "exception"})
            self._record_routing_stats_ref = None
        elif (
            time.time() - self._last_record_routing_stats_time
            > self.request_routing_stats_timeout_s
        ):
            # Record routing stats hasn't returned and the timeout is up, retrying.
            logger.warning(
                "Didn't receive routing stats response for replica "
                f"{self._replica_id} after "
                f"{self.request_routing_stats_timeout_s}s, retrying."
            )
            self._routing_stats_error_counter.inc(tags={"error_type": "timeout"})
            self._record_routing_stats_ref = None

        if self._should_record_routing_stats():
            self._last_record_routing_stats_time = time.time()
            self._record_routing_stats_ref = (
                self._actor_handle.record_routing_stats.remote()
            )

        return self._routing_stats

    def force_stop(self, log_shutdown_message: bool = False):
        """Force the actor to exit without shutting down gracefully."""
        if (
            self._ingress
            and RAY_SERVE_DISABLE_SHUTTING_DOWN_INGRESS_REPLICAS_FORCEFULLY
        ):
            if log_shutdown_message:
                logger.info(
                    f"{self.replica_id} did not shut down because it had not finished draining requests. "
                    "Going to wait until the draining is complete. You can force-stop the replica by "
                    "setting RAY_SERVE_DISABLE_SHUTTING_DOWN_INGRESS_REPLICAS_FORCEFULLY to 0."
                )
            return

        try:
            ray.kill(ray.get_actor(self._actor_name, namespace=SERVE_NAMESPACE))
        except ValueError:
            pass

    def get_outbound_deployments(self) -> Optional[List[DeploymentID]]:
        return self._outbound_deployments


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
        self._shutdown_start_time: Optional[float] = None
        self._actor_details = ReplicaDetails(
            actor_name=replica_id.to_full_id_str(),
            replica_id=self._replica_id.unique_id,
            state=ReplicaState.STARTING,
            start_time_s=0,
        )
        self._multiplexed_model_ids: List[str] = []
        self._routing_stats: Dict[str, Any] = {}
        self._logged_shutdown_message = False

    def get_running_replica_info(
        self, cluster_node_info_cache: ClusterNodeInfoCache
    ) -> RunningReplicaInfo:
        return RunningReplicaInfo(
            replica_id=self._replica_id,
            node_id=self.actor_node_id,
            node_ip=self._actor.node_ip,
            availability_zone=cluster_node_info_cache.get_node_az(self.actor_node_id),
            actor_name=self._actor._actor_name,
            max_ongoing_requests=self._actor.max_ongoing_requests,
            is_cross_language=self._actor.is_cross_language,
            multiplexed_model_ids=self.multiplexed_model_ids,
            routing_stats=self.routing_stats,
            port=self._actor._internal_grpc_port,
        )

    def record_multiplexed_model_ids(self, multiplexed_model_ids: List[str]):
        """Record the multiplexed model ids for this replica."""
        self._multiplexed_model_ids = multiplexed_model_ids

    def record_routing_stats(self, routing_stats: Optional[Dict[str, Any]]):
        """Record the routing stats for this replica.

        Recording routing_stats as an empty dictionary is valid. But skip
        update if the routing_stats is None.
        """
        if routing_stats is not None:
            self._routing_stats = routing_stats

    @property
    def multiplexed_model_ids(self) -> List[str]:
        return self._multiplexed_model_ids

    @property
    def routing_stats(self) -> Dict[str, Any]:
        return self._routing_stats

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
    def docs_path(self) -> Optional[str]:
        return self._actor.docs_path

    @property
    def route_patterns(self) -> Optional[List[str]]:
        return self._actor.route_patterns

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
    def actor_http_port(self) -> Optional[int]:
        return self._actor.http_port

    @property
    def actor_grpc_port(self) -> Optional[int]:
        return self._actor.grpc_port

    @property
    def actor_pid(self) -> Optional[int]:
        """Returns the node id of the actor, None if not placed."""
        return self._actor.pid

    @property
    def initialization_latency_s(self) -> Optional[float]:
        """Returns how long the replica took to initialize."""

        return self._actor.initialization_latency_s

    @property
    def reconfigure_start_time(self) -> Optional[float]:
        """Returns the start time of the last reconfigure operation."""
        return self._actor.reconfigure_start_time

    @property
    def last_health_check_latency_ms(self) -> Optional[float]:
        """Returns the latency of the last completed health check in milliseconds."""
        return self._actor.last_health_check_latency_ms

    @property
    def last_health_check_failed(self) -> Optional[bool]:
        """Returns whether the last completed health check failed."""
        return self._actor.last_health_check_failed

    @property
    def shutdown_start_time(self) -> Optional[float]:
        """Returns the start time of the shutdown operation."""
        return self._shutdown_start_time

    def start(
        self,
        deployment_info: DeploymentInfo,
        assign_rank_callback: Callable[[ReplicaID], ReplicaRank],
    ) -> ReplicaSchedulingRequest:
        """
        Start a new actor for current DeploymentReplica instance.
        """
        replica_scheduling_request = self._actor.start(
            deployment_info, assign_rank_callback=assign_rank_callback
        )
        self._start_time = time.time()
        self._logged_shutdown_message = False
        self.update_actor_details(start_time_s=self._start_time)
        return replica_scheduling_request

    def reconfigure(
        self,
        version: DeploymentVersion,
        rank: ReplicaRank,
    ) -> bool:
        """
        Update replica version. Also, updates the deployment config on the actor
        behind this DeploymentReplica instance if necessary.

        Returns: whether the actor is being updated.
        """
        return self._actor.reconfigure(version, rank=rank)

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

    @property
    def rank(self) -> Optional[ReplicaRank]:
        """Get the rank assigned to the replica."""
        return self._actor.rank

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
            node_instance_id=self._actor.node_instance_id,
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
        self._shutdown_start_time = time.time()
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
            if (
                not self._logged_shutdown_message
                and not RAY_SERVE_DISABLE_SHUTTING_DOWN_INGRESS_REPLICAS_FORCEFULLY
            ):
                logger.info(
                    f"{self.replica_id} did not shut down after grace "
                    "period, force-killing it. "
                )

            self._actor.force_stop(
                log_shutdown_message=not self._logged_shutdown_message
            )
            self._logged_shutdown_message = True
        return False

    def check_health(self) -> bool:
        """Check if the replica is healthy.

        Returns `True` if the replica is healthy, else `False`.
        """
        return self._actor.check_health()

    def pull_routing_stats(self) -> Optional[Dict[str, Any]]:
        """Get the latest response from the routing stats on the replica.

        Returns None if the replica is still calculating the stats.
        """
        return self._actor.get_routing_stats()

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

    def get_outbound_deployments(self) -> Optional[List[DeploymentID]]:
        return self._actor.get_outbound_deployments()


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


class RankManager:
    """Manages ranks for a single node."""

    def __init__(self):
        self._ranks: Dict[str, int] = {}
        self._released_ranks: Set[int] = set()
        self._next_rank: int = 0

    def assign_rank(self, key: str) -> int:
        if key in self._ranks:
            raise RuntimeError(f"Rank for {key} already assigned: {self._ranks[key]}")

        if self._released_ranks:
            # Reuse the smallest released rank
            rank = min(self._released_ranks)
            self._released_ranks.remove(rank)
        else:
            # Assign the next available rank
            # This is the first time we're assigning a rank to this replica
            rank = self._next_rank
            self._next_rank += 1

        self._ranks[key] = rank
        return rank

    def release_rank(self, key: str) -> None:
        if key not in self._ranks:
            raise RuntimeError(f"Rank for {key} not assigned")
        rank = self._ranks.pop(key)
        # Add the released rank to the set of released ranks
        # This rank can be reused for a new replica
        self._released_ranks.add(rank)

    def recover_rank(self, key: str, rank: int) -> None:
        if key in self._ranks:
            raise RuntimeError(f"Rank for {key} already assigned: {self._ranks[key]}")
        self._ranks[key] = rank
        self._released_ranks.discard(rank)
        if rank >= self._next_rank:
            self._next_rank = rank + 1

    def get_rank(self, key: str) -> int:
        if key not in self._ranks:
            raise RuntimeError(f"Rank for {key} not assigned")
        return self._ranks[key]

    def has_rank(self, key: str) -> bool:
        return key in self._ranks

    def get_ranks_mapping(self) -> Dict[str, int]:
        return self._ranks.copy()

    def clear(self) -> None:
        self._ranks.clear()
        self._released_ranks.clear()
        self._next_rank = 0

    def check_rank_consistency_and_reassign_minimally(
        self,
        active_keys: List[str],
    ) -> List[str]:
        """Verify rank system invariants and reassign ranks when needed.

        This method ensures:
        1. All active keys have ranks
        2. No duplicate ranks exist
        3. Ranks are contiguous when at target count

        Args:
            active_keys: List of currently active keys

        Returns:
            List of keys that need to be reconfigured with new ranks

        Raises:
            RuntimeError: If rank system invariants are violated and fail_on_error=True
        """
        if not active_keys:
            return []

        active_keys_set = set(active_keys)

        # Check for stale ranks - this should never happen
        stale_keys = set(self._ranks.keys()) - active_keys_set
        if stale_keys:
            logger.error(
                f"Found stale ranks for keys: {stale_keys}. "
                "This should never happen. Please report this as a bug."
            )
            raise RuntimeError("Rank system is in an invalid state.")

        # Verify system invariants - all active keys must have ranks
        unranked_keys = active_keys_set - set(self._ranks.keys())
        if unranked_keys:
            logger.error(
                f"Found active keys without ranks: {unranked_keys}. "
                "This should never happen. Please report this as a bug."
            )
            raise RuntimeError("Rank system is in an invalid state.")

        # Check for duplicate ranks - this should never happen
        rank_counts = {}
        for key, rank in self._ranks.copy().items():
            if key in active_keys_set:  # Only check active keys
                rank_counts[rank] = rank_counts.get(rank, 0) + 1
                if rank_counts[rank] > 1:
                    logger.error(
                        f"Found duplicate rank {rank} assigned to multiple keys. "
                        "This should never happen. Please report this as a bug."
                    )
                    raise RuntimeError("Rank system is in an invalid state.")

        # Check if we need to reassign ranks for contiguity
        # Only force contiguity when at target count (e.g., after autoscaling down)
        current_ranks = sorted(self._ranks.values())
        expected_ranks = list(range(len(active_keys)))

        keys_needing_reconfiguration_from_reassignment = []

        if current_ranks != expected_ranks:
            logger.debug(
                f"At target count but ranks are not contiguous. "
                f"Current: {current_ranks}, Expected: {expected_ranks}. "
                "Performing minimal reassignment."
            )
            keys_needing_reconfiguration_from_reassignment = (
                self._perform_minimal_rank_reassignment(active_keys)
            )

        return keys_needing_reconfiguration_from_reassignment

    def _perform_minimal_rank_reassignment(self, active_keys: List[str]) -> List[str]:
        """Perform minimal rank reassignment to achieve contiguity.

        This method reassigns ranks while minimizing the number of keys that need
        to be reconfigured. It prioritizes keeping existing ranks when possible.

        Args:
            active_keys: List of currently active keys

        Returns:
            List of keys that need to be reconfigured with new ranks
        """
        target_ranks_set = set(range(len(active_keys)))

        # Find which keys need new ranks
        keys_needing_ranks = []
        keys_keeping_ranks = []

        for key in active_keys:
            current_rank = self.get_rank(key)

            if current_rank in target_ranks_set:
                # This key can keep its rank
                target_ranks_set.remove(current_rank)  # O(1) operation
                keys_keeping_ranks.append(key)
            else:
                # This key needs a new rank
                keys_needing_ranks.append(key)

        # Convert remaining target ranks to sorted list for deterministic assignment
        available_ranks = sorted(target_ranks_set)

        # Assign new ranks to keys that need them
        for i, key in enumerate(keys_needing_ranks):
            new_rank = available_ranks[i]  # O(1) operation

            # Store the old rank before updating
            old_rank = self._ranks[key]

            logger.debug(f"Reassigning key {key}: rank {old_rank} -> {new_rank}")

            # Update the rank mapping
            self._ranks[key] = new_rank
            # Remove the newly assigned rank from available ranks
            self._released_ranks.discard(new_rank)
            # Add the old rank back to available ranks for reuse
            self._released_ranks.add(old_rank)

        # Log the reassignment summary
        logger.debug(
            f"Minimal reassignment complete: {len(keys_keeping_ranks)} keys kept ranks, "
            f"{len(keys_needing_ranks)} keys reassigned"
        )

        return keys_needing_ranks


class DeploymentRankManager:
    """Manages replica ranks for a deployment.
    This class handles rank assignment, release, consistency checking, and reassignment.
    It maintains the rank system invariants and provides a clean interface for rank operations.

    Maintains three levels of rank tracking:
    - Global rank: Replica-level rank across all nodes (0, 1, 2, ...)
    - Local rank: Replica's rank within its node (0, 1, 2, ... per node)
    - Node rank ID: Index assigned to each node (0, 1, 2, ...)
    """

    def __init__(self, fail_on_rank_error: bool = True):
        # Global rank manager (existing replica-level rank)
        self._replica_rank_manager = RankManager()
        self._fail_on_rank_error = fail_on_rank_error

        # Node rank manager (assigns rank IDs to nodes)
        self._node_rank_manager = RankManager()

        # Local rank managers (one per node, manages replica ranks within each node)
        self._local_rank_managers: Dict[str, RankManager] = {}

        # Track which node each replica is on
        self._replica_to_node: Dict[str, str] = {}

    def _execute_with_error_handling(self, func, safe_default, *args, **kwargs):
        if self._fail_on_rank_error:
            # Let exceptions propagate
            return func(*args, **kwargs)
        else:
            # Catch exceptions and return safe default
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Error executing function {func.__name__}: {e}")
                return safe_default

    def assign_rank(self, replica_id: str, node_id: str) -> ReplicaRank:
        """Assign a rank to a new replica.

        Args:
            replica_id: The unique ID of the replica
            node_id: The unique ID of the node

        Returns:
            ReplicaRank object with the assigned rank

        Raises:
            RuntimeError: If the replica already has a rank assigned
        """

        def _assign_rank_impl():
            if self.has_replica_rank(replica_id):
                raise RuntimeError(
                    f"Rank for {replica_id} already assigned: {self._replica_rank_manager.get_rank(replica_id)}"
                )

            # Track the replica-to-node mapping
            self._replica_to_node[replica_id] = node_id

            # Assign global rank
            rank = self._replica_rank_manager.assign_rank(replica_id)

            # Assign node rank if this node doesn't have one yet
            if node_id not in self._local_rank_managers:
                self._node_rank_manager.assign_rank(node_id)
                self._local_rank_managers[node_id] = RankManager()

            node_rank = self._node_rank_manager.get_rank(node_id)
            # Assign local rank within the node
            local_rank = self._local_rank_managers[node_id].assign_rank(replica_id)

            return ReplicaRank(rank=rank, node_rank=node_rank, local_rank=local_rank)

        return self._execute_with_error_handling(
            _assign_rank_impl, ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )

    def release_rank(self, replica_id: str) -> None:
        """Release rank for a replica.

        Args:
            replica_id: ID of the replica

        Raises:
            RuntimeError: If replica doesn't have ranks
        """

        def _release_rank_impl():
            if not self.has_replica_rank(replica_id):
                raise RuntimeError(f"Rank for {replica_id} not assigned")

            # Get the node_id from the replica mapping
            node_id = self._replica_to_node[replica_id]

            # Release global rank
            self._replica_rank_manager.release_rank(replica_id)

            # Release local rank
            self._local_rank_managers[node_id].release_rank(replica_id)

            # Release node rank if this was the last replica on the node
            if len(self._local_rank_managers[node_id].get_ranks_mapping()) == 0:
                self._node_rank_manager.release_rank(node_id)
                del self._local_rank_managers[node_id]

            # Remove replica from node mapping
            del self._replica_to_node[replica_id]

        return self._execute_with_error_handling(_release_rank_impl, None)

    def recover_rank(
        self,
        replica_id: str,
        node_id: str,
        rank: ReplicaRank,
    ) -> None:
        """Recover rank for a replica (e.g., after controller restart).

        Args:
            replica_id: ID of the replica
            node_id: ID of the node
            rank: The rank to recover

        Raises:
            RuntimeError: If replica already has ranks assigned
        """

        def _recover_rank_impl():
            if self.has_replica_rank(replica_id):
                raise RuntimeError(
                    f"Rank for {replica_id} already assigned: {self._replica_rank_manager.get_rank(replica_id)}"
                )

            # Recover global rank
            self._replica_rank_manager.recover_rank(replica_id, rank.rank)

            # Recover node rank only if this node doesn't already have one
            if not self._node_rank_manager.has_rank(node_id):
                self._node_rank_manager.recover_rank(node_id, rank.node_rank)

            # Recover local rank
            if node_id not in self._local_rank_managers:
                self._local_rank_managers[node_id] = RankManager()
            self._local_rank_managers[node_id].recover_rank(replica_id, rank.local_rank)

            # Track the replica-to-node mapping
            self._replica_to_node[replica_id] = node_id

        return self._execute_with_error_handling(_recover_rank_impl, None)

    def has_replica_rank(self, replica_id: str) -> bool:
        """Check if replica has a rank assigned.

        Args:
            replica_id: The unique ID of the replica

        Returns:
            True if the replica has a rank assigned, False otherwise

        Raises:
            RuntimeError: If the replica doesn't have ranks assigned
        """
        if replica_id not in self._replica_to_node:
            return False

        node_id = self._replica_to_node[replica_id]
        return (
            self._replica_rank_manager.has_rank(replica_id)
            and node_id in self._local_rank_managers
            and self._node_rank_manager.has_rank(node_id)
            and self._local_rank_managers[node_id].has_rank(replica_id)
        )

    def get_replica_rank(self, replica_id: str) -> ReplicaRank:
        """Get the rank for a replica.

        Args:
            replica_id: ID of the replica

        Returns:
            ReplicaRank object

        Raises:
            RuntimeError: If replica doesn't have ranks assigned
        """

        def _get_replica_rank_impl():
            if not self.has_replica_rank(replica_id):
                raise RuntimeError(f"Rank for {replica_id} not assigned")

            global_rank = self._replica_rank_manager.get_rank(replica_id)
            node_id = self._replica_to_node[replica_id]
            node_rank = self._node_rank_manager.get_rank(node_id)
            local_rank = self._local_rank_managers[node_id].get_rank(replica_id)
            return ReplicaRank(
                rank=global_rank, node_rank=node_rank, local_rank=local_rank
            )

        return self._execute_with_error_handling(
            _get_replica_rank_impl, ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )

    def check_rank_consistency_and_reassign_minimally(
        self,
        active_replicas: List["DeploymentReplica"],
    ) -> List["DeploymentReplica"]:
        """Verify rank system invariants and reassign ranks when needed across all three levels.

        This method ensures:
        1. Global ranks are contiguous [0, N-1] for N replicas
        2. Node ranks are contiguous [0, M-1] for M nodes
        3. Local ranks are contiguous [0, K-1] for K replicas on each node

        Args:
            active_replicas: List of currently active replicas

        Returns:
            List of replicas that need to be reconfigured with new ranks
        """

        def _check_rank_consistency_impl():
            if not active_replicas:
                return []

            # Extract replica IDs from replicas
            active_replica_ids = [
                replica.replica_id.unique_id for replica in active_replicas
            ]

            # Create a mapping from replica ID to replica object for quick lookup
            replica_id_to_replica = {
                replica.replica_id.unique_id: replica for replica in active_replicas
            }

            # Track all replicas needing reconfiguration from any rank system
            all_replica_ids_needing_reconfiguration = set()

            # STEP 1: Check global rank consistency
            replica_ids_from_global = self._replica_rank_manager.check_rank_consistency_and_reassign_minimally(
                active_replica_ids
            )
            all_replica_ids_needing_reconfiguration.update(replica_ids_from_global)

            # STEP 2: Group replicas by node and check local rank consistency per node
            replicas_by_node: Dict[str, List[str]] = {}
            for replica_id in active_replica_ids:
                node_id = self._replica_to_node.get(replica_id)
                assert (
                    node_id is not None
                ), f"Replica {replica_id} not assigned to any node"
                if node_id not in replicas_by_node:
                    replicas_by_node[node_id] = []
                replicas_by_node[node_id].append(replica_id)

            for node_id, replica_ids_on_node in replicas_by_node.items():
                replica_ids_from_local = self._local_rank_managers[
                    node_id
                ].check_rank_consistency_and_reassign_minimally(replica_ids_on_node)
                all_replica_ids_needing_reconfiguration.update(replica_ids_from_local)

            # STEP 3: Check node rank consistency
            active_node_ids = list(replicas_by_node.keys())
            if active_node_ids:
                node_ids_needing_reassignment = self._node_rank_manager.check_rank_consistency_and_reassign_minimally(
                    active_node_ids,
                )
                # If any nodes were reassigned, all replicas on those nodes need reconfiguration
                for node_id in node_ids_needing_reassignment:
                    all_replica_ids_needing_reconfiguration.update(
                        replicas_by_node[node_id]
                    )

            # Convert replica IDs back to replica objects
            # Filter out stale replicas that are not in the active set
            replicas_needing_reconfiguration = [
                replica_id_to_replica[replica_id]
                for replica_id in all_replica_ids_needing_reconfiguration
                if replica_id in replica_id_to_replica
            ]

            return replicas_needing_reconfiguration

        return self._execute_with_error_handling(_check_rank_consistency_impl, [])

    def clear(self) -> None:
        self._replica_rank_manager.clear()
        self._node_rank_manager.clear()
        self._local_rank_managers.clear()
        self._replica_to_node.clear()

    def get_replica_ranks_mapping(self) -> Dict[str, ReplicaRank]:
        """Get the current mapping of replica IDs to ReplicaRank objects.

        Returns:
            Dict mapping replica_id to ReplicaRank object
        """
        result = {}
        for replica_id in self._replica_rank_manager.get_ranks_mapping().keys():
            result[replica_id] = self.get_replica_rank(replica_id)
        return result


class DeploymentState:
    """Manages the target state and replicas for a single deployment."""

    FORCE_STOP_UNHEALTHY_REPLICAS = RAY_SERVE_FORCE_STOP_UNHEALTHY_REPLICAS
    MAX_CONSTRUCTOR_RETRY_COUNT_WARNING_LOGGED = False

    def __init__(
        self,
        id: DeploymentID,
        long_poll_host: LongPollHost,
        deployment_scheduler: DeploymentScheduler,
        cluster_node_info_cache: ClusterNodeInfoCache,
        autoscaling_state_manager: AutoscalingStateManager,
    ):
        self._id = id
        self._long_poll_host: LongPollHost = long_poll_host
        self._deployment_scheduler = deployment_scheduler
        self._cluster_node_info_cache = cluster_node_info_cache
        self._autoscaling_state_manager = autoscaling_state_manager

        # Each time we set a new deployment goal, we're trying to save new
        # DeploymentInfo and bring current deployment to meet new status.
        self._target_state: DeploymentTargetState = DeploymentTargetState.default()

        self._prev_startup_warning: float = time.time()
        self._replica_constructor_error_msg: Optional[str] = None
        # Counter for how many times replicas failed to start. This is reset to 0 when:
        # (1) The deployment is deployed / re-deployed.
        # (2) The deployment reaches the HEALTHY state.
        self._replica_constructor_retry_counter: int = 0
        # Flag for whether any replicas of the target version has successfully started.
        # This is reset to False when the deployment is re-deployed.
        self._replica_has_started: bool = False

        self._replicas: ReplicaStateContainer = ReplicaStateContainer()
        self._curr_status_info: DeploymentStatusInfo = DeploymentStatusInfo(
            self._id.name,
            DeploymentStatus.UPDATING,
            DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
        )

        self._rank_manager = DeploymentRankManager(
            fail_on_rank_error=RAY_SERVE_FAIL_ON_RANK_ERROR
        )

        self.replica_average_ongoing_requests: Dict[str, float] = {}

        self.health_check_gauge = metrics.Gauge(
            "serve_deployment_replica_healthy",
            description=(
                "Tracks whether this deployment replica is healthy. 1 means "
                "healthy, 0 means unhealthy."
            ),
            tag_keys=("deployment", "replica", "application"),
        )
        self.health_check_gauge.set_default_tags(
            {"deployment": self._id.name, "application": self._id.app_name}
        )

        # Histogram for replica startup latency (time from creation to ready state).
        self.replica_startup_latency_histogram = metrics.Histogram(
            "serve_replica_startup_latency_ms",
            description=("Time from replica creation to ready state in milliseconds."),
            boundaries=REPLICA_STARTUP_SHUTDOWN_LATENCY_BUCKETS_MS,
            tag_keys=("deployment", "replica", "application"),
        )
        self.replica_startup_latency_histogram.set_default_tags(
            {"deployment": self._id.name, "application": self._id.app_name}
        )

        # Histogram for replica initialization latency.
        self.replica_initialization_latency_histogram = metrics.Histogram(
            "serve_replica_initialization_latency_ms",
            description=("Time for replica to initialize in milliseconds."),
            boundaries=REPLICA_STARTUP_SHUTDOWN_LATENCY_BUCKETS_MS,
            tag_keys=("deployment", "replica", "application"),
        )
        self.replica_initialization_latency_histogram.set_default_tags(
            {"deployment": self._id.name, "application": self._id.app_name}
        )

        # Histogram for replica reconfigure latency.
        # NOTE(abrar): value of this metric represents reconfigure + time until next controller loop
        self.replica_reconfigure_latency_histogram = metrics.Histogram(
            "serve_replica_reconfigure_latency_ms",
            description=("Time for replica to complete reconfigure in milliseconds."),
            boundaries=REQUEST_LATENCY_BUCKETS_MS,
            tag_keys=("deployment", "replica", "application"),
        )
        self.replica_reconfigure_latency_histogram.set_default_tags(
            {"deployment": self._id.name, "application": self._id.app_name}
        )

        # Histogram for health check latency.
        self.health_check_latency_histogram = metrics.Histogram(
            "serve_health_check_latency_ms",
            description=("Duration of health check calls in milliseconds."),
            boundaries=REQUEST_LATENCY_BUCKETS_MS,
            tag_keys=("deployment", "replica", "application"),
        )
        self.health_check_latency_histogram.set_default_tags(
            {"deployment": self._id.name, "application": self._id.app_name}
        )

        # Counter for health check failures.
        self.health_check_failures_counter = metrics.Counter(
            "serve_health_check_failures_total",
            description=("Count of failed health checks."),
            tag_keys=("deployment", "replica", "application"),
        )
        self.health_check_failures_counter.set_default_tags(
            {"deployment": self._id.name, "application": self._id.app_name}
        )

        # Histogram for replica shutdown duration.
        self.replica_shutdown_duration_histogram = metrics.Histogram(
            "serve_replica_shutdown_duration_ms",
            description=(
                "Time from shutdown signal to replica fully stopped in milliseconds."
            ),
            boundaries=REPLICA_STARTUP_SHUTDOWN_LATENCY_BUCKETS_MS,
            tag_keys=("deployment", "replica", "application"),
        )
        self.replica_shutdown_duration_histogram.set_default_tags(
            {"deployment": self._id.name, "application": self._id.app_name}
        )

        self.target_replicas_gauge = metrics.Gauge(
            "serve_autoscaling_target_replicas",
            description=(
                "The target number of replicas for this deployment. "
                "This is the number the autoscaler is trying to reach."
            ),
            tag_keys=("deployment", "application"),
        )
        self.target_replicas_gauge.set_default_tags(
            {"deployment": self._id.name, "application": self._id.app_name}
        )

        # Whether the request routing info have been updated since the last
        # time we checked.
        self._request_routing_info_updated = False

        self._last_broadcasted_running_replica_infos: List[RunningReplicaInfo] = []
        self._last_broadcasted_availability: bool = True
        self._last_broadcasted_deployment_config = None

        self._docs_path: Optional[str] = None
        self._route_patterns: Optional[List[str]] = None

    def should_autoscale(self) -> bool:
        """
        Check if the deployment is under autoscaling
        """
        return self._autoscaling_state_manager.should_autoscale_deployment(self._id)

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
    def docs_path(self) -> Optional[str]:
        return self._docs_path

    @property
    def route_patterns(self) -> Optional[List[str]]:
        return self._route_patterns

    @property
    def _failed_to_start_threshold(self) -> int:
        # Use global override if set, otherwise use deployment config
        value = MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT
        if value is not None and not self.MAX_CONSTRUCTOR_RETRY_COUNT_WARNING_LOGGED:
            logger.warning(
                "MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT is deprecated and will be removed in the future. "
                "Please use 'max_constructor_retry_count' instead in configurations."
            )
            self.MAX_CONSTRUCTOR_RETRY_COUNT_WARNING_LOGGED = True
        base_retry_count = (
            value
            if value is not None
            else self._target_state.info.deployment_config.max_constructor_retry_count
        )

        return min(
            base_retry_count,
            self._target_state.target_num_replicas * MAX_PER_REPLICA_RETRY_COUNT,
        )

    def _replica_startup_failing(self) -> bool:
        """Check whether replicas are currently failing and the number of
        failures has exceeded a threshold.
        """
        return (
            self._target_state.target_num_replicas > 0
            and self._replica_constructor_retry_counter
            >= self._failed_to_start_threshold
        )

    def _terminally_failed(self) -> bool:
        """Check whether the current version is terminally errored.

        The version is considered terminally errored if the number of
        replica failures has exceeded a threshold, and there hasn't been
        any replicas of the target version that has successfully started.
        """
        return not self._replica_has_started and self._replica_startup_failing()

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
        is_available = not self._terminally_failed()

        running_replicas_changed = (
            set(self._last_broadcasted_running_replica_infos)
            != set(running_replica_infos)
            or self._request_routing_info_updated
        )
        availability_changed = is_available != self._last_broadcasted_availability
        if not running_replicas_changed and not availability_changed:
            return

        deployment_metadata = DeploymentTargetInfo(
            is_available=is_available,
            running_replicas=running_replica_infos,
        )
        self._long_poll_host.notify_changed(
            {
                (
                    LongPollNamespace.DEPLOYMENT_TARGETS,
                    self._id,
                ): deployment_metadata,
                # NOTE(zcin): notify changed for Java routers. Since Java only
                # supports 1.x API, there is no concept of applications in Java,
                # so the key should remain a string describing the deployment
                # name. If there are no Java routers, this is a no-op.
                (
                    LongPollNamespace.DEPLOYMENT_TARGETS,
                    self._id.name,
                ): deployment_metadata,
            }
        )
        self._last_broadcasted_running_replica_infos = running_replica_infos
        self._last_broadcasted_availability = is_available
        self._request_routing_info_updated = False

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
        target_state = DeploymentTargetState.create(
            info=self._target_state.info,
            target_num_replicas=0,
            deleting=True,
        )

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
        updated_via_api: bool = False,
    ) -> None:
        """Set the target state for the deployment to the provided info.

        Args:
            target_info: The info with which to set the target state.
            target_num_replicas: The number of replicas that this deployment
                should attempt to run.
            status_trigger: The driver that triggered this change of state.
            updated_via_api: Whether the target state update was triggered via API.
        """
        new_target_state = DeploymentTargetState.create(
            target_info, target_num_replicas, deleting=False
        )

        if self._target_state.version == new_target_state.version:
            # Record either num replica or autoscaling config lightweight update
            if (
                self._target_state.version.deployment_config.autoscaling_config
                != new_target_state.version.deployment_config.autoscaling_config
            ):
                ServeUsageTag.AUTOSCALING_CONFIG_LIGHTWEIGHT_UPDATED.record("True")
            elif updated_via_api:
                ServeUsageTag.NUM_REPLICAS_VIA_API_CALL_UPDATED.record("True")
            elif (
                self._target_state.version.deployment_config.num_replicas
                != new_target_state.version.deployment_config.num_replicas
            ):
                ServeUsageTag.NUM_REPLICAS_LIGHTWEIGHT_UPDATED.record("True")

        self._target_state = new_target_state

        # Emit target replicas metric
        self.target_replicas_gauge.set(target_num_replicas)

    def deploy(self, deployment_info: DeploymentInfo) -> bool:
        """Deploy the deployment.

        If the deployment already exists with the same version, config,
        target_capacity, and target_capacity_direction,
        this method returns False.

        Returns:
            bool: Whether the target state has changed.
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
                or curr_deployment_info.route_prefix != deployment_info.route_prefix
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

        logger.debug(f"Deploying '{self._id}': {deployment_info.to_dict()}")
        logger.debug(
            f"Current target state for '{self._id}': "
            f"{self._target_state.info.to_dict() if self._target_state.info is not None else None}"
        )

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
        # Although the else branch handles the CONFIG_UPDATE, we also take this branch
        # for a config update whose only effect is changing `num_replicas`.
        # Treating it as a scaling event keeps the user-visible deployment status more
        # consistent for observability.
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
        self._replica_has_started = False
        return True

    def autoscale(self, decision_num_replicas: int) -> bool:
        """
        Apply the given scaling decision by updating the target replica count.

        Skips if deleting, if `decision_num_replicas` is None, or matches the
        current target. Otherwise updates the state and logs an up/down scaling.

        Args:
            decision_num_replicas: target replica count to apply.

        Returns:
            bool: True if the target state was updated, False if no change occurred.
        """

        if self._target_state.deleting:
            return False

        if decision_num_replicas == self._target_state.target_num_replicas:
            return False

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
            return True

        curr_stats_str = (
            f"Current ongoing requests: "
            f"{self._autoscaling_state_manager.get_total_num_requests_for_deployment(self._id):.2f}, "
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
            self._autoscaling_state_manager.record_scale_up(self._id)
        elif new_num < old_num:
            logger.info(
                f"Downscaling {self._id} from {old_num} to {new_num} replicas. "
                f"{curr_stats_str}"
            )
            self._curr_status_info = self._curr_status_info.handle_transition(
                trigger=DeploymentStatusInternalTrigger.AUTOSCALE_DOWN,
                message=f"Downscaling from {old_num} to {new_num} replicas.",
            )
            self._autoscaling_state_manager.record_scale_down(self._id)

        return True

    def delete(self) -> bool:
        if not self._target_state.deleting:
            self._set_target_state_deleting()
            return True

        return False

    def set_target_num_replicas(
        self,
        target_num_replicas: int,
    ) -> None:
        """Set the target state for the deployment to the provided info."""
        self._set_target_state(
            self._target_state.info, target_num_replicas, updated_via_api=True
        )

    def _stop_or_update_outdated_version_replicas(self, max_to_stop=math.inf) -> bool:
        """Stop or update replicas with outdated versions.

        Stop replicas with versions that require the actor to be restarted, and
        reconfigure replicas that require refreshing deployment config values.

        Args:
            max_to_stop: max number of replicas to stop, by default,
                         it stops all replicas with an outdated version.
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
                # Get current rank for the replica
                current_rank = self._rank_manager.get_replica_rank(
                    replica.replica_id.unique_id
                )
                actor_updating = replica.reconfigure(
                    self._target_state.version, rank=current_rank.rank
                )
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
            if to_add > 0 and not self._terminally_failed():
                logger.info(f"Adding {to_add} replica{'s' * (to_add>1)} to {self._id}.")
                for _ in range(to_add):
                    replica_id = ReplicaID(get_random_string(), deployment_id=self._id)

                    new_deployment_replica = DeploymentReplica(
                        replica_id,
                        self._target_state.version,
                    )
                    scheduling_request = new_deployment_replica.start(
                        self._target_state.info,
                        assign_rank_callback=self._rank_manager.assign_rank,
                    )

                    upscale.append(scheduling_request)

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

        # Got to make a call to complete current deploy() goal after
        # start failure threshold reached, while we might still have
        # pending replicas in current goal.
        if running_at_target_version_replica_cnt > 0:
            # At least one RUNNING replica at target state, partial
            # success; We can stop tracking constructor failures and
            # leave it to the controller to fully scale to target
            # number of replicas and only return as completed once
            # reached target replica count
            self._replica_has_started = True
        elif self._replica_startup_failing():
            self._curr_status_info = self._curr_status_info.handle_transition(
                trigger=DeploymentStatusInternalTrigger.REPLICA_STARTUP_FAILED,
                message=(
                    "The deployment failed to start "
                    f"{self._replica_constructor_retry_counter} times "
                    "in a row. This may be due to a problem with its "
                    "constructor or initial health check failing. See "
                    "controller logs for details. Error:\n"
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
                self._replica_constructor_retry_counter = 0
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
        for replica in self._replicas.pop(states=[original_state]):
            start_status, error_msg = replica.check_started()
            if start_status == ReplicaStartupStatus.SUCCEEDED:
                if original_state == ReplicaState.RECOVERING:
                    # If the previous state was RECOVERING, that mean the replica
                    # crashed and is now starting up again. We need to recover the rank
                    # from the replica actor. The invariant is that the rank is assigned
                    # during startup and before the replica is added to the replicas
                    # data structure with RUNNING state.
                    # Recover rank from the replica actor during controller restart
                    replica_id = replica.replica_id.unique_id
                    self._rank_manager.recover_rank(
                        replica_id, replica.actor_node_id, replica.rank
                    )
                # This replica should be now be added to handle's replica
                # set.
                self._replicas.add(ReplicaState.RUNNING, replica)
                self._deployment_scheduler.on_replica_running(
                    replica.replica_id, replica.actor_node_id
                )

                # if replica version is the same as the target version,
                # we update the docs path and route patterns
                if replica.version == self._target_state.version:
                    self._docs_path = replica.docs_path
                    self._route_patterns = replica.route_patterns

                # Log the startup latency.
                e2e_replica_start_latency = time.time() - replica._start_time
                replica_startup_message = (
                    f"{replica.replica_id} started successfully "
                    f"on node '{replica.actor_node_id}' after "
                    f"{e2e_replica_start_latency:.1f}s (PID: {replica.actor_pid})."
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

                # Record startup or reconfigure latency metrics.
                metric_tags = {
                    "replica": replica.replica_id.unique_id,
                }
                if original_state == ReplicaState.STARTING:
                    # Record replica startup latency (end-to-end from creation to ready).
                    # This includes the time taken from starting a node, scheduling the replica,
                    # and the replica constructor.
                    e2e_replica_start_latency_ms = e2e_replica_start_latency * 1000
                    self.replica_startup_latency_histogram.observe(
                        e2e_replica_start_latency_ms, tags=metric_tags
                    )
                    # Record replica initialization latency.
                    if replica.initialization_latency_s is not None:
                        initialization_latency_ms = (
                            replica.initialization_latency_s * 1000
                        )
                        self.replica_initialization_latency_histogram.observe(
                            initialization_latency_ms, tags=metric_tags
                        )
                elif original_state == ReplicaState.UPDATING:
                    # Record replica reconfigure latency.
                    if replica.reconfigure_start_time is not None:
                        reconfigure_latency_ms = (
                            time.time() - replica.reconfigure_start_time
                        ) * 1000
                        self.replica_reconfigure_latency_histogram.observe(
                            reconfigure_latency_ms, tags=metric_tags
                        )

            elif start_status == ReplicaStartupStatus.FAILED:
                # Replica reconfigure (deploy / upgrade) failed
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

        return slow_replicas

    def record_replica_startup_failure(self, error_msg: str):
        """Record that a replica failed to start."""

        # There is no need to record replica failures if the target is 0.
        if self._target_state.target_num_replicas == 0:
            return

        # Increase startup failure counter
        self._replica_constructor_retry_counter += 1
        self._replica_constructor_error_msg = error_msg

        # Update the deployment message only if replicas are failing during
        # the very first time the controller is trying to start replicas of
        # this version.
        retrying_msg = ""
        if not self._replica_has_started:
            remaining_retries = max(
                self._failed_to_start_threshold
                - self._replica_constructor_retry_counter,
                0,
            )
            retrying_msg = f" {remaining_retries} more time(s)"

        message = (
            f"A replica failed to start with exception. Retrying{retrying_msg}. "
            f"Error:\n{error_msg}"
        )
        self._curr_status_info = self._curr_status_info.update_message(message)

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
                "replica": replica.replica_id.unique_id,
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
            is_healthy = replica.check_health()

            # Record health check latency and failure metrics.
            metric_tags = {
                "replica": replica.replica_id.unique_id,
            }
            if replica.last_health_check_latency_ms is not None:
                self.health_check_latency_histogram.observe(
                    replica.last_health_check_latency_ms, tags=metric_tags
                )
            if replica.last_health_check_failed:
                self.health_check_failures_counter.inc(tags=metric_tags)

            if is_healthy:
                self._replicas.add(replica.actor_details.state, replica)
                self.health_check_gauge.set(
                    1,
                    tags={
                        "replica": replica.replica_id.unique_id,
                    },
                )
                routing_stats = replica.pull_routing_stats()
                replica.record_routing_stats(routing_stats)
            else:
                logger.warning(
                    f"Replica {replica.replica_id} failed health check, stopping it."
                )
                self.health_check_gauge.set(
                    0,
                    tags={
                        "replica": replica.replica_id.unique_id,
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
                    f"have taken more than {SLOW_STARTUP_WARNING_S}s to be scheduled. "
                    "This may be due to waiting for the cluster to auto-scale or for a "
                    "runtime environment to be installed. "
                    f"Resources required for each replica: {required}, "
                    f"total resources available: {available}. "
                    "Use `ray status` for more details."
                )
                logger.warning(message)
                if _SCALING_LOG_ENABLED:
                    print_verbose_scaling_log()
                # If status is UNHEALTHY, leave the status and message as is.
                # The issue that caused the deployment to be unhealthy should be
                # prioritized over this resource availability issue.
                if self._curr_status_info.status not in [
                    DeploymentStatus.UNHEALTHY,
                    DeploymentStatus.DEPLOY_FAILED,
                ]:
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
                if self._curr_status_info.status not in [
                    DeploymentStatus.UNHEALTHY,
                    DeploymentStatus.DEPLOY_FAILED,
                ]:
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

                # Record shutdown duration metric.
                if replica.shutdown_start_time is not None:
                    shutdown_duration_ms = (
                        time.time() - replica.shutdown_start_time
                    ) * 1000
                    self.replica_shutdown_duration_histogram.observe(
                        shutdown_duration_ms,
                        tags={
                            "replica": replica.replica_id.unique_id,
                        },
                    )

                # Release rank only after replica is successfully stopped
                # This ensures rank is available during draining/graceful shutdown
                replica_id = replica.replica_id.unique_id
                if self._rank_manager.has_replica_rank(replica_id):
                    # Only release rank if assigned. Replicas that failed allocation
                    # or never reached RUNNING state won't have ranks.
                    self._rank_manager.release_rank(replica_id)
                    logger.debug(
                        f"Released rank from replica {replica_id} in deployment {self._id}"
                    )
                self._autoscaling_state_manager.on_replica_stopped(replica.replica_id)

        # After replica state updates, check rank consistency and perform minimal reassignment if needed
        # This ensures ranks are continuous after lifecycle events
        # Only do consistency check when deployment is stable (not during active updates)
        # maybe this constraint need to be relaxed in the future. The implication is that
        # if we delay the rank reassignment, the rank system will be in an invalid state
        # for a longer period of time. Abrar made this decision because he is not confident
        # about how rollouts work in the deployment state machine.
        active_replicas = self._replicas.get()
        if (
            active_replicas
            and self._curr_status_info.status == DeploymentStatus.HEALTHY
        ):
            replicas_to_reconfigure = (
                self._rank_manager.check_rank_consistency_and_reassign_minimally(
                    active_replicas,
                )
            )

            # Reconfigure replicas that had their ranks reassigned
            self._reconfigure_replicas_with_new_ranks(replicas_to_reconfigure)

    def _reconfigure_replicas_with_new_ranks(
        self, replicas_to_reconfigure: List["DeploymentReplica"]
    ):
        """Reconfigure replicas with their new ranks after reassignment.
        This uses the reconfigure() mechanism to update replicas with their new ranks.
        """
        if not replicas_to_reconfigure:
            return

        logger.debug(
            f"Reconfiguring {len(replicas_to_reconfigure)} replicas with rank changes in deployment {self._id}"
        )

        updated_count = 0
        for replica in replicas_to_reconfigure:
            replica_id = replica.replica_id.unique_id
            new_rank = self._rank_manager.get_replica_rank(replica_id)

            # Use reconfigure() to update rank
            # World size is calculated automatically from deployment config
            _ = replica.reconfigure(
                self._target_state.version,
                rank=new_rank,
            )
            updated_count += 1

        logger.debug(
            f"Successfully reconfigured {updated_count} replicas with new ranks in deployment {self._id}"
        )

    def _get_replica_ranks_mapping(self) -> Dict[str, ReplicaRank]:
        """Get the current mapping of replica IDs to ReplicaRank objects.

        Returns:
            Dictionary mapping replica_id to ReplicaRank object (with rank, node_rank, local_rank).
        """
        return self._rank_manager.get_replica_ranks_mapping()

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
        to_stop = []
        remaining = []

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

    def record_request_routing_info(self, info: RequestRoutingInfo) -> None:
        """Records the multiplexed model IDs of a replica.

        Args:
            info: RequestRoutingInfo including deployment name, replica tag,
                multiplex model ids, and routing stats.
        """
        # Find the replica
        for replica in self._replicas.get():
            if replica.replica_id == info.replica_id:
                if info.multiplexed_model_ids is not None:
                    replica.record_multiplexed_model_ids(info.multiplexed_model_ids)
                if info.routing_stats is not None:
                    replica.record_routing_stats(info.routing_stats)
                self._request_routing_info_updated = True
                return

        logger.warning(f"{info.replica_id} not found.")

    def _stop_one_running_replica_for_testing(self):
        running_replicas = self._replicas.pop(states=[ReplicaState.RUNNING])
        replica_to_stop = running_replicas.pop()
        replica_to_stop.stop(graceful=False)
        self._replicas.add(ReplicaState.STOPPING, replica_to_stop)
        for replica in running_replicas:
            self._replicas.add(ReplicaState.RUNNING, replica)

    def is_ingress(self) -> bool:
        return self._target_state.info.ingress

    def get_outbound_deployments(self) -> Optional[List[DeploymentID]]:
        """Get the outbound deployments.

        Returns:
            Sorted list of deployment IDs that this deployment calls. None if
            outbound deployments are not yet polled.
        """
        result: Set[DeploymentID] = set()
        has_outbound_deployments = False
        for replica in self._replicas.get([ReplicaState.RUNNING]):
            if replica.version != self._target_state.version:
                # Only consider replicas of the target version
                continue
            outbound_deployments = replica.get_outbound_deployments()
            if outbound_deployments is not None:
                result.update(outbound_deployments)
                has_outbound_deployments = True
        if not has_outbound_deployments:
            return None
        return sorted(result, key=lambda d: (d.name))


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

        self._shutting_down = False

        self._deployment_states: Dict[DeploymentID, DeploymentState] = {}
        self._app_deployment_mapping: Dict[str, Set[str]] = defaultdict(set)

        # Metric for tracking deployment status
        self._deployment_status_gauge = ray_metrics.Gauge(
            "serve_deployment_status",
            description=(
                "Numeric status of deployment. "
                "0=UNKNOWN, 1=DEPLOY_FAILED, 2=UNHEALTHY, 3=UPDATING, "
                "4=UPSCALING, 5=DOWNSCALING, 6=HEALTHY."
            ),
            tag_keys=("deployment", "application"),
        )

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
                self._app_deployment_mapping[deployment_id.app_name].add(
                    deployment_id.name
                )

    def shutdown(self):
        """
        Shutdown all running replicas by notifying the controller, and leave
        it to the controller event loop to take actions afterwards.

        Once shutdown signal is received, it will also prevent any new
        deployments or replicas from being created.

        One can send multiple shutdown signals but won't effectively make any
        difference compare to calling it once.
        """
        self._shutting_down = True

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
            self._shutting_down
            and len(self._deployment_states) == 0
            and self._kv_store.get(CHECKPOINT_KEY) is None
        )

    def save_checkpoint(self) -> None:
        """Write a checkpoint of all deployment states."""
        if self._shutting_down:
            # Once we're told to shut down, stop writing checkpoints.
            # Calling .shutdown() deletes any existing checkpoint.
            return

        deployment_state_info = {
            deployment_id: deployment_state.get_checkpoint_data()
            for deployment_id, deployment_state in self._deployment_states.items()
        }

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

    def get_deployment_docs_path(self, deployment_id: DeploymentID) -> Optional[str]:
        if deployment_id in self._deployment_states:
            return self._deployment_states[deployment_id].docs_path

    def get_deployment_route_patterns(
        self, deployment_id: DeploymentID
    ) -> Optional[List[str]]:
        """Get route patterns for a deployment if available."""
        if deployment_id in self._deployment_states:
            return self._deployment_states[deployment_id].route_patterns
        return None

    def get_deployment_target_num_replicas(
        self, deployment_id: DeploymentID
    ) -> Optional[int]:
        if deployment_id not in self._deployment_states:
            return None
        return self._deployment_states[deployment_id].target_num_replicas

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
                required_resources=deployment_state.target_info.replica_config.resource_dict,
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
            bool: Whether the target state has changed.
        """
        if deployment_id not in self._deployment_states:
            self._deployment_states[deployment_id] = self._create_deployment_state(
                deployment_id
            )
            self._app_deployment_mapping[deployment_id.app_name].add(deployment_id.name)
            self._record_deployment_usage()

        return self._deployment_states[deployment_id].deploy(deployment_info)

    def get_deployments_in_application(self, app_name: str) -> List[str]:
        """Return list of deployment names in application."""
        return list(self._app_deployment_mapping[app_name])

    def delete_deployment(self, id: DeploymentID):
        # This method must be idempotent. We should validate that the
        # specified deployment exists on the client.
        if id in self._deployment_states:
            return self._deployment_states[id].delete()

        return False

    def _validate_deployment_state_for_num_replica_update(
        self, deployment_id: DeploymentID
    ):
        """Validate the state of a deployment for num replica update."""
        statuses = self.get_deployment_statuses([deployment_id])

        if statuses is None or len(statuses) == 0:
            raise ValueError(f"Deployment {deployment_id} not found")
        elif statuses[0].status_trigger == DeploymentStatusTrigger.DELETING:
            raise DeploymentIsBeingDeletedError(
                f"Deployment {deployment_id} is being deleted. Scaling operations are not allowed."
            )

    def set_target_num_replicas(
        self, deployment_id: DeploymentID, target_num_replicas: int
    ):
        """Set target number of replicas for a deployment."""
        self._validate_deployment_state_for_num_replica_update(deployment_id)

        deployment_state = self._deployment_states[deployment_id]
        if target_num_replicas != deployment_state.target_num_replicas:
            logger.info(
                f"Target number of replicas changed from {deployment_state.target_num_replicas} to {target_num_replicas} for deployment {deployment_id}"
            )
            deployment_state.set_target_num_replicas(target_num_replicas)
            self.save_checkpoint()
        else:
            logger.info(
                f"Skipping updating target number of replicas as it did not change for deployment {deployment_id}"
            )

    def update(self) -> bool:
        """Updates the state of all deployments to match their goal state.

        Returns True if any of the deployments have replicas in the RECOVERING state.
        """

        deleted_ids = []
        any_recovering = False
        upscales: Dict[DeploymentID, List[ReplicaSchedulingRequest]] = {}
        downscales: Dict[DeploymentID, DeploymentDownscaleRequest] = {}
        target_state_changed = False

        # STEP 1: Update current state
        for deployment_state in self._deployment_states.values():
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
        if RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY:
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

        # STEP 8: Record deployment status metrics
        for deployment_id, deployment_state in self._deployment_states.items():
            status = deployment_state.curr_status_info.status
            self._deployment_status_gauge.set(
                status.to_numeric(),
                tags={
                    "deployment": deployment_id.name,
                    "application": deployment_id.app_name,
                },
            )

        # STEP 9: Cleanup
        for deployment_id in deleted_ids:
            self._deployment_scheduler.on_deployment_deleted(deployment_id)
            self._autoscaling_state_manager.deregister_deployment(deployment_id)
            del self._deployment_states[deployment_id]
            if (
                deployment_id.app_name in self._app_deployment_mapping
                and deployment_id.name
                in self._app_deployment_mapping[deployment_id.app_name]
            ):
                self._app_deployment_mapping[deployment_id.app_name].remove(
                    deployment_id.name
                )
                # Clean up the app_name entry if no deployments are left
                if not self._app_deployment_mapping[deployment_id.app_name]:
                    del self._app_deployment_mapping[deployment_id.app_name]

        if len(deleted_ids):
            self._record_deployment_usage()

        if target_state_changed:
            self.save_checkpoint()

        return any_recovering

    def autoscale(self, deployment_id: DeploymentID, target_num_replicas: int) -> bool:
        """Autoscale the deployment to the target number of replicas.

        Args:
            deployment_id: The deployment ID.
            target_num_replicas: The target number of replicas.

        Returns:
            True if the deployment was autoscaled, False otherwise.
        """
        if deployment_id not in self._deployment_states:
            return False

        return self._deployment_states[deployment_id].autoscale(target_num_replicas)

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

    def record_request_routing_info(self, info: RequestRoutingInfo) -> None:
        """
        Record request routing information for a replica.

        Args:
            info: Request routing info including deployment name, replica tag,
                multiplex model ids, and routing stats.
        """
        deployment_id = info.replica_id.deployment_id
        if deployment_id not in self._deployment_states:
            app_msg = f" in application '{deployment_id.app_name}'"
            logger.error(
                f"Deployment '{deployment_id.name}'{app_msg} not found in state "
                "manager."
            )
            return
        self._deployment_states[deployment_id].record_request_routing_info(info)

    def get_active_node_ids(self) -> Set[str]:
        """Return set of node ids with running replicas of any deployment.

        This is used to determine which node has replicas. Only nodes with replicas and
        head node should have active proxies.
        """
        node_ids = set()
        for deployment_state in self._deployment_states.values():
            node_ids.update(deployment_state.get_active_node_ids())
        return node_ids

    def get_ingress_replicas_info(self) -> List[Tuple[str, str, int, int]]:
        """Get all ingress replicas info for all deployments."""
        ingress_replicas_list = [
            deployment_state._replicas.get()
            for deployment_state in self._deployment_states.values()
            if deployment_state.is_ingress()
        ]

        ingress_replicas_info = []
        for replicas in ingress_replicas_list:
            for replica in replicas:
                ingress_replicas_info.append(
                    (
                        replica.actor_node_id,
                        replica.replica_id.unique_id,
                        replica.actor_http_port,
                        replica.actor_grpc_port,
                    )
                )
        return ingress_replicas_info

    def _get_replica_ranks_mapping(
        self, deployment_id: DeploymentID
    ) -> Dict[str, ReplicaRank]:
        """Get the current rank mapping for all replicas in a deployment.
        Args:
            deployment_id: The deployment ID to get ranks for.
        Returns:
            Dictionary mapping replica_id to ReplicaRank object (with rank, node_rank, local_rank).
        """
        deployment_state = self._deployment_states.get(deployment_id)
        if deployment_state is None:
            return {}

        return deployment_state._get_replica_ranks_mapping()

    def get_deployment_outbound_deployments(
        self, deployment_id: DeploymentID
    ) -> Optional[List[DeploymentID]]:
        """Get the cached outbound deployments for a specific deployment.

        Args:
            deployment_id: The deployment ID to get outbound deployments for.

        Returns:
            List of deployment IDs that this deployment calls, or None if
            the deployment doesn't exist or hasn't been polled yet.
        """
        deployment_state = self._deployment_states.get(deployment_id)
        if deployment_state is None:
            return None

        return deployment_state.get_outbound_deployments()

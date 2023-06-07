from dataclasses import dataclass
import itertools
import json
import logging
import math
import os
import random
import time
import traceback
from collections import defaultdict, OrderedDict
from copy import copy
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import ray
from ray import ObjectRef, cloudpickle
from ray._private.usage.usage_lib import (
    TagKey,
    record_extra_usage_tag,
)
from ray.actor import ActorHandle
from ray.exceptions import RayActorError, RayError, RayTaskError

from ray.serve._private.autoscaling_metrics import InMemoryMetricsStore
from ray.serve._private.common import (
    DeploymentInfo,
    DeploymentStatus,
    DeploymentStatusInfo,
    Duration,
    ReplicaName,
    ReplicaTag,
    RunningReplicaInfo,
    ReplicaState,
    MultiplexedReplicaInfo,
)
from ray.serve.schema import (
    DeploymentDetails,
    ReplicaDetails,
    _deployment_info_to_schema,
)
from ray.serve.config import DeploymentConfig
from ray.serve._private.constants import (
    DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_S,
    DEFAULT_HEALTH_CHECK_PERIOD_S,
    DEFAULT_HEALTH_CHECK_TIMEOUT_S,
    DEFAULT_MAX_CONCURRENT_QUERIES,
    MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT,
    MAX_NUM_DELETED_DEPLOYMENTS,
    REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve.generated.serve_pb2 import DeploymentLanguage
from ray.serve._private.long_poll import LongPollHost, LongPollNamespace
from ray.serve._private.storage.kv_store import KVStoreBase
from ray.serve._private.utils import (
    JavaActorHandleProxy,
    format_actor_name,
    get_random_letters,
    msgpack_serialize,
    msgpack_deserialize,
    get_all_node_ids,
)
from ray.serve._private.version import DeploymentVersion, VersionedReplica

from ray.serve import metrics
from ray._raylet import GcsClient
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

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
    info: Optional[DeploymentInfo]
    num_replicas: int
    version: Optional[DeploymentVersion]
    deleting: bool

    @classmethod
    def default(cls) -> "DeploymentTargetState":
        return cls(None, -1, None, False)

    @classmethod
    def from_deployment_info(
        cls, info: DeploymentInfo, *, deleting: bool = False
    ) -> "DeploymentTargetState":
        if deleting:
            num_replicas = 0
            version = None
        else:
            # If autoscaling config is not none, num replicas should be decided based on
            # the autoscaling policy and passed in as autoscaled_num_replicas
            if info.autoscaled_num_replicas is not None:
                num_replicas = info.autoscaled_num_replicas
            else:
                num_replicas = info.deployment_config.num_replicas
            version = DeploymentVersion(
                info.version,
                deployment_config=info.deployment_config,
                ray_actor_options=info.replica_config.ray_actor_options,
            )

        return cls(info, num_replicas, version, deleting)


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


def rank_replicas_for_stopping(
    all_available_replicas: List["DeploymentReplica"],
) -> List["DeploymentReplica"]:
    """Prioritize replicas that have fewest copies on a node.

    This algorithm helps to scale down more intelligently because it can
    relinquish node faster. Note that this algorithm doesn't consider other
    deployments or other actors on the same node. See more at
    https://github.com/ray-project/ray/issues/20599.
    """
    # Categorize replicas to node they belong to.
    node_to_replicas = defaultdict(list)
    for replica in all_available_replicas:
        node_to_replicas[replica.actor_node_id].append(replica)

    # Replicas not in running state might have _node_id = None.
    # We will prioritize those first.
    node_to_replicas.setdefault(None, [])
    return list(
        itertools.chain.from_iterable(
            [
                node_to_replicas.pop(None),
            ]
            + sorted(node_to_replicas.values(), key=lambda lst: len(lst))
        )
    )


class ActorReplicaWrapper:
    """Wraps a Ray actor for a deployment replica.

    This is primarily defined so that we can mock out actual Ray operations
    for unit testing.

    *All Ray API calls should be made here, not in DeploymentState.*
    """

    def __init__(
        self,
        actor_name: str,
        detached: bool,
        controller_name: str,
        replica_tag: ReplicaTag,
        deployment_name: str,
        # Spread replicas to avoid correlated failures on a single node.
        # This is a soft spread, so if there is only space on a single node
        # the replicas will be placed there.
        scheduling_strategy: Union[str, NodeAffinitySchedulingStrategy] = "SPREAD",
    ):
        self._actor_name = actor_name
        self._detached = detached
        self._controller_name = controller_name

        self._replica_tag = replica_tag
        self._deployment_name = deployment_name

        # Populated in either self.start() or self.recover()
        self._allocated_obj_ref: ObjectRef = None
        self._ready_obj_ref: ObjectRef = None

        self._actor_resources: Dict[str, float] = None
        self._version: DeploymentVersion = None
        self._healthy: bool = True
        self._health_check_ref: Optional[ObjectRef] = None
        self._last_health_check_time: float = 0.0
        self._consecutive_health_check_failures = 0
        # NOTE: storing these is necessary to keep the actor and PG alive in
        # the non-detached case.
        self._actor_handle: ActorHandle = None

        self._pid: int = None
        self._actor_id: str = None
        self._worker_id: str = None
        if isinstance(scheduling_strategy, NodeAffinitySchedulingStrategy):
            self._node_id = scheduling_strategy.node_id
        else:
            # Populated after replica is allocated.
            self._node_id: str = None
        self._node_ip: str = None
        self._log_file_path: str = None

        # Populated in self.stop().
        self._graceful_shutdown_ref: ObjectRef = None

        # todo: will be confused with deployment_config.is_cross_language
        self._is_cross_language = False
        self._deployment_is_cross_language = False

        self.scheduling_strategy = scheduling_strategy

    @property
    def replica_tag(self) -> str:
        return self._replica_tag

    @property
    def deployment_name(self) -> str:
        return self._deployment_name

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
    def version(self) -> Optional[DeploymentVersion]:
        return self._version

    @property
    def deployment_config(self) -> Optional[DeploymentConfig]:
        if self._version:
            return self._version.deployment_config

    @property
    def max_concurrent_queries(self) -> int:
        if self.deployment_config:
            return self.deployment_config.max_concurrent_queries
        else:
            # The value in deployment_config won't be respected.
            # Issue: https://github.com/ray-project/ray/issues/36035
            logger.warn(
                "Deployment config is not found, "
                "using default value for max_concurrent_queries"
            )
            return DEFAULT_MAX_CONCURRENT_QUERIES

    @property
    def graceful_shutdown_timeout_s(self) -> float:
        if self.deployment_config:
            return self.deployment_config.graceful_shutdown_timeout_s
        else:
            # The value in deployment_config won't be respected.
            # Issue: https://github.com/ray-project/ray/issues/36035
            logger.warn(
                "Deployment config is not found, "
                "using default value for graceful_shutdown_timeout_s"
            )
            return DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_S

    @property
    def health_check_period_s(self) -> float:
        if self.deployment_config:
            return self.deployment_config.health_check_period_s
        else:
            # The value in deployment_config won't be respected.
            # Issue: https://github.com/ray-project/ray/issues/36035
            logger.warn(
                "Deployment config is not found, "
                "using default value for health_check_period_s"
            )
            return DEFAULT_HEALTH_CHECK_PERIOD_S

    @property
    def health_check_timeout_s(self) -> float:
        if self.deployment_config:
            return self.deployment_config.health_check_timeout_s
        else:
            # The value in deployment_config won't be respected.
            # Issue: https://github.com/ray-project/ray/issues/36035
            logger.warn(
                "Deployment config is not found, "
                "using default value for health_check_timeout_s"
            )
            return DEFAULT_HEALTH_CHECK_TIMEOUT_S

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

    def _check_obj_ref_ready(self, obj_ref: ObjectRef) -> bool:
        ready, _ = ray.wait([obj_ref], timeout=0)
        return len(ready) == 1

    def start(self, deployment_info: DeploymentInfo, version: DeploymentVersion):
        """
        Start a new actor for current DeploymentReplica instance.
        """
        self._version = version

        self._actor_resources = deployment_info.replica_config.resource_dict
        # it is currently not possible to create a placement group
        # with no resources (https://github.com/ray-project/ray/issues/20401)
        self._deployment_is_cross_language = (
            deployment_info.deployment_config.is_cross_language
        )

        logger.info(
            f"Starting replica {self.replica_tag} for deployment "
            f"{self.deployment_name}.",
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
                self.deployment_name,
                self.replica_tag,
                cloudpickle.dumps(deployment_info.replica_config.deployment_def)
                if self._deployment_is_cross_language
                else deployment_info.replica_config.serialized_deployment_def,
                serialized_init_args,
                deployment_info.replica_config.serialized_init_kwargs
                if deployment_info.replica_config.serialized_init_kwargs
                else cloudpickle.dumps({}),
                deployment_info.deployment_config.to_proto_bytes(),
                version,
                self._controller_name,
                self._detached,
                deployment_info.app_name,
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
                # String replicaTag,
                self.replica_tag,
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
                version.to_proto().SerializeToString(),
                # String controllerName
                self._controller_name,
            )

        self._actor_handle = actor_def.options(
            name=self._actor_name,
            namespace=SERVE_NAMESPACE,
            lifetime="detached" if self._detached else None,
            scheduling_strategy=self.scheduling_strategy,
            **deployment_info.replica_config.ray_actor_options,
        ).remote(*init_args)

        # Perform auto method name translation for java handles.
        # See https://github.com/ray-project/ray/issues/21474
        deployment_config = copy(deployment_info.deployment_config)
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
                # Ensure that `is_allocated` will execute before `reconfigure`,
                # because `reconfigure` runs user code that could block the replica
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

    def recover(self):
        """
        Recover states in DeploymentReplica instance by fetching running actor
        status
        """
        logger.info(
            f"Recovering replica {self.replica_tag} for deployment "
            f"{self.deployment_name}."
        )
        self._actor_handle = self.actor_handle

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

    def check_ready(self) -> Tuple[ReplicaStartupStatus, Optional[str]]:
        """
        Check if current replica has started by making ray API calls on
        relevant actor / object ref.

        Replica initialization calls __init__(), reconfigure(), and check_health().

        Returns:
            state (ReplicaStartupStatus):
                PENDING_ALLOCATION:
                    - replica is waiting for a worker to start
                PENDING_INITIALIZATION
                    - replica initialization hasn't finished.
                FAILED:
                    - replica initialization failed.
                SUCCEEDED:
                    - replica initialization succeeded.
            error_msg:
                None:
                    - for PENDING_ALLOCATION, PENDING_INITIALIZATION or SUCCEEDED states
                str:
                    - for FAILED state
        """

        # Check whether the replica has been allocated.
        if not self._check_obj_ref_ready(self._allocated_obj_ref):
            return ReplicaStartupStatus.PENDING_ALLOCATION, None

        # Check whether relica initialization has completed.
        replica_ready = self._check_obj_ref_ready(self._ready_obj_ref)
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
                    _, self._version = ray.get(self._ready_obj_ref)

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
                    f"Exception in replica '{self._replica_tag}', "
                    "the replica will be stopped."
                )
                # NOTE(zcin): we should use str(e) instead of traceback.format_exc()
                # here because the full details of the error is not displayed properly
                # with traceback.format_exc().
                return ReplicaStartupStatus.FAILED, str(e.as_instanceof_cause())
            except Exception as e:
                logger.exception(
                    f"Exception in replica '{self._replica_tag}', "
                    "the replica will be stopped."
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
            self._graceful_shutdown_ref = handle.prepare_for_shutdown.remote()
        except ValueError:
            pass

        return self.graceful_shutdown_timeout_s

    def check_stopped(self) -> bool:
        """Check if the actor has exited."""
        try:
            handle = ray.get_actor(self._actor_name, namespace=SERVE_NAMESPACE)
            stopped = self._check_obj_ref_ready(self._graceful_shutdown_ref)
            if stopped:
                ray.kill(handle, no_restart=True)
        except ValueError:
            stopped = True

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
        elif self._check_obj_ref_ready(self._health_check_ref):
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
                logger.warning(
                    f"Health check for replica {self._replica_tag} failed: {e}"
                )
                response = ReplicaHealthCheckResponse.APP_FAILURE
        elif time.time() - self._last_health_check_time > self.health_check_timeout_s:
            # Health check hasn't returned and the timeout is up, consider it failed.
            logger.warning(
                "Didn't receive health check response for replica "
                f"{self._replica_tag} after "
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
                    f"Replica {self._replica_tag} failed the health "
                    f"check {self._consecutive_health_check_failures} "
                    "times in a row, marking it unhealthy."
                )
                self._healthy = False
        elif response is ReplicaHealthCheckResponse.ACTOR_CRASHED:
            # Actor crashed, mark the replica unhealthy immediately.
            logger.warning(
                f"Actor for replica {self._replica_tag} crashed, marking "
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


class DeploymentReplica(VersionedReplica):
    """Manages state transitions for deployment replicas.

    This is basically a checkpointable lightweight state machine.
    """

    def __init__(
        self,
        controller_name: str,
        detached: bool,
        replica_tag: ReplicaTag,
        deployment_name: str,
        version: DeploymentVersion,
        # Spread replicas to avoid correlated failures on a single node.
        # This is a soft spread, so if there is only space on a single node
        # the replicas will be placed there.
        scheduling_strategy: Union[str, NodeAffinitySchedulingStrategy] = "SPREAD",
    ):
        self._actor = ActorReplicaWrapper(
            f"{ReplicaName.prefix}{format_actor_name(replica_tag)}",
            detached,
            controller_name,
            replica_tag,
            deployment_name,
            scheduling_strategy,
        )
        self._controller_name = controller_name
        self._deployment_name = deployment_name
        self._replica_tag = replica_tag
        self._start_time = None
        self._prev_slow_startup_warning_time = None
        self._actor_details = ReplicaDetails(
            actor_name=self._actor._actor_name,
            replica_id=self._replica_tag,
            state=ReplicaState.STARTING,
            start_time_s=0,
        )
        self._multiplexed_model_ids: List = []

    def get_running_replica_info(self) -> RunningReplicaInfo:
        return RunningReplicaInfo(
            deployment_name=self._deployment_name,
            replica_tag=self._replica_tag,
            actor_handle=self._actor.actor_handle,
            max_concurrent_queries=self._actor.max_concurrent_queries,
            is_cross_language=self._actor.is_cross_language,
            multiplexed_model_ids=self.multiplexed_model_ids,
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
    def replica_tag(self) -> ReplicaTag:
        return self._replica_tag

    @property
    def deployment_name(self) -> str:
        return self._deployment_name

    @property
    def version(self):
        return self._actor.version

    @property
    def actor_handle(self) -> ActorHandle:
        return self._actor.actor_handle

    @property
    def actor_node_id(self) -> Optional[str]:
        """Returns the node id of the actor, None if not placed."""
        return self._actor.node_id

    def start(self, deployment_info: DeploymentInfo, version: DeploymentVersion):
        """
        Start a new actor for current DeploymentReplica instance.
        """
        self._actor.start(deployment_info, version)
        self._start_time = time.time()
        self._prev_slow_startup_warning_time = time.time()
        self.update_actor_details(start_time_s=self._start_time)

    def reconfigure(self, version: DeploymentVersion) -> bool:
        """
        Update replica version. Also, updates the deployment config on the actor
        behind this DeploymentReplica instance if necessary.

        Returns: whether the actor is being updated.
        """
        return self._actor.reconfigure(version)

    def recover(self):
        """
        Recover states in DeploymentReplica instance by fetching running actor
        status
        """
        self._actor.recover()
        self._start_time = time.time()
        self.update_actor_details(start_time_s=self._start_time)

    def check_started(self) -> Tuple[ReplicaStartupStatus, Optional[str]]:
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
        logger.info(
            f"Stopping replica {self.replica_tag} for deployment "
            f"{self.deployment_name}.",
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

        timeout_passed = time.time() > self._shutdown_deadline
        if timeout_passed:
            # Graceful period passed, kill it forcefully.
            # This will be called repeatedly until the replica shuts down.
            logger.info(
                f"Replica {self.replica_tag} did not shut down after grace "
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
        # NOTE(edoakes):
        if self._actor.actor_resources is None:
            return "UNKNOWN", "UNKNOWN"

        required = {
            k: v
            for k, v in self._actor.actor_resources.items()
            if v is not None and v > 0
        }
        available = {
            k: v for k, v in self._actor.available_resources.items() if k in required
        }

        # Use json.dumps() instead of str() here to avoid double-quoting keys
        # when dumping these dictionaries. See
        # https://github.com/ray-project/ray/issues/26210 for the issue.
        return json.dumps(required), json.dumps(available)


class ReplicaStateContainer:
    """Container for mapping ReplicaStates to lists of DeploymentReplicas."""

    def __init__(self):
        self._replicas: Dict[ReplicaState, List[DeploymentReplica]] = defaultdict(list)

    def add(self, state: ReplicaState, replica: VersionedReplica):
        """Add the provided replica under the provided state.

        Args:
            state: state to add the replica under.
            replica: replica to add.
        """
        assert isinstance(state, ReplicaState)
        assert isinstance(replica, VersionedReplica)
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
        ranking_function: Optional[
            Callable[[List["DeploymentReplica"]], List["DeploymentReplica"]]
        ] = None,
    ) -> List[VersionedReplica]:
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
            ranking_function: optional function to sort the replicas
                within each state before they are truncated to max_replicas and
                returned.
        """
        if states is None:
            states = ALL_REPLICA_STATES

        assert exclude_version is None or isinstance(exclude_version, DeploymentVersion)
        assert isinstance(states, list)

        replicas = []
        for state in states:
            popped = []
            remaining = []

            replicas_to_process = self._replicas[state]
            if ranking_function:
                replicas_to_process = ranking_function(replicas_to_process)

            for replica in replicas_to_process:
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

    def __init__(
        self,
        name: str,
        controller_name: str,
        detached: bool,
        long_poll_host: LongPollHost,
        _save_checkpoint_func: Callable,
    ):

        self._name = name
        self._controller_name: str = controller_name
        self._detached: bool = detached
        self._long_poll_host: LongPollHost = long_poll_host
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
            self._name, DeploymentStatus.UPDATING
        )

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

    def should_autoscale(self) -> bool:
        """
        Check if the deployment is under autoscaling
        """
        return self._target_state.info.autoscaling_policy is not None

    def get_autoscale_metric_lookback_period(self) -> float:
        """
        Return the autoscaling metrics look back period
        """
        return self._target_state.info.autoscaling_policy.config.look_back_period_s

    def get_checkpoint_data(self) -> DeploymentTargetState:
        """
        Return deployment's target state submitted by user's deployment call.
        Should be persisted and outlive current ray cluster.
        """
        return self._target_state

    def recover_target_state_from_checkpoint(
        self, target_state_checkpoint: DeploymentTargetState
    ):
        logger.info(
            f"Recovering target state for deployment {self._name} from checkpoint."
        )
        self._target_state = target_state_checkpoint

    def recover_current_state_from_replica_actor_names(
        self, replica_actor_names: List[str]
    ):
        assert self._target_state is not None, (
            "Target state should be recovered successfully first before "
            "recovering current state from replica actor names."
        )

        logger.info(
            "Recovering current state for deployment "
            f"{self._name} from {len(replica_actor_names)} total actors."
        )
        # All current states use default value, only attach running replicas.
        for replica_actor_name in replica_actor_names:
            replica_name: ReplicaName = ReplicaName.from_str(replica_actor_name)
            new_deployment_replica = DeploymentReplica(
                self._controller_name,
                self._detached,
                replica_name.replica_tag,
                replica_name.deployment_tag,
                None,
            )
            new_deployment_replica.recover()
            self._replicas.add(ReplicaState.RECOVERING, new_deployment_replica)
            logger.debug(
                f"RECOVERING replica: {new_deployment_replica.replica_tag}, "
                f"deployment: {self._name}."
            )

        # TODO(jiaodong): this currently halts all traffic in the cluster
        # briefly because we will broadcast a replica update with everything in
        # RECOVERING. We should have a grace period where we recover the state
        # of the replicas before doing this update.

    @property
    def target_info(self) -> DeploymentInfo:
        return self._target_state.info

    @property
    def curr_status_info(self) -> DeploymentStatusInfo:
        return self._curr_status_info

    @property
    def app_name(self) -> str:
        if self.target_info.app_name:
            return self.target_info.app_name
        return ""

    def get_running_replica_infos(self) -> List[RunningReplicaInfo]:
        return [
            replica.get_running_replica_info()
            for replica in self._replicas.get([ReplicaState.RUNNING])
        ]

    def list_replica_details(self) -> List[ReplicaDetails]:
        return [replica.actor_details for replica in self._replicas.get()]

    def _notify_running_replicas_changed(self):
        self._long_poll_host.notify_changed(
            (LongPollNamespace.RUNNING_REPLICAS, self._name),
            self.get_running_replica_infos(),
        )

    def _set_target_state_deleting(self) -> None:
        """Set the target state for the deployment to be deleted."""

        # We must write ahead the target state in case of GCS failure (we don't
        # want to set the target state, then fail because we can't checkpoint it).
        target_state = DeploymentTargetState.from_deployment_info(
            self._target_state.info, deleting=True
        )
        self._save_checkpoint_func(writeahead_checkpoints={self._name: target_state})

        self._target_state = target_state
        self._curr_status_info = DeploymentStatusInfo(
            self._name, DeploymentStatus.UPDATING
        )
        logger.info(f"Deleting deployment {self._name}.")

    def _set_target_state(self, target_info: DeploymentInfo) -> None:
        """Set the target state for the deployment to the provided info."""

        # We must write ahead the target state in case of GCS failure (we don't
        # want to set the target state, then fail because we can't checkpoint it).
        target_state = DeploymentTargetState.from_deployment_info(target_info)
        self._save_checkpoint_func(writeahead_checkpoints={self._name: target_state})

        if self._target_state.version == target_state.version:
            # Record either num replica or autoscaling config lightweight update
            if (
                self._target_state.version.deployment_config.autoscaling_config
                != target_state.version.deployment_config.autoscaling_config
            ):
                record_extra_usage_tag(
                    TagKey.SERVE_AUTOSCALING_CONFIG_LIGHTWEIGHT_UPDATED, "True"
                )
            elif (
                self._target_state.version.deployment_config.num_replicas
                != target_state.version.deployment_config.num_replicas
            ):
                record_extra_usage_tag(
                    TagKey.SERVE_NUM_REPLICAS_LIGHTWEIGHT_UPDATED, "True"
                )

        self._target_state = target_state
        self._curr_status_info = DeploymentStatusInfo(
            self._name, DeploymentStatus.UPDATING
        )
        self._replica_constructor_retry_counter = 0
        self._backoff_time_s = 1

        logger.info(f"Deploying new version of deployment {self._name}.")

    def deploy(self, deployment_info: DeploymentInfo) -> bool:
        """Deploy the deployment.

        If the deployment already exists with the same version and config,
        this is a no-op and returns False.

        Returns:
            bool: Whether or not the deployment is being updated.
        """
        # Ensures this method is idempotent.
        existing_info = self._target_state.info
        if existing_info is not None:
            # Redeploying should not reset the deployment's start time.
            if not self._target_state.deleting:
                deployment_info.start_time_ms = existing_info.start_time_ms

            if (
                not self._target_state.deleting
                and existing_info.deployment_config == deployment_info.deployment_config
                and existing_info.replica_config.ray_actor_options
                == deployment_info.replica_config.ray_actor_options
                and deployment_info.version is not None
                and existing_info.version == deployment_info.version
            ):
                return False

        # If autoscaling config is not none, decide initial num replicas
        autoscaling_config = deployment_info.deployment_config.autoscaling_config
        if autoscaling_config is not None:
            if autoscaling_config.initial_replicas is not None:
                autoscaled_num_replicas = autoscaling_config.initial_replicas
            else:
                if existing_info is not None:
                    autoscaled_num_replicas = self._target_state.num_replicas
                else:
                    autoscaled_num_replicas = autoscaling_config.min_replicas
            deployment_info.set_autoscaled_num_replicas(autoscaled_num_replicas)

        self._set_target_state(deployment_info)
        return True

    def autoscale(
        self,
        current_num_ongoing_requests: List[float],
        current_handle_queued_queries: int,
    ):
        """
        Autoscale the deployment based on metrics

        Args:
            current_num_ongoing_requests: a list of number of running requests of all
                replicas in the deployment
            current_handle_queued_queries: The number of handle queued queries,
                if there are multiple handles, the max number of queries at
                a single handle should be passed in
        """
        if self._target_state.deleting:
            return

        curr_info = self._target_state.info
        autoscaling_policy = self._target_state.info.autoscaling_policy
        decision_num_replicas = autoscaling_policy.get_decision_num_replicas(
            curr_target_num_replicas=self._target_state.num_replicas,
            current_num_ongoing_requests=current_num_ongoing_requests,
            current_handle_queued_queries=current_handle_queued_queries,
        )
        if decision_num_replicas == self._target_state.num_replicas:
            return

        new_config = copy(curr_info)
        new_config.set_autoscaled_num_replicas(decision_num_replicas)
        if new_config.version is None:
            new_config.version = self._target_state.version.code_version

        self._set_target_state(new_config)

    def delete(self) -> None:
        self._set_target_state_deleting()

    def _stop_or_update_outdated_version_replicas(self, max_to_stop=math.inf) -> int:
        """Stop or update replicas with outdated versions.

        Stop replicas with versions that require the actor to be restarted, and
        reconfigure replicas that require refreshing deployment config values.

        Args:
            max_to_stop: max number of replicas to stop, by default,
            it will stop all replicas with wrong version.
        """
        replicas_to_update = self._replicas.pop(
            exclude_version=self._target_state.version,
            states=[ReplicaState.STARTING, ReplicaState.RUNNING],
            max_replicas=max_to_stop,
            ranking_function=rank_replicas_for_stopping,
        )
        replicas_changed = False
        code_version_changes = 0
        reconfigure_changes = 0
        for replica in replicas_to_update:
            # If the new version requires the actors to be restarted, stop the replica.
            # A new one with the correct version will be started later as part of the
            # normal scale-up process.
            if replica.version.requires_actor_restart(self._target_state.version):
                code_version_changes += 1
                self._stop_replica(replica)
                replicas_changed = True
            # Otherwise, only lightweight options in deployment config is a mismatch, so
            # we update it dynamically without restarting the replica.
            else:
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
                logger.debug(
                    "Adding UPDATING to replica_tag: "
                    f"{replica.replica_tag}, deployment_name: {self._name}"
                )

        if code_version_changes > 0:
            logger.info(
                f"Stopping {code_version_changes} replicas of "
                f"deployment '{self._name}' with outdated versions."
            )

        if reconfigure_changes > 0:
            logger.info(
                f"Updating {reconfigure_changes} replicas of deployment '{self._name}' "
                "with outdated deployment configs."
            )
            # Record user config lightweight update
            record_extra_usage_tag(TagKey.SERVE_USER_CONFIG_LIGHTWEIGHT_UPDATED, "True")

        return replicas_changed

    def _check_and_stop_wrong_version_replicas(self) -> bool:
        """Stops replicas with outdated versions to implement rolling updates.

        This includes both explicit code version updates and changes to the
        user_config.

        Returns whether any replicas were stopped.
        """
        # Short circuit if target replicas is 0 (the deployment is being
        # deleted) because this will be handled in the main loop.
        if self._target_state.num_replicas == 0:
            return False

        # We include STARTING and UPDATING replicas here
        # because if there are replicas still pending startup, we may as well
        # terminate them and start new version replicas instead.
        old_running_replicas = self._replicas.count(
            exclude_version=self._target_state.version,
            states=[ReplicaState.STARTING, ReplicaState.UPDATING, ReplicaState.RUNNING],
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
            self._target_state.num_replicas
            < old_running_replicas + old_stopping_replicas
        ):
            return 0

        # The number of replicas that are currently in transition between
        # an old version and the new version. Note that we cannot directly
        # count the number of stopping replicas because once replicas finish
        # stopping, they are removed from the data structure.
        pending_replicas = (
            self._target_state.num_replicas
            - new_running_replicas
            - old_running_replicas
        )

        # Maximum number of replicas that can be updating at any given time.
        # There should never be more than rollout_size old replicas stopping
        # or rollout_size new replicas starting.
        rollout_size = max(int(0.2 * self._target_state.num_replicas), 1)
        max_to_stop = max(rollout_size - pending_replicas, 0)

        return self._stop_or_update_outdated_version_replicas(max_to_stop)

    def _scale_deployment_replicas(self) -> bool:
        """Scale the given deployment to the number of replicas."""

        assert (
            self._target_state.num_replicas >= 0
        ), "Number of replicas must be greater than or equal to 0."

        replicas_changed = self._check_and_stop_wrong_version_replicas()

        current_replicas = self._replicas.count(
            states=[ReplicaState.STARTING, ReplicaState.UPDATING, ReplicaState.RUNNING]
        )
        recovering_replicas = self._replicas.count(states=[ReplicaState.RECOVERING])

        delta_replicas = (
            self._target_state.num_replicas - current_replicas - recovering_replicas
        )
        if delta_replicas == 0:
            return replicas_changed

        elif delta_replicas > 0:
            # Don't ever exceed self._target_state.num_replicas.
            stopping_replicas = self._replicas.count(
                states=[
                    ReplicaState.STOPPING,
                ]
            )
            to_add = max(delta_replicas - stopping_replicas, 0)
            if to_add > 0:
                # Exponential backoff
                failed_to_start_threshold = min(
                    MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT,
                    self._target_state.num_replicas * 3,
                )
                if self._replica_constructor_retry_counter >= failed_to_start_threshold:
                    # Wait 1, 2, 4, ... seconds before consecutive retries, with random
                    # offset added to avoid synchronization
                    if (
                        time.time() - self._last_retry
                        < self._backoff_time_s + random.uniform(0, 3)
                    ):
                        return replicas_changed

                self._last_retry = time.time()
                logger.info(
                    f"Adding {to_add} replica{'s' if to_add > 1 else ''} "
                    f"to deployment {self._name}."
                )
                for _ in range(to_add):
                    replica_name = ReplicaName(self._name, get_random_letters())
                    new_deployment_replica = DeploymentReplica(
                        self._controller_name,
                        self._detached,
                        replica_name.replica_tag,
                        replica_name.deployment_tag,
                        self._target_state.version,
                    )
                    new_deployment_replica.start(
                        self._target_state.info, self._target_state.version
                    )

                    self._replicas.add(ReplicaState.STARTING, new_deployment_replica)
                    logger.debug(
                        "Adding STARTING to replica_tag: "
                        f"{replica_name}, deployment: {self._name}"
                    )

        elif delta_replicas < 0:
            replicas_changed = True
            to_remove = -delta_replicas
            logger.info(
                f"Removing {to_remove} replica{'s' if to_remove > 1 else ''} "
                f"from deployment '{self._name}'."
            )
            replicas_to_stop = self._replicas.pop(
                states=[
                    ReplicaState.STARTING,
                    ReplicaState.UPDATING,
                    ReplicaState.RECOVERING,
                    ReplicaState.RUNNING,
                ],
                max_replicas=to_remove,
                ranking_function=rank_replicas_for_stopping,
            )

            for replica in replicas_to_stop:
                logger.debug(
                    f"Adding STOPPING to replica_tag: {replica}, "
                    f"deployment_name: {self._name}"
                )
                self._stop_replica(replica)

        return replicas_changed

    def _check_curr_status(self) -> Tuple[bool, bool]:
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
        target_replica_count = self._target_state.num_replicas

        any_replicas_recovering = (
            self._replicas.count(states=[ReplicaState.RECOVERING]) > 0
        )
        all_running_replica_cnt = self._replicas.count(states=[ReplicaState.RUNNING])
        running_at_target_version_replica_cnt = self._replicas.count(
            states=[ReplicaState.RUNNING], version=target_version
        )

        failed_to_start_count = self._replica_constructor_retry_counter
        failed_to_start_threshold = min(
            MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT, target_replica_count * 3
        )

        # Got to make a call to complete current deploy() goal after
        # start failure threshold reached, while we might still have
        # pending replicas in current goal.
        if (
            failed_to_start_count >= failed_to_start_threshold
            and failed_to_start_threshold != 0
        ):
            if running_at_target_version_replica_cnt > 0:
                # At least one RUNNING replica at target state, partial
                # success; We can stop tracking constructor failures and
                # leave it to the controller to fully scale to target
                # number of replicas and only return as completed once
                # reached target replica count
                self._replica_constructor_retry_counter = -1
            else:
                self._curr_status_info = DeploymentStatusInfo(
                    name=self._name,
                    status=DeploymentStatus.UNHEALTHY,
                    message=(
                        f"The Deployment failed to start {failed_to_start_count} times "
                        "in a row. This may be due to a problem with the deployment "
                        "constructor or the initial health check failing. See "
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
            # Check for deleting.
            if self._target_state.deleting and all_running_replica_cnt == 0:
                return True, any_replicas_recovering

            # Check for a non-zero number of deployments.
            if target_replica_count == running_at_target_version_replica_cnt:
                self._curr_status_info = DeploymentStatusInfo(
                    self._name, DeploymentStatus.HEALTHY
                )
                return False, any_replicas_recovering

        return False, any_replicas_recovering

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
        replicas_failed = False
        for replica in self._replicas.pop(states=[original_state]):
            start_status, error_msg = replica.check_started()
            if start_status == ReplicaStartupStatus.SUCCEEDED:
                # This replica should be now be added to handle's replica
                # set.
                self._replicas.add(ReplicaState.RUNNING, replica)
                transitioned_to_running = True
                logger.info(
                    f"Replica {replica.replica_tag} started successfully.",
                    extra={"log_to_stderr": False},
                )
            elif start_status == ReplicaStartupStatus.FAILED:
                # Replica reconfigure (deploy / upgrade) failed
                if self._replica_constructor_retry_counter >= 0:
                    # Increase startup failure counter if we're tracking it
                    self._replica_constructor_retry_counter += 1
                    self._replica_constructor_error_msg = error_msg

                replicas_failed = True
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

        # If replicas have failed enough times, execute exponential backoff
        # Wait 1, 2, 4, ... seconds before consecutive retries (or use a custom
        # backoff factor by setting EXPONENTIAL_BACKOFF_FACTOR)
        failed_to_start_threshold = min(
            MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT,
            self._target_state.num_replicas * 3,
        )
        if (
            replicas_failed
            and self._replica_constructor_retry_counter > failed_to_start_threshold
        ):
            self._backoff_time_s = min(
                EXPONENTIAL_BACKOFF_FACTOR * self._backoff_time_s, MAX_BACKOFF_TIME_S
            )

        return slow_replicas, transitioned_to_running

    def _stop_replica(self, replica, graceful_stop=True):
        """Stop replica
        1. Stop the replica.
        2. Change the replica into stopping state.
        3. Set the health replica stats to 0.
        """
        replica.stop(graceful=graceful_stop)
        self._replicas.add(ReplicaState.STOPPING, replica)
        self.health_check_gauge.set(
            0,
            tags={
                "deployment": self._name,
                "replica": replica.replica_tag,
                "application": self.app_name,
            },
        )

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
                self.health_check_gauge.set(
                    1,
                    tags={
                        "deployment": self._name,
                        "replica": replica.replica_tag,
                        "application": self.app_name,
                    },
                )
            else:
                running_replicas_changed = True
                logger.warning(
                    f"Replica {replica.replica_tag} of deployment "
                    f"{self._name} failed health check, stopping it."
                )
                self.health_check_gauge.set(
                    0,
                    tags={
                        "deployment": self._name,
                        "replica": replica.replica_tag,
                        "application": self.app_name,
                    },
                )
                self._stop_replica(replica, graceful_stop=False)
                # If this is a replica of the target version, the deployment
                # enters the "UNHEALTHY" status until the replica is
                # recovered or a new deploy happens.
                if replica.version == self._target_state.version:
                    self._curr_status_info: DeploymentStatusInfo = DeploymentStatusInfo(
                        name=self._name,
                        status=DeploymentStatus.UNHEALTHY,
                        message="A replica's health check failed. This "
                        "deployment will be UNHEALTHY until the replica "
                        "recovers or a new deploy happens.",
                    )

        slow_start_replicas = []
        slow_start, starting_to_running = self._check_startup_replicas(
            ReplicaState.STARTING
        )
        slow_update, updating_to_running = self._check_startup_replicas(
            ReplicaState.UPDATING
        )
        slow_recover, recovering_to_running = self._check_startup_replicas(
            ReplicaState.RECOVERING, stop_on_slow=True
        )

        slow_start_replicas = slow_start + slow_update + slow_recover
        running_replicas_changed = (
            running_replicas_changed
            or starting_to_running
            or updating_to_running
            or recovering_to_running
        )

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
                    f'Deployment "{self._name}" has '
                    f"{len(pending_allocation)} replicas that have taken "
                    f"more than {SLOW_STARTUP_WARNING_S}s to be scheduled. "
                    f"This may be caused by waiting for the cluster to "
                    f"auto-scale, or waiting for a runtime environment "
                    f"to install. "
                    f"Resources required for each replica: {required}, "
                    f"resources available: {available}."
                )
                logger.warning(message)
                if _SCALING_LOG_ENABLED:
                    print_verbose_scaling_log()
                # If status is UNHEALTHY, leave the status and message as is.
                # The issue that caused the deployment to be unhealthy should be
                # prioritized over this resource availability issue.
                if self._curr_status_info.status != DeploymentStatus.UNHEALTHY:
                    self._curr_status_info = DeploymentStatusInfo(
                        name=self._name,
                        status=DeploymentStatus.UPDATING,
                        message=message,
                    )

            if len(pending_initialization) > 0:
                message = (
                    f"Deployment {self._name} has "
                    f"{len(pending_initialization)} replicas that have taken "
                    f"more than {SLOW_STARTUP_WARNING_S}s to initialize. This "
                    f"may be caused by a slow __init__ or reconfigure method."
                )
                logger.warning(message)
                # If status is UNHEALTHY, leave the status and message as is.
                # The issue that caused the deployment to be unhealthy should be
                # prioritized over this resource availability issue.
                if self._curr_status_info.status != DeploymentStatus.UNHEALTHY:
                    self._curr_status_info = DeploymentStatusInfo(
                        name=self._name,
                        status=DeploymentStatus.UPDATING,
                        message=message,
                    )

            self._prev_startup_warning = time.time()

        for replica in self._replicas.pop(states=[ReplicaState.STOPPING]):
            stopped = replica.check_stopped()
            if not stopped:
                self._replicas.add(ReplicaState.STOPPING, replica)

        return running_replicas_changed

    def update(self) -> Tuple[bool, bool]:
        """Attempts to reconcile this deployment to match its goal state.

        This is an asynchronous call; it's expected to be called repeatedly.

        Also updates the internal DeploymentStatusInfo based on the current
        state of the system.

        Returns (deleted, any_replicas_recovering).
        """
        deleted, any_replicas_recovering = False, False
        try:
            # Add or remove DeploymentReplica instances in self._replicas.
            # This should be the only place we adjust total number of replicas
            # we manage.

            running_replicas_changed = self._scale_deployment_replicas()

            # Check the state of existing replicas and transition if necessary.
            running_replicas_changed |= self._check_and_update_replicas()

            # Check if the model_id has changed.
            running_replicas_changed |= self._multiplexed_model_ids_updated

            if running_replicas_changed:
                self._notify_running_replicas_changed()
                self._multiplexed_model_ids_updated = False

            deleted, any_replicas_recovering = self._check_curr_status()
        except Exception:
            self._curr_status_info = DeploymentStatusInfo(
                name=self._name,
                status=DeploymentStatus.UNHEALTHY,
                message="Failed to update deployment:" f"\n{traceback.format_exc()}",
            )

        return deleted, any_replicas_recovering

    def record_multiplexed_model_ids(
        self, replica_name: str, multiplexed_model_ids: List[str]
    ) -> None:
        """Records the multiplexed model IDs of a replica.

        Args:
            replica_name: Name of the replica.
            multiplexed_model_ids: List of model IDs that replica is serving.
        """
        # Find the replica
        for replica in self._replicas.get():
            if replica.replica_tag == replica_name:
                replica.record_multiplexed_model_ids(multiplexed_model_ids)
                self._multiplexed_model_ids_updated = True
                return
        logger.warn(f"Replia {replica_name} not found in deployment {self._name}")

    def _stop_one_running_replica_for_testing(self):
        running_replicas = self._replicas.pop(states=[ReplicaState.RUNNING])
        replica_to_stop = running_replicas.pop()
        replica_to_stop.stop(graceful=False)
        self._replicas.add(ReplicaState.STOPPING, replica_to_stop)
        for replica in running_replicas:
            self._replicas.add(ReplicaState.RUNNING, replica)


class DriverDeploymentState(DeploymentState):
    """Manages the target state and replicas for a single driver deployment."""

    def __init__(
        self,
        name: str,
        controller_name: str,
        detached: bool,
        long_poll_host: LongPollHost,
        _save_checkpoint_func: Callable,
        gcs_client: GcsClient = None,
    ):
        super().__init__(
            name, controller_name, detached, long_poll_host, _save_checkpoint_func
        )
        if gcs_client:
            self._gcs_client = gcs_client
        else:
            self._gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    def _get_all_node_ids(self):
        # Test mock purpose
        return get_all_node_ids(self._gcs_client)

    def _deploy_driver(self) -> bool:
        """Deploy the driver deployment to each node."""
        all_nodes = self._get_all_node_ids()
        deployed_nodes = set()
        for replica in self._replicas.get(
            [
                ReplicaState.STARTING,
                ReplicaState.RUNNING,
                ReplicaState.RECOVERING,
                ReplicaState.UPDATING,
                ReplicaState.STOPPING,
            ]
        ):
            if replica.actor_node_id:
                deployed_nodes.add(replica.actor_node_id)
        replica_changed = False
        for node_id, _ in all_nodes:
            if node_id in deployed_nodes:
                continue
            replica_name = ReplicaName(self._name, get_random_letters())
            new_deployment_replica = DeploymentReplica(
                self._controller_name,
                self._detached,
                replica_name.replica_tag,
                replica_name.deployment_tag,
                self._target_state.version,
                NodeAffinitySchedulingStrategy(node_id, soft=False),
            )
            new_deployment_replica.start(
                self._target_state.info, self._target_state.version
            )

            self._replicas.add(ReplicaState.STARTING, new_deployment_replica)
            replica_changed = True
        return replica_changed

    def _stop_all_replicas(self) -> bool:
        replica_changed = False
        for replica in self._replicas.pop(
            states=[
                ReplicaState.STARTING,
                ReplicaState.RUNNING,
                ReplicaState.RECOVERING,
            ]
        ):
            self._stop_replica(replica)
            replica_changed = True
        return replica_changed

    def _calculate_max_replicas_to_stop(self) -> int:
        nums_nodes = len(self._get_all_node_ids())
        rollout_size = max(int(0.2 * nums_nodes), 1)
        old_running_replicas = self._replicas.count(
            exclude_version=self._target_state.version,
            states=[ReplicaState.STARTING, ReplicaState.UPDATING, ReplicaState.RUNNING],
        )
        new_running_replicas = self._replicas.count(
            version=self._target_state.version, states=[ReplicaState.RUNNING]
        )
        pending_replicas = nums_nodes - new_running_replicas - old_running_replicas
        return max(rollout_size - pending_replicas, 0)

    def update(self) -> Tuple[bool, bool]:
        """Returns (deleted, any_replicas_recovering)."""
        try:
            if self._target_state.deleting:
                self._stop_all_replicas()
            else:
                num_nodes = len(self._get_all_node_ids())
                # For driver deployment, when there are new node,
                # it is supposed to update the target state.
                if self._target_state.num_replicas != num_nodes:
                    self._target_state.num_replicas = num_nodes
                    curr_info = self._target_state.info
                    new_config = copy(curr_info)
                    new_config.deployment_config.num_replicas = num_nodes
                    if new_config.version is None:
                        new_config.version = self._target_state.version.code_version
                    self._set_target_state(new_config)
                max_to_stop = self._calculate_max_replicas_to_stop()
                self._stop_or_update_outdated_version_replicas(max_to_stop)
                self._deploy_driver()
            self._check_and_update_replicas()
            return self._check_curr_status()
        except Exception:
            self._curr_status_info = DeploymentStatusInfo(
                name=self._name,
                status=DeploymentStatus.UNHEALTHY,
                message="Failed to update deployment:" f"\n{traceback.format_exc()}",
            )
            return False, False

    def should_autoscale(self) -> bool:
        return False


class DeploymentStateManager:
    """Manages all state for deployments in the system.

    This class is *not* thread safe, so any state-modifying methods should be
    called with a lock held.
    """

    def __init__(
        self,
        controller_name: str,
        detached: bool,
        kv_store: KVStoreBase,
        long_poll_host: LongPollHost,
        all_current_actor_names: List[str],
    ):

        self._controller_name = controller_name
        self._detached = detached
        self._kv_store = kv_store
        self._long_poll_host = long_poll_host

        self._create_deployment_state: Callable = lambda name: DeploymentState(
            name,
            controller_name,
            detached,
            long_poll_host,
            self._save_checkpoint_func,
        )

        self._create_driver_deployment_state: Callable = (
            lambda name: DriverDeploymentState(
                name,
                controller_name,
                detached,
                long_poll_host,
                self._save_checkpoint_func,
            )
        )

        self._deployment_states: Dict[str, DeploymentState] = dict()
        self._deleted_deployment_metadata: Dict[str, DeploymentInfo] = OrderedDict()

        self._recover_from_checkpoint(all_current_actor_names)

        # TODO(simon): move autoscaling related stuff into a manager.
        self.autoscaling_metrics_store = InMemoryMetricsStore()
        self.handle_metrics_store = InMemoryMetricsStore()

    def record_autoscaling_metrics(self, data: Dict[str, float], send_timestamp: float):
        self.autoscaling_metrics_store.add_metrics_point(data, send_timestamp)

    def record_handle_metrics(self, data: Dict[str, float], send_timestamp: float):
        self.handle_metrics_store.add_metrics_point(data, send_timestamp)

    def get_autoscaling_metrics(self):
        """
        Return autoscaling metrics (used for dumping from controller)
        """
        return self.autoscaling_metrics_store.data

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
            if ReplicaName.is_replica_name(actor_name)
        ]
        deployment_to_current_replicas = defaultdict(list)
        if len(all_replica_names) > 0:
            # Each replica tag is formatted as "deployment_name#random_letter"
            for replica_name in all_replica_names:
                replica_tag = ReplicaName.from_str(replica_name)
                deployment_to_current_replicas[replica_tag.deployment_tag].append(
                    replica_name
                )

        return deployment_to_current_replicas

    def _recover_from_checkpoint(self, all_current_actor_names: List[str]) -> None:
        """
        Recover from checkpoint upon controller failure with all actor names
        found in current cluster.

        Each deployment resumes target state from checkpoint if available.

        For current state it will prioritize reconstructing from current
        actor names found that matches deployment tag if applicable.
        """
        deployment_to_current_replicas = self._map_actor_names_to_deployment(
            all_current_actor_names
        )
        checkpoint = self._kv_store.get(CHECKPOINT_KEY)
        if checkpoint is not None:
            (
                deployment_state_info,
                self._deleted_deployment_metadata,
            ) = cloudpickle.loads(checkpoint)

            for deployment_tag, checkpoint_data in deployment_state_info.items():
                if checkpoint_data.info.is_driver_deployment:
                    deployment_state = self._create_driver_deployment_state(
                        deployment_tag
                    )
                else:
                    deployment_state = self._create_deployment_state(deployment_tag)
                deployment_state.recover_target_state_from_checkpoint(checkpoint_data)
                if len(deployment_to_current_replicas[deployment_tag]) > 0:
                    deployment_state.recover_current_state_from_replica_actor_names(  # noqa: E501
                        deployment_to_current_replicas[deployment_tag]
                    )
                self._deployment_states[deployment_tag] = deployment_state

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
            deployment_name: deployment_state.get_checkpoint_data()
            for deployment_name, deployment_state in self._deployment_states.items()
        }

        if writeahead_checkpoints is not None:
            deployment_state_info.update(writeahead_checkpoints)

        self._kv_store.put(
            CHECKPOINT_KEY,
            cloudpickle.dumps(
                (deployment_state_info, self._deleted_deployment_metadata)
            ),
        )

    def get_running_replica_infos(
        self,
    ) -> Dict[str, List[RunningReplicaInfo]]:
        return {
            name: deployment_state.get_running_replica_infos()
            for name, deployment_state in self._deployment_states.items()
        }

    def get_deployment_configs(
        self, filter_tag: Optional[str] = None, include_deleted: Optional[bool] = False
    ) -> Dict[str, DeploymentConfig]:
        configs: Dict[str, DeploymentConfig] = {}
        for deployment_name, deployment_state in self._deployment_states.items():
            if filter_tag is None or deployment_name == filter_tag:
                configs[
                    deployment_name
                ] = deployment_state.target_info.deployment_config

        if include_deleted:
            for name, info in self._deleted_deployment_metadata.items():
                if filter_tag is None or name == filter_tag:
                    configs[name] = info.deployment_config

        return configs

    def get_deployment(
        self, deployment_name: str, include_deleted: Optional[bool] = False
    ) -> Optional[DeploymentInfo]:
        if deployment_name in self._deployment_states:
            return self._deployment_states[deployment_name].target_info
        elif include_deleted and deployment_name in self._deleted_deployment_metadata:
            return self._deleted_deployment_metadata[deployment_name]
        else:
            return None

    def get_deployment_details(
        self, deployment_name: str
    ) -> Optional[DeploymentDetails]:
        """Gets detailed info on a deployment.

        Returns:
            DeploymentDetails: if the deployment is live.
            None: if the deployment is deleted.
        """
        statuses = self.get_deployment_statuses([deployment_name])
        if len(statuses) == 0:
            return None
        else:
            status_info = statuses[0]
            return DeploymentDetails(
                name=deployment_name,
                status=status_info.status,
                message=status_info.message,
                deployment_config=_deployment_info_to_schema(
                    deployment_name, self.get_deployment(deployment_name)
                ),
                replicas=self._deployment_states[
                    deployment_name
                ].list_replica_details(),
            )

    def get_deployment_statuses(
        self, names: List[str] = None
    ) -> List[DeploymentStatusInfo]:
        statuses = []
        for name, state in self._deployment_states.items():
            if not names or name in names:
                statuses.append(state.curr_status_info)
        return statuses

    def deploy(self, deployment_name: str, deployment_info: DeploymentInfo) -> bool:
        """Deploy the deployment.

        If the deployment already exists with the same version and config,
        this is a no-op and returns False.

        Returns:
            bool: Whether or not the deployment is being updated.
        """
        if deployment_name in self._deleted_deployment_metadata:
            del self._deleted_deployment_metadata[deployment_name]

        if deployment_name not in self._deployment_states:
            if deployment_info.is_driver_deployment:
                self._deployment_states[
                    deployment_name
                ] = self._create_driver_deployment_state(deployment_name)
            else:
                self._deployment_states[
                    deployment_name
                ] = self._create_deployment_state(deployment_name)
            self._record_deployment_usage()

        return self._deployment_states[deployment_name].deploy(deployment_info)

    def get_deployments_in_application(self, app_name: str) -> List[str]:
        """Return list of deployment names in application."""
        states = []
        for name, deployment_state in self._deployment_states.items():
            if deployment_state.target_info.app_name == app_name:
                states.append(name)

        return states

    def delete_deployment(self, deployment_name: str):
        # This method must be idempotent. We should validate that the
        # specified deployment exists on the client.
        if deployment_name in self._deployment_states:
            self._deployment_states[deployment_name].delete()

    def get_replica_ongoing_request_metrics(
        self, deployment_name: str, look_back_period_s
    ) -> List[float]:
        """
        Return replica average ongoing requests
        Args:
            deployment_name: deployment name
            look_back_period_s: the look back time period to collect the requests
                metrics
        Returns:
            List of ongoing requests, the length of list indicate the number
                of replicas
        """

        replicas = self._deployment_states[deployment_name]._replicas
        running_replicas = replicas.get([ReplicaState.RUNNING])

        current_num_ongoing_requests = []
        for replica in running_replicas:
            replica_tag = replica.replica_tag
            num_ongoing_requests = self.autoscaling_metrics_store.window_average(
                replica_tag,
                time.time() - look_back_period_s,
            )
            if num_ongoing_requests is not None:
                current_num_ongoing_requests.append(num_ongoing_requests)
        return current_num_ongoing_requests

    def get_handle_queueing_metrics(
        self, deployment_name: str, look_back_period_s
    ) -> int:
        """
        Return handle queue length metrics
        Args:
            deployment_name: deployment name
            look_back_period_s: the look back time period to collect the requests
                metrics
        Returns:
            if multiple handles queue length, return the max number of queue length.
        """
        current_handle_queued_queries = self.handle_metrics_store.max(
            deployment_name,
            time.time() - look_back_period_s,
        )

        if current_handle_queued_queries is None:
            current_handle_queued_queries = 0
        return current_handle_queued_queries

    def update(self) -> bool:
        """Updates the state of all deployments to match their goal state.

        Returns True if any of the deployments have replicas in the RECOVERING state.
        """
        deleted_tags = []
        any_recovering = False
        for deployment_name, deployment_state in self._deployment_states.items():
            if deployment_state.should_autoscale():
                current_num_ongoing_requests = self.get_replica_ongoing_request_metrics(
                    deployment_name,
                    deployment_state.get_autoscale_metric_lookback_period(),
                )
                current_handle_queued_queries = self.get_handle_queueing_metrics(
                    deployment_name,
                    deployment_state.get_autoscale_metric_lookback_period(),
                )
                deployment_state.autoscale(
                    current_num_ongoing_requests, current_handle_queued_queries
                )
            deleted, recovering = deployment_state.update()
            if deleted:
                deleted_tags.append(deployment_name)
                deployment_info = deployment_state.target_info
                deployment_info.end_time_ms = int(time.time() * 1000)
                if len(self._deleted_deployment_metadata) > MAX_NUM_DELETED_DEPLOYMENTS:
                    self._deleted_deployment_metadata.popitem(last=False)
                self._deleted_deployment_metadata[deployment_name] = deployment_info

            any_recovering |= recovering

        for tag in deleted_tags:
            del self._deployment_states[tag]

        if len(deleted_tags):
            self._record_deployment_usage()

        return any_recovering

    def _record_deployment_usage(self):
        record_extra_usage_tag(
            TagKey.SERVE_NUM_DEPLOYMENTS, str(len(self._deployment_states))
        )

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
        record_extra_usage_tag(
            TagKey.SERVE_NUM_GPU_DEPLOYMENTS, str(num_gpu_deployments)
        )

    def record_multiplexed_replica_info(self, info: MultiplexedReplicaInfo):
        """
        Record multiplexed model ids for a multiplexed replica.

        Args:
            info: Multiplexed replica info including deployment name,
                replica tag and model ids.
        """
        if info.deployment_name not in self._deployment_states:
            logger.error(
                f"Deployment {info.deployment_name} not found in state manager."
            )
            return
        self._deployment_states[info.deployment_name].record_multiplexed_model_ids(
            info.replica_tag, info.model_ids
        )

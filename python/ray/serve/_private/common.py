import json
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import List, NamedTuple, Optional

from ray.actor import ActorHandle
from ray.serve.generated.serve_pb2 import ApplicationStatus as ApplicationStatusProto
from ray.serve.generated.serve_pb2 import (
    ApplicationStatusInfo as ApplicationStatusInfoProto,
)
from ray.serve.generated.serve_pb2 import DeploymentStatus as DeploymentStatusProto
from ray.serve.generated.serve_pb2 import (
    DeploymentStatusInfo as DeploymentStatusInfoProto,
)
from ray.serve.generated.serve_pb2 import (
    DeploymentStatusInfoList as DeploymentStatusInfoListProto,
)
from ray.serve.generated.serve_pb2 import (
    DeploymentStatusTrigger as DeploymentStatusTriggerProto,
)
from ray.serve.generated.serve_pb2 import StatusOverview as StatusOverviewProto


class DeploymentID(NamedTuple):
    name: str
    app: str

    def __str__(self):
        # TODO(zcin): remove this once we no longer use the concatenated
        # string for metrics
        if self.app:
            return f"{self.app}_{self.name}"
        else:
            return self.name

    def to_replica_actor_class_name(self):
        if self.app:
            return f"ServeReplica:{self.app}:{self.name}"
        else:
            return f"ServeReplica:{self.name}"


EndpointTag = DeploymentID
ReplicaTag = str
NodeId = str
Duration = float
ApplicationName = str


@dataclass
class EndpointInfo:
    route: str
    app_is_cross_language: bool = False


# Keep in sync with ServeReplicaState in dashboard/client/src/type/serve.ts
class ReplicaState(str, Enum):
    STARTING = "STARTING"
    UPDATING = "UPDATING"
    RECOVERING = "RECOVERING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"


class ApplicationStatus(str, Enum):
    NOT_STARTED = "NOT_STARTED"
    DEPLOYING = "DEPLOYING"
    DEPLOY_FAILED = "DEPLOY_FAILED"
    RUNNING = "RUNNING"
    UNHEALTHY = "UNHEALTHY"
    DELETING = "DELETING"


@dataclass(eq=True)
class ApplicationStatusInfo:
    status: ApplicationStatus
    message: str = ""
    deployment_timestamp: float = 0

    def debug_string(self):
        return json.dumps(asdict(self), indent=4)

    def to_proto(self):
        return ApplicationStatusInfoProto(
            status=f"APPLICATION_STATUS_{self.status.name}",
            message=self.message,
            deployment_timestamp=self.deployment_timestamp,
        )

    @classmethod
    def from_proto(cls, proto: ApplicationStatusInfoProto):
        status = ApplicationStatusProto.Name(proto.status)[len("APPLICATION_STATUS_") :]
        return cls(
            status=ApplicationStatus(status),
            message=proto.message,
            deployment_timestamp=proto.deployment_timestamp,
        )


class DeploymentStatus(str, Enum):
    UPDATING = "UPDATING"
    HEALTHY = "HEALTHY"
    UNHEALTHY = "UNHEALTHY"
    UPSCALING = "UPSCALING"
    DOWNSCALING = "DOWNSCALING"


class DeploymentStatusTrigger(str, Enum):
    UNSPECIFIED = "UNSPECIFIED"
    CONFIG_UPDATE_STARTED = "CONFIG_UPDATE_STARTED"
    CONFIG_UPDATE_COMPLETED = "CONFIG_UPDATE_COMPLETED"
    UPSCALE_COMPLETED = "UPSCALE_COMPLETED"
    DOWNSCALE_COMPLETED = "DOWNSCALE_COMPLETED"
    AUTOSCALING = "AUTOSCALING"
    REPLICA_STARTUP_FAILED = "REPLICA_STARTUP_FAILED"
    HEALTH_CHECK_FAILED = "HEALTH_CHECK_FAILED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    DELETING = "DELETING"


# Internal Enum used to manage deployment state machine
class DeploymentStatusTransition(str, Enum):
    HEALTHY = "HEALTHY"
    CONFIG_UPDATE = "CONFIG_UPDATE"
    AUTOSCALE_UP = "AUTOSCALE_UP"
    AUTOSCALE_DOWN = "AUTOSCALE_DOWN"
    MANUALLY_INCREASE_NUM_REPLICAS = "MANUALLY_INCREASE_NUM_REPLICAS"
    MANUALLY_DECREASE_NUM_REPLICAS = "MANUALLY_DECREASE_NUM_REPLICAS"
    REPLICA_STARTUP_FAILED = "REPLICA_STARTUP_FAILED"
    HEALTH_CHECK_FAILED = "HEALTH_CHECK_FAILED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    DELETE = "DELETE"


@dataclass(eq=True)
class DeploymentStatusInfo:
    name: str
    status: DeploymentStatus
    status_trigger: DeploymentStatusTrigger
    message: str = ""

    @staticmethod
    def ranking_order() -> List:
        return [
            # Status ranking order is defined in a following fashion:
            #   1. (Highest) Statuses signalling any failures in the system
            (DeploymentStatus.UNHEALTHY,),
            (DeploymentStatus.UPDATING,),
            (DeploymentStatus.UPSCALING, DeploymentStatusTrigger.CONFIG_UPDATE_STARTED),
            (
                DeploymentStatus.DOWNSCALING,
                DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            ),
            (DeploymentStatus.UPSCALING, DeploymentStatusTrigger.AUTOSCALING),
            (DeploymentStatus.DOWNSCALING, DeploymentStatusTrigger.AUTOSCALING),
            (DeploymentStatus.HEALTHY,),
        ]

    @property
    def rank(self) -> int:
        for i, s in enumerate(DeploymentStatusInfo.ranking_order()):
            if len(s) == 1 and s[0] == self.status:
                return i
            elif len(s) == 2 and s == (self.status, self.status_trigger):
                return i

    def debug_string(self):
        return json.dumps(asdict(self), indent=4)

    def _update(
        self,
        status: DeploymentStatus = None,
        status_trigger: DeploymentStatusTrigger = None,
        message: str = "",
    ):
        return DeploymentStatusInfo(
            name=self.name,
            status=status if status else self.status,
            status_trigger=status_trigger if status_trigger else self.status_trigger,
            message=message,
        )

    def update_message(self, message: str):
        return self._update(message=message)

    def transition(
        self,
        transition: DeploymentStatusTransition,
        message: str = "",
        is_within_autoscaling_bounds: bool = False,
    ):
        # If there was an unexpected internal error during reconciliation, set
        # status to unhealthy immediately and return
        if transition == DeploymentStatusTransition.INTERNAL_ERROR:
            return self._update(
                status=DeploymentStatus.UNHEALTHY,
                status_trigger=DeploymentStatusTrigger.INTERNAL_ERROR,
                message=message,
            )

        # If deployment is being deleted, set status immediately and return
        if transition == DeploymentStatusTransition.DELETE:
            return self._update(
                status=DeploymentStatus.UPDATING,
                status_trigger=DeploymentStatusTrigger.DELETING,
            )

        # Otherwise, go through normal state machine transitions
        if self.status == DeploymentStatus.UPDATING:
            # Finished updating configuration and transition to healthy
            if transition == DeploymentStatusTransition.HEALTHY:
                return self._update(
                    status=DeploymentStatus.HEALTHY,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED,
                    message=message,
                )

            # A new configuration has been deployed before deployment
            # has finished updating
            if transition == DeploymentStatusTransition.CONFIG_UPDATE:
                return self._update(
                    status=DeploymentStatus.UPDATING,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                    message=message,
                )

            # Autoscaling.
            if transition == DeploymentStatusTransition.AUTOSCALE_UP:
                # The deployment should only transition to UPSCALING if
                # it's within the autoscaling bounds
                if is_within_autoscaling_bounds:
                    return self._update(
                        status=DeploymentStatus.UPSCALING,
                        status_trigger=DeploymentStatusTrigger.AUTOSCALING,
                        message=message,
                    )
                # Otherwise, the status doesn't change
                else:
                    return self
            if transition == DeploymentStatusTransition.AUTOSCALE_DOWN:
                # The deployment should only transition to DOWNSCALING if
                # it's within the autoscaling bounds
                if is_within_autoscaling_bounds:
                    return self._update(
                        status=DeploymentStatus.DOWNSCALING,
                        status_trigger=DeploymentStatusTrigger.AUTOSCALING,
                        message=message,
                    )
                # Otherwise, the status doesn't change
                else:
                    return self

            # Manually increasing or decreasing num replicas does not
            # change the status while deployment is still updating.
            if transition in [
                DeploymentStatusTransition.MANUALLY_INCREASE_NUM_REPLICAS,
                DeploymentStatusTransition.MANUALLY_DECREASE_NUM_REPLICAS,
            ]:
                return self

            # Failures occurred
            if transition == DeploymentStatusTransition.HEALTH_CHECK_FAILED:
                return self._update(
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.HEALTH_CHECK_FAILED,
                    message=message,
                )
            if transition == DeploymentStatusTransition.REPLICA_STARTUP_FAILED:
                return self._update(
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.REPLICA_STARTUP_FAILED,
                    message=message,
                )

        if self.status in [DeploymentStatus.UPSCALING, DeploymentStatus.DOWNSCALING]:
            # Deployment transitions to healthy
            if transition == DeploymentStatusTransition.HEALTHY:
                return self._update(
                    status=DeploymentStatus.HEALTHY,
                    status_trigger=DeploymentStatusTrigger.UPSCALE_COMPLETED
                    if self.status == DeploymentStatus.UPSCALING
                    else DeploymentStatusTrigger.DOWNSCALE_COMPLETED,
                    message=message,
                )

            # Configuration is updated before scaling is finished
            if transition == DeploymentStatusTransition.CONFIG_UPDATE:
                return self._update(
                    status=DeploymentStatus.UPDATING,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                    message=message,
                )

            # Upscale replicas before previous upscaling/downscaling has finished
            if (
                self.status_trigger == DeploymentStatusTrigger.AUTOSCALING
                and transition == DeploymentStatusTransition.AUTOSCALE_UP
            ) or (
                self.status_trigger == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
                and transition
                == DeploymentStatusTransition.MANUALLY_INCREASE_NUM_REPLICAS
            ):
                return self._update(status=DeploymentStatus.UPSCALING, message=message)

            # Downscale replicas before previous upscaling/downscaling has finished
            if (
                self.status_trigger == DeploymentStatusTrigger.AUTOSCALING
                and transition == DeploymentStatusTransition.AUTOSCALE_DOWN
            ) or (
                self.status_trigger == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
                and transition
                == DeploymentStatusTransition.MANUALLY_DECREASE_NUM_REPLICAS
            ):
                return self._update(
                    status=DeploymentStatus.DOWNSCALING, message=message
                )

            # Failures occurred
            if transition == DeploymentStatusTransition.REPLICA_STARTUP_FAILED:
                return self._update(
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.REPLICA_STARTUP_FAILED,
                    message=message,
                )
            if transition == DeploymentStatusTransition.HEALTH_CHECK_FAILED:
                return self._update(
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.HEALTH_CHECK_FAILED,
                    message=message,
                )

        if self.status == DeploymentStatus.HEALTHY:
            # Deployment remains healthy
            if transition == DeploymentStatusTransition.HEALTHY:
                return self

            # New configuration is deployed
            if transition == DeploymentStatusTransition.CONFIG_UPDATE:
                return self._update(
                    status=DeploymentStatus.UPDATING,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                    message=message,
                )

            # Manually scaling / autoscaling num replicas
            if transition == DeploymentStatusTransition.MANUALLY_INCREASE_NUM_REPLICAS:
                return self._update(
                    status=DeploymentStatus.UPSCALING,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                    message=message,
                )
            if transition == DeploymentStatusTransition.MANUALLY_DECREASE_NUM_REPLICAS:
                return self._update(
                    status=DeploymentStatus.DOWNSCALING,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                    message=message,
                )
            if transition == DeploymentStatusTransition.AUTOSCALE_UP:
                return self._update(
                    status=DeploymentStatus.UPSCALING,
                    status_trigger=DeploymentStatusTrigger.AUTOSCALING,
                    message=message,
                )
            if transition == DeploymentStatusTransition.AUTOSCALE_DOWN:
                return self._update(
                    status=DeploymentStatus.DOWNSCALING,
                    status_trigger=DeploymentStatusTrigger.AUTOSCALING,
                    message=message,
                )

            # Health check for one or more replicas has failed
            if transition == DeploymentStatusTransition.HEALTH_CHECK_FAILED:
                return self._update(
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.HEALTH_CHECK_FAILED,
                    message=message,
                )

        if self.status == DeploymentStatus.UNHEALTHY:
            # The deployment recovered
            if transition == DeploymentStatusTransition.HEALTHY:
                return self._update(
                    status=DeploymentStatus.HEALTHY,
                    status_trigger=DeploymentStatusTrigger.UNSPECIFIED,
                    message=message,
                )

            # A new configuration is being deployed.
            elif transition == DeploymentStatusTransition.CONFIG_UPDATE:
                return self._update(
                    status=DeploymentStatus.UPDATING,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                    message=message,
                )

            # Old failures keep getting triggered, or new failures occurred.
            elif transition == DeploymentStatusTransition.HEALTH_CHECK_FAILED:
                return self._update(
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.HEALTH_CHECK_FAILED,
                    message=message,
                )
            elif transition == DeploymentStatusTransition.REPLICA_STARTUP_FAILED:
                return self._update(
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.REPLICA_STARTUP_FAILED,
                    message=message,
                )

            # If it's any other transition, ignore it.
            else:
                return self

        # Invalid state transition, this counts as an internal error.
        return self._update(
            status=DeploymentStatus.UNHEALTHY,
            status_trigger=DeploymentStatusTrigger.INTERNAL_ERROR,
            message="OH NO, INVALID STATE TRANSITION",
        )

    def to_proto(self):
        return DeploymentStatusInfoProto(
            name=self.name,
            status=f"DEPLOYMENT_STATUS_{self.status.name}",
            status_trigger=f"DEPLOYMENT_STATUS_TRIGGER_{self.status_trigger.name}",
            message=self.message,
        )

    @classmethod
    def from_proto(cls, proto: DeploymentStatusInfoProto):
        status = DeploymentStatusProto.Name(proto.status)[len("DEPLOYMENT_STATUS_") :]
        status_trigger = DeploymentStatusTriggerProto.Name(proto.status_trigger)[
            len("DEPLOYMENT_STATUS_TRIGGER_") :
        ]
        return cls(
            name=proto.name,
            status=DeploymentStatus(status),
            status_trigger=DeploymentStatusTrigger(status_trigger),
            message=proto.message,
        )


@dataclass(eq=True)
class StatusOverview:
    app_status: ApplicationStatusInfo
    name: str = ""
    deployment_statuses: List[DeploymentStatusInfo] = field(default_factory=list)

    def debug_string(self):
        return json.dumps(asdict(self), indent=4)

    def get_deployment_status(self, name: str) -> Optional[DeploymentStatusInfo]:
        """Get a deployment's status by name.

        Args:
            name: Deployment's name.

        Return (Optional[DeploymentStatusInfo]): Status with a name matching
            the argument, if one exists. Otherwise, returns None.
        """

        for deployment_status in self.deployment_statuses:
            if name == deployment_status.name:
                return deployment_status

        return None

    def to_proto(self):
        # Create a protobuf for the Serve Application info
        app_status_proto = self.app_status.to_proto()

        # Create protobufs for all individual deployment statuses
        deployment_status_protos = map(
            lambda status: status.to_proto(), self.deployment_statuses
        )

        # Create a protobuf list containing all the deployment status protobufs
        deployment_status_proto_list = DeploymentStatusInfoListProto()
        deployment_status_proto_list.deployment_status_infos.extend(
            deployment_status_protos
        )

        # Return protobuf encapsulating application and deployment protos
        return StatusOverviewProto(
            name=self.name,
            app_status=app_status_proto,
            deployment_statuses=deployment_status_proto_list,
        )

    @classmethod
    def from_proto(cls, proto: StatusOverviewProto) -> "StatusOverview":
        # Recreate Serve Application info
        app_status = ApplicationStatusInfo.from_proto(proto.app_status)

        # Recreate deployment statuses
        deployment_statuses = []
        for info_proto in proto.deployment_statuses.deployment_status_infos:
            deployment_statuses.append(DeploymentStatusInfo.from_proto(info_proto))

        # Recreate StatusInfo
        return cls(
            app_status=app_status,
            deployment_statuses=deployment_statuses,
            name=proto.name,
        )


@dataclass
class ReplicaName:
    app_name: str
    deployment_name: str
    replica_suffix: str
    replica_tag: ReplicaTag = ""
    delimiter: str = "#"
    prefix: str = "SERVE_REPLICA::"

    def __init__(self, app_name: str, deployment_name: str, replica_suffix: str):
        self.app_name = app_name
        self.deployment_name = deployment_name
        self.replica_suffix = replica_suffix
        if app_name:
            self.replica_tag = self.delimiter.join(
                [app_name, deployment_name, replica_suffix]
            )
        else:
            self.replica_tag = self.delimiter.join([deployment_name, replica_suffix])

    @property
    def deployment_id(self):
        return DeploymentID(self.deployment_name, self.app_name)

    @staticmethod
    def is_replica_name(actor_name: str) -> bool:
        return actor_name.startswith(ReplicaName.prefix)

    @classmethod
    def from_str(cls, actor_name):
        assert ReplicaName.is_replica_name(actor_name)
        # TODO(simon): this currently conforms the tag and suffix logic. We
        # can try to keep the internal name always hard coded with the prefix.
        replica_tag = actor_name.replace(cls.prefix, "")
        return ReplicaName.from_replica_tag(replica_tag)

    @classmethod
    def from_replica_tag(cls, tag):
        parsed = tag.split(cls.delimiter)
        if len(parsed) == 3:
            return cls(
                app_name=parsed[0], deployment_name=parsed[1], replica_suffix=parsed[2]
            )
        elif len(parsed) == 2:
            return cls("", deployment_name=parsed[0], replica_suffix=parsed[1])
        else:
            raise ValueError(
                f"Given replica tag {tag} didn't match pattern, please "
                "ensure it has either two or three fields with delimiter "
                f"{cls.delimiter}"
            )

    def __str__(self):
        return self.replica_tag


@dataclass(frozen=True)
class RunningReplicaInfo:
    deployment_name: str
    replica_tag: ReplicaTag
    node_id: Optional[str]
    availability_zone: Optional[str]
    actor_handle: ActorHandle
    max_concurrent_queries: int
    is_cross_language: bool = False
    multiplexed_model_ids: List[str] = field(default_factory=list)

    def __post_init__(self):
        # Set hash value when object is constructed.
        # We use _actor_id to hash the ActorHandle object
        # instead of actor_handle itself to make sure
        # it is consistently same actor handle between different
        # object ids.

        hash_val = hash(
            " ".join(
                [
                    self.deployment_name,
                    self.replica_tag,
                    self.node_id if self.node_id else "",
                    str(self.actor_handle._actor_id),
                    str(self.max_concurrent_queries),
                    str(self.is_cross_language),
                    str(self.multiplexed_model_ids),
                ]
            )
        )

        # RunningReplicaInfo class set frozen=True, this is the hacky way to set
        # new attribute for the class.
        object.__setattr__(self, "_hash", hash_val)

    def __hash__(self):
        return self._hash

    def __eq__(self, other):
        return all(
            [
                isinstance(other, RunningReplicaInfo),
                self._hash == other._hash,
            ]
        )


class ServeDeployMode(str, Enum):
    MULTI_APP = "MULTI_APP"


# Keep in sync with ServeSystemActorStatus in
# python/ray/dashboard/client/src/type/serve.ts
class ProxyStatus(str, Enum):
    STARTING = "STARTING"
    HEALTHY = "HEALTHY"
    UNHEALTHY = "UNHEALTHY"
    DRAINING = "DRAINING"
    # The DRAINED status is a momentary state
    # just before the proxy is removed
    # so this status won't show up on the dashboard.
    DRAINED = "DRAINED"


class ServeComponentType(str, Enum):
    REPLICA = "replica"


@dataclass
class MultiplexedReplicaInfo:
    deployment_id: DeploymentID
    replica_tag: str
    model_ids: List[str]


@dataclass
class gRPCRequest:
    """Sent from the GRPC proxy to replicas on both unary and streaming codepaths."""

    grpc_user_request: bytes
    grpc_proxy_handle: ActorHandle


@dataclass
class StreamingHTTPRequest:
    """Sent from the HTTP proxy to replicas on the streaming codepath."""

    pickled_asgi_scope: bytes
    http_proxy_handle: ActorHandle


class RequestProtocol(str, Enum):
    UNDEFINED = "UNDEFINED"
    HTTP = "HTTP"
    GRPC = "gRPC"


class TargetCapacityDirection(str, Enum):
    """Determines what direction the target capacity is scaling."""

    UP = "UP"
    DOWN = "DOWN"

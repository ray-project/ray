import json
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Awaitable, Callable, List, NamedTuple, Optional

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
from ray.serve.grpc_util import RayServegRPCContext


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
class DeploymentStatusInternalTrigger(str, Enum):
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


# List of states in ranked order.
#
# Each ranked state has the format of a tuple with either 1 or 2 items.
# If 1 item: contains a single DeploymentStatus, representing states with
#     that DeploymentStatus and any DeploymentStatusTrigger.
# If 2 items: tuple contains a DeploymentStatus and a DeploymentStatusTrigger,
#     representing a state with that status and status trigger.
DEPLOYMENT_STATUS_RANKING_ORDER = {
    # Status ranking order is defined in a following fashion:
    #   1. (Highest) State signalling any failures in the system
    (DeploymentStatus.UNHEALTHY,): 0,
    #   2. States signaling the user updated the configuration.
    (DeploymentStatus.UPDATING,): 1,
    (DeploymentStatus.UPSCALING, DeploymentStatusTrigger.CONFIG_UPDATE_STARTED): 1,
    (
        DeploymentStatus.DOWNSCALING,
        DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
    ): 1,
    #   3. Steady state or autoscaling.
    (DeploymentStatus.UPSCALING, DeploymentStatusTrigger.AUTOSCALING): 2,
    (DeploymentStatus.DOWNSCALING, DeploymentStatusTrigger.AUTOSCALING): 2,
    (DeploymentStatus.HEALTHY,): 2,
}


@dataclass(eq=True)
class DeploymentStatusInfo:
    name: str
    status: DeploymentStatus
    status_trigger: DeploymentStatusTrigger
    message: str = ""

    @property
    def rank(self) -> int:
        """Get priority of state based on ranking_order().

        The ranked order indicates what the status should be of a
        hierarchically "higher" resource when derived from a group of
        `DeploymentStatusInfo` sub-resources.
        """

        if (self.status,) in DEPLOYMENT_STATUS_RANKING_ORDER:
            return DEPLOYMENT_STATUS_RANKING_ORDER[(self.status,)]
        elif (self.status, self.status_trigger) in DEPLOYMENT_STATUS_RANKING_ORDER:
            return DEPLOYMENT_STATUS_RANKING_ORDER[(self.status, self.status_trigger)]

    def debug_string(self):
        return json.dumps(asdict(self), indent=4)

    def _updated_copy(
        self,
        status: DeploymentStatus = None,
        status_trigger: DeploymentStatusTrigger = None,
        message: str = "",
    ):
        """Returns a copy of the current object with the passed in kwargs updated."""

        return DeploymentStatusInfo(
            name=self.name,
            status=status if status else self.status,
            status_trigger=status_trigger if status_trigger else self.status_trigger,
            message=message,
        )

    def update_message(self, message: str):
        return self._updated_copy(message=message)

    def handle_transition(
        self,
        trigger: DeploymentStatusInternalTrigger,
        message: str = "",
    ) -> "DeploymentStatusInfo":
        """Handles a transition from one state to next state.

        Args:
            trigger: A (internal) trigger that determines the state
                transition.
            message: The message to set in status info.

        Returns: New instance of DeploymentStatusInfo representing the
            next state to transition to.
        """

        # If there was an unexpected internal error during reconciliation, set
        # status to unhealthy immediately and return
        if trigger == DeploymentStatusInternalTrigger.INTERNAL_ERROR:
            return self._updated_copy(
                status=DeploymentStatus.UNHEALTHY,
                status_trigger=DeploymentStatusTrigger.INTERNAL_ERROR,
                message=message,
            )

        # If deployment is being deleted, set status immediately and return
        elif trigger == DeploymentStatusInternalTrigger.DELETE:
            return self._updated_copy(
                status=DeploymentStatus.UPDATING,
                status_trigger=DeploymentStatusTrigger.DELETING,
                message=message,
            )

        # Otherwise, go through normal state machine transitions
        elif self.status == DeploymentStatus.UPDATING:
            # Finished updating configuration and transition to healthy
            if trigger == DeploymentStatusInternalTrigger.HEALTHY:
                return self._updated_copy(
                    status=DeploymentStatus.HEALTHY,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED,
                    message=message,
                )

            # A new configuration has been deployed before deployment
            # has finished updating
            elif trigger == DeploymentStatusInternalTrigger.CONFIG_UPDATE:
                return self._updated_copy(
                    status=DeploymentStatus.UPDATING,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                    message=message,
                )

            # Autoscaling.
            elif trigger == DeploymentStatusInternalTrigger.AUTOSCALE_UP:
                return self._updated_copy(
                    status=DeploymentStatus.UPSCALING,
                    status_trigger=DeploymentStatusTrigger.AUTOSCALING,
                    message=message,
                )
            elif trigger == DeploymentStatusInternalTrigger.AUTOSCALE_DOWN:
                return self._updated_copy(
                    status=DeploymentStatus.DOWNSCALING,
                    status_trigger=DeploymentStatusTrigger.AUTOSCALING,
                    message=message,
                )

            # Manually increasing or decreasing num replicas does not
            # change the status while deployment is still updating.
            elif trigger in {
                DeploymentStatusInternalTrigger.MANUALLY_INCREASE_NUM_REPLICAS,
                DeploymentStatusInternalTrigger.MANUALLY_DECREASE_NUM_REPLICAS,
            }:
                return self

            # Failures occurred
            elif trigger == DeploymentStatusInternalTrigger.HEALTH_CHECK_FAILED:
                return self._updated_copy(
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.HEALTH_CHECK_FAILED,
                    message=message,
                )
            elif trigger == DeploymentStatusInternalTrigger.REPLICA_STARTUP_FAILED:
                return self._updated_copy(
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.REPLICA_STARTUP_FAILED,
                    message=message,
                )

        elif self.status in {DeploymentStatus.UPSCALING, DeploymentStatus.DOWNSCALING}:
            # Deployment transitions to healthy
            if trigger == DeploymentStatusInternalTrigger.HEALTHY:
                return self._updated_copy(
                    status=DeploymentStatus.HEALTHY,
                    status_trigger=DeploymentStatusTrigger.UPSCALE_COMPLETED
                    if self.status == DeploymentStatus.UPSCALING
                    else DeploymentStatusTrigger.DOWNSCALE_COMPLETED,
                    message=message,
                )

            # Configuration is updated before scaling is finished
            elif trigger == DeploymentStatusInternalTrigger.CONFIG_UPDATE:
                return self._updated_copy(
                    status=DeploymentStatus.UPDATING,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                    message=message,
                )

            # Upscale replicas before previous upscaling/downscaling has finished
            elif (
                self.status_trigger == DeploymentStatusTrigger.AUTOSCALING
                and trigger == DeploymentStatusInternalTrigger.AUTOSCALE_UP
            ) or (
                self.status_trigger == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
                and trigger
                == DeploymentStatusInternalTrigger.MANUALLY_INCREASE_NUM_REPLICAS
            ):
                return self._updated_copy(
                    status=DeploymentStatus.UPSCALING, message=message
                )

            # Downscale replicas before previous upscaling/downscaling has finished
            elif (
                self.status_trigger == DeploymentStatusTrigger.AUTOSCALING
                and trigger == DeploymentStatusInternalTrigger.AUTOSCALE_DOWN
            ) or (
                self.status_trigger == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
                and trigger
                == DeploymentStatusInternalTrigger.MANUALLY_DECREASE_NUM_REPLICAS
            ):
                return self._updated_copy(
                    status=DeploymentStatus.DOWNSCALING, message=message
                )

            # Failures occurred
            elif trigger == DeploymentStatusInternalTrigger.REPLICA_STARTUP_FAILED:
                return self._updated_copy(
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.REPLICA_STARTUP_FAILED,
                    message=message,
                )
            elif trigger == DeploymentStatusInternalTrigger.HEALTH_CHECK_FAILED:
                return self._updated_copy(
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.HEALTH_CHECK_FAILED,
                    message=message,
                )

        elif self.status == DeploymentStatus.HEALTHY:
            # Deployment remains healthy
            if trigger == DeploymentStatusInternalTrigger.HEALTHY:
                return self

            # New configuration is deployed
            elif trigger == DeploymentStatusInternalTrigger.CONFIG_UPDATE:
                return self._updated_copy(
                    status=DeploymentStatus.UPDATING,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                    message=message,
                )

            # Manually scaling / autoscaling num replicas
            elif (
                trigger
                == DeploymentStatusInternalTrigger.MANUALLY_INCREASE_NUM_REPLICAS
            ):
                return self._updated_copy(
                    status=DeploymentStatus.UPSCALING,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                    message=message,
                )
            elif (
                trigger
                == DeploymentStatusInternalTrigger.MANUALLY_DECREASE_NUM_REPLICAS
            ):
                return self._updated_copy(
                    status=DeploymentStatus.DOWNSCALING,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                    message=message,
                )
            elif trigger == DeploymentStatusInternalTrigger.AUTOSCALE_UP:
                return self._updated_copy(
                    status=DeploymentStatus.UPSCALING,
                    status_trigger=DeploymentStatusTrigger.AUTOSCALING,
                    message=message,
                )
            elif trigger == DeploymentStatusInternalTrigger.AUTOSCALE_DOWN:
                return self._updated_copy(
                    status=DeploymentStatus.DOWNSCALING,
                    status_trigger=DeploymentStatusTrigger.AUTOSCALING,
                    message=message,
                )

            # Health check for one or more replicas has failed
            elif trigger == DeploymentStatusInternalTrigger.HEALTH_CHECK_FAILED:
                return self._updated_copy(
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.HEALTH_CHECK_FAILED,
                    message=message,
                )

        elif self.status == DeploymentStatus.UNHEALTHY:
            # The deployment recovered
            if trigger == DeploymentStatusInternalTrigger.HEALTHY:
                return self._updated_copy(
                    status=DeploymentStatus.HEALTHY,
                    status_trigger=DeploymentStatusTrigger.UNSPECIFIED,
                    message=message,
                )

            # A new configuration is being deployed.
            elif trigger == DeploymentStatusInternalTrigger.CONFIG_UPDATE:
                return self._updated_copy(
                    status=DeploymentStatus.UPDATING,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                    message=message,
                )

            # Old failures keep getting triggered, or new failures occurred.
            elif trigger == DeploymentStatusInternalTrigger.HEALTH_CHECK_FAILED:
                return self._updated_copy(
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.HEALTH_CHECK_FAILED,
                    message=message,
                )
            elif trigger == DeploymentStatusInternalTrigger.REPLICA_STARTUP_FAILED:
                return self._updated_copy(
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.REPLICA_STARTUP_FAILED,
                    message=message,
                )

        # If it's any other transition, ignore it.
        return self

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


@dataclass
class StreamingHTTPRequest:
    """Sent from the HTTP proxy to replicas on the streaming codepath."""

    pickled_asgi_scope: bytes
    # Takes request_id, returns a pickled list of ASGI messages.
    receive_asgi_messages: Callable[[str], Awaitable[bytes]]


class RequestProtocol(str, Enum):
    UNDEFINED = "UNDEFINED"
    HTTP = "HTTP"
    GRPC = "gRPC"


@dataclass
class RequestMetadata:
    request_id: str
    endpoint: str
    call_method: str = "__call__"

    # HTTP route path of the request.
    route: str = ""

    # Application name.
    app_name: str = ""

    # Multiplexed model ID.
    multiplexed_model_id: str = ""

    # If this request expects a streaming response.
    is_streaming: bool = False

    # The protocol to serve this request
    _request_protocol: RequestProtocol = RequestProtocol.UNDEFINED

    # Serve's gRPC context associated with this request for getting and setting metadata
    grpc_context: Optional[RayServegRPCContext] = None

    @property
    def is_http_request(self) -> bool:
        return self._request_protocol == RequestProtocol.HTTP

    @property
    def is_grpc_request(self) -> bool:
        return self._request_protocol == RequestProtocol.GRPC


class TargetCapacityDirection(str, Enum):
    """Determines what direction the target capacity is scaling."""

    UP = "UP"
    DOWN = "DOWN"

import json
import pickle
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Optional

from starlette.types import Scope

from ray.actor import ActorHandle
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.generated.serve_pb2 import DeploymentStatus as DeploymentStatusProto
from ray.serve.generated.serve_pb2 import (
    DeploymentStatusInfo as DeploymentStatusInfoProto,
)
from ray.serve.generated.serve_pb2 import (
    DeploymentStatusTrigger as DeploymentStatusTriggerProto,
)
from ray.serve.grpc_util import RayServegRPCContext

REPLICA_ID_FULL_ID_STR_PREFIX = "SERVE_REPLICA::"


@dataclass(frozen=True)
class DeploymentID:
    name: str
    app_name: str = SERVE_DEFAULT_APP_NAME

    def to_replica_actor_class_name(self):
        return f"ServeReplica:{self.app_name}:{self.name}"

    def __str__(self):
        return f"Deployment(name='{self.name}', app='{self.app_name}')"

    def __repr__(self):
        return str(self)


@dataclass(frozen=True)
class ReplicaID:
    unique_id: str
    deployment_id: DeploymentID

    def to_full_id_str(self) -> str:
        s = f"{self.deployment_id.name}#{self.unique_id}"
        if self.deployment_id.app_name:
            s = f"{self.deployment_id.app_name}#{s}"

        return f"{REPLICA_ID_FULL_ID_STR_PREFIX}{s}"

    @staticmethod
    def is_full_id_str(s: str) -> bool:
        return s.startswith(REPLICA_ID_FULL_ID_STR_PREFIX)

    @classmethod
    def from_full_id_str(cls, s: str):
        assert cls.is_full_id_str(s)

        parsed = s[len(REPLICA_ID_FULL_ID_STR_PREFIX) :].split("#")
        if len(parsed) == 3:
            app_name, deployment_name, unique_id = parsed
        elif len(parsed) == 2:
            app_name = ""
            deployment_name, unique_id = parsed
        else:
            raise ValueError(
                f"Given replica ID string {s} didn't match expected pattern, "
                "ensure it has either two or three fields with delimiter '#'."
            )

        return cls(
            unique_id,
            deployment_id=DeploymentID(name=deployment_name, app_name=app_name),
        )

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        """Returns a human-readable string.

        This is used in user-facing log messages, so take care when updating it.
        """
        return (
            f"Replica("
            f"id='{self.unique_id}', "
            f"deployment='{self.deployment_id.name}', "
            f"app='{self.deployment_id.app_name}'"
            ")"
        )


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
    PENDING_MIGRATION = "PENDING_MIGRATION"


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


@dataclass(frozen=True)
class RunningReplicaInfo:
    replica_id: ReplicaID
    node_id: Optional[str]
    node_ip: Optional[str]
    availability_zone: Optional[str]
    actor_handle: ActorHandle
    max_ongoing_requests: int
    is_cross_language: bool = False
    multiplexed_model_ids: List[str] = field(default_factory=list)
    port: Optional[int] = None

    def __post_init__(self):
        # Set hash value when object is constructed.
        # We use _actor_id to hash the ActorHandle object
        # instead of actor_handle itself to make sure
        # it is consistently same actor handle between different
        # object ids.

        hash_val = hash(
            " ".join(
                [
                    self.replica_id.to_full_id_str(),
                    self.node_id if self.node_id else "",
                    str(self.actor_handle._actor_id),
                    str(self.max_ongoing_requests),
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


class ServeComponentType(str, Enum):
    REPLICA = "replica"


@dataclass
class MultiplexedReplicaInfo:
    replica_id: ReplicaID
    model_ids: List[str]


@dataclass
class gRPCRequest:
    """Sent from the GRPC proxy to replicas on both unary and streaming codepaths."""

    grpc_user_request: bytes


class RequestProtocol(str, Enum):
    UNDEFINED = "UNDEFINED"
    HTTP = "HTTP"
    GRPC = "gRPC"


class DeploymentHandleSource(str, Enum):
    UNKNOWN = "UNKNOWN"
    PROXY = "PROXY"
    REPLICA = "REPLICA"


@dataclass
class RequestMetadata:
    # request_id can be passed by the client and is only generated by the proxy if the
    # client did not pass it in the headers. It is used for logging across different
    # system. We can not guarantee the uniqueness of its value.
    request_id: str
    # internal_request_id is always generated by the proxy and is used for tracking
    # request objects. We can assume this is always unique between requests.
    internal_request_id: str

    # Method of the user callable to execute.
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


@dataclass
class StreamingHTTPRequest:
    """Sent from the HTTP proxy to replicas on the streaming codepath."""

    asgi_scope: Scope
    # Takes request metadata, returns a pickled list of ASGI messages.
    receive_asgi_messages: Callable[[RequestMetadata], Awaitable[bytes]]

    def __getstate__(self) -> Dict[str, Any]:
        """Custom serializer to use vanilla `pickle` for the ASGI scope.

        This is possible because we know the scope is a dictionary containing
        only Python primitive types. Vanilla `pickle` is much faster than cloudpickle.
        """
        return {
            "pickled_asgi_scope": pickle.dumps(self.asgi_scope),
            "receive_asgi_messages": self.receive_asgi_messages,
        }

    def __setstate__(self, state: Dict[str, Any]):
        """Custom deserializer to use vanilla `pickle` for the ASGI scope.

        This is possible because we know the scope is a dictionary containing
        only Python primitive types. Vanilla `pickle` is much faster than cloudpickle.
        """
        self.asgi_scope = pickle.loads(state["pickled_asgi_scope"])
        self.receive_asgi_messages = state["receive_asgi_messages"]


class TargetCapacityDirection(str, Enum):
    """Determines what direction the target capacity is scaling."""

    UP = "UP"
    DOWN = "DOWN"


@dataclass(frozen=True)
class ReplicaQueueLengthInfo:
    accepted: bool
    num_ongoing_requests: int

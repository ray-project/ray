from enum import Enum
from dataclasses import dataclass, field, asdict
import json
from typing import Any, List, Dict, Optional, NamedTuple

import ray
from ray.actor import ActorHandle
from ray.serve._private.config import DeploymentConfig, ReplicaConfig
from ray.serve.generated.serve_pb2 import (
    DeploymentInfo as DeploymentInfoProto,
    DeploymentStatusInfo as DeploymentStatusInfoProto,
    DeploymentStatus as DeploymentStatusProto,
    DeploymentStatusInfoList as DeploymentStatusInfoListProto,
    ApplicationStatus as ApplicationStatusProto,
    ApplicationStatusInfo as ApplicationStatusInfoProto,
    StatusOverview as StatusOverviewProto,
)
from ray.serve._private.autoscaling_policy import BasicAutoscalingPolicy


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


@dataclass(eq=True)
class DeploymentStatusInfo:
    name: str
    status: DeploymentStatus
    message: str = ""

    def debug_string(self):
        return json.dumps(asdict(self), indent=4)

    def to_proto(self):
        return DeploymentStatusInfoProto(
            name=self.name,
            status=f"DEPLOYMENT_STATUS_{self.status.name}",
            message=self.message,
        )

    @classmethod
    def from_proto(cls, proto: DeploymentStatusInfoProto):
        status = DeploymentStatusProto.Name(proto.status)[len("DEPLOYMENT_STATUS_") :]
        return cls(
            name=proto.name,
            status=DeploymentStatus(status),
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


# Concurrency group used for operations that cannot be blocked by user code
# (e.g., health checks and fetching queue length).
CONTROL_PLANE_CONCURRENCY_GROUP = "control_plane"
REPLICA_DEFAULT_ACTOR_OPTIONS = {
    "concurrency_groups": {CONTROL_PLANE_CONCURRENCY_GROUP: 1}
}


class DeploymentInfo:
    def __init__(
        self,
        deployment_config: DeploymentConfig,
        replica_config: ReplicaConfig,
        start_time_ms: int,
        deployer_job_id: str,
        actor_name: Optional[str] = None,
        version: Optional[str] = None,
        end_time_ms: Optional[int] = None,
        is_driver_deployment: Optional[bool] = False,
        route_prefix: str = None,
        docs_path: str = None,
        ingress: bool = False,
    ):
        self.deployment_config = deployment_config
        self.replica_config = replica_config
        # The time when .deploy() was first called for this deployment.
        self.start_time_ms = start_time_ms
        self.actor_name = actor_name
        self.version = version
        self.deployer_job_id = deployer_job_id
        # The time when this deployment was deleted.
        self.end_time_ms = end_time_ms

        # ephermal state
        self._cached_actor_def = None

        self.is_driver_deployment = is_driver_deployment

        self.route_prefix = route_prefix
        self.docs_path = docs_path
        self.ingress = ingress
        if deployment_config.autoscaling_config is not None:
            self.autoscaling_policy = BasicAutoscalingPolicy(
                deployment_config.autoscaling_config
            )
        else:
            self.autoscaling_policy = None
        # Num replicas decided by the autoscaling policy. This is mutually exclusive
        # from deployment_config.num_replicas. This value is updated through
        # set_autoscaled_num_replicas()
        self.autoscaled_num_replicas = None

    def __getstate__(self) -> Dict[Any, Any]:
        clean_dict = self.__dict__.copy()
        del clean_dict["_cached_actor_def"]
        return clean_dict

    def __setstate__(self, d: Dict[Any, Any]) -> None:
        self.__dict__ = d
        self._cached_actor_def = None

    def set_autoscaled_num_replicas(self, autoscaled_num_replicas):
        self.autoscaled_num_replicas = autoscaled_num_replicas

    def update(
        self,
        deployment_config: DeploymentConfig = None,
        replica_config: ReplicaConfig = None,
        version: str = None,
        is_driver_deployment: bool = None,
        route_prefix: str = None,
    ) -> "DeploymentInfo":
        return DeploymentInfo(
            deployment_config=deployment_config or self.deployment_config,
            replica_config=replica_config or self.replica_config,
            start_time_ms=self.start_time_ms,
            deployer_job_id=self.deployer_job_id,
            actor_name=self.actor_name,
            version=version or self.version,
            end_time_ms=self.end_time_ms,
            is_driver_deployment=is_driver_deployment
            if is_driver_deployment is not None
            else self.is_driver_deployment,
            route_prefix=route_prefix or self.route_prefix,
            docs_path=self.docs_path,
            ingress=self.ingress,
        )

    @property
    def actor_def(self):
        # Delayed import as replica depends on this file.
        from ray.serve._private.replica import create_replica_wrapper

        if self._cached_actor_def is None:
            assert self.actor_name is not None

            self._cached_actor_def = ray.remote(**REPLICA_DEFAULT_ACTOR_OPTIONS)(
                create_replica_wrapper(self.actor_name)
            )

        return self._cached_actor_def

    @classmethod
    def from_proto(cls, proto: DeploymentInfoProto):
        deployment_config = (
            DeploymentConfig.from_proto(proto.deployment_config)
            if proto.deployment_config
            else None
        )
        data = {
            "deployment_config": deployment_config,
            "replica_config": ReplicaConfig.from_proto(
                proto.replica_config,
                deployment_config.needs_pickle() if deployment_config else True,
            ),
            "start_time_ms": proto.start_time_ms,
            "actor_name": proto.actor_name if proto.actor_name != "" else None,
            "version": proto.version if proto.version != "" else None,
            "end_time_ms": proto.end_time_ms if proto.end_time_ms != 0 else None,
            "deployer_job_id": ray.get_runtime_context().get_job_id(),
        }

        return cls(**data)

    def to_proto(self):
        data = {
            "start_time_ms": self.start_time_ms,
            "actor_name": self.actor_name,
            "version": self.version,
            "end_time_ms": self.end_time_ms,
        }
        if self.deployment_config:
            data["deployment_config"] = self.deployment_config.to_proto()
        if self.replica_config:
            data["replica_config"] = self.replica_config.to_proto()
        return DeploymentInfoProto(**data)


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
    UNSET = "UNSET"
    SINGLE_APP = "SINGLE_APP"
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
    DEPLOYMENT = "deployment"


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

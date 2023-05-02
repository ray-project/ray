import json
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Any, List, Dict, Optional

import ray
from ray.actor import ActorHandle
from ray.serve.config import DeploymentConfig, ReplicaConfig
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

EndpointTag = str
ReplicaTag = str
NodeId = str
Duration = float
ApplicationName = str


@dataclass
class EndpointInfo:
    route: str
    app_name: str


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
    RUNNING = "RUNNING"
    DEPLOY_FAILED = "DEPLOY_FAILED"
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
            status=f"APPLICATION_STATUS_{self.status}",
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
            status=f"DEPLOYMENT_STATUS_{self.status}",
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


HEALTH_CHECK_CONCURRENCY_GROUP = "health_check"
REPLICA_DEFAULT_ACTOR_OPTIONS = {
    "concurrency_groups": {HEALTH_CHECK_CONCURRENCY_GROUP: 1}
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
        app_name: Optional[str] = None,
        route_prefix: str = None,
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

        self.app_name = app_name
        self.route_prefix = route_prefix
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
            "app_name": proto.app_name,
        }

        return cls(**data)

    def to_proto(self):
        data = {
            "start_time_ms": self.start_time_ms,
            "actor_name": self.actor_name,
            "version": self.version,
            "end_time_ms": self.end_time_ms,
            "app_name": self.app_name,
        }
        if self.deployment_config:
            data["deployment_config"] = self.deployment_config.to_proto()
        if self.replica_config:
            data["replica_config"] = self.replica_config.to_proto()
        return DeploymentInfoProto(**data)


@dataclass
class ReplicaName:
    deployment_tag: str
    replica_suffix: str
    replica_tag: ReplicaTag = ""
    delimiter: str = "#"
    prefix: str = "SERVE_REPLICA::"

    def __init__(self, deployment_tag: str, replica_suffix: str):
        self.deployment_tag = deployment_tag
        self.replica_suffix = replica_suffix
        self.replica_tag = f"{deployment_tag}{self.delimiter}{replica_suffix}"

    @staticmethod
    def is_replica_name(actor_name: str) -> bool:
        return actor_name.startswith(ReplicaName.prefix)

    @classmethod
    def from_str(cls, actor_name):
        assert ReplicaName.is_replica_name(actor_name)
        # TODO(simon): this currently conforms the tag and suffix logic. We
        # can try to keep the internal name always hard coded with the prefix.
        replica_name = actor_name.replace(cls.prefix, "")
        parsed = replica_name.split(cls.delimiter)
        assert len(parsed) == 2, (
            f"Given replica name {replica_name} didn't match pattern, please "
            f"ensure it has exactly two fields with delimiter {cls.delimiter}"
        )
        return cls(deployment_tag=parsed[0], replica_suffix=parsed[1])

    def __str__(self):
        return self.replica_tag


@dataclass(frozen=True)
class RunningReplicaInfo:
    deployment_name: str
    replica_tag: ReplicaTag
    actor_handle: ActorHandle
    max_concurrent_queries: int
    is_cross_language: bool = False

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
                    str(self.actor_handle._actor_id),
                    str(self.max_concurrent_queries),
                    str(self.is_cross_language),
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

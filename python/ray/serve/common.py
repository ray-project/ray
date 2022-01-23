from dataclasses import dataclass
from typing import Any, Dict, Optional
from uuid import UUID

import ray
from ray.actor import ActorHandle
from ray.serve.config import DeploymentConfig, ReplicaConfig
from ray.serve.autoscaling_policy import AutoscalingPolicy

str = str
EndpointTag = str
ReplicaTag = str
NodeId = str
GoalId = UUID
Duration = float


@dataclass
class EndpointInfo:
    route: str


class DeploymentInfo:
    def __init__(self,
                 deployment_config: DeploymentConfig,
                 replica_config: ReplicaConfig,
                 start_time_ms: int,
                 deployer_job_id: "ray._raylet.JobID",
                 actor_name: Optional[str] = None,
                 serialized_deployment_def: Optional[bytes] = None,
                 version: Optional[str] = None,
                 end_time_ms: Optional[int] = None,
                 autoscaling_policy: Optional[AutoscalingPolicy] = None):
        self.deployment_config = deployment_config
        self.replica_config = replica_config
        # The time when .deploy() was first called for this deployment.
        self.start_time_ms = start_time_ms
        self.actor_name = actor_name
        self.serialized_deployment_def = serialized_deployment_def
        self.version = version
        self.deployer_job_id = deployer_job_id
        # The time when this deployment was deleted.
        self.end_time_ms = end_time_ms
        self.autoscaling_policy = autoscaling_policy

        # ephermal state
        self._cached_actor_def = None

    def __getstate__(self) -> Dict[Any, Any]:
        clean_dict = self.__dict__.copy()
        del clean_dict["_cached_actor_def"]
        return clean_dict

    def __setstate__(self, d: Dict[Any, Any]) -> None:
        self.__dict__ = d
        self._cached_actor_def = None

    @property
    def actor_def(self):
        # Delayed import as replica depends on this file.
        from ray.serve.replica import create_replica_wrapper
        if self._cached_actor_def is None:
            assert self.actor_name is not None
            assert self.serialized_deployment_def is not None
            self._cached_actor_def = ray.remote(
                create_replica_wrapper(self.actor_name,
                                       self.serialized_deployment_def))
        return self._cached_actor_def


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
            f"ensure it has exactly two fields with delimiter {cls.delimiter}")
        return cls(deployment_tag=parsed[0], replica_suffix=parsed[1])

    def __str__(self):
        return self.replica_tag


@dataclass(frozen=True)
class RunningReplicaInfo:
    deployment_name: str
    replica_tag: ReplicaTag
    actor_handle: ActorHandle
    max_concurrent_queries: int

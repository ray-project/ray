from dataclasses import dataclass
from typing import Optional
from uuid import UUID

import ray
from ray.actor import ActorClass, ActorHandle
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
    route: Optional[str] = None


class DeploymentInfo:
    def __init__(self,
                 deployment_config: DeploymentConfig,
                 replica_config: ReplicaConfig,
                 start_time_ms: int,
                 actor_def: Optional[ActorClass] = None,
                 version: Optional[str] = None,
                 deployer_job_id: "Optional[ray._raylet.JobID]" = None,
                 end_time_ms: Optional[int] = None,
                 autoscaling_policy: Optional[AutoscalingPolicy] = None):
        self.deployment_config = deployment_config
        self.replica_config = replica_config
        # The time when .deploy() was first called for this deployment.
        self.start_time_ms = start_time_ms
        self.actor_def = actor_def
        self.version = version
        self.deployer_job_id = deployer_job_id
        # The time when this deployment was deleted.
        self.end_time_ms = end_time_ms
        self.autoscaling_policy = autoscaling_policy


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
        return (actor_name.startswith(ReplicaName.prefix)
                and actor_name.count("#") == 1)

    @classmethod
    def from_str(cls, actor_name):
        assert actor_name.startswith(cls.prefix), "Actor "
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

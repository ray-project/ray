import ray

from dataclasses import dataclass, field
from typing import List, Optional
from uuid import UUID

from ray.actor import ActorClass
from ray.serve.config import ReplicaConfig, BackendConfig

BackendTag = str
EndpointTag = str
ReplicaTag = str
NodeId = str
GoalId = UUID
Duration = float


@dataclass
class EndpointInfo:
    python_methods: Optional[List[str]] = field(default_factory=list)
    route: Optional[str] = None


class BackendInfo:
    def __init__(self,
                 backend_config: BackendConfig,
                 replica_config: ReplicaConfig,
                 start_time_ms: int,
                 actor_def: Optional[ActorClass] = None,
                 version: Optional[str] = None,
                 deployer_job_id: "Optional[ray._raylet.JobID]" = None,
                 end_time_ms: Optional[int] = None):
        self.backend_config = backend_config
        self.replica_config = replica_config
        # The time when .deploy() was first called for this deployment.
        self.start_time_ms = start_time_ms
        self.actor_def = actor_def
        self.version = version
        self.deployer_job_id = deployer_job_id
        # The time when this deployment was deleted.
        self.end_time_ms = end_time_ms

    def __reduce__(self):
        # TODO(simon): This hack exist .. move to protobuf
        def deserializer(data: dict):
            data["backend_config"] = BackendConfig.from_proto_bytes(
                data["backend_config"])
            return BackendInfo(**data)

        serialized_data = self.__dict__.copy()
        serialized_data["backend_config"] = serialized_data[
            "backend_config"].to_proto_bytes()

        return deserializer, (serialized_data, )

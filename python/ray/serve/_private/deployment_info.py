from typing import Any, Dict, Optional

import ray
from ray.serve._private.common import TargetCapacityDirection
from ray.serve._private.config import DeploymentConfig, ReplicaConfig
from ray.serve.generated.serve_pb2 import (
    DeploymentInfo as DeploymentInfoProto,
    TargetCapacityDirection as TargetCapacityDirectionProto,
)


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
        route_prefix: str = None,
        ingress: bool = False,
        target_capacity: Optional[float] = None,
        target_capacity_direction: Optional[TargetCapacityDirection] = None,
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

        self.route_prefix = route_prefix
        self.ingress = ingress

        self.target_capacity = target_capacity
        self.target_capacity_direction = target_capacity_direction

    def __getstate__(self) -> Dict[Any, Any]:
        clean_dict = self.__dict__.copy()
        del clean_dict["_cached_actor_def"]
        return clean_dict

    def __setstate__(self, d: Dict[Any, Any]) -> None:
        self.__dict__ = d
        self._cached_actor_def = None

    def update(
        self,
        deployment_config: DeploymentConfig = None,
        replica_config: ReplicaConfig = None,
        version: str = None,
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
            route_prefix=route_prefix or self.route_prefix,
            ingress=self.ingress,
            target_capacity=self.target_capacity,
            target_capacity_direction=self.target_capacity_direction,
        )

    def set_target_capacity(
        self,
        new_target_capacity: Optional[float],
        new_target_capacity_direction: Optional[TargetCapacityDirection],
    ):
        self.target_capacity = new_target_capacity
        self.target_capacity_direction = new_target_capacity_direction

    def config_changed(self, other) -> bool:
        return (
            self.deployment_config != other.deployment_config
            or self.replica_config.ray_actor_options
            != other.replica_config.ray_actor_options
            or other.version is None
            or self.version != other.version
        )

    @property
    def actor_def(self):
        if self._cached_actor_def is None:
            assert self.actor_name is not None

            # Break circular import :(.
            from ray.serve._private.replica import ReplicaActor

            # Dynamically create a new class with custom name here so Ray picks it up
            # correctly in actor metadata table and observability stack.
            self._cached_actor_def = ray.remote(
                type(
                    self.actor_name,
                    (ReplicaActor,),
                    dict(ReplicaActor.__dict__),
                )
            )

        return self._cached_actor_def

    @classmethod
    def from_proto(cls, proto: DeploymentInfoProto):
        deployment_config = (
            DeploymentConfig.from_proto(proto.deployment_config)
            if proto.deployment_config
            else None
        )

        target_capacity = proto.target_capacity if proto.target_capacity != -1 else None

        target_capacity_direction = TargetCapacityDirectionProto.Name(
            proto.target_capacity_direction
        )
        if target_capacity_direction == "UNSET":
            target_capacity_direction = None
        else:
            target_capacity_direction = TargetCapacityDirection(
                target_capacity_direction
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
            "target_capacity": target_capacity,
            "target_capacity_direction": target_capacity_direction,
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
        if self.target_capacity is None:
            data["target_capacity"] = -1
        else:
            data["target_capacity"] = self.target_capacity
        if self.target_capacity_direction is None:
            data["target_capacity_direction"] = TargetCapacityDirectionProto.UNSET
        else:
            data["target_capacity_direction"] = self.target_capacity_direction.name
        return DeploymentInfoProto(**data)
